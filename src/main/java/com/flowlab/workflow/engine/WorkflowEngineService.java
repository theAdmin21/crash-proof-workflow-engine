package com.flowlab.workflow.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.domain.StepExecution;
import com.flowlab.workflow.domain.StepStatus;
import com.flowlab.workflow.domain.WorkflowExecution;
import com.flowlab.workflow.domain.WorkflowStatus;
import com.flowlab.workflow.metrics.WorkflowMetricsService;
import com.flowlab.workflow.repository.StepExecutionRepository;
import com.flowlab.workflow.repository.WorkflowExecutionRepository;
import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkflowEngineService {

    private final WorkflowExecutionRepository executionRepository;
    private final StepExecutionRepository stepRepository;
    private final List<WorkflowStep> registeredSteps;
    private final WorkflowTaskProducer taskProducer;
    private final WorkflowMetricsService metricsService;
    private final ObjectMapper objectMapper;

    @Value("${workflow.engine.max-retry-attempts:3}")
    private int maxRetries;

    @Value("${workflow.engine.retry-backoff-sec:2}")
    private long backoffSeconds;

    private final String workerId = ManagementFactory.getRuntimeMXBean().getName();

    public List<WorkflowStep> getSortedSteps() {
        return registeredSteps.stream()
                .sorted(Comparator.comparingInt(WorkflowStep::getOrder))
                .toList();
    }

    private WorkflowStep findStepDefinition(int order) {
        return registeredSteps.stream()
                .filter(s -> s.getOrder() == order)
                .findFirst()
                .orElse(null);
    }

    public WorkflowExecution startWorkflow(String workflowId, String initialPayload) {
        String executionId = "exec_" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Initiating durable DAG workflow '{}' with executionId: {}", workflowId, executionId);

        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId(workflowId)
                .status(WorkflowStatus.RUNNING)
                .currentStepIndex(0)
                .payload(initialPayload)
                .build();
        executionRepository.save(execution);
        metricsService.recordWorkflowStarted(workflowId);

        // Pre-create all steps in PENDING state so UI visualizes full pipeline
        List<WorkflowStep> steps = getSortedSteps();
        for (WorkflowStep step : steps) {
            StepExecution stepExec = StepExecution.builder()
                    .id("step_" + UUID.randomUUID().toString().substring(0, 8))
                    .executionId(executionId)
                    .stepName(step.getStepName())
                    .stepIndex(step.getOrder())
                    .status(StepStatus.PENDING)
                    .inputPayload(initialPayload)
                    .attemptCount(0)
                    .build();
            stepRepository.save(stepExec);
        }

        // Trigger root steps (dependencies are empty)
        advanceDAG(executionId);

        return execution;
    }

    public void dispatchStep(String executionId, int stepIndex, String stepName) {
        try {
            taskProducer.dispatchStepTask(executionId, stepIndex, stepName);
        } catch (Exception e) {
            log.warn("⚠️ Kafka dispatch failed, falling back to in-memory async thread for step #{}: {}", stepIndex, e.getMessage());
            CompletableFuture.runAsync(() -> executeStepDirect(executionId, stepIndex));
        }
    }

    public void executeStepDirect(String executionId, int stepIndex) {
        try {
            WorkflowStep currentStep = findStepDefinition(stepIndex);
            if (currentStep == null) {
                log.error("[Execution {}] Step definition not found for index {}", executionId, stepIndex);
                return;
            }

            StepExecution stepRecord = null;
            for (int r = 0; r < 5; r++) {
                var opt = stepRepository.findByExecutionIdAndStepIndex(executionId, stepIndex);
                if (opt.isPresent()) {
                    stepRecord = opt.get();
                    break;
                }
                Thread.sleep(100);
            }

            if (stepRecord == null) {
                log.error("[Execution {}] Step record not found for index {}", executionId, stepIndex);
                return;
            }

            if (stepRecord.getStatus() == StepStatus.COMPLETED || stepRecord.getStatus() == StepStatus.COMPENSATED) {
                log.info("[Execution {}] Step #{} already in terminal state {}. Skipping execution.",
                        executionId, stepIndex, stepRecord.getStatus());
                return;
            }

            Instant startTime = Instant.now();
            stepRecord.setStatus(StepStatus.RUNNING);
            stepRecord.setStartedAt(startTime);
            stepRecord.setLastHeartbeat(Instant.now());
            stepRecord.setWorkerId(workerId);
            stepRecord.setAttemptCount(stepRecord.getAttemptCount() + 1);
            stepRepository.save(stepRecord);

            log.info("[Execution {}] Running Step #{}: {} (Attempt #{})",
                    executionId, stepIndex, currentStep.getStepName(), stepRecord.getAttemptCount());

            try {
                StepExecutionContext context = StepExecutionContext.builder()
                        .executionId(executionId)
                        .stepIndex(stepIndex)
                        .stepName(currentStep.getStepName())
                        .attempt(stepRecord.getAttemptCount())
                        .inputPayload(stepRecord.getInputPayload())
                        .build();

                StepResult result = currentStep.execute(context);

                Instant endTime = Instant.now();
                long duration = Duration.between(startTime, endTime).toMillis();
                stepRecord.setCompletedAt(endTime);
                stepRecord.setDurationMs(duration);

                if (result.isSuccess()) {
                    stepRecord.setStatus(StepStatus.COMPLETED);
                    stepRecord.setOutputPayload(result.getOutputPayload());
                    stepRepository.save(stepRecord);
                    metricsService.recordStepDuration(currentStep.getStepName(), "COMPLETED", duration);

                    log.info("[Execution {}] Step '{}' COMPLETED in {}ms", executionId, currentStep.getStepName(), duration);

                    // Evaluate DAG and advance ready downstream steps
                    advanceDAG(executionId);

                } else {
                    metricsService.recordStepDuration(currentStep.getStepName(), "FAILED", duration);
                    handleStepFailure(executionId, stepRecord, result.getErrorMessage(), result.isShouldRetry());
                }

            } catch (Exception ex) {
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                metricsService.recordStepDuration(currentStep.getStepName(), "CRASHED", duration);
                log.error("[Execution {}] Step '{}' crashed with exception: {}", executionId, currentStep.getStepName(), ex.getMessage());
                handleStepFailure(executionId, stepRecord, ex.getMessage(), true);
            }

        } catch (Throwable t) {
            log.error("[Execution {}] Critical error in executeStepDirect for step index {}: {}", executionId, stepIndex, t.getMessage(), t);
        }
    }

    public synchronized void advanceDAG(String executionId) {
        List<StepExecution> allSteps = stepRepository.findByExecutionIdOrderByStepIndexAsc(executionId);
        if (allSteps.isEmpty()) return;

        Map<Integer, StepStatus> statusMap = new HashMap<>();
        for (StepExecution s : allSteps) {
            statusMap.put(s.getStepIndex(), s.getStatus());
        }

        // Check if any step failed or is compensating
        boolean anyFailed = allSteps.stream().anyMatch(s -> s.getStatus() == StepStatus.FAILED || s.getStatus() == StepStatus.COMPENSATED);
        if (anyFailed) return;

        List<WorkflowStep> stepDefs = getSortedSteps();
        int dispatchedCount = 0;

        for (WorkflowStep stepDef : stepDefs) {
            StepStatus currentStatus = statusMap.get(stepDef.getOrder());
            if (currentStatus == StepStatus.PENDING) {
                List<Integer> deps = stepDef.getDependencies();
                boolean allDepsMet = deps.isEmpty() || deps.stream().allMatch(d -> statusMap.get(d) == StepStatus.COMPLETED);

                if (allDepsMet) {
                    // Step is ready! Merge input from dependencies
                    StepExecution pendingExec = stepRepository.findByExecutionIdAndStepIndex(executionId, stepDef.getOrder()).orElse(null);
                    if (pendingExec != null) {
                        String mergedPayload = resolveMergedInputPayload(executionId, deps, pendingExec.getInputPayload());
                        pendingExec.setInputPayload(mergedPayload);
                        stepRepository.save(pendingExec);

                        log.info("⚡ [DAG-FORK] Step #{}: '{}' dependencies satisfied ({}) -> Dispatching!",
                                stepDef.getOrder(), stepDef.getStepName(), deps);

                        dispatchStep(executionId, stepDef.getOrder(), stepDef.getStepName());
                        dispatchedCount++;
                    }
                }
            }
        }

        // Update workflow execution currentStepIndex
        long completedCount = allSteps.stream().filter(s -> s.getStatus() == StepStatus.COMPLETED).count();
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution != null) {
            execution.setCurrentStepIndex((int) completedCount);
            execution.setUpdatedAt(Instant.now());
            executionRepository.save(execution);

            // If all steps completed, mark workflow finished!
            if (completedCount == stepDefs.size()) {
                execution.setStatus(WorkflowStatus.COMPLETED);
                executionRepository.save(execution);
                metricsService.recordWorkflowCompleted(execution.getWorkflowId());
                log.info("🎉 [Execution {}] Complete DAG Workflow COMPLETED successfully!", executionId);
            }
        }
    }

    private String resolveMergedInputPayload(String executionId, List<Integer> depIndices, String defaultPayload) {
        if (depIndices.isEmpty()) {
            return defaultPayload;
        }

        Map<String, Object> merged = new HashMap<>();
        for (int depIdx : depIndices) {
            stepRepository.findByExecutionIdAndStepIndex(executionId, depIdx).ifPresent(depStep -> {
                if (depStep.getOutputPayload() != null) {
                    try {
                        JsonNode node = objectMapper.readTree(depStep.getOutputPayload());
                        node.fields().forEachRemaining(e -> {
                            JsonNode v = e.getValue();
                            if (v.isBoolean()) merged.put(e.getKey(), v.asBoolean());
                            else if (v.isInt()) merged.put(e.getKey(), v.asInt());
                            else if (v.isNumber()) merged.put(e.getKey(), v.numberValue());
                            else merged.put(e.getKey(), v.asText());
                        });
                    } catch (Exception ignored) {}
                }
            });
        }

        try {
            return merged.isEmpty() ? defaultPayload : objectMapper.writeValueAsString(merged);
        } catch (Exception e) {
            return defaultPayload;
        }
    }

    private void handleStepFailure(String executionId, StepExecution stepRecord, String errorMessage, boolean retryable) {
        stepRecord.setErrorMessage(errorMessage);
        stepRecord.setCompletedAt(Instant.now());

        if (retryable && stepRecord.getAttemptCount() < maxRetries) {
            stepRecord.setStatus(StepStatus.RETRYING);
            stepRepository.save(stepRecord);
            metricsService.recordStepRetry(stepRecord.getStepName());
            log.warn("[Execution {}] Step '{}' will retry. Attempt {}/{}", executionId, stepRecord.getStepName(), stepRecord.getAttemptCount(), maxRetries);

            long delay = backoffSeconds * (long) Math.pow(2, stepRecord.getAttemptCount() - 1);
            new Thread(() -> {
                try {
                    Thread.sleep(delay * 1000);
                    dispatchStep(executionId, stepRecord.getStepIndex(), stepRecord.getStepName());
                } catch (InterruptedException ignored) {}
            }).start();

        } else {
            stepRecord.setStatus(StepStatus.FAILED);
            stepRepository.save(stepRecord);

            WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
            if (execution != null) {
                execution.setStatus(WorkflowStatus.COMPENSATING);
                execution.setErrorMessage("Step '" + stepRecord.getStepName() + "' failed: " + errorMessage + ". Initiating Saga rollback...");
                execution.setUpdatedAt(Instant.now());
                executionRepository.save(execution);
                metricsService.recordWorkflowFailed(execution.getWorkflowId());
            }
            log.error("[Execution {}] Workflow failed at step '{}'. Starting backward Saga rollback...", executionId, stepRecord.getStepName());

            triggerSagaCompensation(executionId, stepRecord.getStepIndex());
        }
    }

    public void triggerSagaCompensation(String executionId, int failedStepIndex) {
        CompletableFuture.runAsync(() -> {
            log.warn("🚨 [SAGA-ROLLBACK] Executing compensating transactions for execution: {}", executionId);
            List<WorkflowStep> steps = getSortedSteps();

            for (int i = failedStepIndex - 1; i >= 0; i--) {
                WorkflowStep stepToRollback = findStepDefinition(i);
                if (stepToRollback == null) continue;

                StepExecution stepRecord = stepRepository.findByExecutionIdAndStepIndex(executionId, i).orElse(null);

                if (stepRecord != null && stepRecord.getStatus() == StepStatus.COMPLETED) {
                    try {
                        log.info("↩️ Rolling back step #{}: {}", i, stepToRollback.getStepName());
                        StepExecutionContext context = StepExecutionContext.builder()
                                .executionId(executionId)
                                .stepIndex(i)
                                .stepName(stepToRollback.getStepName())
                                .inputPayload(stepRecord.getOutputPayload())
                                .build();

                        StepResult compResult = stepToRollback.compensate(context);
                        stepRecord.setStatus(StepStatus.COMPENSATED);
                        stepRecord.setOutputPayload(compResult.getOutputPayload());
                        stepRecord.setUpdatedAt(Instant.now());
                        stepRepository.save(stepRecord);
                    } catch (Exception ex) {
                        log.error("Failed to compensate step #{}: {}", i, ex.getMessage());
                    }
                }
            }

            WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
            if (execution != null) {
                execution.setStatus(WorkflowStatus.COMPENSATED);
                execution.setUpdatedAt(Instant.now());
                executionRepository.save(execution);
                metricsService.recordWorkflowCompensated(execution.getWorkflowId());
                log.info("🛡️ [SAGA-ROLLBACK] Compensation complete. Workflow marked as COMPENSATED.");
            }
        });
    }

    public void resumeFromLastFailure(String executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));

        List<StepExecution> steps = stepRepository.findByExecutionIdOrderByStepIndexAsc(executionId);
        for (StepExecution step : steps) {
            if (step.getStatus() == StepStatus.FAILED || step.getStatus() == StepStatus.RETRYING || step.getStatus() == StepStatus.RUNNING) {
                log.info("Resuming execution {} from checkpoint step #{} ({})", executionId, step.getStepIndex(), step.getStepName());
                execution.setStatus(WorkflowStatus.RUNNING);
                executionRepository.save(execution);
                dispatchStep(executionId, step.getStepIndex(), step.getStepName());
                return;
            }
        }
    }
}

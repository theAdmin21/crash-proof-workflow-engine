package com.flowlab.workflow.engine;

import com.flowlab.workflow.domain.StepExecution;
import com.flowlab.workflow.domain.StepStatus;
import com.flowlab.workflow.domain.WorkflowExecution;
import com.flowlab.workflow.domain.WorkflowStatus;
import com.flowlab.workflow.repository.StepExecutionRepository;
import com.flowlab.workflow.repository.WorkflowExecutionRepository;
import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkflowEngineService {

    private final WorkflowExecutionRepository executionRepository;
    private final StepExecutionRepository stepRepository;
    private final List<WorkflowStep> registeredSteps;

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

    @Transactional
    public WorkflowExecution startWorkflow(String workflowId, String initialPayload) {
        String executionId = "exec_" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Initiating durable workflow '{}' with executionId: {}", workflowId, executionId);

        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId(workflowId)
                .status(WorkflowStatus.RUNNING)
                .currentStepIndex(0)
                .payload(initialPayload)
                .build();
        executionRepository.save(execution);

        // Pre-create all steps in PENDING state so UI visualizes full pipeline
        List<WorkflowStep> steps = getSortedSteps();
        for (int i = 0; i < steps.size(); i++) {
            WorkflowStep step = steps.get(i);
            StepExecution stepExec = StepExecution.builder()
                    .id("step_" + UUID.randomUUID().toString().substring(0, 8))
                    .executionId(executionId)
                    .stepName(step.getStepName())
                    .stepIndex(step.getOrder())
                    .status(i == 0 ? StepStatus.RUNNING : StepStatus.PENDING)
                    .inputPayload(initialPayload)
                    .attemptCount(0)
                    .build();
            stepRepository.save(stepExec);
        }

        // Kick off step execution asynchronously
        executeStepAsync(executionId, 0);

        return execution;
    }

    @Async
    public void executeStepAsync(String executionId, int stepIndex) {
        List<WorkflowStep> steps = getSortedSteps();
        if (stepIndex >= steps.size()) {
            markWorkflowCompleted(executionId);
            return;
        }

        WorkflowStep currentStep = steps.get(stepIndex);
        StepExecution stepRecord = stepRepository.findByExecutionIdAndStepIndex(executionId, stepIndex)
                .orElseThrow(() -> new IllegalStateException("Step execution not found: " + stepIndex));

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
            stepRecord.setCompletedAt(endTime);
            stepRecord.setDurationMs(Duration.between(startTime, endTime).toMillis());

            if (result.isSuccess()) {
                stepRecord.setStatus(StepStatus.COMPLETED);
                stepRecord.setOutputPayload(result.getOutputPayload());
                stepRepository.save(stepRecord);
                log.info("[Execution {}] Step '{}' COMPLETED in {}ms", executionId, currentStep.getStepName(), stepRecord.getDurationMs());

                // Advance to next step
                advanceToNextStep(executionId, stepIndex + 1);
            } else {
                handleStepFailure(executionId, stepRecord, result.getErrorMessage(), result.isShouldRetry());
            }

        } catch (Exception ex) {
            log.error("[Execution {}] Step '{}' crashed with exception: {}", executionId, currentStep.getStepName(), ex.getMessage());
            handleStepFailure(executionId, stepRecord, ex.getMessage(), true);
        }
    }

    private void handleStepFailure(String executionId, StepExecution stepRecord, String errorMessage, boolean retryable) {
        stepRecord.setErrorMessage(errorMessage);
        stepRecord.setCompletedAt(Instant.now());

        if (retryable && stepRecord.getAttemptCount() < maxRetries) {
            stepRecord.setStatus(StepStatus.RETRYING);
            stepRepository.save(stepRecord);
            log.warn("[Execution {}] Step '{}' will retry. Attempt {}/{}", executionId, stepRecord.getStepName(), stepRecord.getAttemptCount(), maxRetries);

            // Schedule retry with exponential backoff
            long delay = backoffSeconds * (long) Math.pow(2, stepRecord.getAttemptCount() - 1);
            new Thread(() -> {
                try {
                    Thread.sleep(delay * 1000);
                    executeStepAsync(executionId, stepRecord.getStepIndex());
                } catch (InterruptedException ignored) {}
            }).start();

        } else {
            stepRecord.setStatus(StepStatus.FAILED);
            stepRepository.save(stepRecord);

            WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
            if (execution != null) {
                execution.setStatus(WorkflowStatus.FAILED);
                execution.setErrorMessage("Step '" + stepRecord.getStepName() + "' failed: " + errorMessage);
                execution.setUpdatedAt(Instant.now());
                executionRepository.save(execution);
            }
            log.error("[Execution {}] Workflow FAILED at step '{}'", executionId, stepRecord.getStepName());
        }
    }

    private void advanceToNextStep(String executionId, int nextIndex) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution != null) {
            execution.setCurrentStepIndex(nextIndex);
            execution.setUpdatedAt(Instant.now());
            executionRepository.save(execution);
        }
        executeStepAsync(executionId, nextIndex);
    }

    private void markWorkflowCompleted(String executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution != null) {
            execution.setStatus(WorkflowStatus.COMPLETED);
            execution.setUpdatedAt(Instant.now());
            executionRepository.save(execution);
            log.info("🎉 [Execution {}] Durable Workflow COMPLETED successfully!", executionId);
        }
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
                executeStepAsync(executionId, step.getStepIndex());
                return;
            }
        }
    }
}

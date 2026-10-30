package com.flowlab.workflow.engine;

import com.flowlab.workflow.domain.StepExecution;
import com.flowlab.workflow.domain.StepStatus;
import com.flowlab.workflow.repository.StepExecutionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class CrashRecoveryService {

    private final StepExecutionRepository stepRepository;
    private final WorkflowEngineService engineService;
    private final org.redisson.api.RedissonClient redissonClient;

    @Value("${workflow.engine.worker-heartbeat-timeout-sec:30}")
    private long heartbeatTimeoutSec;

    /**
     * Periodically reaps dead worker tasks and guarantees crash recovery.
     * If an engine/worker node crashed mid-step, its heartbeat is stale.
     * This scanner picks up the exact orphaned step and resumes execution.
     */
    @Scheduled(fixedDelayString = "${workflow.engine.recovery-poll-interval-ms:10000}")
    public void scanAndRecoverOrphanedExecutions() {
        Instant cutoff = Instant.now().minusSeconds(heartbeatTimeoutSec);
        List<StepExecution> staleSteps = stepRepository.findStaleRunningSteps(StepStatus.RUNNING, cutoff);

        if (!staleSteps.isEmpty()) {
            log.warn("🚨 [CRASH-RECOVERY] Detected {} orphaned running step(s) with stale heartbeat! Resuming...", staleSteps.size());
            for (StepExecution step : staleSteps) {
                log.info("Resuming crashed step '{}' for execution {}", step.getStepName(), step.getExecutionId());
                step.setStatus(StepStatus.RETRYING);
                stepRepository.save(step);

                // Reclaim and unlock any orphaned Redis locks held by the terminated worker
                String lockKey = "lock:step:" + step.getExecutionId() + ":" + step.getStepIndex();
                try {
                    redissonClient.getLock(lockKey).forceUnlock();
                    log.info("🔓 [LOCK-RECLAIM] Force-unlocked orphaned Redis lock for {}:{}", step.getExecutionId(), step.getStepIndex());
                } catch (Exception ignored) {}

                engineService.dispatchStep(step.getExecutionId(), step.getStepIndex(), step.getStepName());
            }
        }
    }
}

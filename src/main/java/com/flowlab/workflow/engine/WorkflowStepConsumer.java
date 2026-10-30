package com.flowlab.workflow.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.event.StepTaskMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkflowStepConsumer {

    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final WorkflowEngineService engineService;

    @KafkaListener(topics = "${workflow.engine.step-topic:workflow-step-tasks}", groupId = "flowlab-engine-workers")
    public void consumeStepTask(String messageJson) {
        StepTaskMessage task;
        try {
            task = objectMapper.readValue(messageJson, StepTaskMessage.class);
        } catch (Exception e) {
            log.error("Failed to parse Kafka StepTaskMessage: {}", messageJson, e);
            return;
        }

        String lockKey = "lock:step:" + task.getExecutionId() + ":" + task.getStepIndex();
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // Attempt to acquire distributed lock for 2 seconds, lease for 15 seconds
            boolean acquired = lock.tryLock(2, 15, TimeUnit.SECONDS);
            if (!acquired) {
                // Wait briefly in case a dying worker lock is releasing
                Thread.sleep(2500);
                acquired = lock.tryLock(2, 15, TimeUnit.SECONDS);
            }
            if (!acquired) {
                log.warn("⚠️ [KAFKA-CONSUMER] Lock already held for {} step #{}. Skipping duplicate event.",
                        task.getExecutionId(), task.getStepIndex());
                return;
            }

            log.info("📥 [KAFKA-CONSUME] Worker picked up Step #{}: '{}' for execution {}",
                    task.getStepIndex(), task.getStepName(), task.getExecutionId());

            engineService.executeStepDirect(task.getExecutionId(), task.getStepIndex());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error executing step from Kafka message: {}", e.getMessage(), e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}

package com.flowlab.workflow.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.event.StepTaskMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkflowTaskProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${workflow.engine.step-topic:workflow-step-tasks}")
    private String stepTopic;

    public void dispatchStepTask(String executionId, int stepIndex, String stepName) {
        try {
            StepTaskMessage message = StepTaskMessage.builder()
                    .executionId(executionId)
                    .stepIndex(stepIndex)
                    .stepName(stepName)
                    .timestamp(System.currentTimeMillis())
                    .build();

            String payload = objectMapper.writeValueAsString(message);
            // Key by executionId so all tasks for the same workflow maintain consistent partition affinity
            kafkaTemplate.send(stepTopic, executionId, payload);
            log.info("📨 [KAFKA-PRODUCE] Dispatched Step #{}: '{}' to topic '{}' (Key: {})",
                    stepIndex, stepName, stepTopic, executionId);
        } catch (Exception e) {
            log.error("Failed to publish step task to Kafka topic {}: {}", stepTopic, e.getMessage());
            throw new RuntimeException("Kafka dispatch failed", e);
        }
    }
}

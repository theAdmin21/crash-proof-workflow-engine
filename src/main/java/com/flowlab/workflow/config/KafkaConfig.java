package com.flowlab.workflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${workflow.engine.step-topic:workflow-step-tasks}")
    private String stepTopic;

    @Bean
    public NewTopic workflowStepTasksTopic() {
        return TopicBuilder.name(stepTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

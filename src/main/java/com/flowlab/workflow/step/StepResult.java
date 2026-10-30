package com.flowlab.workflow.step;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepResult {
    private boolean success;
    private String outputPayload;
    private String errorMessage;
    private boolean shouldRetry;

    public static StepResult ok(String output) {
        return StepResult.builder()
                .success(true)
                .outputPayload(output)
                .build();
    }

    public static StepResult retryableFailure(String error) {
        return StepResult.builder()
                .success(false)
                .errorMessage(error)
                .shouldRetry(true)
                .build();
    }

    public static StepResult fatalFailure(String error) {
        return StepResult.builder()
                .success(false)
                .errorMessage(error)
                .shouldRetry(false)
                .build();
    }
}

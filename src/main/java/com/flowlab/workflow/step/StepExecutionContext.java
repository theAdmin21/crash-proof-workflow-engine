package com.flowlab.workflow.step;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepExecutionContext {
    private String executionId;
    private String workflowId;
    private int stepIndex;
    private String stepName;
    private int attempt;
    private String inputPayload;
}

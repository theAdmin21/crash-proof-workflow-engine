package com.flowlab.workflow.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepTaskMessage implements Serializable {
    private String executionId;
    private int stepIndex;
    private String stepName;
    private long timestamp;
}

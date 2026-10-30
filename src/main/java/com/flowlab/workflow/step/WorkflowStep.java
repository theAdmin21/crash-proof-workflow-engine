package com.flowlab.workflow.step;

public interface WorkflowStep {
    String getStepName();
    int getOrder();
    StepResult execute(StepExecutionContext context) throws Exception;
}

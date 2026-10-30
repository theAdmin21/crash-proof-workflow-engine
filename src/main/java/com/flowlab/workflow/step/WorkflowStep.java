package com.flowlab.workflow.step;

import java.util.List;

public interface WorkflowStep {
    String getStepName();
    int getOrder();
    StepResult execute(StepExecutionContext context) throws Exception;

    default List<Integer> getDependencies() {
        return getOrder() == 0 ? List.of() : List.of(getOrder() - 1);
    }

    default StepResult compensate(StepExecutionContext context) throws Exception {
        return StepResult.ok("{}");
    }
}

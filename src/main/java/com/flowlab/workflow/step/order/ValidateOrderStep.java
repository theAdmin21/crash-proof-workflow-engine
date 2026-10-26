package com.flowlab.workflow.step.order;

import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import org.springframework.stereotype.Component;

@Component
public class ValidateOrderStep implements WorkflowStep {

    @Override
    public String getStepName() {
        return "Validate Order";
    }

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        // Simulate order validation latency
        Thread.sleep(1200);
        return StepResult.ok("{\"valid\": true, \"orderTotal\": 249.99, \"itemsCount\": 3}");
    }
}

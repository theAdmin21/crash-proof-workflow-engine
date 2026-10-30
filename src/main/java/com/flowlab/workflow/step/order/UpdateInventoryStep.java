package com.flowlab.workflow.step.order;

import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import org.springframework.stereotype.Component;

@Component
public class UpdateInventoryStep implements WorkflowStep {

    @Override
    public String getStepName() {
        return "Update Inventory";
    }

    @Override
    public int getOrder() {
        return 2;
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        // Simulated failure / crash scenario for testing resilience
        if (context.getInputPayload() != null && context.getInputPayload().contains("simulate_crash") && context.getAttempt() <= 1) {
            Thread.sleep(1000);
            throw new RuntimeException("Simulated worker process crash during inventory reservation!");
        }

        Thread.sleep(1500);
        return StepResult.ok("{\"inventoryUpdated\": true, \"warehouse\": \"US-EAST-1\", \"itemsReserved\": [\"SKU-101\", \"SKU-204\"]}");
    }
}

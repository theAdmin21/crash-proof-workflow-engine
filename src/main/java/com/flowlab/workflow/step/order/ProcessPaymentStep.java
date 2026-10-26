package com.flowlab.workflow.step.order;

import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ProcessPaymentStep implements WorkflowStep {

    @Override
    public String getStepName() {
        return "Process Payment";
    }

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        // Simulate payment gateway call with idempotency key
        Thread.sleep(1800);
        String transactionId = "txn_" + UUID.randomUUID().toString().substring(0, 8);
        return StepResult.ok("{\"paymentStatus\": \"CHARGED\", \"transactionId\": \"" + transactionId + "\", \"amount\": 249.99}");
    }
}

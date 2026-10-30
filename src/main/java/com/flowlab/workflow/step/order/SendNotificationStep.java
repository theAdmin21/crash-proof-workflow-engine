package com.flowlab.workflow.step.order;

import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import org.springframework.stereotype.Component;

@Component
public class SendNotificationStep implements WorkflowStep {

    @Override
    public String getStepName() {
        return "Send Notification";
    }

    @Override
    public int getOrder() {
        return 3;
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        Thread.sleep(800);
        return StepResult.ok("{\"emailSent\": true, \"channel\": \"SES_EMAIL\", \"recipient\": \"customer@example.com\"}");
    }
}

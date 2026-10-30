package com.flowlab.workflow.step.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class FraudCheckStep implements WorkflowStep {

    private final ObjectMapper objectMapper;

    @Override
    public String getStepName() {
        return "Fraud Risk Check";
    }

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public List<Integer> getDependencies() {
        return List.of(0); // Runs in parallel with WarehouseRoutingStep right after Step 0
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        Thread.sleep(600); // Simulate ML fraud scoring model
        JsonNode prev = objectMapper.readTree(context.getInputPayload());

        boolean simulateFraud = prev.has("simulate_fraud") && prev.get("simulate_fraud").asBoolean();
        String customer = prev.has("customerName") ? prev.get("customerName").asText() : "Customer";

        if (simulateFraud) {
            log.warn("🚨 [FRAUD-ALERT] Suspicious velocity detected for {}. Risk score: 98/100 (HIGH RISK)", customer);
            return StepResult.fatalFailure("Fraud Detection Alert: Transaction flagged as high risk (Score 98/100)");
        }

        int riskScore = 8 + (int)(Math.random() * 10);
        log.info("🛡️ [FRAUD-PASS] Customer '{}' cleared fraud inspection. Risk score: {}/100 (LOW RISK)", customer, riskScore);

        Map<String, Object> output = new HashMap<>();
        prev.fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            if (v.isBoolean()) output.put(e.getKey(), v.asBoolean());
            else if (v.isInt()) output.put(e.getKey(), v.asInt());
            else if (v.isNumber()) output.put(e.getKey(), v.numberValue());
            else output.put(e.getKey(), v.asText());
        });
        output.put("fraudCheckPassed", true);
        output.put("riskScore", riskScore);

        return StepResult.ok(objectMapper.writeValueAsString(output));
    }

    @Override
    public StepResult compensate(StepExecutionContext context) throws Exception {
        log.info("↩️ [SAGA] Revoking fraud score reservation for execution {}", context.getExecutionId());
        return StepResult.ok("{\"fraudCheckRevoked\": true}");
    }
}

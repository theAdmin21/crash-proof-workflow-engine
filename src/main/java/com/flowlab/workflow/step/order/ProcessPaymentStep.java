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
import java.util.Map;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessPaymentStep implements WorkflowStep {

    private final ObjectMapper objectMapper;

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
        Thread.sleep(1000);
        JsonNode prev = objectMapper.readTree(context.getInputPayload());

        String total = prev.has("totalAmount") ? prev.get("totalAmount").asText() : "249.99";
        String customer = prev.has("customerName") ? prev.get("customerName").asText() : "Customer";
        boolean simulateCrash = prev.has("simulate_crash") && prev.get("simulate_crash").asBoolean();

        String idempotencyKey = "idemp_" + context.getExecutionId() + "_pay";
        String txnId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        log.info("Processing Payment: ₹{} charged for {} (Txn: {}, IdempotencyKey: {})",
                total, customer, txnId, idempotencyKey);

        Map<String, Object> outputMap = new HashMap<>();
        prev.fields().forEachRemaining(entry -> outputMap.put(entry.getKey(), entry.getValue().asText()));
        outputMap.put("paymentStatus", "CHARGED");
        outputMap.put("transactionId", txnId);
        outputMap.put("idempotencyKey", idempotencyKey);
        outputMap.put("amountCharged", total);
        outputMap.put("simulate_crash", simulateCrash);

        return StepResult.ok(objectMapper.writeValueAsString(outputMap));
    }
}

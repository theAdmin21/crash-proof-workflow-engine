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
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessPaymentStep implements WorkflowStep {

    private final ObjectMapper objectMapper;
    private final com.flowlab.workflow.notification.NotificationService notificationService;

    @Override
    public String getStepName() {
        return "Process Payment";
    }

    @Override
    public int getOrder() {
        return 3;
    }

    @Override
    public List<Integer> getDependencies() {
        return List.of(1, 2); // Synchronization barrier: waits for both Fraud Risk Check and Warehouse Routing
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
        prev.fields().forEachRemaining(entry -> {
            JsonNode val = entry.getValue();
            if (val.isBoolean()) outputMap.put(entry.getKey(), val.asBoolean());
            else if (val.isInt()) outputMap.put(entry.getKey(), val.asInt());
            else if (val.isNumber()) outputMap.put(entry.getKey(), val.numberValue());
            else outputMap.put(entry.getKey(), val.asText());
        });
        outputMap.put("paymentStatus", "CHARGED");
        outputMap.put("transactionId", txnId);
        outputMap.put("idempotencyKey", idempotencyKey);
        outputMap.put("amountCharged", total);

        return StepResult.ok(objectMapper.writeValueAsString(outputMap));
    }

    @Override
    public StepResult compensate(StepExecutionContext context) throws Exception {
        Thread.sleep(600);
        JsonNode prev = context.getInputPayload() != null ? objectMapper.readTree(context.getInputPayload()) : null;
        String txnId = (prev != null && prev.has("transactionId")) ? prev.get("transactionId").asText() : "TXN-UNKNOWN";
        String amount = (prev != null && prev.has("amountCharged")) ? prev.get("amountCharged").asText() : "0.00";
        String customer = (prev != null && prev.has("customerName")) ? prev.get("customerName").asText() : "Customer";

        String refundId = "REF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.warn("↩️ [SAGA-COMPENSATION] Auto-refunded payment of ₹{} for {} (Original Txn: {}, Refund ID: {})",
                amount, customer, txnId, refundId);

        if (prev != null && prev.has("webhookUrl") && !prev.get("webhookUrl").asText().isBlank()) {
            notificationService.sendWebhookNotification(
                    prev.get("webhookUrl").asText(),
                    "↩️ FlowLab Payment Auto-Refunded!",
                    "A downstream failure triggered an automatic Saga compensation rollback.",
                    Map.of(
                            "Customer", customer,
                            "Refund ID", refundId,
                            "Original Txn", txnId,
                            "Amount Refunded", "₹" + amount,
                            "Execution ID", context.getExecutionId()
                    ),
                    0x8B5CF6 // Purple
            );
        }

        String result = objectMapper.writeValueAsString(Map.of(
                "compensationStatus", "REFUND_PROCESSED",
                "refundTransactionId", refundId,
                "originalTransactionId", txnId,
                "amountRefunded", "₹" + amount
        ));
        return StepResult.ok(result);
    }
}

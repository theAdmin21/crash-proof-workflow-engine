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

@Component
@Slf4j
@RequiredArgsConstructor
public class SendNotificationStep implements WorkflowStep {

    private final ObjectMapper objectMapper;
    private final com.flowlab.workflow.notification.NotificationService notificationService;

    @Override
    public String getStepName() {
        return "Send Notification";
    }

    @Override
    public int getOrder() {
        return 5;
    }

    @Override
    public java.util.List<Integer> getDependencies() {
        return java.util.List.of(4);
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        Thread.sleep(800);
        JsonNode prev = objectMapper.readTree(context.getInputPayload());

        String email = prev.has("customerEmail") ? prev.get("customerEmail").asText() : "customer@example.com";
        String customer = prev.has("customerName") ? prev.get("customerName").asText() : "Customer";
        String product = prev.has("productName") ? prev.get("productName").asText() : "Item";
        String total = prev.has("totalAmount") ? prev.get("totalAmount").asText() : "0.00";
        String txn = prev.has("transactionId") ? prev.get("transactionId").asText() : "TXN-DEFAULT";
        String res = prev.has("reservationId") ? prev.get("reservationId").asText() : "RES-DEFAULT";
        String webhookUrl = prev.has("webhookUrl") ? prev.get("webhookUrl").asText() : null;

        log.info("Dispatched confirmation email to {} for {}. Receipt Total: ₹{}, TxnId: {}, Reservation: {}",
                email, customer, total, txn, res);

        boolean webhookSent = false;
        if (webhookUrl != null && !webhookUrl.isBlank()) {
            webhookSent = notificationService.sendWebhookNotification(
                    webhookUrl,
                    "🎉 FlowLab Order Confirmed!",
                    "Your multi-step workflow completed flawlessly with zero duplicate side effects.",
                    Map.of(
                            "Customer", customer,
                            "Product", product,
                            "Amount Paid", "₹" + total,
                            "Transaction ID", txn,
                            "Tracking ID", res,
                            "Execution ID", context.getExecutionId()
                    ),
                    0x10B981 // Emerald green
            );
        }

        Map<String, Object> output = new HashMap<>();
        output.put("emailDelivered", true);
        output.put("recipient", email);
        output.put("customerName", customer);
        output.put("product", product);
        output.put("totalCharged", "₹" + total);
        output.put("transactionId", txn);
        output.put("shipmentTracking", res);
        output.put("webhookDelivered", webhookSent);

        return StepResult.ok(objectMapper.writeValueAsString(output));
    }
}

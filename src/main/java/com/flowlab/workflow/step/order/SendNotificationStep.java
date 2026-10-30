package com.flowlab.workflow.step.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class SendNotificationStep implements WorkflowStep {

    private final ObjectMapper objectMapper;

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
        JsonNode prev = objectMapper.readTree(context.getInputPayload());

        String email = prev.has("customerEmail") ? prev.get("customerEmail").asText() : "customer@example.com";
        String customer = prev.has("customerName") ? prev.get("customerName").asText() : "Customer";
        String product = prev.has("productName") ? prev.get("productName").asText() : "Item";
        String total = prev.has("totalAmount") ? prev.get("totalAmount").asText() : "0.00";
        String txn = prev.has("transactionId") ? prev.get("transactionId").asText() : "TXN-DEFAULT";
        String res = prev.has("reservationId") ? prev.get("reservationId").asText() : "RES-DEFAULT";

        log.info("Dispatched confirmation email to {} for {}. Receipt Total: ₹{}, TxnId: {}, Reservation: {}",
                email, customer, total, txn, res);

        String output = objectMapper.writeValueAsString(Map.of(
                "emailDelivered", true,
                "recipient", email,
                "customerName", customer,
                "product", product,
                "totalCharged", "₹" + total,
                "transactionId", txn,
                "shipmentTracking", res
        ));

        return StepResult.ok(output);
    }
}

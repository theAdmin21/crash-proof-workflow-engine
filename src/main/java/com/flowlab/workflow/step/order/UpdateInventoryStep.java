package com.flowlab.workflow.step.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowlab.workflow.domain.Product;
import com.flowlab.workflow.repository.ProductRepository;
import com.flowlab.workflow.step.StepExecutionContext;
import com.flowlab.workflow.step.StepResult;
import com.flowlab.workflow.step.WorkflowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class UpdateInventoryStep implements WorkflowStep {

    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;

    @Override
    public String getStepName() {
        return "Update Inventory";
    }

    @Override
    public int getOrder() {
        return 2;
    }

    @Override
    @Transactional
    public StepResult execute(StepExecutionContext context) throws Exception {
        JsonNode prev = objectMapper.readTree(context.getInputPayload());
        boolean simulateFatal = prev.has("simulate_fatal") && prev.get("simulate_fatal").asBoolean();
        boolean simulateCrash = prev.has("simulate_crash") && prev.get("simulate_crash").asBoolean();

        if (simulateFatal) {
            Thread.sleep(800);
            return StepResult.fatalFailure("Warehouse stock physically damaged/compromised! Unrecoverable error.");
        }

        if (simulateCrash && context.getAttempt() <= 1) {
            Thread.sleep(800);
            throw new RuntimeException("Simulated worker process crash during inventory reservation!");
        }

        Thread.sleep(1200);
        String sku = prev.has("sku") ? prev.get("sku").asText() : "SKU-IPHONE";
        int quantity = prev.has("quantity") ? prev.get("quantity").asInt() : 1;

        Product product = productRepository.findById(sku)
                .orElseThrow(() -> new IllegalArgumentException("Product not found: " + sku));

        int newStock = Math.max(0, product.getStockQuantity() - quantity);
        product.setStockQuantity(newStock);
        productRepository.save(product);

        String reservationId = "RES-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("Decremented inventory for {}: remaining stock is now {}", product.getName(), newStock);

        Map<String, Object> outputMap = new HashMap<>();
        prev.fields().forEachRemaining(entry -> outputMap.put(entry.getKey(), entry.getValue().asText()));
        outputMap.put("inventoryReserved", true);
        outputMap.put("reservationId", reservationId);
        outputMap.put("remainingStock", newStock);

        return StepResult.ok(objectMapper.writeValueAsString(outputMap));
    }

    @Override
    @Transactional
    public StepResult compensate(StepExecutionContext context) throws Exception {
        JsonNode prev = context.getInputPayload() != null ? objectMapper.readTree(context.getInputPayload()) : null;
        if (prev != null && prev.has("sku") && prev.has("quantity")) {
            String sku = prev.get("sku").asText();
            int qty = prev.get("quantity").asInt();
            productRepository.findById(sku).ifPresent(p -> {
                p.setStockQuantity(p.getStockQuantity() + qty);
                productRepository.save(p);
                log.warn("↩️ [SAGA-COMPENSATION] Restored {} units of stock for {}", qty, p.getName());
            });
        }
        return StepResult.ok("{\"compensation\": \"INVENTORY_RELEASED\"}");
    }
}

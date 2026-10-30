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

import java.math.BigDecimal;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class ValidateOrderStep implements WorkflowStep {

    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;

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
        Thread.sleep(800);
        JsonNode root = objectMapper.readTree(context.getInputPayload());
        String sku = root.has("sku") ? root.get("sku").asText() : "SKU-IPHONE";
        int quantity = root.has("quantity") ? root.get("quantity").asInt() : 1;
        String customerEmail = root.has("customerEmail") ? root.get("customerEmail").asText() : "user@example.com";
        String customerName = root.has("customerName") ? root.get("customerName").asText() : "Customer";
        boolean simulateCrash = root.has("simulate_crash") && root.get("simulate_crash").asBoolean();

        Product product = productRepository.findById(sku)
                .orElseThrow(() -> new IllegalArgumentException("Product not found with SKU: " + sku));

        if (product.getStockQuantity() < quantity) {
            return StepResult.fatalFailure(String.format("Insufficient stock for %s. Requested: %d, Available: %d",
                    product.getName(), quantity, product.getStockQuantity()));
        }

        BigDecimal total = product.getPrice().multiply(BigDecimal.valueOf(quantity));
        log.info("Validated order for {} ({} x {}). Total: ₹{}", customerName, quantity, product.getName(), total);

        String output = objectMapper.writeValueAsString(Map.of(
                "valid", true,
                "sku", sku,
                "productName", product.getName(),
                "quantity", quantity,
                "unitPrice", product.getPrice(),
                "totalAmount", total,
                "customerName", customerName,
                "customerEmail", customerEmail,
                "simulate_crash", simulateCrash
        ));

        return StepResult.ok(output);
    }
}

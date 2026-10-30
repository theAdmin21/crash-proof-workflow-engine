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
public class WarehouseRoutingStep implements WorkflowStep {

    private final ObjectMapper objectMapper;

    @Override
    public String getStepName() {
        return "Warehouse Routing";
    }

    @Override
    public int getOrder() {
        return 2;
    }

    @Override
    public List<Integer> getDependencies() {
        return List.of(0); // Runs in parallel with FraudCheckStep right after Step 0
    }

    @Override
    public StepResult execute(StepExecutionContext context) throws Exception {
        Thread.sleep(700); // Simulate geo-routing proximity algorithm
        JsonNode prev = objectMapper.readTree(context.getInputPayload());

        String[] fulfillmentHubs = {"Hub-Delhi-North", "Hub-Mumbai-West", "Hub-Bengaluru-East"};
        String chosenHub = fulfillmentHubs[(int)(Math.random() * fulfillmentHubs.length)];

        log.info("📍 [ROUTING-OPTIMAL] Order allocated to nearest fulfillment facility: {} (Next-Day Delivery guaranteed)", chosenHub);

        Map<String, Object> output = new HashMap<>();
        prev.fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            if (v.isBoolean()) output.put(e.getKey(), v.asBoolean());
            else if (v.isInt()) output.put(e.getKey(), v.asInt());
            else if (v.isNumber()) output.put(e.getKey(), v.numberValue());
            else output.put(e.getKey(), v.asText());
        });
        output.put("fulfillmentHub", chosenHub);
        output.put("deliveryEstimate", "Next-Day Priority Air");

        return StepResult.ok(objectMapper.writeValueAsString(output));
    }

    @Override
    public StepResult compensate(StepExecutionContext context) throws Exception {
        log.info("↩️ [SAGA] Releasing warehouse routing slot for execution {}", context.getExecutionId());
        return StepResult.ok("{\"routingSlotReleased\": true}");
    }
}

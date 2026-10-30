package com.flowlab.workflow.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    public boolean sendWebhookNotification(String webhookUrl, String title, String description, Map<String, String> fields, int colorCode) {
        if (webhookUrl == null || webhookUrl.isBlank()) {
            return false;
        }

        try {
            Map<String, Object> payload;
            if (webhookUrl.contains("discord.com")) {
                // Discord Embed Format
                List<Map<String, String>> embedFields = fields.entrySet().stream()
                        .map(e -> Map.of("name", e.getKey(), "value", e.getValue(), "inline", "true"))
                        .toList();

                payload = Map.of(
                        "username", "FlowLab Engine",
                        "embeds", List.of(Map.of(
                                "title", title,
                                "description", description,
                                "color", colorCode,
                                "fields", embedFields
                        ))
                );
            } else if (webhookUrl.contains("slack.com")) {
                // Slack Webhook Format
                payload = Map.of(
                        "text", "*" + title + "*\n" + description + "\n" + fields.toString()
                );
            } else {
                // Standard Generic Webhook Format (e.g. webhook.site, pipedream, zapier)
                Map<String, Object> generic = new HashMap<>(fields);
                generic.put("title", title);
                generic.put("description", description);
                generic.put("system", "FlowLab Crash-Proof Engine");
                payload = generic;
            }

            String jsonBody = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(webhookUrl))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("📢 [WEBHOOK-DISPATCH] Status: {} sent to {}", response.statusCode(), webhookUrl);
            return response.statusCode() >= 200 && response.statusCode() < 300;

        } catch (Exception e) {
            log.warn("⚠️ [WEBHOOK-DISPATCH] Failed to send webhook to {}: {}", webhookUrl, e.getMessage());
            return false;
        }
    }
}

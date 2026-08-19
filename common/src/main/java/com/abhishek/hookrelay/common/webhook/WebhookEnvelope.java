package com.abhishek.hookrelay.common.webhook;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * The webhook request body.
 *
 * <pre>
 * {
 *   "id":         "0198f2c1-...-7a3b",
 *   "event_id":   "0198f2c0-...-1c4d",
 *   "event_type": "payment.succeeded",
 *   "created_at": "2026-08-19T15:02:47Z",
 *   "data":       { "order_id": "A-1", "amount": 4200 }
 * }
 * </pre>
 *
 * <p>The producer's payload is nested under {@code data} rather than merged into the top level, so
 * a payload that happens to contain its own {@code id} or {@code event_type} cannot collide with
 * the envelope's fields.
 *
 * <p>{@code id} is the delivery id, not the event id — it is what the receiver deduplicates on, and
 * it is stable across retries.
 */
public record WebhookEnvelope(
        @JsonProperty("id") UUID id,
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("data") JsonNode data
) {
}

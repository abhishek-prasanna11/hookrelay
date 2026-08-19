package com.abhishek.hookrelay.api.web.dto;

import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.DeliveryStatus;
import com.abhishek.hookrelay.common.domain.Event;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public record EventResponse(
        UUID id,
        String eventType,
        JsonNode payload,
        String idempotencyKey,
        OffsetDateTime createdAt,
        List<DeliverySummary> deliveries
) {

    public record DeliverySummary(
            UUID id,
            UUID endpointId,
            DeliveryStatus status,
            int attemptCount,
            OffsetDateTime nextAttemptAt,
            String lastError
    ) {
        public static DeliverySummary from(Delivery delivery) {
            return new DeliverySummary(
                    delivery.getId(),
                    delivery.getEndpointId(),
                    delivery.getStatus(),
                    delivery.getAttemptCount(),
                    delivery.getNextAttemptAt(),
                    delivery.getLastError());
        }
    }

    public static EventResponse from(Event event, List<Delivery> deliveries) {
        return new EventResponse(
                event.getId(),
                event.getEventType(),
                event.getPayload(),
                event.getIdempotencyKey(),
                event.getCreatedAt(),
                deliveries.stream().map(DeliverySummary::from).toList());
    }
}

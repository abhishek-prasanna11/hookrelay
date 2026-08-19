package com.abhishek.hookrelay.api.web.dto;

import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.DeliveryAttempt;
import com.abhishek.hookrelay.common.domain.DeliveryStatus;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public record DeliveryResponse(
        UUID id,
        UUID eventId,
        UUID endpointId,
        DeliveryStatus status,
        int attemptCount,
        OffsetDateTime nextAttemptAt,
        String lastError,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        List<AttemptResponse> attempts
) {

    public record AttemptResponse(
            int attemptNo,
            OffsetDateTime startedAt,
            int durationMs,
            Integer responseStatus,
            String errorClass,
            String responseBody
    ) {
        public static AttemptResponse from(DeliveryAttempt attempt) {
            return new AttemptResponse(
                    attempt.getAttemptNo(),
                    attempt.getStartedAt(),
                    attempt.getDurationMs(),
                    attempt.getResponseStatus(),
                    attempt.getErrorClass(),
                    attempt.getResponseBody());
        }
    }

    public static DeliveryResponse from(Delivery delivery, List<DeliveryAttempt> attempts) {
        return new DeliveryResponse(
                delivery.getId(),
                delivery.getEventId(),
                delivery.getEndpointId(),
                delivery.getStatus(),
                delivery.getAttemptCount(),
                delivery.getNextAttemptAt(),
                delivery.getLastError(),
                delivery.getCreatedAt(),
                delivery.getUpdatedAt(),
                attempts.stream().map(AttemptResponse::from).toList());
    }
}

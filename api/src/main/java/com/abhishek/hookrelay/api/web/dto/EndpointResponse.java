package com.abhishek.hookrelay.api.web.dto;

import com.abhishek.hookrelay.common.domain.Endpoint;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * @param secret present only in the response to endpoint creation. Every later read omits it —
 *               it is a signing key, and an API that will hand it back on demand turns any read
 *               access into the ability to forge webhooks.
 */
public record EndpointResponse(
        UUID id,
        String url,
        List<String> eventTypes,
        boolean active,
        int maxConcurrency,
        String secret,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {

    public static EndpointResponse withoutSecret(Endpoint endpoint) {
        return new EndpointResponse(
                endpoint.getId(),
                endpoint.getUrl(),
                endpoint.getEventTypes(),
                endpoint.isActive(),
                endpoint.getMaxConcurrency(),
                null,
                endpoint.getCreatedAt(),
                endpoint.getUpdatedAt());
    }

    public static EndpointResponse withSecret(Endpoint endpoint) {
        return new EndpointResponse(
                endpoint.getId(),
                endpoint.getUrl(),
                endpoint.getEventTypes(),
                endpoint.isActive(),
                endpoint.getMaxConcurrency(),
                endpoint.getSecret(),
                endpoint.getCreatedAt(),
                endpoint.getUpdatedAt());
    }
}

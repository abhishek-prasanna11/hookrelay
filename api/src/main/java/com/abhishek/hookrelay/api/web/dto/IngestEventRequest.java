package com.abhishek.hookrelay.api.web.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record IngestEventRequest(
        String eventType,
        JsonNode payload
) {
}

package com.abhishek.hookrelay.api.web.dto;

import java.util.UUID;

/**
 * @param deliveriesCreated how many endpoints matched. Zero is a success, not an error: an event
 *                          nobody subscribes to is still a valid event, and returning an error
 *                          would push producers toward not sending events at all.
 */
public record IngestEventResponse(
        UUID eventId,
        int deliveriesCreated
) {
}

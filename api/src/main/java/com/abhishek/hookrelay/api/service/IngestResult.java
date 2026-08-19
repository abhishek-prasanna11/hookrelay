package com.abhishek.hookrelay.api.service;

import java.util.UUID;

/**
 * @param created true when this call is what committed the event (→ 202 Accepted); false when the
 *                event already existed and this was a duplicate submission (→ 200 OK). The producer
 *                can therefore distinguish "I created this" from "this already existed" without
 *                inspecting the body.
 */
public record IngestResult(UUID eventId, int deliveriesCreated, boolean created) {
}

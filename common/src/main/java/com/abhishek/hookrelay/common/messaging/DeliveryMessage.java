package com.abhishek.hookrelay.common.messaging;

import java.util.UUID;

/**
 * The queue message body: a claim check, not the payload.
 *
 * <p>Carrying only the delivery id rather than the whole event is deliberate. The worker has to
 * read the delivery row regardless — it must check whether the delivery already succeeded before
 * attempting it (BLUEPRINT.md section 11) — so the database read is not avoidable and duplicating
 * the payload into the broker would only create a second copy that can go stale. Small messages
 * also keep broker memory low exactly when a backlog is building, which is the situation the
 * autoscaling work in phase 8 exists for.
 */
public record DeliveryMessage(UUID deliveryId) {
}

package com.abhishek.hookrelay.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A recorded intent to publish one {@link Delivery} to the broker.
 *
 * <p>Written in the same transaction as the delivery it refers to, which is the entire point: two
 * inserts into one database are atomic, whereas a database commit followed by a broker publish is
 * not. If the process dies between the two, the row is still here and the next poll picks it up.
 *
 * <p>{@code publishedAt} is set only after RabbitMQ has <em>confirmed</em> the message. Marking it
 * before the confirm would defeat the whole mechanism — the row would look done while the message
 * never existed.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "delivery_id", nullable = false)
    private UUID deliveryId;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    /** Null until the broker has confirmed the message. Null is what makes a row eligible to poll. */
    @Column(name = "published_at")
    private OffsetDateTime publishedAt;

    /** Failed publish attempts. Diagnostic only — it never causes the row to be given up on. */
    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    @Column(name = "last_error")
    private String lastError;

    protected OutboxEvent() {
        // for JPA
    }

    public OutboxEvent(UUID id, UUID deliveryId, OffsetDateTime now) {
        this.id = id;
        this.deliveryId = deliveryId;
        this.createdAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getDeliveryId() {
        return deliveryId;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getPublishedAt() {
        return publishedAt;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public String getLastError() {
        return lastError;
    }
}

package com.abhishek.hookrelay.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One event's obligation to one endpoint — the unit of work the whole system is built around.
 *
 * <p>{@code id} is published to the customer as {@code X-HookRelay-Delivery-Id} and is stable
 * across every retry, which is what lets a receiver deduplicate under our at-least-once contract.
 *
 * <p>References to {@link Event} and {@link Endpoint} are plain UUID columns rather than JPA
 * associations. Workers only ever need the identifiers, and avoiding {@code @ManyToOne} keeps
 * loading a delivery to exactly one query with no lazy-initialization traps.
 *
 * <p>Note on {@code nextAttemptAt}: from phase 4 onward the retry <em>schedule</em> lives in
 * RabbitMQ's delay queues, not here. This column records when the next attempt is expected so it
 * is visible in the API and in queries — it is observability, not a scheduler. Nothing polls it.
 */
@Entity
@Table(name = "deliveries")
public class Delivery {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "endpoint_id", nullable = false)
    private UUID endpointId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status = DeliveryStatus.PENDING;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    @Column(name = "next_attempt_at")
    private OffsetDateTime nextAttemptAt;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Delivery() {
        // for JPA
    }

    public Delivery(UUID id, UUID eventId, UUID endpointId, OffsetDateTime now) {
        this.id = id;
        this.eventId = eventId;
        this.endpointId = endpointId;
        this.status = DeliveryStatus.PENDING;
        this.attemptCount = 0;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getEndpointId() {
        return endpointId;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public OffsetDateTime getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}

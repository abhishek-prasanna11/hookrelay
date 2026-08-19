package com.abhishek.hookrelay.common.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * An immutable record that something happened. Never updated after insert.
 *
 * <p>An event is a <em>fact</em>; a {@link Delivery} is a <em>task</em>. One fact produces many
 * tasks, each with its own independent lifecycle.
 *
 * <p>Uniqueness on {@code (tenant_id, idempotency_key)} is what makes repeated submission safe —
 * see {@code events_idempotency_uq} in {@code V1__init.sql}. The constraint, not an application
 * level existence check, is what holds under concurrent requests from multiple API replicas.
 */
@Entity
@Table(name = "events")
public class Event {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode payload;

    @Column(name = "idempotency_key", nullable = false)
    private String idempotencyKey;

    /**
     * SHA-256 over the canonicalized request. Distinguishes an honest retry (same key, same hash)
     * from a producer reusing a key for different content (same key, different hash → 409).
     */
    @Column(name = "request_hash", nullable = false)
    private String requestHash;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected Event() {
        // for JPA
    }

    public Event(UUID id, UUID tenantId, String eventType, JsonNode payload,
                 String idempotencyKey, String requestHash, OffsetDateTime now) {
        this.id = id;
        this.tenantId = tenantId;
        this.eventType = eventType;
        this.payload = payload;
        this.idempotencyKey = idempotencyKey;
        this.requestHash = requestHash;
        this.createdAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public String getEventType() {
        return eventType;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}

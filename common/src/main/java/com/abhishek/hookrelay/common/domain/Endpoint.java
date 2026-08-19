package com.abhishek.hookrelay.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * A customer-registered destination for webhooks.
 *
 * <p>{@code eventTypes} maps to a PostgreSQL {@code text[]} column rather than a join table, so
 * fan-out is a single indexed containment query ({@code event_types @> ARRAY[?]}) with no join.
 */
@Entity
@Table(name = "endpoints")
public class Endpoint {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(nullable = false)
    private String url;

    /** HMAC signing key. Used from phase 3 onward; never returned by the API after creation. */
    @Column(nullable = false)
    private String secret;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "event_types", nullable = false, columnDefinition = "text[]")
    private List<String> eventTypes;

    @Column(nullable = false)
    private boolean active = true;

    /** Per-endpoint in-flight cap, enforced by the worker's semaphore in phase 5. */
    @Column(name = "max_concurrency", nullable = false)
    private int maxConcurrency = 5;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Endpoint() {
        // for JPA
    }

    public Endpoint(UUID id, UUID tenantId, String url, String secret, List<String> eventTypes,
                    int maxConcurrency, OffsetDateTime now) {
        this.id = id;
        this.tenantId = tenantId;
        this.url = url;
        this.secret = secret;
        this.eventTypes = eventTypes;
        this.maxConcurrency = maxConcurrency;
        this.active = true;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public String getUrl() {
        return url;
    }

    public String getSecret() {
        return secret;
    }

    public List<String> getEventTypes() {
        return eventTypes;
    }

    public boolean isActive() {
        return active;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void deactivate(OffsetDateTime now) {
        this.active = false;
        this.updatedAt = now;
    }
}

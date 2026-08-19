package com.abhishek.hookrelay.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Append-only audit trail: one row per HTTP attempt made against a {@link Delivery}.
 *
 * <p>Never updated, never deleted. This is what makes "the destination was down for six hours and
 * here is exactly what we tried" answerable, and it is the raw material for the phase 10 failure
 * demonstrations.
 *
 * <p>The surrogate key is a {@code bigserial} rather than a UUID because, unlike {@code deliveries},
 * these rows are internal — nothing publishes an attempt id, so there is nothing to guess.
 *
 * <p>{@code responseBody} is capped at 512 bytes (blueprint section 16) and the cap is also a CHECK
 * constraint in the schema, so a truncation bug in the worker fails the insert instead of writing
 * an unbounded value.
 */
@Entity
@Table(name = "delivery_attempts")
public class DeliveryAttempt {

    public static final int MAX_RESPONSE_BODY_BYTES = 512;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "delivery_id", nullable = false)
    private UUID deliveryId;

    @Column(name = "attempt_no", nullable = false)
    private int attemptNo;

    @Column(name = "started_at", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "duration_ms", nullable = false)
    private int durationMs;

    /** Null when the attempt failed before any HTTP response arrived (timeout, DNS, connect). */
    @Column(name = "response_status")
    private Integer responseStatus;

    @Column(name = "error_class")
    private String errorClass;

    @Column(name = "response_body")
    private String responseBody;

    protected DeliveryAttempt() {
        // for JPA
    }

    public Long getId() {
        return id;
    }

    public UUID getDeliveryId() {
        return deliveryId;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public OffsetDateTime getStartedAt() {
        return startedAt;
    }

    public int getDurationMs() {
        return durationMs;
    }

    public Integer getResponseStatus() {
        return responseStatus;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public String getResponseBody() {
        return responseBody;
    }
}

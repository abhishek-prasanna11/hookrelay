package com.abhishek.hookrelay.api.service;

import com.abhishek.hookrelay.api.error.ApiException;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.DeliveryRepository;
import com.abhishek.hookrelay.common.repo.EventRepository;
import com.abhishek.hookrelay.common.util.CanonicalJson;
import com.abhishek.hookrelay.common.util.Hashing;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Accepts events: validates, deduplicates, and hands off to the single ingest transaction.
 *
 * <p>The interesting part is the duplicate path. This class does <em>not</em> ask whether the
 * idempotency key already exists before inserting. Two API replicas handling a producer's original
 * request and its retry would both see "no" and both insert. Instead the insert is attempted
 * unconditionally and the {@code events_idempotency_uq} constraint arbitrates; the loser catches
 * the violation and reads back the winner's row.
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private static final String IDEMPOTENCY_CONSTRAINT = "events_idempotency_uq";
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 255;
    private static final int MAX_EVENT_TYPE_LENGTH = 255;

    private final IngestTransactions transactions;
    private final EventRepository events;
    private final DeliveryRepository deliveries;
    private final int maxPayloadBytes;

    public IngestService(IngestTransactions transactions,
                         EventRepository events,
                         DeliveryRepository deliveries,
                         @Value("${hookrelay.ingest.max-payload-bytes}") int maxPayloadBytes) {
        this.transactions = transactions;
        this.events = events;
        this.deliveries = deliveries;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public IngestResult ingest(UUID tenantId, String idempotencyKey, String eventType, JsonNode payload) {
        validate(idempotencyKey, eventType, payload);

        String canonicalPayload = CanonicalJson.canonicalize(payload);
        int payloadBytes = canonicalPayload.getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > maxPayloadBytes) {
            throw ApiException.unprocessable("payload_too_large",
                    "payload is " + payloadBytes + " bytes, limit is " + maxPayloadBytes);
        }

        String requestHash = requestHash(tenantId, eventType, canonicalPayload);

        try {
            return transactions.createEventAndFanOut(tenantId, idempotencyKey, eventType, payload, requestHash);
        } catch (RuntimeException e) {
            // Only a violation of the idempotency constraint means "this is a duplicate". Any other
            // integrity failure is a real bug and must not be reported to the producer as success.
            if (!Constraints.isViolationOf(e, IDEMPOTENCY_CONSTRAINT)) {
                throw e;
            }
            log.debug("idempotency key {} already exists for tenant {}, resolving duplicate",
                    idempotencyKey, tenantId);
            return resolveDuplicate(tenantId, idempotencyKey, requestHash);
        }
    }

    /**
     * Runs after the insert transaction has been rolled back, so it gets a fresh transaction. This
     * ordering is mandatory, not stylistic: once PostgreSQL raises an error inside a transaction,
     * every subsequent statement in it fails with "current transaction is aborted" until rollback.
     */
    private IngestResult resolveDuplicate(UUID tenantId, String idempotencyKey, String requestHash) {
        Event existing = events.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey)
                .orElseThrow(() -> new IllegalStateException(
                        "unique violation on " + IDEMPOTENCY_CONSTRAINT
                                + " but no row found for tenant " + tenantId + " key " + idempotencyKey));

        if (!existing.getRequestHash().equals(requestHash)) {
            // Same key, different content. Returning the original event here would silently discard
            // this request while telling the producer it succeeded, so refuse loudly instead.
            throw ApiException.conflict("idempotency_key_reused",
                    "Idempotency-Key '" + idempotencyKey + "' was already used with a different request body");
        }

        int deliveriesCreated = (int) deliveries.countByEventId(existing.getId());
        return new IngestResult(existing.getId(), deliveriesCreated, false);
    }

    /**
     * The hash covers tenant, event type and canonicalized payload — everything that defines the
     * request. Canonicalization means a producer that re-serializes its payload before retrying,
     * emitting the same keys in a different order, is still recognised as an honest retry.
     */
    private static String requestHash(UUID tenantId, String eventType, String canonicalPayload) {
        return Hashing.sha256Hex(tenantId + "\n" + eventType + "\n" + canonicalPayload);
    }

    private void validate(String idempotencyKey, String eventType, JsonNode payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw ApiException.badRequest("missing_idempotency_key",
                    "Idempotency-Key header is required");
        }
        if (idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw ApiException.badRequest("idempotency_key_too_long",
                    "Idempotency-Key must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }
        if (eventType == null || eventType.isBlank()) {
            throw ApiException.badRequest("missing_event_type", "event_type is required");
        }
        if (eventType.length() > MAX_EVENT_TYPE_LENGTH) {
            throw ApiException.badRequest("event_type_too_long",
                    "event_type must be at most " + MAX_EVENT_TYPE_LENGTH + " characters");
        }
        if (payload == null || payload.isNull() || payload.isMissingNode()) {
            throw ApiException.badRequest("missing_payload", "payload is required");
        }
        if (!payload.isObject()) {
            throw ApiException.badRequest("invalid_payload", "payload must be a JSON object");
        }
    }
}

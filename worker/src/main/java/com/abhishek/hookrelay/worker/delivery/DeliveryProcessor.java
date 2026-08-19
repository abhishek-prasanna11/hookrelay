package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.DeliveryStatus;
import com.abhishek.hookrelay.common.domain.Endpoint;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.DeliveryRepository;
import com.abhishek.hookrelay.common.repo.EndpointRepository;
import com.abhishek.hookrelay.common.repo.EventRepository;
import com.abhishek.hookrelay.common.retry.RetryPolicy;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.common.webhook.WebhookEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

/**
 * Processes one delivery: claim, load, sign, send, record.
 *
 * <p>The order is load-bearing. Nothing is acknowledged to the broker until the outcome is committed
 * to PostgreSQL, so a worker that dies at any point leaves the message unacknowledged and it is
 * redelivered rather than lost.
 */
@Component
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);

    private final DeliveryRepository deliveries;
    private final EndpointRepository endpoints;
    private final EventRepository events;
    private final DeliveryStore store;
    private final WebhookSender sender;
    private final RetryPublisher retries;
    private final ObjectMapper objectMapper;

    public DeliveryProcessor(DeliveryRepository deliveries,
                             EndpointRepository endpoints,
                             EventRepository events,
                             DeliveryStore store,
                             WebhookSender sender,
                             RetryPublisher retries,
                             ObjectMapper objectMapper) {
        this.deliveries = deliveries;
        this.endpoints = endpoints;
        this.events = events;
        this.store = store;
        this.sender = sender;
        this.retries = retries;
        this.objectMapper = objectMapper;
    }

    /**
     * @return the outcome, or empty when there was nothing to do — the delivery had already
     *         succeeded, or its delivery/endpoint/event rows are gone. Either way the caller
     *         acknowledges: redelivering would not change the answer.
     */
    public Optional<AttemptOutcome> process(UUID deliveryId) {
        // Claiming is what makes a redelivered message safe. An empty result means another worker
        // already completed this delivery — most likely one that died between the customer's 200
        // and its acknowledgement, which is a window that cannot be closed, only handled.
        Optional<Integer> claimed = store.claimAttempt(deliveryId);
        if (claimed.isEmpty()) {
            log.debug("delivery {} already succeeded or missing, skipping", deliveryId);
            return Optional.empty();
        }
        int attemptNo = claimed.get();

        Delivery delivery = deliveries.findById(deliveryId).orElse(null);
        if (delivery == null) {
            log.warn("delivery {} vanished after claim", deliveryId);
            return Optional.empty();
        }

        Endpoint endpoint = endpoints.findById(delivery.getEndpointId()).orElse(null);
        Event event = events.findById(delivery.getEventId()).orElse(null);
        if (endpoint == null || event == null) {
            store.markDeadWithoutAttempt(deliveryId,
                    endpoint == null ? "endpoint no longer exists" : "event no longer exists");
            return Optional.empty();
        }

        byte[] body;
        try {
            body = serializeEnvelope(delivery, event);
        } catch (JsonProcessingException e) {
            log.error("could not serialize envelope for delivery {}", deliveryId, e);
            store.markDeadWithoutAttempt(deliveryId, "envelope serialization failed");
            return Optional.empty();
        }

        OffsetDateTime startedAt = OffsetDateTime.now();
        AttemptOutcome outcome = sender.send(
                endpoint.getUrl(), endpoint.getSecret(), body,
                delivery.getId(), event.getId(), event.getEventType(), attemptNo);

        route(deliveryId, endpoint.getId(), attemptNo, startedAt, outcome);

        log.debug("delivery {} attempt {} -> {} ({}ms)",
                deliveryId, attemptNo, outcome.classification(), outcome.durationMs());
        return Optional.of(outcome);
    }

    /**
     * Decides what happens to the delivery next, and records it.
     *
     * <pre>
     *   SUCCESS            → SUCCEEDED, nothing queued
     *   PERMANENT          → DEAD, dead-lettered
     *   RETRYABLE, tier    → FAILED, scheduled on that tier
     *   RETRYABLE, no tier → DEAD, dead-lettered (attempts exhausted)
     * </pre>
     *
     * <p>The publish happens before the caller acknowledges the original message, so a crash in
     * between duplicates a retry rather than abandoning the delivery.
     */
    private void route(UUID deliveryId, UUID endpointId, int attemptNo,
                       OffsetDateTime startedAt, AttemptOutcome outcome) {
        if (outcome.succeeded()) {
            store.recordAttempt(deliveryId, attemptNo, startedAt, outcome,
                    DeliveryStatus.SUCCEEDED, null);
            return;
        }

        if (outcome.classification() == AttemptOutcome.Classification.PERMANENT) {
            store.recordAttempt(deliveryId, attemptNo, startedAt, outcome, DeliveryStatus.DEAD, null);
            retries.deadLetter(deliveryId, endpointId, RetryPublisher.REASON_PERMANENT_FAILURE,
                    attemptNo, describe(outcome));
            return;
        }

        Optional<RetryTier> tier = RetryPolicy.tierAfter(attemptNo);
        if (tier.isEmpty()) {
            store.recordAttempt(deliveryId, attemptNo, startedAt, outcome, DeliveryStatus.DEAD, null);
            retries.deadLetter(deliveryId, endpointId, RetryPublisher.REASON_ATTEMPTS_EXHAUSTED,
                    attemptNo, describe(outcome));
            return;
        }

        long delayMillis = RetryPolicy.jitteredDelayMillis(tier.get());
        store.recordAttempt(deliveryId, attemptNo, startedAt, outcome, DeliveryStatus.FAILED,
                OffsetDateTime.now().plus(Duration.ofMillis(delayMillis)));
        retries.scheduleRetry(deliveryId, tier.get(), delayMillis);
    }

    private static String describe(AttemptOutcome outcome) {
        return outcome.responseStatus() != null
                ? "HTTP " + outcome.responseStatus()
                : outcome.errorClass();
    }

    /**
     * Serializes the body exactly once. These bytes are both what gets signed and what gets written
     * to the socket — signing one serialization and transmitting another produces signatures that
     * fail at the customer while looking like a key problem. The risk is real here because the
     * payload round-trips through {@code jsonb}, which reorders keys and strips whitespace.
     */
    private byte[] serializeEnvelope(Delivery delivery, Event event) throws JsonProcessingException {
        WebhookEnvelope envelope = new WebhookEnvelope(
                delivery.getId(),
                event.getId(),
                event.getEventType(),
                event.getCreatedAt().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                event.getPayload());
        return objectMapper.writeValueAsString(envelope).getBytes(StandardCharsets.UTF_8);
    }

}

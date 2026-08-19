package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.DeliveryStatus;
import com.abhishek.hookrelay.common.domain.Endpoint;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.net.DestinationPolicy;
import com.abhishek.hookrelay.common.net.SsrfGuard;
import com.abhishek.hookrelay.common.repo.DeliveryRepository;
import com.abhishek.hookrelay.common.repo.EndpointRepository;
import com.abhishek.hookrelay.common.repo.EventRepository;
import com.abhishek.hookrelay.common.retry.RetryPolicy;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.common.webhook.WebhookEnvelope;
import com.abhishek.hookrelay.worker.isolation.CircuitBreakerRegistry;
import com.abhishek.hookrelay.worker.isolation.EndpointSemaphores;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

/**
 * Processes one delivery: gate, claim, sign, send, record, route.
 *
 * <p>The order of the gates is load-bearing, and it is the reason this is not simply a sequence of
 * {@code if} statements in the order they were invented:
 *
 * <pre>
 *   already SUCCEEDED?              → acknowledge, no work            (phase 3)
 *   endpoint permit available?      → NO: defer, and DO NOT claim     (phase 5)
 *   claim the attempt atomically                                      (phase 3)
 *   circuit breaker permits?        → NO: record CIRCUIT_OPEN         (phase 5)
 *   destination address allowed?    → NO: record SSRF_BLOCKED, dead   (phase 5)
 *   sign, POST, record, route                                    (phases 3, 4)
 * </pre>
 *
 * <p>The permit check sits <em>before</em> the claim because a deferral is not an attempt: nothing
 * was sent and nothing failed, so consuming one of the eight attempts would let a busy period
 * exhaust the retry ladder for deliveries that were never tried. The breaker check sits
 * <em>after</em> it, because refusing to call a known-dead endpoint <em>is</em> a failed attempt and
 * should be recorded as one.
 */
@Component
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);

    public static final String ERROR_CIRCUIT_OPEN = "CIRCUIT_OPEN";
    public static final String ERROR_CAPACITY = "CAPACITY";
    public static final String ERROR_SSRF_BLOCKED = "SSRF_BLOCKED";

    private final DeliveryRepository deliveries;
    private final EndpointRepository endpoints;
    private final EventRepository events;
    private final DeliveryStore store;
    private final WebhookSender sender;
    private final RetryPublisher retries;
    private final EndpointSemaphores permits;
    private final CircuitBreakerRegistry breakers;
    private final DestinationPolicy destinations;
    private final ObjectMapper objectMapper;
    private final int maxDeferrals;

    public DeliveryProcessor(DeliveryRepository deliveries,
                             EndpointRepository endpoints,
                             EventRepository events,
                             DeliveryStore store,
                             WebhookSender sender,
                             RetryPublisher retries,
                             EndpointSemaphores permits,
                             CircuitBreakerRegistry breakers,
                             DestinationPolicy destinations,
                             ObjectMapper objectMapper,
                             @Value("${hookrelay.isolation.max-deferrals}") int maxDeferrals) {
        this.deliveries = deliveries;
        this.endpoints = endpoints;
        this.events = events;
        this.store = store;
        this.sender = sender;
        this.retries = retries;
        this.permits = permits;
        this.breakers = breakers;
        this.destinations = destinations;
        this.objectMapper = objectMapper;
        this.maxDeferrals = maxDeferrals;
    }

    public Optional<AttemptOutcome> process(UUID deliveryId) {
        return process(deliveryId, 0);
    }

    /**
     * @param deferrals how many times this delivery has already been put back for lack of a permit
     * @return the outcome, or empty when nothing was attempted — already succeeded, rows missing, or
     *         deferred. In every one of those cases the caller acknowledges.
     */
    public Optional<AttemptOutcome> process(UUID deliveryId, int deferrals) {
        Delivery delivery = deliveries.findById(deliveryId).orElse(null);
        if (delivery == null) {
            log.warn("delivery {} does not exist", deliveryId);
            return Optional.empty();
        }
        if (delivery.getStatus() == DeliveryStatus.SUCCEEDED) {
            log.debug("delivery {} already succeeded, skipping", deliveryId);
            return Optional.empty();
        }

        Endpoint endpoint = endpoints.findById(delivery.getEndpointId()).orElse(null);
        Event event = events.findById(delivery.getEventId()).orElse(null);
        if (endpoint == null || event == null) {
            store.markDeadWithoutAttempt(deliveryId,
                    endpoint == null ? "endpoint no longer exists" : "event no longer exists");
            return Optional.empty();
        }

        // Non-blocking. A blocking acquire would respect the limit and still hand the thread to the
        // slow endpoint, which is the failure this whole phase exists to remove.
        if (!permits.tryAcquire(endpoint.getId(), endpoint.getMaxConcurrency())) {
            if (deferrals < maxDeferrals) {
                retries.defer(deliveryId, deferrals);
                return Optional.empty();
            }
            // Bounded: a permanently saturated endpoint would otherwise cycle forever. Hand it to
            // the retry ladder, which is already bounded and ends at the DLQ.
            log.warn("delivery {} deferred {} times, recording a capacity failure",
                    deliveryId, deferrals);
            return claimAndRoute(deliveryId, endpoint.getId(),
                    synthetic(AttemptOutcome.Classification.RETRYABLE, ERROR_CAPACITY));
        }

        try {
            if (!breakers.tryPermit(endpoint.getId())) {
                return claimAndRoute(deliveryId, endpoint.getId(),
                        synthetic(AttemptOutcome.Classification.RETRYABLE, ERROR_CIRCUIT_OPEN));
            }

            // Re-validated here, on a fresh lookup, because a hostname that was public at
            // registration can point somewhere internal by now.
            SsrfGuard.Result destination = destinations.check(endpoint.getUrl(), false);
            if (!destination.allowed()) {
                log.warn("delivery {} blocked: {}", deliveryId, destination.reason());
                breakers.recordFailure(endpoint.getId());
                return claimAndRoute(deliveryId, endpoint.getId(),
                        synthetic(AttemptOutcome.Classification.PERMANENT, ERROR_SSRF_BLOCKED));
            }

            Optional<Integer> claimed = store.claimAttempt(deliveryId);
            if (claimed.isEmpty()) {
                breakers.recordSuccess(endpoint.getId());
                return Optional.empty();
            }
            int attemptNo = claimed.get();

            byte[] body;
            try {
                body = serializeEnvelope(delivery, event);
            } catch (JsonProcessingException e) {
                log.error("could not serialize envelope for delivery {}", deliveryId, e);
                store.markDeadWithoutAttempt(deliveryId, "envelope serialization failed");
                breakers.recordSuccess(endpoint.getId());
                return Optional.empty();
            }

            OffsetDateTime startedAt = OffsetDateTime.now();
            AttemptOutcome outcome = sender.send(
                    endpoint.getUrl(), endpoint.getSecret(), body,
                    delivery.getId(), event.getId(), event.getEventType(), attemptNo);

            if (outcome.succeeded()) {
                breakers.recordSuccess(endpoint.getId());
            } else {
                breakers.recordFailure(endpoint.getId());
            }

            route(deliveryId, endpoint.getId(), attemptNo, startedAt, outcome);
            log.debug("delivery {} attempt {} -> {} ({}ms)",
                    deliveryId, attemptNo, outcome.classification(), outcome.durationMs());
            return Optional.of(outcome);

        } finally {
            permits.release(endpoint.getId());
        }
    }

    /** Records a failure that happened without an HTTP request, and routes it. */
    private Optional<AttemptOutcome> claimAndRoute(UUID deliveryId, UUID endpointId,
                                                   AttemptOutcome outcome) {
        Optional<Integer> claimed = store.claimAttempt(deliveryId);
        if (claimed.isEmpty()) {
            return Optional.empty();
        }
        route(deliveryId, endpointId, claimed.get(), OffsetDateTime.now(), outcome);
        return Optional.of(outcome);
    }

    private static AttemptOutcome synthetic(AttemptOutcome.Classification classification,
                                            String errorClass) {
        return new AttemptOutcome(classification, null, errorClass, null, 0);
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

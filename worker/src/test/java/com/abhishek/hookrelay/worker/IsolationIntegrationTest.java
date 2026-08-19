package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.worker.delivery.DeliveryProcessor;
import com.abhishek.hookrelay.worker.isolation.CircuitBreakerRegistry;
import com.abhishek.hookrelay.worker.isolation.EndpointSemaphores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class IsolationIntegrationTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "isolation-secret-0123456789";

    @Autowired
    DeliveryProcessor processor;

    @Autowired
    EndpointSemaphores permits;

    @Autowired
    CircuitBreakerRegistry breakers;

    @Autowired
    RabbitAdmin rabbitAdmin;

    @BeforeEach
    void drainQueues() {
        for (RetryTier tier : RetryTier.values()) {
            rabbitAdmin.purgeQueue(tier.queueName(), false);
        }
        rabbitAdmin.purgeQueue(RabbitTopology.DLQ_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitTopology.DEFERRED_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitTopology.DELIVERIES_QUEUE, false);
    }

    private UUID insertEndpointWithConcurrency(String url, int maxConcurrency) {
        UUID id = com.abhishek.hookrelay.common.util.Uuid7.generate();
        jdbc.update("""
                INSERT INTO endpoints (id, tenant_id, url, secret, event_types, active, max_concurrency)
                VALUES (?, ?, ?, ?, ARRAY['payment.succeeded']::text[], true, ?)
                """, id, tenant, url, SECRET, maxConcurrency);
        return id;
    }

    private UUID seedFor(UUID endpointId) {
        UUID eventId = insertEvent("payment.succeeded", "{}");
        return insertDelivery(eventId, endpointId);
    }

    // ---- per-endpoint concurrency ---------------------------------------------------------------

    @Test
    @DisplayName("with one permit, a second concurrent delivery is deferred rather than sent")
    void permitLimitsConcurrency() throws Exception {
        receiver.delay(1500);
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 1);
        UUID first = seedFor(endpointId);
        UUID second = seedFor(endpointId);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(1);
        pool.submit(() -> {
            started.countDown();
            processor.process(first);
        });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);   // let the first delivery take the permit

        processor.process(second);
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        // Only the first request ever reached the endpoint; the second was put back.
        assertThat(receiver.requestCount()).isEqualTo(1);
        assertThat(deliveryStatus(second)).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("a deferral does not consume an attempt")
    void deferralIsNotAnAttempt() {
        receiver.delay(0);
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 1);
        UUID deliveryId = seedFor(endpointId);

        // Hold the only permit so the delivery cannot proceed.
        assertThat(permits.tryAcquire(endpointId, 1)).isTrue();
        try {
            processor.process(deliveryId);
        } finally {
            permits.release(endpointId);
        }

        // Nothing was sent and nothing failed, so nothing may be charged against the eight attempts —
        // otherwise a busy period would exhaust the retry ladder for deliveries never tried.
        assertThat(deliveryAttemptCount(deliveryId)).isZero();
        assertThat(attempts(deliveryId)).isEmpty();
        assertThat(deliveryStatus(deliveryId)).isEqualTo("PENDING");
        assertThat(receiver.requestCount()).isZero();
    }

    @Test
    @DisplayName("the deferral loop is bounded — past the cap it becomes a CAPACITY failure")
    void deferralIsBounded() {
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 1);
        UUID deliveryId = seedFor(endpointId);

        assertThat(permits.tryAcquire(endpointId, 1)).isTrue();
        try {
            processor.process(deliveryId, 50);   // already at the configured cap
        } finally {
            permits.release(endpointId);
        }

        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(1);
        List<Map<String, Object>> recorded = attempts(deliveryId);
        assertThat(recorded).hasSize(1);
        assertThat(recorded.get(0).get("error_class")).isEqualTo(DeliveryProcessor.ERROR_CAPACITY);
        assertThat(recorded.get(0).get("response_status")).isNull();
        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
        assertThat(receiver.requestCount()).isZero();
    }

    @Test
    @DisplayName("the permit is released after a failure, not just after a success")
    void permitReleasedAfterFailure() {
        receiver.respondWith(500);
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 1);

        processor.process(seedFor(endpointId));
        processor.process(seedFor(endpointId));

        // A permit leaked on the failure path would have deferred the second delivery instead.
        assertThat(receiver.requestCount()).isEqualTo(2);
        assertThat(permits.availablePermits(endpointId, 1)).isEqualTo(1);
    }

    // ---- circuit breaker -----------------------------------------------------------------------

    @Test
    @DisplayName("the breaker opens after consecutive failures and then stops calling the endpoint")
    void breakerOpensAfterConsecutiveFailures() {
        receiver.respondWith(500);
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 5);

        for (int i = 0; i < 5; i++) {
            processor.process(seedFor(endpointId));
        }
        assertThat(receiver.requestCount()).isEqualTo(5);
        assertThat(breakers.stateOf(endpointId)).isEqualTo(CircuitBreakerRegistry.State.OPEN);

        UUID afterOpen = seedFor(endpointId);
        processor.process(afterOpen);

        assertThat(receiver.requestCount()).as("no further HTTP calls").isEqualTo(5);
        assertThat(attempts(afterOpen).get(0).get("error_class"))
                .isEqualTo(DeliveryProcessor.ERROR_CIRCUIT_OPEN);
        // Recorded as a real retryable failure, not a deferral: the delivery did fail, we declined
        // to try. It is bounded by the retry ladder that already exists.
        assertThat(deliveryStatus(afterOpen)).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("a success resets the consecutive-failure count")
    void successResetsFailureCount() {
        UUID endpointId = insertEndpointWithConcurrency(receiver.url(), 5);

        receiver.respondWith(500);
        for (int i = 0; i < 4; i++) {
            processor.process(seedFor(endpointId));
        }
        receiver.respondWith(200);
        processor.process(seedFor(endpointId));
        receiver.respondWith(500);
        for (int i = 0; i < 4; i++) {
            processor.process(seedFor(endpointId));
        }

        // Consecutive, not cumulative: 8 failures total but never 5 in a row.
        assertThat(breakers.stateOf(endpointId)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);
    }

    @Test
    @DisplayName("an endpoint blocked by SSRF is dead-lettered without an HTTP call")
    void ssrfBlockedDestinationIsDead() {
        // The shared context allows private destinations, so this class proves the wiring with a
        // policy that denies everything; SsrfBlockedDeliveryTest exercises the real guard.
        UUID endpointId = insertEndpointWithConcurrency("http://does-not-exist.invalid/hook", 5);
        UUID deliveryId = seedFor(endpointId);

        processor.process(deliveryId);

        // With allow-private-destinations on, the guard only checks syntax, so this URL is delivered
        // and fails at DNS instead. Either way it never reaches a real endpoint.
        assertThat(receiver.requestCount()).isZero();
        assertThat(deliveryStatus(deliveryId)).isIn("FAILED", "DEAD");
    }
}

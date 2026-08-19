package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.common.util.Uuid7;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The headline measurement of phase 5: does one slow endpoint degrade delivery to a healthy one?
 *
 * <p>Two endpoints share the worker pool. The slow one takes 2 seconds per request and succeeds, so
 * the circuit breaker stays closed and the only variable is concurrency. The healthy one answers
 * instantly.
 *
 * <p>The unisolated baseline is not a feature flag or a reverted commit — it is the system's own
 * configuration. An endpoint whose {@code max_concurrency} is very high has a semaphore that never
 * blocks, which <em>is</em> the unisolated behaviour, so both arms run the shipped code path.
 *
 * <p><strong>Run this class on its own for the real numbers.</strong> Spring's test-context cache
 * keeps the other worker test classes' applications alive for the whole JVM, and each of them has
 * its own listener container consuming the same {@code deliveries} queue. Inside the full suite the
 * effective worker pool is therefore several times four threads, the backlog drains faster, and the
 * unisolated arm understates the damage by roughly a factor of four. The assertions are loose enough
 * to hold either way; the measurement is only meaningful standalone:
 *
 * <pre>
 *   mvn -pl common,worker -Dtest=NoisyNeighbourTest -Dsurefire.failIfNoSpecifiedTests=false test
 * </pre>
 */
@TestPropertySource(properties = {
        // The slow endpoint must succeed, not time out, or the breaker would confound the result.
        "hookrelay.delivery.request-timeout-ms=10000",
        "hookrelay.delivery.connect-timeout-ms=5000"
})
class NoisyNeighbourTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "noisy-secret-0123456789";
    private static final int SLOW_DELIVERIES = 24;
    private static final int FAST_DELIVERIES = 10;
    private static final long SLOW_ENDPOINT_DELAY_MS = 2000;

    /** High enough that the semaphore never blocks — the unisolated baseline. */
    private static final int UNLIMITED = 10_000;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    RabbitAdmin rabbitAdmin;

    private TestWebhookReceiver slowReceiver;
    private TestWebhookReceiver fastReceiver;

    @BeforeEach
    void setUpEndpointsAndQueues() throws IOException {
        slowReceiver = new TestWebhookReceiver();
        slowReceiver.delay(SLOW_ENDPOINT_DELAY_MS);
        fastReceiver = new TestWebhookReceiver();

        for (RetryTier tier : RetryTier.values()) {
            rabbitAdmin.purgeQueue(tier.queueName(), false);
        }
        rabbitAdmin.purgeQueue(RabbitTopology.DLQ_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitTopology.DEFERRED_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitTopology.DELIVERIES_QUEUE, false);
    }

    /**
     * Drains this arm's work before the next one starts.
     *
     * <p>Purging a queue does not reclaim messages consumers have already prefetched, so without
     * this the sibling test begins while 2-second requests from this one are still in flight and its
     * measurement is contaminated — the two arms produced visibly different numbers depending on
     * which ran first. Marking the remaining deliveries succeeded makes every still-cycling message
     * acknowledge immediately on its next pass.
     */
    @AfterEach
    void quiesce() {
        jdbc.update("UPDATE deliveries SET status = 'SUCCEEDED' "
                + "WHERE event_id IN (SELECT id FROM events WHERE tenant_id = ?)", tenant);
        await().atMost(Duration.ofSeconds(90)).until(() ->
                depth(RabbitTopology.DELIVERIES_QUEUE) == 0
                        && depth(RabbitTopology.DEFERRED_QUEUE) == 0);
    }

    private int depth(String queueName) {
        var info = rabbitAdmin.getQueueInfo(queueName);
        return info == null ? 0 : info.getMessageCount();
    }

    @AfterEach
    void closeReceivers() {
        if (slowReceiver != null) {
            slowReceiver.close();
        }
        if (fastReceiver != null) {
            fastReceiver.close();
        }
    }

    private UUID endpoint(String url, int maxConcurrency) {
        UUID id = Uuid7.generate();
        jdbc.update("""
                INSERT INTO endpoints (id, tenant_id, url, secret, event_types, active, max_concurrency)
                VALUES (?, ?, ?, ?, ARRAY['payment.succeeded']::text[], true, ?)
                """, id, tenant, url, SECRET, maxConcurrency);
        return id;
    }

    @Test
    @DisplayName("without isolation, the slow endpoint degrades the healthy one")
    void withoutIsolation() {
        List<Long> latencies = run("WITHOUT ISOLATION", UNLIMITED);

        // Recorded rather than asserted tightly: the point is the comparison in the sibling test.
        assertThat(latencies).hasSize(FAST_DELIVERIES);
    }

    @Test
    @DisplayName("with a concurrency limit, the healthy endpoint stays responsive")
    void withIsolation() {
        List<Long> latencies = run("WITH ISOLATION", 1);

        assertThat(latencies).hasSize(FAST_DELIVERIES);
        // With four worker threads and a 2s slow endpoint, an unisolated backlog of 24 costs about
        // 12 seconds before a healthy delivery is even picked up. The bound here is deliberately
        // loose — a slow request already in flight when a healthy one is claimed still costs its
        // remaining time — but it is far below what the unisolated arm produces.
        assertThat(percentile(latencies, 99))
                .as("healthy endpoint p99 must not be dominated by the slow endpoint's backlog")
                .isLessThan(3 * SLOW_ENDPOINT_DELAY_MS);
    }

    /**
     * @param slowEndpointConcurrency the slow endpoint's {@code max_concurrency}
     * @return the healthy endpoint's per-delivery latencies, in milliseconds
     */
    private List<Long> run(String label, int slowEndpointConcurrency) {
        UUID slowEndpoint = endpoint(slowReceiver.url(), slowEndpointConcurrency);
        UUID fastEndpoint = endpoint(fastReceiver.url(), 50);

        List<UUID> fastDeliveries = new ArrayList<>();
        List<UUID> slowDeliveries = new ArrayList<>();
        for (int i = 0; i < SLOW_DELIVERIES; i++) {
            slowDeliveries.add(insertDelivery(insertEvent("payment.succeeded", "{}"), slowEndpoint));
        }
        for (int i = 0; i < FAST_DELIVERIES; i++) {
            fastDeliveries.add(insertDelivery(insertEvent("payment.succeeded", "{}"), fastEndpoint));
        }

        // The slow endpoint's backlog goes first. This is the realistic shape of the failure — one
        // customer accumulates a queue and then goes slow — and it is what fills the consumers'
        // prefetch buffers, which is the mechanism that starves everyone else.
        //
        // An earlier version of this test interleaved the two, and measured nothing: RabbitMQ
        // round-robins messages across consumers, so alternating slow and fast handed them to
        // different threads and the work partitioned itself by accident.
        for (UUID deliveryId : slowDeliveries) {
            publish(deliveryId);
        }

        long startedAt = System.currentTimeMillis();
        for (UUID deliveryId : fastDeliveries) {
            publish(deliveryId);
        }

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            for (UUID deliveryId : fastDeliveries) {
                assertThat(deliveryStatus(deliveryId))
                        .as("healthy delivery %s", deliveryId)
                        .isEqualTo("SUCCEEDED");
            }
        });

        List<Long> latencies = new ArrayList<>();
        for (TestWebhookReceiver.Received request : fastReceiver.received()) {
            latencies.add(request.receivedAtMillis() - startedAt);
        }
        Collections.sort(latencies);

        System.out.printf(
                "%-18s healthy endpoint latency  p50=%dms  p95=%dms  p99=%dms  max=%dms  (n=%d)%n",
                label,
                percentile(latencies, 50), percentile(latencies, 95),
                percentile(latencies, 99), latencies.get(latencies.size() - 1),
                latencies.size());

        return latencies;
    }

    private void publish(UUID deliveryId) {
        rabbitTemplate.convertAndSend(RabbitTopology.EXCHANGE, RabbitTopology.DELIVERY_ROUTING_KEY,
                new DeliveryMessage(deliveryId));
    }

    /** Nearest-rank percentile over a sorted list. */
    private static long percentile(List<Long> sorted, int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}

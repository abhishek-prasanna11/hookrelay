package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.common.util.Uuid7;
import com.abhishek.hookrelay.worker.delivery.DeliveryProcessor;
import com.abhishek.hookrelay.worker.metrics.QueueDepthMetrics;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.Search;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsIntegrationTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "metrics-secret-0123456789";

    /** Anything that looks like a UUID must never appear as a metric label value. */
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    @Autowired
    DeliveryProcessor processor;

    @Autowired
    MeterRegistry registry;

    @Autowired
    QueueDepthMetrics queueDepthMetrics;

    @Autowired
    com.abhishek.hookrelay.worker.isolation.EndpointSemaphores permits;

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

    private double counter(String name, String tagKey, String tagValue) {
        Search search = registry.find(name);
        if (tagKey != null) {
            search = search.tag(tagKey, tagValue);
        }
        var found = search.counter();
        return found == null ? 0 : found.count();
    }

    private long timerCount(String name) {
        var timer = registry.find(name).timer();
        return timer == null ? 0 : timer.count();
    }

    // ---- delivery metrics -----------------------------------------------------------------------

    @Test
    @DisplayName("a successful delivery increments the success counter and both timers")
    void successIsCounted() {
        double before = counter("hookrelay.deliveries.total", "result", "success");
        long durationsBefore = timerCount("hookrelay.delivery.duration");
        long endToEndBefore = timerCount("hookrelay.end.to.end.latency");

        processor.process(seedDelivery(SECRET));

        assertThat(counter("hookrelay.deliveries.total", "result", "success")).isEqualTo(before + 1);
        assertThat(timerCount("hookrelay.delivery.duration")).isEqualTo(durationsBefore + 1);
        assertThat(timerCount("hookrelay.end.to.end.latency")).isEqualTo(endToEndBefore + 1);
    }

    @Test
    @DisplayName("a failure is counted by result and by error class")
    void failureIsCountedByErrorClass() {
        receiver.respondWith(500);
        double resultBefore = counter("hookrelay.deliveries.total", "result", "retryable");
        double classBefore = counter("hookrelay.attempt.failures.total", "error_class", "HTTP_5XX");

        processor.process(seedDelivery(SECRET));

        assertThat(counter("hookrelay.deliveries.total", "result", "retryable")).isEqualTo(resultBefore + 1);
        assertThat(counter("hookrelay.attempt.failures.total", "error_class", "HTTP_5XX")).isEqualTo(classBefore + 1);
    }

    @Test
    @DisplayName("attempts are counted by attempt number")
    void attemptsCountedByNumber() {
        receiver.respondWith(500);
        double before = counter("hookrelay.attempts.total", "attempt_no", "1");

        processor.process(seedDelivery(SECRET));

        assertThat(counter("hookrelay.attempts.total", "attempt_no", "1")).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("an outcome with no HTTP request does not pollute the duration histogram")
    void syntheticFailureIsNotTimed() {
        UUID endpointId = insertEndpoint(receiver.url(), SECRET);
        jdbc.update("UPDATE endpoints SET max_concurrency = 1 WHERE id = ?", endpointId);
        UUID deliveryId = insertDelivery(insertEvent("payment.succeeded", "{}"), endpointId);
        long durationsBefore = timerCount("hookrelay.delivery.duration");

        // Hold the only permit, so the delivery cannot proceed. Past the deferral cap this becomes a
        // CAPACITY failure, recorded without ever contacting the endpoint.
        assertThat(permits.tryAcquire(endpointId, 1)).isTrue();
        try {
            processor.process(deliveryId, 999);
        } finally {
            permits.release(endpointId);
        }

        assertThat(counter("hookrelay.attempt.failures.total", "error_class",
                DeliveryProcessor.ERROR_CAPACITY)).isPositive();
        // Recording a zero here would drag the latency distribution toward zero exactly when the
        // system is unhealthy.
        assertThat(timerCount("hookrelay.delivery.duration")).isEqualTo(durationsBefore);
    }

    @Test
    @DisplayName("dead-lettering is counted by reason")
    void dlqCountedByReason() {
        receiver.respondWith(400);
        double before = counter("hookrelay.dlq.total", "reason", "permanent_failure");

        processor.process(seedDelivery(SECRET));

        assertThat(counter("hookrelay.dlq.total", "reason", "permanent_failure")).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("end-to-end latency exceeds attempt duration once a delivery has been retried")
    void endToEndExceedsAttemptDuration() throws Exception {
        UUID endpointId = insertEndpoint(receiver.url(), SECRET);
        UUID eventId = insertEvent("payment.succeeded", "{}");
        UUID deliveryId = insertDelivery(eventId, endpointId);

        // Backdate the event: the delivery is being attempted well after it was accepted, which is
        // what a retried delivery looks like.
        jdbc.update("UPDATE events SET created_at = now() - interval '30 seconds' WHERE id = ?", eventId);

        processor.process(deliveryId);

        var endToEnd = registry.find("hookrelay.end.to.end.latency").timer();
        assertThat(endToEnd).isNotNull();
        assertThat(endToEnd.max(java.util.concurrent.TimeUnit.SECONDS))
                .as("end-to-end must reflect the wait since acceptance, not the 10ms request")
                .isGreaterThan(20);
    }

    // ---- saturation gauges ----------------------------------------------------------------------

    @Test
    @DisplayName("queue depth is sampled per queue")
    void queueDepthIsSampled() {
        receiver.respondWith(500);
        processor.process(seedDelivery(SECRET));

        queueDepthMetrics.sample();

        assertThat(queueDepthMetrics.depthOf(RetryTier.T5S.queueName())).isEqualTo(1);
        assertThat(registry.find("hookrelay.queue.depth")
                .tag("queue", RetryTier.T5S.queueName()).gauge().value()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("circuit breakers are reported as a count per state, not one series per endpoint")
    void breakersReportedAsCounts() {
        receiver.respondWith(500);
        UUID endpointId = insertEndpoint(receiver.url(), SECRET);
        for (int i = 0; i < 5; i++) {
            processor.process(insertDelivery(insertEvent("payment.succeeded", "{}"), endpointId));
        }

        queueDepthMetrics.sample();

        assertThat(registry.find("hookrelay.circuit.breakers").tag("state", "open").gauge().value())
                .isGreaterThanOrEqualTo(1.0);
        // Three series total, whatever the endpoint count — the whole point of counting rather than
        // labelling by endpoint.
        assertThat(registry.find("hookrelay.circuit.breakers").gauges()).hasSize(3);
    }

    // ---- the cardinality rule --------------------------------------------------------------------

    @Test
    @DisplayName("no metric carries a UUID-valued label")
    void noHighCardinalityLabels() {
        receiver.respondWith(500);
        processor.process(seedDelivery(SECRET));
        receiver.respondWith(200);
        processor.process(seedDelivery(SECRET));
        queueDepthMetrics.sample();

        List<String> offenders = registry.getMeters().stream()
                .filter(meter -> meter.getId().getName().startsWith("hookrelay"))
                .flatMap(meter -> meter.getId().getTags().stream()
                        .filter(tag -> UUID_PATTERN.matcher(tag.getValue()).find())
                        .map(tag -> meter.getId().getName() + "{" + tag.getKey() + "=" + tag.getValue() + "}"))
                .toList();

        // endpoint_id, delivery_id and event_id are exactly the labels you want during an incident,
        // and exactly the ones that multiply every series by the number of customers. They live in
        // the structured logs instead.
        assertThat(offenders)
                .as("metrics answer 'how much and how bad'; logs answer 'which one'")
                .isEmpty();
    }

    @Test
    @DisplayName("every hookrelay label has a small, bounded set of values")
    void labelsAreBounded() {
        for (Meter meter : registry.getMeters()) {
            if (!meter.getId().getName().startsWith("hookrelay")) {
                continue;
            }
            for (Tag tag : meter.getId().getTags()) {
                assertThat(tag.getKey())
                        .as("unexpected label %s on %s", tag.getKey(), meter.getId().getName())
                        // "le" is a histogram bucket boundary, bounded by the SLOs declared in
                        // DeliveryMetrics — not a user-controlled value space.
                        .isIn("result", "attempt_no", "error_class", "reason", "state", "queue", "le");
            }
        }
    }

    @Test
    @DisplayName("the queue-depth gauge covers every queue, and only those")
    void queueDepthCoversAllQueues() {
        // 3 fixed queues plus one per retry tier — bounded, and every value is one we would look at.
        assertThat(registry.find("hookrelay.queue.depth").gauges())
                .hasSize(3 + RetryTier.values().length);
    }
}

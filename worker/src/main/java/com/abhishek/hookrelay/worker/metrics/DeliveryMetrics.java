package com.abhishek.hookrelay.worker.metrics;

import com.abhishek.hookrelay.common.retry.RetryPolicy;
import com.abhishek.hookrelay.worker.delivery.AttemptOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delivery instrumentation.
 *
 * <p><strong>No label here has an unbounded value space.</strong> {@code result} has three values,
 * {@code attempt_no} eight, {@code error_class} about ten, {@code reason} two. Adding
 * {@code endpoint_id} — the label you actually want during an incident — would multiply every series
 * by the number of registered endpoints and grow the metrics system every time a customer signs up.
 * Those identifiers go to the structured logs instead, where cardinality costs nothing:
 * metrics answer <em>how much and how bad</em>, logs answer <em>which one</em>.
 */
@Component
public class DeliveryMetrics {

    private final MeterRegistry registry;

    private final Timer deliveryDuration;
    private final Timer endToEndLatency;
    private final Counter deferrals;
    private final AtomicInteger inFlight = new AtomicInteger();

    public DeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Buckets straddle the range that matters: a fast endpoint answers in single-digit
        // milliseconds, and the request timeout is 15s. Boundaries are declared up front because a
        // percentile falling between two buckets is interpolated — the boundaries are the
        // resolution.
        this.deliveryDuration = Timer.builder("hookrelay.delivery.duration")
                .description("Duration of a single HTTP delivery attempt")
                .serviceLevelObjectives(
                        Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10),
                        Duration.ofSeconds(15))
                .register(registry);

        // A different range entirely. A delivery that succeeds on attempt 8 has waited hours, so
        // reusing the attempt-duration buckets would put every retried delivery in +Inf and make the
        // metric useless exactly when it matters.
        this.endToEndLatency = Timer.builder("hookrelay.end.to.end.latency")
                .description("Accepted (202) until successfully delivered")
                .serviceLevelObjectives(
                        Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(2),
                        Duration.ofMinutes(10), Duration.ofMinutes(30), Duration.ofHours(1),
                        Duration.ofHours(5))
                .register(registry);

        this.deferrals = Counter.builder("hookrelay.deferrals.total")
                .description("Deliveries put back because the endpoint had no free permit")
                .register(registry);

        Gauge.builder("hookrelay.worker.inflight", inFlight, AtomicInteger::get)
                .description("HTTP deliveries currently in flight in this worker")
                .register(registry);
    }

    public void recordAttempt(AttemptOutcome outcome, int attemptNo) {
        Counter.builder("hookrelay.deliveries.total")
                .tag("result", outcome.classification().name().toLowerCase())
                .description("Delivery attempts by outcome")
                .register(registry)
                .increment();

        // attempt_no is bounded by the retry ladder, so this is at most MAX_ATTEMPTS series. Without
        // that bound it would be exactly the kind of label this class avoids.
        Counter.builder("hookrelay.attempts.total")
                .tag("attempt_no", Integer.toString(Math.min(attemptNo, RetryPolicy.MAX_ATTEMPTS)))
                .description("Attempts by attempt number")
                .register(registry)
                .increment();

        if (outcome.errorClass() != null) {
            Counter.builder("hookrelay.attempt.failures.total")
                    .tag("error_class", outcome.errorClass())
                    .description("Failed attempts by error class")
                    .register(registry)
                    .increment();
        }

        // Only real HTTP attempts have a duration. A CIRCUIT_OPEN or CAPACITY outcome never made a
        // request, and recording its zero would drag the latency distribution toward zero precisely
        // when the system is unhealthy.
        if (outcome.durationMs() > 0) {
            deliveryDuration.record(Duration.ofMillis(outcome.durationMs()));
        }
    }

    /**
     * The real SLO: how long the customer waited from the moment we accepted the event.
     *
     * <p>Kept separate from attempt duration because a delivery that fails seven times over four
     * hours and then succeeds in 40 ms has an excellent attempt duration and a terrible end-to-end
     * latency. A dashboard showing only the former would have looked perfect throughout.
     */
    public void recordEndToEnd(Duration latency) {
        endToEndLatency.record(latency);
    }

    public void recordDeadLettered(String reason) {
        Counter.builder("hookrelay.dlq.total")
                .tag("reason", reason)
                .description("Deliveries given up on, by reason")
                .register(registry)
                .increment();
    }

    public void recordDeferral() {
        deferrals.increment();
    }

    public void deliveryStarted() {
        inFlight.incrementAndGet();
    }

    public void deliveryFinished() {
        inFlight.decrementAndGet();
    }

    public int inFlight() {
        return inFlight.get();
    }
}

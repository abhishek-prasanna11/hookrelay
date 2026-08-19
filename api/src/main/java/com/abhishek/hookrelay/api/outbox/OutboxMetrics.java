package com.abhishek.hookrelay.api.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The one metric this phase needs.
 *
 * <p>{@code hookrelay_outbox_lag_seconds} — the age of the oldest unpublished row — is the number
 * that reveals a wedged publisher. Counters of events accepted and messages published would both
 * look perfectly healthy while the backlog silently grows behind them.
 *
 * <p>The gauge reads a field rather than querying the database on scrape. A gauge whose value
 * function hits the database runs on Prometheus's schedule, not ours, and multiplies by the number
 * of scrapers; the publisher already knows the lag from the batch it just claimed, so it costs
 * nothing to record it there.
 */
@Component
public class OutboxMetrics {

    private final AtomicReference<Double> lagSeconds = new AtomicReference<>(0.0);
    private final Counter publishedTotal;

    public OutboxMetrics(MeterRegistry registry) {
        Gauge.builder("hookrelay.outbox.lag.seconds", lagSeconds, AtomicReference::get)
                .description("Age of the oldest unpublished outbox row")
                .baseUnit("seconds")
                .register(registry);

        this.publishedTotal = Counter.builder("hookrelay.outbox.published.total")
                .description("Outbox rows confirmed by the broker")
                .register(registry);
    }

    public void recordLag(double seconds) {
        lagSeconds.set(seconds);
    }

    public void recordPublished(int count) {
        if (count > 0) {
            publishedTotal.increment(count);
        }
    }

    public double currentLagSeconds() {
        return lagSeconds.get();
    }
}

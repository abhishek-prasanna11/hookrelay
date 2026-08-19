package com.abhishek.hookrelay.api.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Ingest instrumentation: the rate, errors and duration of accepting events.
 *
 * <p>{@code result} distinguishes the three outcomes the API already distinguishes by status code,
 * so a spike in duplicates or conflicts is visible without parsing access logs. Tenant id is
 * deliberately absent — it is exactly the unbounded label that ruins a metrics system.
 */
@Component
public class IngestMetrics {

    public static final String RESULT_ACCEPTED = "accepted";
    public static final String RESULT_DUPLICATE = "duplicate";
    public static final String RESULT_CONFLICT = "conflict";
    public static final String RESULT_REJECTED = "rejected";

    private final MeterRegistry registry;
    private final Timer ingestLatency;
    private final Counter fanoutDeliveries;

    public IngestMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.ingestLatency = Timer.builder("hookrelay.ingest.latency")
                .description("Time to validate, persist and fan out an event")
                .serviceLevelObjectives(
                        Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10),
                        Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1))
                .register(registry);

        this.fanoutDeliveries = Counter.builder("hookrelay.fanout.deliveries.total")
                .description("Delivery rows created by fan-out")
                .register(registry);
    }

    public void recordIngest(String result, Duration elapsed, int deliveriesCreated) {
        Counter.builder("hookrelay.events.total")
                .tag("result", result)
                .description("Events by ingest outcome")
                .register(registry)
                .increment();

        ingestLatency.record(elapsed);

        if (deliveriesCreated > 0) {
            fanoutDeliveries.increment(deliveriesCreated);
        }
    }
}

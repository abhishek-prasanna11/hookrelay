package com.abhishek.hookrelay.worker.metrics;

import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.worker.isolation.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Saturation signals: how much work is waiting, and how many endpoints are being shed.
 *
 * <p>Depths are sampled on a timer into fields the gauges read, rather than queried from inside the
 * gauge. A gauge whose value function calls the broker runs on Prometheus's schedule and multiplies
 * by the number of scrapers — the same reasoning as the outbox lag gauge in phase 2.
 *
 * <p>{@code hookrelay_circuit_breakers} is published as a <em>count of endpoints per state</em>
 * rather than one series per endpoint. BLUEPRINT.md §23 lists a circuit-breaker metric and also
 * forbids endpoint-id labels; three series regardless of endpoint count satisfies both, and "how
 * many endpoints are we currently shedding?" is the question an operator actually asks.
 */
@Component
public class QueueDepthMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueDepthMetrics.class);

    private final RabbitAdmin rabbitAdmin;
    private final CircuitBreakerRegistry breakers;
    private final Map<String, AtomicInteger> depths = new ConcurrentHashMap<>();
    private final Map<CircuitBreakerRegistry.State, AtomicInteger> breakerCounts =
            new ConcurrentHashMap<>();
    private final List<String> monitoredQueues = new ArrayList<>();

    public QueueDepthMetrics(MeterRegistry registry,
                             RabbitAdmin rabbitAdmin,
                             CircuitBreakerRegistry breakers) {
        this.rabbitAdmin = rabbitAdmin;
        this.breakers = breakers;

        monitoredQueues.add(RabbitTopology.DELIVERIES_QUEUE);
        monitoredQueues.add(RabbitTopology.DEFERRED_QUEUE);
        monitoredQueues.add(RabbitTopology.DLQ_QUEUE);
        for (RetryTier tier : RetryTier.values()) {
            monitoredQueues.add(tier.queueName());
        }

        // Ten queues, so ten series — bounded, and every value is one we would look at.
        for (String queue : monitoredQueues) {
            AtomicInteger depth = depths.computeIfAbsent(queue, q -> new AtomicInteger());
            Gauge.builder("hookrelay.queue.depth", depth, AtomicInteger::get)
                    .tag("queue", queue)
                    .description("Messages waiting on a queue")
                    .register(registry);
        }

        for (CircuitBreakerRegistry.State state : CircuitBreakerRegistry.State.values()) {
            AtomicInteger count = breakerCounts.computeIfAbsent(state, s -> new AtomicInteger());
            Gauge.builder("hookrelay.circuit.breakers", count, AtomicInteger::get)
                    .tag("state", state.name().toLowerCase())
                    .description("Endpoints whose circuit breaker is in this state")
                    .register(registry);
        }
    }

    @Scheduled(fixedDelayString = "${hookrelay.metrics.queue-poll-interval-ms:5000}")
    public void sample() {
        for (String queue : monitoredQueues) {
            try {
                QueueInformation info = rabbitAdmin.getQueueInfo(queue);
                depths.get(queue).set(info == null ? 0 : info.getMessageCount());
            } catch (Exception e) {
                log.debug("could not sample depth of {}", queue, e);
            }
        }

        Map<CircuitBreakerRegistry.State, Integer> counts = breakers.countByState();
        for (CircuitBreakerRegistry.State state : CircuitBreakerRegistry.State.values()) {
            breakerCounts.get(state).set(counts.getOrDefault(state, 0));
        }
    }

    /** Test seam: sample now rather than waiting for the timer. */
    public int depthOf(String queue) {
        AtomicInteger depth = depths.get(queue);
        return depth == null ? 0 : depth.get();
    }
}

package com.abhishek.hookrelay.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the head-of-line blocking that motivates the tiered retry design.
 *
 * <p>BLUEPRINT.md §10 asks for the naive single-queue design to be built, measured, then replaced.
 * It is measured here instead of being shipped and reverted, because the behaviour belongs to
 * <em>RabbitMQ</em>, not to HookRelay: it depends only on how a queue expires messages. Declaring
 * both topologies directly measures exactly the thing that motivates the design, leaves a
 * re-runnable artifact rather than a number in a document, and never requires shipping a design
 * already known to be wrong.
 *
 * <p>The mechanism under test: RabbitMQ only inspects the message at the <strong>head</strong> of a
 * queue for expiry. A message expires when it reaches the head <em>and</em> its TTL has elapsed —
 * so in a shared queue, a long-lived message at the front holds up every short-lived message behind
 * it.
 */
class DelayQueueHeadOfLineTest extends AbstractWorkerIntegrationTest {

    private static final String LANDING_EXCHANGE = "exp.landing.ex";
    private static final String LANDING_QUEUE = "exp.landing";
    private static final String LANDING_KEY = "landed";

    private static final String NAIVE_QUEUE = "exp.naive";
    private static final String TIER_LONG_QUEUE = "exp.tier.long";
    private static final String TIER_SHORT_QUEUE = "exp.tier.short";

    private static final long LONG_DELAY_MS = 6000;
    private static final long SHORT_DELAY_MS = 400;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    RabbitAdmin rabbitAdmin;

    @BeforeEach
    void declareExperimentTopology() {
        DirectExchange landingExchange = ExchangeBuilder.directExchange(LANDING_EXCHANGE).durable(false).build();
        Queue landing = QueueBuilder.nonDurable(LANDING_QUEUE).build();
        rabbitAdmin.declareExchange(landingExchange);
        rabbitAdmin.declareQueue(landing);
        rabbitAdmin.declareBinding(BindingBuilder.bind(landing).to(landingExchange).with(LANDING_KEY));

        for (String queueName : new String[]{NAIVE_QUEUE, TIER_LONG_QUEUE, TIER_SHORT_QUEUE}) {
            rabbitAdmin.declareQueue(QueueBuilder.nonDurable(queueName)
                    .deadLetterExchange(LANDING_EXCHANGE)
                    .deadLetterRoutingKey(LANDING_KEY)
                    .build());
            rabbitAdmin.purgeQueue(queueName, false);
        }
        rabbitAdmin.purgeQueue(LANDING_QUEUE, false);
    }

    @AfterEach
    void removeExperimentTopology() {
        for (String queueName : new String[]{NAIVE_QUEUE, TIER_LONG_QUEUE, TIER_SHORT_QUEUE, LANDING_QUEUE}) {
            rabbitAdmin.deleteQueue(queueName);
        }
        rabbitAdmin.deleteExchange(LANDING_EXCHANGE);
    }

    /** Publishes a labelled message straight to a queue with a per-message TTL. */
    private void publish(String queueName, String label, long ttlMillis) {
        rabbitTemplate.send("", queueName, org.springframework.amqp.core.MessageBuilder
                .withBody(label.getBytes(StandardCharsets.UTF_8))
                .setExpiration(Long.toString(ttlMillis))
                .build());
    }

    /** Waits for {@code expected} messages to land, returning each label's delay from {@code startedAt}. */
    private Map<String, Long> collectArrivals(long startedAt, int expected, long timeoutMillis) {
        Map<String, Long> arrivals = new LinkedHashMap<>();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (arrivals.size() < expected && System.currentTimeMillis() < deadline) {
            Message message = rabbitTemplate.receive(LANDING_QUEUE, 200);
            if (message != null) {
                String label = new String(message.getBody(), StandardCharsets.UTF_8);
                arrivals.put(label, System.currentTimeMillis() - startedAt);
            }
        }
        return arrivals;
    }

    @Test
    @DisplayName("one shared delay queue: a short retry is stuck behind a long one")
    void naiveSharedQueueBlocks() {
        long startedAt = System.currentTimeMillis();
        publish(NAIVE_QUEUE, "long", LONG_DELAY_MS);
        publish(NAIVE_QUEUE, "short", SHORT_DELAY_MS);

        Map<String, Long> arrivals = collectArrivals(startedAt, 2, 20_000);

        System.out.printf("NAIVE   short nominal=%dms actual=%dms  (long nominal=%dms actual=%dms)%n",
                SHORT_DELAY_MS, arrivals.get("short"), LONG_DELAY_MS, arrivals.get("long"));

        assertThat(arrivals).containsKeys("long", "short");
        // The short message's TTL elapsed after 400ms, but it cannot expire until it reaches the
        // head — which does not happen until the long message ahead of it expires.
        assertThat(arrivals.get("short"))
                .as("short retry inherits the long retry's delay")
                .isGreaterThan(LONG_DELAY_MS - 500);
    }

    @Test
    @DisplayName("separate tier queues: the short retry fires on time")
    void tieredQueuesDoNotBlock() {
        long startedAt = System.currentTimeMillis();
        publish(TIER_LONG_QUEUE, "long", LONG_DELAY_MS);
        publish(TIER_SHORT_QUEUE, "short", SHORT_DELAY_MS);

        Map<String, Long> arrivals = collectArrivals(startedAt, 2, 20_000);

        System.out.printf("TIERED  short nominal=%dms actual=%dms  (long nominal=%dms actual=%dms)%n",
                SHORT_DELAY_MS, arrivals.get("short"), LONG_DELAY_MS, arrivals.get("long"));

        assertThat(arrivals).containsKeys("long", "short");
        assertThat(arrivals.get("short"))
                .as("the short retry is at the head of its own queue")
                .isLessThan(2000L);
    }

    @Test
    @DisplayName("within one tier, jitter reintroduces blocking — but bounded by the jitter spread")
    void withinTierBlockingIsBoundedByJitter() {
        // Both messages belong to the same tier, so their TTLs differ only by jitter. The worst case
        // is that the message with the top-of-range TTL is enqueued first.
        long slowest = 1200;   // nominal 1000ms at +20%
        long fastest = 800;    // nominal 1000ms at -20%

        long startedAt = System.currentTimeMillis();
        publish(TIER_SHORT_QUEUE, "slowest", slowest);
        publish(TIER_SHORT_QUEUE, "fastest", fastest);

        Map<String, Long> arrivals = collectArrivals(startedAt, 2, 20_000);
        long blocking = arrivals.get("fastest") - fastest;

        System.out.printf("WITHIN-TIER  fastest nominal=%dms actual=%dms  blocking=%dms (jitter spread=%dms)%n",
                fastest, arrivals.get("fastest"), blocking, slowest - fastest);

        assertThat(arrivals).containsKeys("slowest", "fastest");
        // Bounded by the spread, not by the schedule's full range: 400ms here rather than hours.
        assertThat(blocking).isLessThan((slowest - fastest) + 1000);
    }
}

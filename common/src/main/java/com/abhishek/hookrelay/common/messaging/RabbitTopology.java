package com.abhishek.hookrelay.common.messaging;

import com.abhishek.hookrelay.common.retry.RetryTier;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Broker topology, declared by both the API and the worker so either can start first.
 *
 * <pre>
 *   publish (routing key "delivery", mandatory, persistent)
 *             │
 *             ▼
 *   ┌──────────────────────┐
 *   │ exchange: hookrelay  │   direct, durable
 *   └──────────┬───────────┘
 *              │ binding: "delivery"
 *              ▼
 *   ┌──────────────────────┐
 *   │ queue: deliveries    │   durable
 *   └──────────────────────┘
 *
 *   a retryable failure                       attempts exhausted, or
 *             │                               a permanent failure
 *             ▼                                        │
 *   ┌──────────────────────┐                           ▼
 *   │ exchange:            │              ┌──────────────────────┐
 *   │   hookrelay.retry    │              │ exchange:            │
 *   └──────────┬───────────┘              │   hookrelay.dlq      │
 *              │ routing key = tier       └──────────┬───────────┘
 *              ▼                                     ▼
 *   retry.5s .. retry.3h                  ┌──────────────────────┐
 *   (no consumers, TTL + DLX)             │ queue:               │
 *              │                          │   deliveries.dlq     │
 *              │ on expiry, back to       │ (terminal)           │
 *              └──► hookrelay ──► deliveries └────────────────────┘
 * </pre>
 *
 * <p>Durability is three separate settings and all three are required: a durable exchange, a
 * durable queue, and persistent messages. A durable queue holding non-persistent messages loses
 * everything on broker restart, quietly — which is why the message properties are set explicitly
 * at publish time rather than left to a default.
 */
@Configuration
public class RabbitTopology {

    public static final String EXCHANGE = "hookrelay";
    public static final String DELIVERIES_QUEUE = "deliveries";
    public static final String DELIVERY_ROUTING_KEY = "delivery";

    public static final String RETRY_EXCHANGE = "hookrelay.retry";
    public static final String DLQ_EXCHANGE = "hookrelay.dlq";
    public static final String DLQ_QUEUE = "deliveries.dlq";
    public static final String DLQ_ROUTING_KEY = "dead";

    public static final String DEFERRED_QUEUE = "deliveries.deferred";
    public static final int DEFERRAL_DELAY_MS = 2000;

    @Bean
    public DirectExchange hookrelayExchange() {
        return ExchangeBuilder.directExchange(EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue deliveriesQueue() {
        return QueueBuilder.durable(DELIVERIES_QUEUE).build();
    }

    @Bean
    public Binding deliveriesBinding(Queue deliveriesQueue, DirectExchange hookrelayExchange) {
        return BindingBuilder.bind(deliveriesQueue).to(hookrelayExchange).with(DELIVERY_ROUTING_KEY);
    }

    // ---- retry tiers --------------------------------------------------------------------------

    @Bean
    public DirectExchange retryExchange() {
        return ExchangeBuilder.directExchange(RETRY_EXCHANGE).durable(true).build();
    }

    /**
     * One queue per delay bucket, each with no consumer.
     *
     * <p>A message published here expires after its TTL, and because the queue nominates
     * {@code hookrelay} as its dead-letter exchange, the broker republishes it onto
     * {@code deliveries} instead of discarding it. That is the whole timer: TTL plus dead-lettering,
     * held durably by the broker rather than by a sleeping worker thread that a deploy would kill.
     *
     * <p>The queue-level TTL is the top of the tier's jitter range and exists only as a backstop —
     * per-message expiration normally decides, and RabbitMQ honours whichever is lower. Without it, a
     * message published with no expiration would sit here forever, since nothing consumes these
     * queues.
     */
    @Bean
    public Declarables retryQueues() {
        List<Declarable> declarables = new ArrayList<>();
        for (RetryTier tier : RetryTier.values()) {
            Queue queue = QueueBuilder.durable(tier.queueName())
                    .ttl((int) tier.maxTtl().toMillis())
                    .deadLetterExchange(EXCHANGE)
                    .deadLetterRoutingKey(DELIVERY_ROUTING_KEY)
                    .build();
            declarables.add(queue);
            declarables.add(new Binding(tier.queueName(), Binding.DestinationType.QUEUE,
                    RETRY_EXCHANGE, tier.queueName(), null));
        }
        return new Declarables(declarables);
    }

    // ---- deferrals ----------------------------------------------------------------------------

    /**
     * Holds deliveries that were put back because their endpoint had no free permit.
     *
     * <p>Separate from the retry tiers because a deferral is not a retry: nothing was attempted, so
     * it must not consume one of the eight attempts, and it should come back in seconds rather than
     * on the backoff ladder.
     *
     * <p>A single fixed TTL, not a jittered one. Every message in this queue then has the same delay,
     * so no message can be stuck behind another — the head-of-line lesson from phase 4 applied by
     * construction rather than discovered by measurement. Mixing 2-second deferrals into
     * {@code retry.5s}, whose messages carry 4-6 second TTLs, would have reintroduced exactly that
     * bug.
     */
    @Bean
    public Declarables deferredQueue() {
        Queue queue = QueueBuilder.durable(DEFERRED_QUEUE)
                .ttl(DEFERRAL_DELAY_MS)
                .deadLetterExchange(EXCHANGE)
                .deadLetterRoutingKey(DELIVERY_ROUTING_KEY)
                .build();
        return new Declarables(queue,
                new Binding(DEFERRED_QUEUE, Binding.DestinationType.QUEUE,
                        RETRY_EXCHANGE, DEFERRED_QUEUE, null));
    }

    // ---- dead letters -------------------------------------------------------------------------

    @Bean
    public DirectExchange dlqExchange() {
        return ExchangeBuilder.directExchange(DLQ_EXCHANGE).durable(true).build();
    }

    /**
     * Terminal by construction: no TTL, no dead-letter exchange, no consumer. A delivery that
     * exhausted its attempts or failed permanently stops here, and stays until someone looks.
     */
    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }

    @Bean
    public Binding dlqBinding(Queue dlqQueue, DirectExchange dlqExchange) {
        return BindingBuilder.bind(dlqQueue).to(dlqExchange).with(DLQ_ROUTING_KEY);
    }

    /**
     * Declared explicitly rather than relying on auto-configuration.
     *
     * <p>Spring Boot's {@code RabbitAutoConfiguration} declares this bean with the return type
     * {@code AmqpAdmin}, so injecting it as a {@code RabbitAdmin} only resolves once something else
     * has already instantiated it — which made it work everywhere until a bean needed it during
     * startup, and then failed with "no qualifying bean of type RabbitAdmin". Declaring the concrete
     * type here removes the ordering dependency; the auto-configured bean backs off on
     * {@code @ConditionalOnMissingBean}.
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * {@code mandatory} is what turns an unroutable message from a silent discard into an
     * observable event. Without it, publishing to an exchange that routes nowhere — a mistyped
     * routing key, a queue that was never declared — is acknowledged by the broker and thrown away,
     * which is indistinguishable from success at the publisher.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter jsonMessageConverter,
                                         ReturnedMessageTracker returnedMessageTracker) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        template.setMandatory(true);
        template.setReturnsCallback(returnedMessageTracker);
        return template;
    }
}

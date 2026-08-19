package com.abhishek.hookrelay.common.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
 * </pre>
 *
 * <p>Retry queues and the dead-letter queue are added in phase 4.
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

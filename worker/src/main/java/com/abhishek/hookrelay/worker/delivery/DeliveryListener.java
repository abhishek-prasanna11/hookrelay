package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumes {@code deliveries} with manual acknowledgement.
 *
 * <p><strong>Manual acknowledgement is the entire crash-safety mechanism.</strong> With automatic
 * acknowledgement the broker considers a message delivered the moment it writes it to the socket,
 * so a worker holding a prefetch window of messages that dies has silently lost all of them before
 * doing any work. Here the broker holds each message as unacknowledged until this method says
 * otherwise, and a dropped connection means redelivery to another worker.
 *
 * <p>The acknowledgement is always last, after the outcome is committed to PostgreSQL. That leaves
 * a window — committed, then killed before the ack — in which the message is redelivered even though
 * the work is done. That window cannot be removed by reordering: acknowledging first would convert
 * a duplicate into a lost delivery. It is handled instead, by
 * {@link DeliveryStore#claimAttempt} refusing to re-attempt a delivery that already succeeded, and
 * by the receiver deduplicating on the stable delivery id.
 */
@Component
public class DeliveryListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryListener.class);

    private final DeliveryProcessor processor;

    public DeliveryListener(DeliveryProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(queues = RabbitTopology.DELIVERIES_QUEUE)
    public void onDelivery(DeliveryMessage message,
                           Channel channel,
                           @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                           @Header(name = RetryPublisher.HEADER_DEFERRALS, required = false)
                           Integer deferrals) throws IOException {
        try {
            // Deferral count survives the round trip through the deferred queue, because RabbitMQ
            // preserves headers when it dead-letters a message. It is what bounds the loop for an
            // endpoint that is permanently at its concurrency limit.
            processor.process(message.deliveryId(), deferrals == null ? 0 : deferrals);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            // An unexpected failure here is a bug in the worker, not a failing customer endpoint.
            // The message is returned to the queue so a healthy worker (or this one after a fix and
            // restart) can take it, rather than dropped into a queue that does not exist yet.
            log.error("unexpected error processing delivery {}, requeueing", message.deliveryId(), e);
            channel.basicNack(deliveryTag, false, true);
        }
    }
}

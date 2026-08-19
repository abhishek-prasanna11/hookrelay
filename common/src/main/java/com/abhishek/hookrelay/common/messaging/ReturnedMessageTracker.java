package com.abhishek.hookrelay.common.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records messages the broker handed back as unroutable.
 *
 * <p>A publisher confirm alone is not enough to conclude a message was queued. If an exchange has
 * no binding matching the routing key, RabbitMQ <em>acks</em> the message and discards it — the
 * publisher sees success for a message that no queue ever received. Publishing with the
 * {@code mandatory} flag makes the broker return such a message instead, and this class captures
 * those returns so the outbox publisher can refuse to mark the corresponding rows published.
 *
 * <p>Ordering note: RabbitMQ sends {@code basic.return} before the {@code basic.ack} for the same
 * message, so any return for a batch has arrived by the time {@code waitForConfirms} succeeds.
 * Messages are correlated by the outbox row id, carried in the AMQP {@code message-id} property.
 */
@Component
public class ReturnedMessageTracker implements RabbitTemplate.ReturnsCallback {

    private static final Logger log = LoggerFactory.getLogger(ReturnedMessageTracker.class);

    private final Set<String> returnedMessageIds = ConcurrentHashMap.newKeySet();

    @Override
    public void returnedMessage(ReturnedMessage returned) {
        String messageId = returned.getMessage().getMessageProperties().getMessageId();
        log.error("message returned as unroutable: messageId={} exchange={} routingKey={} reply={} {}",
                messageId,
                returned.getExchange(),
                returned.getRoutingKey(),
                returned.getReplyCode(),
                returned.getReplyText());
        if (messageId != null) {
            returnedMessageIds.add(messageId);
        }
    }

    /** Clears any stale entries for these ids before a batch is published. */
    public void forget(Collection<String> messageIds) {
        returnedMessageIds.removeAll(messageIds);
    }

    /** Returns and removes the subset of these ids that came back unroutable. */
    public List<String> drainReturned(Collection<String> messageIds) {
        List<String> returned = new ArrayList<>();
        for (String messageId : messageIds) {
            if (returnedMessageIds.remove(messageId)) {
                returned.add(messageId);
            }
        }
        return returned;
    }
}

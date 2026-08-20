package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Schedules the next attempt, or gives up.
 *
 * <p>Both publishes happen <em>before</em> the original message is acknowledged. Acknowledging first
 * would mean a crash in between silently abandons the delivery after one failed attempt; publishing
 * first means a crash in between produces a duplicate retry, which costs at most one extra HTTP call
 * because {@code attempt_count} is the authority and is capped. Same trade as the outbox in phase 2,
 * for the same reason.
 */
@Component
public class RetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(RetryPublisher.class);

    public static final String HEADER_REASON = "x-hookrelay-reason";
    public static final String HEADER_ATTEMPTS = "x-hookrelay-attempts";
    public static final String HEADER_LAST_ERROR = "x-hookrelay-last-error";
    public static final String HEADER_ENDPOINT_ID = "x-hookrelay-endpoint-id";
    public static final String HEADER_FAILED_AT = "x-hookrelay-failed-at";
    public static final String HEADER_DEFERRALS = "x-hookrelay-deferrals";

    public static final String REASON_ATTEMPTS_EXHAUSTED = "attempts_exhausted";
    public static final String REASON_PERMANENT_FAILURE = "permanent_failure";

    private final RabbitTemplate rabbit;
    private final long confirmTimeoutMs;

    public RetryPublisher(RabbitTemplate rabbit,
                          @Value("${hookrelay.publish.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        this.rabbit = rabbit;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    /**
     * Publishes the delivery into a tier queue with a jittered expiration. Nothing consumes that
     * queue; the message expires and the queue's dead-letter exchange puts it back on
     * {@code deliveries}.
     */
    public void scheduleRetry(UUID deliveryId, RetryTier tier, long delayMillis) {
        publishConfirmed(RabbitTopology.RETRY_EXCHANGE, tier.queueName(), deliveryId,
                message -> {
                    message.getMessageProperties().setExpiration(Long.toString(delayMillis));
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                });
        log.debug("delivery {} scheduled on {} in {}ms", deliveryId, tier.queueName(), delayMillis);
    }

    /**
     * Puts a delivery back because its endpoint had no free permit.
     *
     * <p>Not a retry: nothing was attempted, so this must not consume one of the eight attempts. The
     * deferral count rides on the message so the loop can be bounded — an endpoint that is
     * permanently saturated would otherwise cycle forever.
     */
    public void defer(UUID deliveryId, int previousDeferrals) {
        publishConfirmed(RabbitTopology.RETRY_EXCHANGE, RabbitTopology.DEFERRED_QUEUE, deliveryId,
                message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    message.getMessageProperties().setHeader(HEADER_DEFERRALS, previousDeferrals + 1);
                    return message;
                });
        log.debug("delivery {} deferred ({} so far)", deliveryId, previousDeferrals + 1);
    }

    /**
     * Publishes to the dead-letter queue.
     *
     * <p>The reason travels as headers rather than living only in the database, so the queue is
     * self-describing to whoever is looking at it in the management UI during an incident.
     */
    public void deadLetter(UUID deliveryId, UUID endpointId, String reason,
                           int attempts, String lastError) {
        publishConfirmed(RabbitTopology.DLQ_EXCHANGE, RabbitTopology.DLQ_ROUTING_KEY, deliveryId,
                message -> {
                    var properties = message.getMessageProperties();
                    properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    properties.setHeader(HEADER_REASON, reason);
                    properties.setHeader(HEADER_ATTEMPTS, attempts);
                    properties.setHeader(HEADER_LAST_ERROR, lastError);
                    properties.setHeader(HEADER_ENDPOINT_ID, endpointId == null ? null : endpointId.toString());
                    properties.setHeader(HEADER_FAILED_AT, Instant.now().toString());
                    return message;
                });
        log.info("delivery {} dead-lettered after {} attempt(s): {} ({})",
                deliveryId, attempts, reason, lastError);
    }

    /**
     * Publishes and <strong>waits for the broker to confirm</strong>, throwing if it does not.
     *
     * <p>This is the same rule the outbox publisher follows in phase 2, and not applying it here was
     * a real bug found by the phase 7 rolling-deploy experiment. The caller acknowledges the original
     * message immediately after this returns, so a fire-and-forget publish that quietly failed left
     * the delivery orphaned: the original was acknowledged, the replacement never existed, and the
     * delivery sat in {@code PENDING} forever with no queue message and no error — a silent loss of
     * accepted work, which is precisely what BLUEPRINT.md §1 forbids.
     *
     * <p>Measured: 3 of 1578 deliveries stranded across one rolling restart before this fix.
     *
     * <p>Throwing is what makes it safe. {@code DeliveryListener} catches, negatively acknowledges
     * with requeue, and the delivery is processed again — at worst producing a duplicate attempt,
     * which this system tolerates by design and a lost delivery is not.
     */
    // Package-private rather than private: PublishFailureTest calls it with a deliberately
    // unroutable exchange to prove that an unconfirmed publish throws instead of returning quietly.
    void publishConfirmed(String exchange, String routingKey, UUID deliveryId,
                                  MessagePostProcessor postProcessor) {
        Boolean confirmed = rabbit.invoke(operations -> {
            operations.convertAndSend(exchange, routingKey, new DeliveryMessage(deliveryId), postProcessor);
            return operations.waitForConfirms(confirmTimeoutMs);
        });

        if (!Boolean.TRUE.equals(confirmed)) {
            throw new AmqpException("broker did not confirm publish of delivery " + deliveryId
                    + " to " + exchange + "/" + routingKey + " within " + confirmTimeoutMs + "ms");
        }
    }
}

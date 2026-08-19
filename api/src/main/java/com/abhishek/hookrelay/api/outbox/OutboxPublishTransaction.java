package com.abhishek.hookrelay.api.outbox;

import com.abhishek.hookrelay.common.domain.OutboxEvent;
import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.messaging.ReturnedMessageTracker;
import com.abhishek.hookrelay.common.repo.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Publishes one batch of outbox rows, in one transaction.
 *
 * <p>The ordering of the two side effects is the important part, and it is not symmetric:
 *
 * <pre>
 *   Mark published, then publish       Publish, then mark published  ← what this does
 *
 *     UPDATE published_at ✓              publish + confirm ✓
 *        ↓                                   ↓
 *     ✗ CRASH                            ✗ CRASH
 *        ↓                                   ↓
 *     publish never happens              UPDATE never happens
 *        ↓                                   ↓
 *   MESSAGE LOST. The row looks         Row is still unpublished, so the
 *   done and is never retried.          next poll republishes. DUPLICATE.
 * </pre>
 *
 * <p>The second failure mode is survivable — this system has stable delivery ids and receiver-side
 * deduplication precisely so that duplicates are cheap — while the first violates the contract that
 * accepted work is never silently lost.
 *
 * <p>Row locks are held across the network call to the broker. That is bounded deliberately: at
 * most {@code batchSize} rows for at most {@code confirmTimeoutMs}, and other pollers are not
 * blocked by it because the claim query uses {@code SKIP LOCKED}.
 */
@Component
public class OutboxPublishTransaction {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublishTransaction.class);

    private static final int MAX_ERROR_LENGTH = 500;

    private final OutboxEventRepository outbox;
    private final RabbitTemplate rabbit;
    private final ReturnedMessageTracker returnedMessages;
    private final int batchSize;
    private final long confirmTimeoutMs;

    public OutboxPublishTransaction(OutboxEventRepository outbox,
                                    RabbitTemplate rabbit,
                                    ReturnedMessageTracker returnedMessages,
                                    @Value("${hookrelay.outbox.publisher.batch-size}") int batchSize,
                                    @Value("${hookrelay.outbox.publisher.confirm-timeout-ms}") long confirmTimeoutMs) {
        this.outbox = outbox;
        this.rabbit = rabbit;
        this.returnedMessages = returnedMessages;
        this.batchSize = batchSize;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @Transactional
    public PublishBatchResult publishBatch() {
        List<OutboxEvent> batch = outbox.lockUnpublishedBatch(batchSize);
        if (batch.isEmpty()) {
            return PublishBatchResult.empty();
        }

        // Ordered by created_at, so the head of the batch is the oldest unpublished row overall.
        double lagSeconds = Duration.between(batch.get(0).getCreatedAt(), OffsetDateTime.now())
                .toMillis() / 1000.0;

        List<UUID> ids = batch.stream().map(OutboxEvent::getId).toList();
        List<String> messageIds = ids.stream().map(UUID::toString).toList();
        returnedMessages.forget(messageIds);

        String failure = publishAndAwaitConfirms(batch);
        if (failure != null) {
            // Nothing is marked published. Every row in the batch stays eligible and the next poll
            // tries again — the broker being unreachable must never consume work.
            outbox.recordPublishFailure(ids, truncate(failure));
            log.warn("outbox batch of {} not confirmed, will retry: {}", batch.size(), failure);
            return new PublishBatchResult(0, batch.size(), lagSeconds);
        }

        Set<UUID> unroutable = returnedMessages.drainReturned(messageIds).stream()
                .map(UUID::fromString)
                .collect(java.util.stream.Collectors.toSet());

        List<UUID> confirmed = ids.stream().filter(id -> !unroutable.contains(id)).toList();
        if (!confirmed.isEmpty()) {
            outbox.markPublished(confirmed);
        }
        if (!unroutable.isEmpty()) {
            outbox.recordPublishFailure(List.copyOf(unroutable),
                    "unroutable: no queue bound for routing key " + RabbitTopology.DELIVERY_ROUTING_KEY);
        }

        return new PublishBatchResult(confirmed.size(), unroutable.size(), lagSeconds);
    }

    /** @return null on success, or a description of why the batch was not confirmed. */
    private String publishAndAwaitConfirms(List<OutboxEvent> batch) {
        try {
            Boolean allConfirmed = rabbit.invoke(operations -> {
                for (OutboxEvent row : batch) {
                    operations.convertAndSend(
                            RabbitTopology.EXCHANGE,
                            RabbitTopology.DELIVERY_ROUTING_KEY,
                            new DeliveryMessage(row.getDeliveryId()),
                            message -> {
                                // Correlates a returned message back to its outbox row.
                                message.getMessageProperties().setMessageId(row.getId().toString());
                                message.getMessageProperties()
                                        .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                                return message;
                            });
                }
                // A single channel for the batch, so one wait covers every message in it.
                return operations.waitForConfirms(confirmTimeoutMs);
            });

            if (!Boolean.TRUE.equals(allConfirmed)) {
                return "broker did not confirm within " + confirmTimeoutMs + "ms";
            }
            return null;
        } catch (RuntimeException e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private static String truncate(String error) {
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}

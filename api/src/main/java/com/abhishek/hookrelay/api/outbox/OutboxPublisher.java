package com.abhishek.hookrelay.api.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link OutboxPublishTransaction} on a timer.
 *
 * <p>Runs inside the API process rather than as its own deployment. The gate on
 * {@code hookrelay.outbox.publisher.enabled} is what keeps that decision cheap to reverse: giving
 * the publisher its own Deployment later means running the same image with the flag off in the API
 * and on in the publisher, with no code change.
 *
 * <p>There is deliberately no backoff when the broker is unavailable. A dead broker is an
 * operational emergency, not a routine per-message condition, and the retry costs one indexed query
 * against a partial index that is nearly empty in the steady state. The exponential-backoff
 * machinery in phase 4 is for customer endpoints, which are expected to fail regularly.
 */
@Component
@ConditionalOnProperty(prefix = "hookrelay.outbox.publisher", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxPublishTransaction publishTransaction;
    private final OutboxMetrics metrics;

    public OutboxPublisher(OutboxPublishTransaction publishTransaction, OutboxMetrics metrics) {
        this.publishTransaction = publishTransaction;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${hookrelay.outbox.publisher.poll-interval-ms}")
    public void poll() {
        try {
            PublishBatchResult result = publishTransaction.publishBatch();
            metrics.recordLag(result.lagSeconds());
            metrics.recordPublished(result.published());
            if (result.published() > 0) {
                log.debug("outbox published {} message(s), lag {}s", result.published(), result.lagSeconds());
            }
        } catch (Exception e) {
            // Swallowed so one bad cycle cannot stop the scheduler. The rows are still unpublished,
            // so the next poll retries them.
            log.error("outbox poll failed", e);
        }
    }
}

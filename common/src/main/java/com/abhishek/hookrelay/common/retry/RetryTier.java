package com.abhishek.hookrelay.common.retry;

import java.time.Duration;

/**
 * One delay bucket in the retry ladder. Each tier is a separate RabbitMQ queue.
 *
 * <p>Separate queues rather than one queue with per-message TTLs, because RabbitMQ only inspects the
 * message at the <em>head</em> of a queue for expiry. In a single shared queue a message with a
 * three-hour TTL sitting at the head blocks every five-second retry behind it for three hours — the
 * queue is FIFO, expiry is not. With one queue per bucket, every message in it has approximately the
 * same TTL, so head-of-line order and expiry order agree.
 *
 * <p>"Approximately" is doing real work there: jitter means TTLs within a tier vary by ±20%, so
 * head-of-line blocking is not eliminated, it is <em>bounded by the jitter spread</em> — two seconds
 * on the 5s tier rather than three hours. See docs/phase04-retries.md §1.6.
 */
public enum RetryTier {

    T5S("retry.5s", Duration.ofSeconds(5)),
    T30S("retry.30s", Duration.ofSeconds(30)),
    T2M("retry.2m", Duration.ofMinutes(2)),
    T10M("retry.10m", Duration.ofMinutes(10)),
    T30M("retry.30m", Duration.ofMinutes(30)),
    T1H("retry.1h", Duration.ofHours(1)),
    T3H("retry.3h", Duration.ofHours(3));

    private final String queueName;
    private final Duration nominalDelay;

    RetryTier(String queueName, Duration nominalDelay) {
        this.queueName = queueName;
        this.nominalDelay = nominalDelay;
    }

    /** Also the routing key that reaches this tier's queue. */
    public String queueName() {
        return queueName;
    }

    public Duration nominalDelay() {
        return nominalDelay;
    }

    /**
     * Queue-level TTL, set to the top of the jitter range. A backstop only: the per-message
     * expiration normally decides, and RabbitMQ applies whichever is lower. Without it, a message
     * published with no expiration would sit in a queue nobody consumes forever.
     */
    public Duration maxTtl() {
        return Duration.ofMillis(Math.round(nominalDelay.toMillis() * RetryPolicy.MAX_JITTER_FACTOR));
    }
}

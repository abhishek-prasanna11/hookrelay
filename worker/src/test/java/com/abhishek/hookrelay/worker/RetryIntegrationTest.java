package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryPolicy;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.worker.delivery.DeliveryProcessor;
import com.abhishek.hookrelay.worker.delivery.RetryPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RetryIntegrationTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "retry-secret-0123456789";

    @Autowired
    DeliveryProcessor processor;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    RabbitAdmin rabbitAdmin;

    @BeforeEach
    void drainQueues() {
        for (RetryTier tier : RetryTier.values()) {
            rabbitAdmin.purgeQueue(tier.queueName(), false);
        }
        rabbitAdmin.purgeQueue(RabbitTopology.DLQ_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitTopology.DELIVERIES_QUEUE, false);
    }

    /**
     * Blocks until a message arrives, and returns which delivery it is for.
     *
     * <p>Asserting on {@code getQueueInfo().getMessageCount()} immediately after a publish is a race:
     * the count is a snapshot taken by the queue process and lags a just-published message. A
     * blocking receive waits for the message to actually exist, and checking the delivery id proves
     * the <em>right</em> one was scheduled rather than merely that something was.
     */
    private UUID receiveDeliveryIdFrom(String queueName) {
        Message message = rabbitTemplate.receive(queueName, 5000);
        assertThat(message).as("expected a message on %s", queueName).isNotNull();
        return ((DeliveryMessage) rabbitTemplate.getMessageConverter().fromMessage(message)).deliveryId();
    }

    private Message receiveFrom(String queueName) {
        Message message = rabbitTemplate.receive(queueName, 5000);
        assertThat(message).as("expected a message on %s", queueName).isNotNull();
        return message;
    }

    private void assertNothingOn(String queueName) {
        assertThat(rabbitTemplate.receive(queueName, 200))
                .as("expected %s to be empty", queueName)
                .isNull();
    }

    private int depth(String queueName) {
        QueueInformation info = rabbitAdmin.getQueueInfo(queueName);
        return info == null ? 0 : info.getMessageCount();
    }

    /** Forces the delivery to have already made {@code attempts} attempts. */
    private void setAttemptCount(UUID deliveryId, int attempts) {
        jdbc.update("UPDATE deliveries SET attempt_count = ? WHERE id = ?", attempts, deliveryId);
    }

    // ---- scheduling ---------------------------------------------------------------------------

    @Test
    @DisplayName("a retryable failure schedules the next attempt on the first tier")
    void retryableFailureSchedulesRetry() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
        assertThat(receiveDeliveryIdFrom(RetryTier.T5S.queueName())).isEqualTo(deliveryId);
        assertNothingOn(RabbitTopology.DLQ_QUEUE);
    }

    @Test
    @DisplayName("the tier follows the attempt number up the ladder")
    void tierFollowsAttemptNumber() {
        receiver.respondWith(500);

        UUID third = seedDelivery(SECRET);
        setAttemptCount(third, 2);          // the claim makes this attempt 3
        processor.process(third);
        assertThat(receiveDeliveryIdFrom(RetryTier.T2M.queueName())).isEqualTo(third);

        UUID seventh = seedDelivery(SECRET);
        setAttemptCount(seventh, 6);        // attempt 7 — the last one that gets a retry
        processor.process(seventh);
        assertThat(receiveDeliveryIdFrom(RetryTier.T3H.queueName())).isEqualTo(seventh);
    }

    @Test
    @DisplayName("the scheduled message carries the jittered delay as its expiration")
    void retryCarriesJitteredExpiration() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        Message message = receiveFrom(RetryTier.T5S.queueName());
        long expiration = Long.parseLong(message.getMessageProperties().getExpiration());
        long nominal = RetryTier.T5S.nominalDelay().toMillis();

        assertThat(expiration).isBetween(
                Math.round(nominal * RetryPolicy.MIN_JITTER_FACTOR),
                Math.round(nominal * RetryPolicy.MAX_JITTER_FACTOR));
    }

    @Test
    @DisplayName("next_attempt_at is recorded for observability")
    void recordsNextAttemptAt() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        Object nextAttemptAt = jdbc.queryForMap(
                "SELECT next_attempt_at FROM deliveries WHERE id = ?", deliveryId).get("next_attempt_at");
        assertThat(nextAttemptAt).isNotNull();
    }

    // ---- giving up ----------------------------------------------------------------------------

    @Test
    @DisplayName("the eighth failed attempt dead-letters instead of scheduling a ninth")
    void attemptsExhaustedGoesToDlq() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);
        setAttemptCount(deliveryId, 7);     // the claim makes this attempt 8

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(receiveDeliveryIdFrom(RabbitTopology.DLQ_QUEUE)).isEqualTo(deliveryId);
        for (RetryTier tier : RetryTier.values()) {
            assertNothingOn(tier.queueName());
        }
    }

    @Test
    @DisplayName("a permanent failure dead-letters immediately, without using up the ladder")
    void permanentFailureGoesToDlqImmediately() {
        receiver.respondWith(400);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(receiveDeliveryIdFrom(RabbitTopology.DLQ_QUEUE)).isEqualTo(deliveryId);
        assertNothingOn(RetryTier.T5S.queueName());
    }

    @Test
    @DisplayName("the dead-lettered message explains itself without a database lookup")
    void dlqMessageCarriesTheReason() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);
        setAttemptCount(deliveryId, 7);

        processor.process(deliveryId);

        var headers = receiveFrom(RabbitTopology.DLQ_QUEUE).getMessageProperties().getHeaders();
        assertThat(headers.get(RetryPublisher.HEADER_REASON))
                .isEqualTo(RetryPublisher.REASON_ATTEMPTS_EXHAUSTED);
        assertThat(headers.get(RetryPublisher.HEADER_ATTEMPTS)).isEqualTo(8);
        assertThat(headers.get(RetryPublisher.HEADER_LAST_ERROR)).isEqualTo("HTTP 500");
        assertThat(headers.get(RetryPublisher.HEADER_ENDPOINT_ID)).isNotNull();
        assertThat(headers.get(RetryPublisher.HEADER_FAILED_AT)).isNotNull();
    }

    @Test
    @DisplayName("a permanent failure is labelled differently from an exhausted one")
    void dlqDistinguishesPermanentFromExhausted() {
        receiver.respondWith(403);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        assertThat(receiveFrom(RabbitTopology.DLQ_QUEUE).getMessageProperties()
                .getHeaders().get(RetryPublisher.HEADER_REASON))
                .isEqualTo(RetryPublisher.REASON_PERMANENT_FAILURE);
    }

    // ---- success ------------------------------------------------------------------------------

    @Test
    @DisplayName("a success queues nothing at all")
    void successQueuesNothing() {
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("SUCCEEDED");
        assertNothingOn(RabbitTopology.DLQ_QUEUE);
        assertNothingOn(RetryTier.T5S.queueName());
    }

    // ---- the delay actually works ---------------------------------------------------------------

    @Test
    @DisplayName("a message on a tier queue reappears on deliveries after its TTL, not before")
    void delayQueueDelaysThenRedelivers() {
        // Published straight to the tier so the test controls the expiration precisely. The point is
        // the broker mechanism: TTL expiry plus the queue's dead-letter exchange is the timer.
        UUID deliveryId = UUID.randomUUID();
        long delayMs = 1500;

        long publishedAt = System.currentTimeMillis();
        rabbitTemplate.convertAndSend(RabbitTopology.RETRY_EXCHANGE, RetryTier.T5S.queueName(),
                new DeliveryMessage(deliveryId),
                message -> {
                    message.getMessageProperties().setExpiration(Long.toString(delayMs));
                    return message;
                });

        await().atMost(Duration.ofSeconds(20))
                .until(() -> depth(RetryTier.T5S.queueName()) == 0);
        long elapsed = System.currentTimeMillis() - publishedAt;

        assertThat(elapsed).as("must not expire early").isGreaterThanOrEqualTo(delayMs - 300);
        assertThat(elapsed).as("must not be stuck").isLessThan(delayMs + 5000);
    }

    @Test
    @DisplayName("end to end: a 500 then a 200 is retried through the broker and succeeds")
    void endToEndRetryThenSuccess() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        // First attempt fails and schedules a retry. The message is left on the tier queue so the
        // broker's TTL expiry, not the test, drives the second attempt.
        processor.process(deliveryId);
        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
        await().atMost(Duration.ofSeconds(10))
                .until(() -> depth(RetryTier.T5S.queueName()) == 1);

        // The endpoint recovers before the retry fires.
        receiver.respondWith(200);

        // The 5s tier's jitter range is 4-6s; allow generously for the listener to pick it up.
        await().atMost(Duration.ofSeconds(45))
                .untilAsserted(() -> assertThat(deliveryStatus(deliveryId)).isEqualTo("SUCCEEDED"));

        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(2);
        assertThat(attempts(deliveryId)).hasSize(2);
        assertThat(attempts(deliveryId).get(0).get("response_status")).isEqualTo(500);
        assertThat(attempts(deliveryId).get(1).get("response_status")).isEqualTo(200);
        assertThat(receiver.requestCount()).isEqualTo(2);
        assertNothingOn(RabbitTopology.DLQ_QUEUE);
    }
}

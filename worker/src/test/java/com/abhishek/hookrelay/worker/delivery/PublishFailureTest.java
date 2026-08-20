package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.retry.RetryTier;
import com.abhishek.hookrelay.worker.AbstractWorkerIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for a silent-loss bug found by the phase 7 rolling-deploy experiment.
 *
 * <p>{@code DeliveryListener} acknowledges the original message immediately after
 * {@link RetryPublisher} returns. The defer, retry and dead-letter publishes were originally
 * fire-and-forget, so a publish that quietly failed left the delivery orphaned: the original was
 * acknowledged, the replacement message never existed, and the delivery sat in {@code PENDING}
 * forever with no queue message and no recorded error — a silent loss of accepted work, which
 * BLUEPRINT.md §1 forbids outright.
 *
 * <p>Measured before the fix: <strong>3 of 1578 deliveries stranded</strong> across one rolling
 * restart of the workers.
 *
 * <p>The fix is the rule the outbox publisher has followed since phase 2 — wait for the broker to
 * confirm before treating a publish as done — and what makes it safe is that failure
 * <strong>throws</strong>, so the listener negatively acknowledges and the message is redelivered.
 */
class PublishFailureTest extends AbstractWorkerIntegrationTest {

    @Autowired
    RetryPublisher retries;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Test
    @DisplayName("publisher confirms are enabled on the worker's connection")
    void confirmsAreEnabled() {
        // The worker's application.yml originally omitted publisher-confirm-type entirely — only the
        // API set it — so waitForConfirms would have thrown rather than returning a verdict. This
        // pins the configuration the whole fix depends on.
        Boolean confirmed = rabbitTemplate.invoke(operations -> {
            operations.convertAndSend(RabbitTopology.RETRY_EXCHANGE, RetryTier.T5S.queueName(),
                    new DeliveryMessage(UUID.randomUUID()));
            return operations.waitForConfirms(5000);
        });

        assertThat(confirmed).isTrue();
    }

    @Test
    @DisplayName("a publish the broker cannot accept throws rather than returning quietly")
    void unconfirmablePublishThrows() {
        // Publishing to an exchange that does not exist makes the broker close the channel with a
        // 404. The original code would have swallowed that and let the caller acknowledge the
        // original message, stranding the delivery; the whole point of the fix is that it must not
        // return normally here.
        assertThatThrownBy(() -> retries.publishConfirmed(
                "exchange.that.does.not.exist", "any.key", UUID.randomUUID(), message -> message))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("a deferral is confirmed before the caller may acknowledge")
    void deferIsConfirmed() {
        UUID deliveryId = UUID.randomUUID();

        retries.defer(deliveryId, 0);

        assertThat(rabbitTemplate.receive(RabbitTopology.DEFERRED_QUEUE, 5000)).isNotNull();
    }

    @Test
    @DisplayName("a dead-letter publish is confirmed before the caller may acknowledge")
    void deadLetterIsConfirmed() {
        UUID deliveryId = UUID.randomUUID();

        retries.deadLetter(deliveryId, UUID.randomUUID(),
                RetryPublisher.REASON_ATTEMPTS_EXHAUSTED, 8, "HTTP 500");

        assertThat(rabbitTemplate.receive(RabbitTopology.DLQ_QUEUE, 5000)).isNotNull();
    }
}

package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.abhishek.hookrelay.common.webhook.WebhookHeaders;
import com.abhishek.hookrelay.common.webhook.WebhookSignature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End to end through the real broker: a message on {@code deliveries} results in a signed HTTP
 * request and an acknowledged message.
 */
class DeliveryListenerTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "listener-secret-abcdef0123456789";

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    RabbitAdmin rabbitAdmin;

    private void publish(UUID deliveryId) {
        rabbitTemplate.convertAndSend(RabbitTopology.EXCHANGE, RabbitTopology.DELIVERY_ROUTING_KEY,
                new DeliveryMessage(deliveryId));
    }

    private int unackedPlusReady() {
        QueueInformation info = rabbitAdmin.getQueueInfo(RabbitTopology.DELIVERIES_QUEUE);
        return info == null ? 0 : info.getMessageCount();
    }

    @Test
    @DisplayName("a queued delivery is delivered, signed, and the message is acknowledged")
    void endToEnd() {
        UUID deliveryId = seedDelivery(SECRET);

        publish(deliveryId);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(deliveryStatus(deliveryId)).isEqualTo("SUCCEEDED"));

        assertThat(receiver.requestCount()).isEqualTo(1);
        TestWebhookReceiver.Received request = receiver.received().get(0);
        assertThat(request.header(WebhookHeaders.DELIVERY_ID)).isEqualTo(deliveryId.toString());
        assertThat(WebhookSignature.verify(SECRET, request.header(WebhookHeaders.SIGNATURE),
                request.body(), Instant.now().getEpochSecond(), 300)).isTrue();

        // Acknowledged, so the broker has dropped it rather than holding it unacknowledged.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(unackedPlusReady()).isZero());
    }

    @Test
    @DisplayName("a failing endpoint still acknowledges — no hot loop in this phase")
    void failingEndpointIsAcknowledged() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        publish(deliveryId);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED"));

        // Requeueing here would spin against a dead endpoint at full speed. Phase 4 replaces this
        // acknowledgement with a publish to a delay queue.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(unackedPlusReady()).isZero());
        assertThat(receiver.requestCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a redelivered message for an already-succeeded delivery makes no second HTTP call")
    void redeliveryDoesNotCallTwice() {
        UUID deliveryId = seedDelivery(SECRET);

        publish(deliveryId);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(deliveryStatus(deliveryId)).isEqualTo("SUCCEEDED"));
        assertThat(receiver.requestCount()).isEqualTo(1);

        // Exactly what the broker does when a worker dies after the customer responded but before
        // the acknowledgement: the same message arrives again.
        publish(deliveryId);
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(unackedPlusReady()).isZero());

        assertThat(receiver.requestCount())
                .as("the duplicate message must not produce a duplicate HTTP call")
                .isEqualTo(1);
        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(1);
    }
}

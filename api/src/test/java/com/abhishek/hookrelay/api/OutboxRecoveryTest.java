package com.abhishek.hookrelay.api;

import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.DockerClientFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The demonstration BLUEPRINT.md section 27 requires for this phase: recovery from "database
 * committed but RabbitMQ publication not completed".
 *
 * <p>The broker is made unavailable by pausing its container rather than stopping it. Pausing
 * freezes the process while preserving the port mapping and the established TCP connection, which
 * reproduces the failure that actually matters — the broker stops acknowledging while the socket
 * still looks alive — and lets the same container be revived. Stopping the container would give it
 * a new port on restart and the application would never reconnect.
 *
 * <p>The publisher's timer is enabled here, unlike every other test class, so this exercises the
 * real polling loop.
 */
@TestPropertySource(properties = {
        "hookrelay.outbox.publisher.enabled=true",
        "hookrelay.outbox.publisher.poll-interval-ms=200",
        "hookrelay.outbox.publisher.confirm-timeout-ms=1000"
})
class OutboxRecoveryTest extends AbstractIntegrationTest {

    @Autowired
    RabbitTemplate rabbitTemplate;

    private boolean paused;

    private void pauseBroker() {
        DockerClientFactory.instance().client().pauseContainerCmd(RABBIT.getContainerId()).exec();
        paused = true;
    }

    private void unpauseBroker() {
        if (paused) {
            DockerClientFactory.instance().client().unpauseContainerCmd(RABBIT.getContainerId()).exec();
            paused = false;
        }
    }

    @AfterEach
    void alwaysUnpause() {
        unpauseBroker();
    }

    @Test
    @DisplayName("events are accepted while the broker is down, and the backlog drains when it returns")
    void survivesBrokerOutage() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));

        // Let the publisher settle so we are not racing its startup.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> countUnpublishedOutboxRowsForTenant() == 0);

        pauseBroker();

        int events = 10;
        for (int i = 0; i < events; i++) {
            ResponseEntity<JsonNode> response = postEvent("key-" + i, "payment.succeeded", "{}");
            assertThat(response.getStatusCode().value())
                    .as("ingest must not depend on broker availability")
                    .isEqualTo(202);
        }

        // Durable in PostgreSQL, and visibly stuck: the rows are unpublished and the publisher is
        // recording failures rather than marking them done.
        assertThat(countOutboxRowsForTenant()).isEqualTo(events);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(countUnpublishedOutboxRowsForTenant()).isEqualTo(events);
            assertThat(maxAttemptCountForTenant()).isGreaterThan(0);
        });

        unpauseBroker();

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(countUnpublishedOutboxRowsForTenant()).isZero());

        Set<UUID> expected = new HashSet<>(outboxDeliveryIdsForTenant());
        Set<UUID> delivered = drainQueue(Duration.ofSeconds(10));

        assertThat(delivered)
                .as("every delivery committed during the outage reached the queue")
                .containsAll(expected);
    }

    @Test
    @DisplayName("nothing is published twice when the broker never fails")
    void steadyStatePublishesExactlyOnce() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));

        int events = 5;
        for (int i = 0; i < events; i++) {
            postEvent("key-" + i, "payment.succeeded", "{}");
        }

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(countUnpublishedOutboxRowsForTenant()).isZero());
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(queueDepth()).isEqualTo(events));
    }

    private int maxAttemptCountForTenant() {
        Integer max = jdbc.queryForObject("""
                SELECT coalesce(max(o.attempt_count), 0) FROM outbox_events o
                  JOIN deliveries d ON d.id = o.delivery_id
                  JOIN events e     ON e.id = d.event_id
                 WHERE e.tenant_id = ?
                """, Integer.class, tenant);
        return max == null ? 0 : max;
    }

    private Set<UUID> drainQueue(Duration timeout) {
        Set<UUID> ids = new HashSet<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Message message = rabbitTemplate.receive(RabbitTopology.DELIVERIES_QUEUE, 500);
            if (message == null) {
                break;
            }
            DeliveryMessage body = (DeliveryMessage) rabbitTemplate.getMessageConverter().fromMessage(message);
            ids.add(body.deliveryId());
        }
        return ids;
    }
}

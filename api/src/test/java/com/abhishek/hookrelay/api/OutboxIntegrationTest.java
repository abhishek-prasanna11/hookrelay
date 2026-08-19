package com.abhishek.hookrelay.api;

import com.abhishek.hookrelay.api.outbox.OutboxPublishTransaction;
import com.abhishek.hookrelay.api.outbox.OutboxPurge;
import com.abhishek.hookrelay.api.outbox.PublishBatchResult;
import com.abhishek.hookrelay.common.messaging.DeliveryMessage;
import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    OutboxPublishTransaction publisher;

    @Autowired
    OutboxPurge purge;

    @Autowired
    RabbitTemplate rabbitTemplate;

    // ---- the transaction ----------------------------------------------------------------------

    @Test
    @DisplayName("one outbox row is written per delivery, in the ingest transaction")
    void writesOneOutboxRowPerDelivery() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded"));
        registerEndpoint("https://c.example.com/hook", List.of("user.created"));

        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", "{}");

        assertThat(deliveriesCreatedOf(response)).isEqualTo(2);
        assertThat(countOutboxRowsForTenant()).isEqualTo(2);
        assertThat(countUnpublishedOutboxRowsForTenant()).isEqualTo(2);
    }

    @Test
    @DisplayName("an event with no matching endpoints writes no outbox rows")
    void writesNoOutboxRowsWithoutEndpoints() {
        postEvent("key-1", "payment.succeeded", "{}");

        assertThat(countOutboxRowsForTenant()).isZero();
    }

    @Test
    @DisplayName("a duplicate submission does not write a second set of outbox rows")
    void duplicateSubmissionWritesNoExtraOutboxRows() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));

        postEvent("key-1", "payment.succeeded", "{}");
        postEvent("key-1", "payment.succeeded", "{}");

        assertThat(countOutboxRowsForTenant()).isEqualTo(1);
    }

    // ---- publishing ---------------------------------------------------------------------------

    @Test
    @DisplayName("publishing marks rows published and enqueues one message each")
    void publishMarksRowsAndEnqueues() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded"));
        postEvent("key-1", "payment.succeeded", "{}");

        PublishBatchResult result = publisher.publishBatch();

        assertThat(result.published()).isEqualTo(2);
        assertThat(result.failed()).isZero();
        assertThat(countUnpublishedOutboxRowsForTenant()).isZero();
        assertThat(queueDepth()).isEqualTo(2);
    }

    @Test
    @DisplayName("the message body is the delivery id, and the message is persistent")
    void messageIsClaimCheckAndPersistent() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        postEvent("key-1", "payment.succeeded", "{}");
        UUID expectedDeliveryId = outboxDeliveryIdsForTenant().get(0);

        publisher.publishBatch();

        Message message = rabbitTemplate.receive(RabbitTopology.DELIVERIES_QUEUE, 5000);
        assertThat(message).isNotNull();

        // Without persistent delivery mode, a broker restart silently empties even a durable queue.
        assertThat(message.getMessageProperties().getReceivedDeliveryMode())
                .isEqualTo(MessageDeliveryMode.PERSISTENT);

        DeliveryMessage body = (DeliveryMessage) rabbitTemplate.getMessageConverter().fromMessage(message);
        assertThat(body.deliveryId()).isEqualTo(expectedDeliveryId);
    }

    @Test
    @DisplayName("a second poll republishes nothing")
    void publishingIsNotRepeatedOnTheNextPoll() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        postEvent("key-1", "payment.succeeded", "{}");

        assertThat(publisher.publishBatch().published()).isEqualTo(1);
        assertThat(publisher.publishBatch().published()).isZero();
        assertThat(queueDepth()).isEqualTo(1);
    }

    @Test
    @DisplayName("an empty outbox is a no-op")
    void emptyOutboxIsNoOp() {
        PublishBatchResult result = publisher.publishBatch();

        assertThat(result.published()).isZero();
        assertThat(result.failed()).isZero();
        assertThat(result.lagSeconds()).isZero();
    }

    @Test
    @DisplayName("lag reflects the age of the oldest unpublished row")
    void reportsLag() throws Exception {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        postEvent("key-1", "payment.succeeded", "{}");
        Thread.sleep(1100);

        PublishBatchResult result = publisher.publishBatch();

        assertThat(result.lagSeconds()).isGreaterThan(1.0).isLessThan(30.0);
    }

    /**
     * The property that makes running the publisher inside a multi-replica API safe. Without
     * {@code FOR UPDATE SKIP LOCKED}, concurrent pollers either read the same rows and publish each
     * message several times, or serialise behind one another's locks.
     */
    @Test
    @DisplayName("concurrent pollers publish each row exactly once")
    void concurrentPollersDoNotDuplicate() throws Exception {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded"));
        for (int i = 0; i < 10; i++) {
            postEvent("key-" + i, "payment.succeeded", "{}");
        }
        int expected = 20;
        assertThat(countUnpublishedOutboxRowsForTenant()).isEqualTo(expected);

        int pollers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(pollers);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(pollers);
        AtomicInteger totalPublished = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < pollers; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    totalPublished.addAndGet(publisher.publishBatch().published());
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    finished.countDown();
                }
            });
        }
        startGate.countDown();
        assertThat(finished.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(errors.get()).isZero();
        assertThat(totalPublished.get()).as("each row published by exactly one poller").isEqualTo(expected);
        assertThat(countUnpublishedOutboxRowsForTenant()).isZero();
        assertThat(queueDepth()).as("no duplicate messages").isEqualTo(expected);

        // Every delivery is represented exactly once on the queue.
        Set<UUID> seen = new HashSet<>();
        List<UUID> duplicates = new ArrayList<>();
        for (int i = 0; i < expected; i++) {
            Message message = rabbitTemplate.receive(RabbitTopology.DELIVERIES_QUEUE, 5000);
            assertThat(message).isNotNull();
            DeliveryMessage body = (DeliveryMessage) rabbitTemplate.getMessageConverter().fromMessage(message);
            if (!seen.add(body.deliveryId())) {
                duplicates.add(body.deliveryId());
            }
        }
        assertThat(duplicates).isEmpty();
        assertThat(seen).containsExactlyInAnyOrderElementsOf(outboxDeliveryIdsForTenant());
    }

    // ---- purge --------------------------------------------------------------------------------

    @Test
    @DisplayName("purge removes old published rows and never touches unpublished ones")
    void purgeRemovesOnlyOldPublishedRows() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        postEvent("published", "payment.succeeded", "{}");
        publisher.publishBatch();
        assertThat(countUnpublishedOutboxRowsForTenant()).isZero();

        postEvent("unpublished", "payment.succeeded", "{}");
        assertThat(countUnpublishedOutboxRowsForTenant()).isEqualTo(1);

        // Cutoff in the future, so every already-published row is eligible.
        int deleted = purge.purgeNow(OffsetDateTime.now().plusDays(1));

        assertThat(deleted).isGreaterThanOrEqualTo(1);
        assertThat(countOutboxRowsForTenant())
                .as("the unpublished row survives")
                .isEqualTo(1);
        assertThat(countUnpublishedOutboxRowsForTenant()).isEqualTo(1);
    }

    @Test
    @DisplayName("purge leaves recently published rows alone")
    void purgeKeepsRecentRows() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        postEvent("key-1", "payment.succeeded", "{}");
        publisher.publishBatch();

        int deleted = purge.purgeNow(OffsetDateTime.now().minusHours(24));

        assertThat(deleted).isZero();
        assertThat(countOutboxRowsForTenant()).isEqualTo(1);
    }
}

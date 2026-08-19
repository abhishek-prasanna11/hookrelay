package com.abhishek.hookrelay.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class IngestIntegrationTest extends AbstractIntegrationTest {

    @Test
    @DisplayName("an event with no matching endpoints is still accepted")
    void acceptsEventWithNoEndpoints() {
        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "amount": 4200}""");

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(deliveriesCreatedOf(response)).isZero();
        assertThat(countEvents()).isEqualTo(1);
        assertThat(countDeliveriesForEvent(eventIdOf(response))).isZero();
    }

    @Test
    @DisplayName("fan-out creates one PENDING delivery per matching endpoint")
    void fansOutToMatchingEndpoints() {
        UUID a = registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        UUID b = registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded", "user.created"));
        registerEndpoint("https://c.example.com/hook", List.of("user.created"));

        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1"}""");

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(deliveriesCreatedOf(response)).isEqualTo(2);

        UUID eventId = eventIdOf(response);
        assertThat(deliveryEndpointIdsForEvent(eventId))
                .containsExactlyInAnyOrder(a, b);
        assertThat(deliveryStatusesForEvent(eventId))
                .containsOnly("PENDING");
    }

    @Test
    @DisplayName("a deactivated endpoint is excluded from fan-out")
    void excludesInactiveEndpoint() {
        UUID active = registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        UUID inactive = registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded"));
        deactivateEndpoint(inactive);

        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", "{}");

        assertThat(deliveriesCreatedOf(response)).isEqualTo(1);
        assertThat(deliveryEndpointIdsForEvent(eventIdOf(response))).containsExactly(active);
    }

    @Test
    @DisplayName("event type matching is exact — no accidental prefix matching")
    void excludesNonMatchingEventType() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded.v2"));
        registerEndpoint("https://b.example.com/hook", List.of("payment"));

        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", "{}");

        assertThat(deliveriesCreatedOf(response)).isZero();
    }

    @Test
    @DisplayName("a repeated Idempotency-Key with the same body returns the original event, not a new one")
    void duplicateKeySameBodyReturnsOriginal() {
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));

        ResponseEntity<JsonNode> first = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "amount": 4200}""");
        ResponseEntity<JsonNode> second = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "amount": 4200}""");

        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(second.getStatusCode().value()).isEqualTo(200);
        assertThat(eventIdOf(second)).isEqualTo(eventIdOf(first));

        assertThat(countEvents()).isEqualTo(1);
        // The critical assertion: the retry must not have fanned out a second time.
        assertThat(countDeliveriesForEvent(eventIdOf(first))).isEqualTo(1);
        assertThat(deliveriesCreatedOf(second)).isEqualTo(1);
    }

    @Test
    @DisplayName("a repeated Idempotency-Key with a different body is refused with 409")
    void duplicateKeyDifferentBodyConflicts() {
        ResponseEntity<JsonNode> first = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "amount": 4200}""");
        ResponseEntity<JsonNode> second = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "amount": 9999}""");

        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(second.getStatusCode().value()).isEqualTo(409);
        assertThat(second.getBody().get("error").asText()).isEqualTo("idempotency_key_reused");
        assertThat(countEvents()).isEqualTo(1);
    }

    @Test
    @DisplayName("a different event_type under the same key is also a conflict")
    void duplicateKeyDifferentEventTypeConflicts() {
        postEvent("key-1", "payment.succeeded", "{}");
        ResponseEntity<JsonNode> second = postEvent("key-1", "payment.refunded", "{}");

        assertThat(second.getStatusCode().value()).isEqualTo(409);
    }

    @Test
    @DisplayName("payload key order does not affect idempotency — canonicalization works")
    void payloadKeyOrderDoesNotCauseSpuriousConflict() {
        ResponseEntity<JsonNode> first = postEvent("key-1", "payment.succeeded", """
                {"amount": 4200, "order_id": "A-1", "nested": {"b": 2, "a": 1}}""");
        ResponseEntity<JsonNode> second = postEvent("key-1", "payment.succeeded", """
                {"order_id": "A-1", "nested": {"a": 1, "b": 2}, "amount": 4200}""");

        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(second.getStatusCode().value()).isEqualTo(200);
        assertThat(eventIdOf(second)).isEqualTo(eventIdOf(first));
    }

    @Test
    @DisplayName("array order DOES affect the hash — [1,2] is not [2,1]")
    void arrayOrderIsSignificant() {
        postEvent("key-1", "payment.succeeded", """
                {"items": [1, 2]}""");
        ResponseEntity<JsonNode> second = postEvent("key-1", "payment.succeeded", """
                {"items": [2, 1]}""");

        assertThat(second.getStatusCode().value()).isEqualTo(409);
    }

    @Test
    @DisplayName("the same key under a different tenant is a different event — the constraint is composite")
    void sameKeyAcrossTenantsIsIndependent() {
        ResponseEntity<JsonNode> first = postEvent("shared-key", "payment.succeeded", "{}");

        UUID otherTenant = UUID.randomUUID();
        HttpHeaders otherHeaders = new HttpHeaders();
        otherHeaders.setContentType(MediaType.APPLICATION_JSON);
        otherHeaders.set("X-Tenant-Id", otherTenant.toString());
        otherHeaders.set("Idempotency-Key", "shared-key");
        ResponseEntity<JsonNode> second = rest.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>("""
                        {"event_type": "payment.succeeded", "payload": {}}""", otherHeaders),
                JsonNode.class);

        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(second.getStatusCode().value()).isEqualTo(202);
        assertThat(eventIdOf(second)).isNotEqualTo(eventIdOf(first));
    }

    @Test
    @DisplayName("a missing Idempotency-Key is rejected")
    void missingIdempotencyKeyIsRejected() {
        ResponseEntity<JsonNode> response = postEvent(null, "payment.succeeded", "{}");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("missing_idempotency_key");
        assertThat(countEvents()).isZero();
    }

    @Test
    @DisplayName("malformed JSON is rejected")
    void malformedJsonIsRejected() {
        ResponseEntity<JsonNode> response = postRawEvent("key-1", "{\"event_type\": \"x\", payload");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(countEvents()).isZero();
    }

    @Test
    @DisplayName("a non-object payload is rejected")
    void nonObjectPayloadIsRejected() {
        ResponseEntity<JsonNode> response = postEvent("key-1", "payment.succeeded", "\"just a string\"");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("invalid_payload");
    }

    @Test
    @DisplayName("a blank event_type is rejected")
    void blankEventTypeIsRejected() {
        ResponseEntity<JsonNode> response = postEvent("key-1", "  ", "{}");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("missing_event_type");
    }

    /**
     * The test that actually justifies the design.
     *
     * <p>Twenty threads submit the identical request simultaneously, as a producer retrying through
     * a load balancer across multiple API replicas would. Exactly one may create the event; the
     * other nineteen must observe it and return it.
     *
     * <p>This is the only test here that would catch a regression to the check-then-act form
     * (`if (!exists) insert`), and it is meaningless against anything but a real database.
     */
    @Test
    @DisplayName("20 concurrent identical submissions create exactly one event and one fan-out")
    void concurrentIdenticalSubmissionsCreateExactlyOneEvent() throws Exception {
        int threads = 20;
        registerEndpoint("https://a.example.com/hook", List.of("payment.succeeded"));
        registerEndpoint("https://b.example.com/hook", List.of("payment.succeeded"));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);

        AtomicInteger accepted = new AtomicInteger();   // 202 — created it
        AtomicInteger duplicate = new AtomicInteger();  // 200 — observed it
        AtomicInteger other = new AtomicInteger();
        var eventIds = java.util.Collections.synchronizedSet(new java.util.HashSet<UUID>());

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    ResponseEntity<JsonNode> response = postEvent("race-key", "payment.succeeded", """
                            {"order_id": "A-1", "amount": 4200}""");
                    int status = response.getStatusCode().value();
                    if (status == 202) {
                        accepted.incrementAndGet();
                        eventIds.add(eventIdOf(response));
                    } else if (status == 200) {
                        duplicate.incrementAndGet();
                        eventIds.add(eventIdOf(response));
                    } else {
                        other.incrementAndGet();
                    }
                } catch (Exception e) {
                    other.incrementAndGet();
                } finally {
                    finished.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finished.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(other.get()).as("no request should fail").isZero();
        assertThat(accepted.get()).as("exactly one caller creates the event").isEqualTo(1);
        assertThat(duplicate.get()).as("every other caller observes it").isEqualTo(threads - 1);

        assertThat(eventIds).as("all callers agree on the event id").hasSize(1);
        assertThat(countEvents()).as("exactly one event row").isEqualTo(1);

        UUID eventId = eventIds.iterator().next();
        assertThat(countDeliveriesForEvent(eventId)).as("fan-out ran exactly once").isEqualTo(2);
    }
}

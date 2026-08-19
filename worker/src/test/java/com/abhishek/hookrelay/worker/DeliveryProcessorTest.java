package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.webhook.WebhookHeaders;
import com.abhishek.hookrelay.common.webhook.WebhookSignature;
import com.abhishek.hookrelay.worker.delivery.AttemptOutcome;
import com.abhishek.hookrelay.worker.delivery.DeliveryProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryProcessorTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "test-secret-0123456789abcdef";

    @Autowired
    DeliveryProcessor processor;

    @Autowired
    ObjectMapper objectMapper;

    // ---- the happy path -----------------------------------------------------------------------

    @Test
    @DisplayName("a successful delivery reaches the endpoint and is recorded")
    void deliversSuccessfully() {
        UUID deliveryId = seedDelivery(SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome).isPresent();
        assertThat(outcome.get().succeeded()).isTrue();
        assertThat(receiver.requestCount()).isEqualTo(1);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("SUCCEEDED");
        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(1);
        assertThat(deliveryLastError(deliveryId)).isNull();

        List<Map<String, Object>> attempts = attempts(deliveryId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).get("attempt_no")).isEqualTo(1);
        assertThat(attempts.get(0).get("response_status")).isEqualTo(200);
        assertThat(attempts.get(0).get("error_class")).isNull();
    }

    @Test
    @DisplayName("the envelope nests the producer payload under data")
    void envelopeShape() throws Exception {
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        JsonNode body = objectMapper.readTree(receiver.received().get(0).body());
        assertThat(body.get("id").asText()).isEqualTo(deliveryId.toString());
        assertThat(body.has("event_id")).isTrue();
        assertThat(body.get("event_type").asText()).isEqualTo("payment.succeeded");
        assertThat(body.has("created_at")).isTrue();
        // Nested rather than merged, so a payload with its own "id" cannot collide with the envelope.
        assertThat(body.get("data").get("order_id").asText()).isEqualTo("A-1");
        assertThat(body.get("data").get("amount").asInt()).isEqualTo(4200);
    }

    @Test
    @DisplayName("identifying headers are present and the delivery id is the database id")
    void headersArePresent() {
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        assertThat(request.header(WebhookHeaders.DELIVERY_ID)).isEqualTo(deliveryId.toString());
        assertThat(request.header(WebhookHeaders.EVENT_TYPE)).isEqualTo("payment.succeeded");
        assertThat(request.header(WebhookHeaders.ATTEMPT)).isEqualTo("1");
        assertThat(request.header("content-type")).isEqualTo("application/json");
        assertThat(request.header("user-agent")).startsWith("HookRelay/");
    }

    // ---- signing ------------------------------------------------------------------------------

    @Test
    @DisplayName("the signature verifies against the bytes the receiver actually got")
    void signatureVerifies() {
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        boolean valid = WebhookSignature.verify(
                SECRET,
                request.header(WebhookHeaders.SIGNATURE),
                request.body(),
                Instant.now().getEpochSecond(),
                300);

        // Verified over the received bytes, not over a re-serialization of the payload. The payload
        // round-trips through jsonb, which reorders keys and strips whitespace, so signing anything
        // other than the transmitted bytes would fail here.
        assertThat(valid).isTrue();
    }

    @Test
    @DisplayName("a tampered body fails verification")
    void signatureCoversBody() {
        UUID deliveryId = seedDelivery(SECRET);
        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        byte[] tampered = request.bodyAsString().replace("4200", "9999").getBytes(StandardCharsets.UTF_8);

        assertThat(WebhookSignature.verify(SECRET, request.header(WebhookHeaders.SIGNATURE),
                tampered, Instant.now().getEpochSecond(), 300)).isFalse();
    }

    @Test
    @DisplayName("a shifted timestamp fails verification — replay is bounded")
    void signatureCoversTimestamp() {
        UUID deliveryId = seedDelivery(SECRET);
        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        String header = request.header(WebhookHeaders.SIGNATURE);
        String forged = header.replaceFirst("t=\\d+", "t=" + (Instant.now().getEpochSecond() + 10));

        assertThat(WebhookSignature.verify(SECRET, forged, request.body(),
                Instant.now().getEpochSecond(), 300)).isFalse();
    }

    @Test
    @DisplayName("a signature outside the tolerance window is rejected even though it is genuine")
    void signatureExpires() {
        UUID deliveryId = seedDelivery(SECRET);
        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        long muchLater = Instant.now().getEpochSecond() + 3600;

        assertThat(WebhookSignature.verify(SECRET, request.header(WebhookHeaders.SIGNATURE),
                request.body(), muchLater, 300)).isFalse();
    }

    @Test
    @DisplayName("the wrong secret fails verification")
    void signatureRequiresTheRightSecret() {
        UUID deliveryId = seedDelivery(SECRET);
        processor.process(deliveryId);
        TestWebhookReceiver.Received request = receiver.received().get(0);

        assertThat(WebhookSignature.verify("wrong-secret", request.header(WebhookHeaders.SIGNATURE),
                request.body(), Instant.now().getEpochSecond(), 300)).isFalse();
    }

    // ---- failure classification ---------------------------------------------------------------

    @Test
    @DisplayName("a 500 is retryable and leaves the delivery FAILED")
    void serverErrorIsRetryable() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.RETRYABLE);
        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
        assertThat(attempts(deliveryId).get(0).get("response_status")).isEqualTo(500);
        assertThat(attempts(deliveryId).get(0).get("error_class")).isEqualTo("HTTP_5XX");
        assertThat(deliveryLastError(deliveryId)).isEqualTo("HTTP 500");
    }

    @Test
    @DisplayName("a 400 is permanent and kills the delivery")
    void badRequestIsPermanent() {
        receiver.respondWith(400);
        UUID deliveryId = seedDelivery(SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.PERMANENT);
        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
    }

    @Test
    @DisplayName("a 429 is retryable, not permanent — it explicitly means try later")
    void tooManyRequestsIsRetryable() {
        receiver.respondWith(429);
        UUID deliveryId = seedDelivery(SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.RETRYABLE);
        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("a redirect is not followed and the target is never called")
    void redirectsAreNotFollowed() {
        receiver.redirectTo(receiver.redirectTargetUrl());
        UUID deliveryId = seedDelivery(SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.PERMANENT);
        assertThat(attempts(deliveryId).get(0).get("response_status")).isEqualTo(302);
        // Following the redirect would let a customer step around the SSRF checks in phase 5 by
        // registering a public URL that redirects to an internal address.
        assertThat(receiver.requestCount()).isEqualTo(1);
    }

    // ---- network failures ---------------------------------------------------------------------

    @Test
    @DisplayName("a refused connection is recorded with no response status")
    void connectionRefused() throws Exception {
        int deadPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            deadPort = socket.getLocalPort();
        }
        UUID deliveryId = seedDeliveryTo("http://127.0.0.1:" + deadPort + "/hook", SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.RETRYABLE);
        assertThat(outcome.get().responseStatus()).isNull();
        assertThat(attempts(deliveryId).get(0).get("response_status")).isNull();
        assertThat(attempts(deliveryId).get(0).get("error_class")).isIn("CONNECT", "IO");
    }

    @Test
    @DisplayName("an unresolvable host is recorded as a DNS failure")
    void unresolvableHost() {
        UUID deliveryId = seedDeliveryTo("http://does-not-exist.invalid/hook", SECRET);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.RETRYABLE);
        assertThat(attempts(deliveryId).get(0).get("error_class")).isIn("DNS", "CONNECT", "IO");
    }

    @Test
    @DisplayName("a slow endpoint hits the request timeout instead of holding the worker forever")
    void slowEndpointTimesOut() {
        receiver.delay(4000);   // request timeout is 1500ms in tests
        UUID deliveryId = seedDelivery(SECRET);

        long started = System.currentTimeMillis();
        Optional<AttemptOutcome> outcome = processor.process(deliveryId);
        long elapsed = System.currentTimeMillis() - started;

        assertThat(outcome.get().classification()).isEqualTo(AttemptOutcome.Classification.RETRYABLE);
        assertThat(outcome.get().errorClass()).isEqualTo("TIMEOUT");
        assertThat(elapsed).as("must not wait for the endpoint").isLessThan(3500);
        assertThat(deliveryStatus(deliveryId)).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("a huge response body is bounded, and the worker survives it")
    void hugeResponseBodyIsBounded() {
        byte[] huge = new byte[2 * 1024 * 1024];
        java.util.Arrays.fill(huge, (byte) 'x');
        receiver.respondWith(500).respondWithBody(huge);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        String stored = (String) attempts(deliveryId).get(0).get("response_body");
        assertThat(stored).isNotNull();
        // The CHECK constraint on the column would have rejected the insert otherwise.
        assertThat(stored.length()).isLessThanOrEqualTo(512);
        assertThat(stored).endsWith("...[truncated]");
    }

    // ---- redelivery safety --------------------------------------------------------------------

    @Test
    @DisplayName("a delivery that already succeeded is skipped without a second HTTP call")
    void alreadySucceededIsSkipped() {
        UUID deliveryId = seedDelivery(SECRET);
        jdbc.update("UPDATE deliveries SET status = 'SUCCEEDED' WHERE id = ?", deliveryId);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome).isEmpty();
        assertThat(receiver.requestCount())
                .as("the customer must not be called twice for one delivery")
                .isZero();
        assertThat(deliveryAttemptCount(deliveryId)).isZero();
    }

    @Test
    @DisplayName("a redelivered failure gets the next attempt number")
    void attemptNumbersIncrement() {
        receiver.respondWith(500);
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);
        processor.process(deliveryId);

        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(2);
        assertThat(attempts(deliveryId)).hasSize(2);
        assertThat(attempts(deliveryId).get(1).get("attempt_no")).isEqualTo(2);
        assertThat(receiver.received().get(1).header(WebhookHeaders.ATTEMPT)).isEqualTo("2");
    }

    @Test
    @DisplayName("a delivery whose endpoint has vanished dies without an HTTP call")
    void missingEndpointIsDead() {
        UUID endpointId = insertEndpoint(receiver.url(), SECRET);
        UUID eventId = insertEvent("payment.succeeded", "{}");
        UUID deliveryId = insertDelivery(eventId, endpointId);
        jdbc.update("DELETE FROM deliveries WHERE id = ?", deliveryId);
        jdbc.update("DELETE FROM endpoints WHERE id = ?", endpointId);

        Optional<AttemptOutcome> outcome = processor.process(deliveryId);

        assertThat(outcome).isEmpty();
        assertThat(receiver.requestCount()).isZero();
    }
}

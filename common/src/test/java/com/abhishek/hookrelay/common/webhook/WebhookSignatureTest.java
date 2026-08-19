package com.abhishek.hookrelay.common.webhook;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class WebhookSignatureTest {

    private static final String SECRET = "whsec_test";
    private static final long TIMESTAMP = 1700000000L;
    private static final byte[] BODY = "{\"hello\":\"world\"}".getBytes(StandardCharsets.UTF_8);

    /**
     * Golden vector. The same secret, timestamp and body appear in {@code tools/webhook_receiver.py}
     * as its self-test, so the two independent implementations are pinned to one specification
     * rather than to each other's behaviour. If this value changes, every receiver in the world
     * breaks — which is exactly why it is written down.
     */
    private static final String EXPECTED =
            "f592bbf3951cfc94e560eecfb5d9dd4da6b0fff2e626235f8ab4b54860925d0b";

    @Test
    @DisplayName("matches the published golden vector")
    void goldenVector() {
        assertThat(WebhookSignature.hex(SECRET, TIMESTAMP, BODY)).isEqualTo(EXPECTED);
    }

    @Test
    @DisplayName("the header carries the timestamp and the v1 signature")
    void headerFormat() {
        assertThat(WebhookSignature.header(SECRET, TIMESTAMP, BODY))
                .isEqualTo("t=" + TIMESTAMP + ",v1=" + EXPECTED);
    }

    @Test
    @DisplayName("a genuine signature verifies inside the tolerance window")
    void verifiesGenuineSignature() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);

        assertThat(WebhookSignature.verify(SECRET, header, BODY, TIMESTAMP + 60, 300)).isTrue();
    }

    @Test
    @DisplayName("verification fails outside the tolerance window — replay is bounded")
    void rejectsStaleSignature() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);

        assertThat(WebhookSignature.verify(SECRET, header, BODY, TIMESTAMP + 301, 300)).isFalse();
    }

    @Test
    @DisplayName("verification fails for a timestamp too far in the future")
    void rejectsFutureSignature() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);

        assertThat(WebhookSignature.verify(SECRET, header, BODY, TIMESTAMP - 301, 300)).isFalse();
    }

    @Test
    @DisplayName("changing the timestamp invalidates the signature")
    void timestampIsSigned() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);
        String forged = header.replace("t=" + TIMESTAMP, "t=" + (TIMESTAMP + 10));

        assertThat(WebhookSignature.verify(SECRET, forged, BODY, TIMESTAMP + 10, 300)).isFalse();
    }

    @Test
    @DisplayName("changing the body invalidates the signature")
    void bodyIsSigned() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);
        byte[] tampered = "{\"hello\":\"mars\"}".getBytes(StandardCharsets.UTF_8);

        assertThat(WebhookSignature.verify(SECRET, header, tampered, TIMESTAMP, 300)).isFalse();
    }

    @Test
    @DisplayName("the wrong secret does not verify")
    void secretIsRequired() {
        String header = WebhookSignature.header(SECRET, TIMESTAMP, BODY);

        assertThat(WebhookSignature.verify("whsec_other", header, BODY, TIMESTAMP, 300)).isFalse();
    }

    @Test
    @DisplayName("malformed headers are rejected rather than throwing")
    void rejectsMalformedHeaders() {
        assertThat(WebhookSignature.verify(SECRET, null, BODY, TIMESTAMP, 300)).isFalse();
        assertThat(WebhookSignature.verify(SECRET, "", BODY, TIMESTAMP, 300)).isFalse();
        assertThat(WebhookSignature.verify(SECRET, "garbage", BODY, TIMESTAMP, 300)).isFalse();
        assertThat(WebhookSignature.verify(SECRET, "t=abc,v1=" + EXPECTED, BODY, TIMESTAMP, 300)).isFalse();
        assertThat(WebhookSignature.verify(SECRET, "t=" + TIMESTAMP, BODY, TIMESTAMP, 300)).isFalse();
        assertThat(WebhookSignature.verify(SECRET, "v1=" + EXPECTED, BODY, TIMESTAMP, 300)).isFalse();
    }

    @Test
    @DisplayName("an empty body still signs deterministically")
    void handlesEmptyBody() {
        byte[] empty = new byte[0];

        assertThat(WebhookSignature.hex(SECRET, TIMESTAMP, empty))
                .isEqualTo(WebhookSignature.hex(SECRET, TIMESTAMP, empty));
        assertThat(WebhookSignature.verify(SECRET,
                WebhookSignature.header(SECRET, TIMESTAMP, empty), empty, TIMESTAMP, 300)).isTrue();
    }
}

package com.abhishek.hookrelay.common.webhook;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * HMAC-SHA256 request signing, in the form receivers must implement.
 *
 * <pre>
 *   X-HookRelay-Signature: t=1755624000,v1=9f8c...e21a
 *
 *   signed string = "&lt;unix_seconds&gt;" + "." + &lt;exact request body bytes&gt;
 *   v1            = lowercase hex HMAC-SHA256(endpoint secret, signed string)
 * </pre>
 *
 * <p><strong>Why an HMAC rather than a shared secret in a header.</strong> Sending the secret itself
 * hands a full copy of it to every proxy, log and TLS terminator between here and a URL the customer
 * chose. An HMAC proves possession of the key without transmitting it, and — unlike a plain hash of
 * the body — also proves the body was not altered in transit.
 *
 * <p><strong>Why the timestamp is inside the signed string.</strong> A captured request would
 * otherwise be valid forever: same bytes, same signature. Binding the timestamp into what is signed
 * means it cannot be changed without invalidating the signature, so a receiver enforcing a tolerance
 * window (±5 minutes) reduces replay to that window.
 *
 * <p><strong>Why the version prefix.</strong> {@code v1=} leaves room to introduce {@code v2} —
 * a different algorithm, or a different signed string — without breaking receivers that only
 * understand v1.
 *
 * <p>This class lives in {@code common} so the sender and the reference receivers cannot drift.
 */
public final class WebhookSignature {

    public static final String HEADER = "X-HookRelay-Signature";

    private static final String ALGORITHM = "HmacSHA256";

    private WebhookSignature() {
    }

    /**
     * @param body the <em>exact</em> bytes that will be written to the socket. Signing one
     *             serialization and sending another produces signatures that fail at the customer,
     *             intermittently, looking like a key problem rather than a serialization one.
     */
    public static String header(String secret, long timestampSeconds, byte[] body) {
        return "t=" + timestampSeconds + ",v1=" + hex(secret, timestampSeconds, body);
    }

    public static String hex(String secret, long timestampSeconds, byte[] body) {
        byte[] prefix = (timestampSeconds + ".").getBytes(StandardCharsets.UTF_8);
        byte[] signed = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, signed, 0, prefix.length);
        System.arraycopy(body, 0, signed, prefix.length, body.length);

        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            return HexFormat.of().formatHex(mac.doFinal(signed));
        } catch (Exception e) {
            throw new IllegalStateException("failed to compute " + ALGORITHM, e);
        }
    }

    /**
     * Verifies a received signature header.
     *
     * <p>The comparison uses {@link MessageDigest#isEqual}, which examines every byte. An ordinary
     * string comparison returns as soon as two bytes differ, so how long it takes reveals how many
     * leading bytes were correct — enough to recover a valid signature byte by byte given enough
     * attempts.
     *
     * @param toleranceSeconds how far the timestamp may be from now, in either direction
     */
    public static boolean verify(String secret, String headerValue, byte[] body,
                                 long nowSeconds, long toleranceSeconds) {
        if (headerValue == null) {
            return false;
        }

        Long timestamp = null;
        String provided = null;
        for (String part : headerValue.split(",")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("t=")) {
                try {
                    timestamp = Long.parseLong(trimmed.substring(2));
                } catch (NumberFormatException e) {
                    return false;
                }
            } else if (trimmed.startsWith("v1=")) {
                provided = trimmed.substring(3);
            }
        }
        if (timestamp == null || provided == null) {
            return false;
        }
        if (Math.abs(nowSeconds - timestamp) > toleranceSeconds) {
            return false;
        }

        byte[] expected = hex(secret, timestamp, body).getBytes(StandardCharsets.UTF_8);
        byte[] actual = provided.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expected, actual);
    }
}

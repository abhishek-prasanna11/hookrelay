package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.webhook.WebhookHeaders;
import com.abhishek.hookrelay.common.webhook.WebhookSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Performs one signed HTTP POST to a customer endpoint.
 *
 * <p>Every bound here exists because the destination is attacker-controlled: an endpoint can accept
 * the connection and never answer, answer one byte per minute, or return two gigabytes.
 */
@Component
public class WebhookSender {

    private static final Logger log = LoggerFactory.getLogger(WebhookSender.class);

    private final HttpClient httpClient;
    private final Duration requestTimeout;
    private final int maxResponseBytes;
    private final String userAgent;

    public WebhookSender(@Value("${hookrelay.delivery.connect-timeout-ms}") long connectTimeoutMs,
                         @Value("${hookrelay.delivery.request-timeout-ms}") long requestTimeoutMs,
                         @Value("${hookrelay.delivery.max-response-bytes}") int maxResponseBytes,
                         @Value("${hookrelay.delivery.user-agent}") String userAgent) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                // Redirects are refused rather than followed. A customer could otherwise register a
                // public URL that passes validation and redirect to an internal address, using the
                // HTTP client to step around the SSRF checks that arrive in phase 5. Following them
                // safely would mean re-validating every hop.
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.requestTimeout = Duration.ofMillis(requestTimeoutMs);
        this.maxResponseBytes = maxResponseBytes;
        this.userAgent = userAgent;
    }

    public AttemptOutcome send(String url, String secret, byte[] body,
                               UUID deliveryId, UUID eventId, String eventType, int attemptNo) {
        long startedNanos = System.nanoTime();
        long timestampSeconds = Instant.now().getEpochSecond();

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(requestTimeout)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", userAgent)
                    .header(WebhookHeaders.DELIVERY_ID, deliveryId.toString())
                    .header(WebhookHeaders.EVENT_ID, eventId.toString())
                    .header(WebhookHeaders.EVENT_TYPE, eventType)
                    .header(WebhookHeaders.ATTEMPT, Integer.toString(attemptNo))
                    // Signed over the exact bytes being sent, never a re-serialization of them.
                    .header(WebhookHeaders.SIGNATURE,
                            WebhookSignature.header(secret, timestampSeconds, body))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
        } catch (IllegalArgumentException e) {
            return new AttemptOutcome(AttemptOutcome.Classification.PERMANENT, null,
                    "INVALID_URL", null, elapsedMs(startedNanos));
        }

        try {
            HttpResponse<InputStream> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            String snippet = readBounded(response.body());
            int status = response.statusCode();

            AttemptOutcome.Classification classification = FailureClassifier.classifyStatus(status);
            String errorClass = classification == AttemptOutcome.Classification.SUCCESS
                    ? null
                    : FailureClassifier.errorClassForStatus(status);

            return new AttemptOutcome(classification, status, errorClass, snippet, elapsedMs(startedNanos));

        } catch (HttpConnectTimeoutException e) {
            return failure("CONNECT_TIMEOUT", startedNanos);
        } catch (HttpTimeoutException e) {
            return failure("TIMEOUT", startedNanos);
        } catch (ConnectException e) {
            return failure("CONNECT", startedNanos);
        } catch (UnknownHostException e) {
            return failure("DNS", startedNanos);
        } catch (javax.net.ssl.SSLException e) {
            return failure("TLS", startedNanos);
        } catch (IOException e) {
            return failure("IO", startedNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failure("INTERRUPTED", startedNanos);
        }
    }

    /**
     * Reads at most {@code maxResponseBytes} of the response and discards the rest by closing the
     * stream.
     *
     * <p>{@code BodyHandlers.ofString()} and {@code ofByteArray()} buffer the whole body, so a
     * hostile endpoint returning a multi-gigabyte response would take the worker down with an
     * {@code OutOfMemoryError}. Java 21 has no built-in limiting body handler, hence the stream.
     *
     * <p>One extra byte is read beyond the cap purely to detect that truncation happened, so the
     * stored snippet can say so.
     */
    private String readBounded(InputStream body) {
        try (InputStream stream = body) {
            byte[] bytes = stream.readNBytes(maxResponseBytes + 1);
            if (bytes.length == 0) {
                return null;
            }
            boolean truncated = bytes.length > maxResponseBytes;
            String text = new String(bytes, 0, Math.min(bytes.length, maxResponseBytes),
                    StandardCharsets.UTF_8);
            if (!truncated) {
                return text;
            }
            // Keep the result within the column's CHECK constraint even after adding the marker.
            String marker = "...[truncated]";
            int keep = Math.max(0, maxResponseBytes - marker.length());
            return text.substring(0, Math.min(text.length(), keep)) + marker;
        } catch (IOException e) {
            log.debug("failed reading response body", e);
            return null;
        }
    }

    private static AttemptOutcome failure(String errorClass, long startedNanos) {
        return new AttemptOutcome(AttemptOutcome.Classification.RETRYABLE, null, errorClass, null,
                elapsedMs(startedNanos));
    }

    private static int elapsedMs(long startedNanos) {
        return (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}

package com.abhishek.hookrelay.worker.delivery;

/**
 * The result of one HTTP attempt.
 *
 * @param classification whether this outcome is worth another attempt
 * @param responseStatus null when no HTTP response ever arrived (timeout, connect failure, DNS)
 * @param errorClass     a short, low-cardinality label — {@code TIMEOUT}, {@code CONNECT},
 *                       {@code DNS}, {@code TLS}, {@code HTTP_5XX} — safe to use as a metric label,
 *                       unlike a raw exception message
 * @param responseBody   truncated to the configured cap; never the whole response
 */
public record AttemptOutcome(
        Classification classification,
        Integer responseStatus,
        String errorClass,
        String responseBody,
        int durationMs
) {

    public enum Classification {
        /** 2xx. Delivered. */
        SUCCESS,
        /** The endpoint is unavailable or overloaded; a later attempt has a real chance. */
        RETRYABLE,
        /** The endpoint understood the request and refused it. Repeating changes nothing. */
        PERMANENT
    }

    public boolean succeeded() {
        return classification == Classification.SUCCESS;
    }
}

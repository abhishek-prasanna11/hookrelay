package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.worker.delivery.AttemptOutcome.Classification;

/**
 * Decides whether an outcome is worth another attempt.
 *
 * <p>The distinction matters because retrying costs the customer as well as us. Repeating a
 * {@code 400 Bad Request} eight times over eleven hours achieves nothing except load on someone who
 * has already said the request is wrong.
 *
 * <p>Two placements are deliberate and easy to get backwards:
 * <ul>
 *   <li><strong>429 is retryable.</strong> It is a 4xx, but it explicitly means "try later" — the
 *       one 4xx where repeating is exactly what the endpoint asked for. 408 likewise.</li>
 *   <li><strong>3xx is permanent.</strong> Redirects are not followed at all, so a 3xx is an
 *       endpoint asking for something this client will never do. See
 *       docs/phase03-worker.md §3.4 for why following them would defeat the SSRF checks.</li>
 * </ul>
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    public static Classification classifyStatus(int status) {
        if (status >= 200 && status < 300) {
            return Classification.SUCCESS;
        }
        if (status >= 500) {
            return Classification.RETRYABLE;
        }
        if (status == 429 || status == 408) {
            return Classification.RETRYABLE;
        }
        return Classification.PERMANENT;
    }

    /** Low-cardinality label for an HTTP response. Safe as a metric dimension. */
    public static String errorClassForStatus(int status) {
        if (status >= 500) {
            return "HTTP_5XX";
        }
        if (status == 429) {
            return "HTTP_429";
        }
        if (status >= 300 && status < 400) {
            return "HTTP_3XX";
        }
        return "HTTP_4XX";
    }
}

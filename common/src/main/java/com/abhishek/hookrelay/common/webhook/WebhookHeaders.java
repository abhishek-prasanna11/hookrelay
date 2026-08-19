package com.abhishek.hookrelay.common.webhook;

/** Header names sent with every webhook. Shared so senders and receivers cannot disagree. */
public final class WebhookHeaders {

    /**
     * Stable across every retry of the same delivery. This is the key a receiver deduplicates on,
     * and it is the only thing that makes at-least-once delivery workable for them.
     */
    public static final String DELIVERY_ID = "X-HookRelay-Delivery-Id";

    public static final String EVENT_ID = "X-HookRelay-Event-Id";

    /** Lets a receiver route the request without parsing the body. */
    public static final String EVENT_TYPE = "X-HookRelay-Event-Type";

    /** 1-based. Present so a receiver's logs show that a repeat was a retry, not a new event. */
    public static final String ATTEMPT = "X-HookRelay-Attempt";

    public static final String SIGNATURE = WebhookSignature.HEADER;

    private WebhookHeaders() {
    }
}

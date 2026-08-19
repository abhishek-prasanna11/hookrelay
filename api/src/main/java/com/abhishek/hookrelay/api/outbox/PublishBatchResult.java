package com.abhishek.hookrelay.api.outbox;

/**
 * @param lagSeconds age of the oldest row in the batch. Computed from the batch itself rather than
 *                   a separate {@code min(created_at)} query, so measuring outbox lag costs nothing.
 */
public record PublishBatchResult(int published, int failed, double lagSeconds) {

    static PublishBatchResult empty() {
        return new PublishBatchResult(0, 0, 0.0);
    }
}

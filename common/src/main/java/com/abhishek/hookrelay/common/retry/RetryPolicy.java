package com.abhishek.hookrelay.common.retry;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The retry schedule: bounded exponential backoff with jitter.
 *
 * <pre>
 *   attempt 1  immediate
 *   attempt 2  +5s        cumulative      5s
 *   attempt 3  +30s                      35s
 *   attempt 4  +2m                     2m 35s
 *   attempt 5  +10m                   12m 35s
 *   attempt 6  +30m                   42m 35s
 *   attempt 7  +1h                     1h 42m
 *   attempt 8  +3h                     4h 42m
 *   → dead
 * </pre>
 *
 * <p><strong>Bounded on purpose.</strong> Retrying forever is not persistence, it is a slow leak of
 * capacity into an endpoint that is never coming back. After the eighth attempt the delivery is
 * dead and says why.
 *
 * <p><strong>Jitter on purpose.</strong> When an endpoint goes down, every delivery to it fails
 * within the same second. Without jitter every one of them retries in the <em>same</em> second five
 * seconds later — a synchronised spike aimed at a service that was just recovering. Multiplying each
 * delay by a random factor in [0.8, 1.2] smears them across a window instead.
 */
public final class RetryPolicy {

    /** Attempt 1 plus seven retries — one per {@link RetryTier}. */
    public static final int MAX_ATTEMPTS = 8;

    public static final double MIN_JITTER_FACTOR = 0.8;
    public static final double MAX_JITTER_FACTOR = 1.2;

    private RetryPolicy() {
    }

    /**
     * @param completedAttempts how many attempts have been made, including the one that just failed
     * @return the tier to schedule the next attempt in, or empty when the schedule is exhausted and
     *         the delivery should be dead-lettered
     */
    public static Optional<RetryTier> tierAfter(int completedAttempts) {
        if (completedAttempts < 1 || completedAttempts >= MAX_ATTEMPTS) {
            return Optional.empty();
        }
        return Optional.of(RetryTier.values()[completedAttempts - 1]);
    }

    public static boolean attemptsExhausted(int completedAttempts) {
        return completedAttempts >= MAX_ATTEMPTS;
    }

    /** The tier's nominal delay multiplied by a uniform random factor in [0.8, 1.2]. */
    public static long jitteredDelayMillis(RetryTier tier) {
        double factor = ThreadLocalRandom.current().nextDouble(MIN_JITTER_FACTOR, MAX_JITTER_FACTOR);
        return Math.round(tier.nominalDelay().toMillis() * factor);
    }
}

package com.abhishek.hookrelay.common.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    @DisplayName("each completed attempt maps to the next tier on the ladder")
    void tierLadder() {
        assertThat(RetryPolicy.tierAfter(1)).contains(RetryTier.T5S);
        assertThat(RetryPolicy.tierAfter(2)).contains(RetryTier.T30S);
        assertThat(RetryPolicy.tierAfter(3)).contains(RetryTier.T2M);
        assertThat(RetryPolicy.tierAfter(4)).contains(RetryTier.T10M);
        assertThat(RetryPolicy.tierAfter(5)).contains(RetryTier.T30M);
        assertThat(RetryPolicy.tierAfter(6)).contains(RetryTier.T1H);
        assertThat(RetryPolicy.tierAfter(7)).contains(RetryTier.T3H);
    }

    @Test
    @DisplayName("the schedule is bounded — the eighth attempt has no tier after it")
    void scheduleIsBounded() {
        assertThat(RetryPolicy.tierAfter(8)).isEmpty();
        assertThat(RetryPolicy.tierAfter(9)).isEmpty();
        assertThat(RetryPolicy.attemptsExhausted(8)).isTrue();
        assertThat(RetryPolicy.attemptsExhausted(7)).isFalse();
    }

    @Test
    @DisplayName("there is exactly one tier per retry")
    void tierCountMatchesSchedule() {
        assertThat(RetryTier.values()).hasSize(RetryPolicy.MAX_ATTEMPTS - 1);
    }

    @Test
    @DisplayName("a nonsensical attempt number yields no tier rather than an exception")
    void rejectsInvalidAttemptNumbers() {
        assertThat(RetryPolicy.tierAfter(0)).isEmpty();
        assertThat(RetryPolicy.tierAfter(-1)).isEmpty();
    }

    @Test
    @DisplayName("jitter stays within ±20% of the nominal delay")
    void jitterIsBounded() {
        for (RetryTier tier : RetryTier.values()) {
            long nominal = tier.nominalDelay().toMillis();
            for (int i = 0; i < 200; i++) {
                long delay = RetryPolicy.jitteredDelayMillis(tier);
                assertThat(delay)
                        .as("%s sample %d", tier, i)
                        .isBetween(Math.round(nominal * 0.8), Math.round(nominal * 1.2));
            }
        }
    }

    @Test
    @DisplayName("jitter actually varies — otherwise the thundering herd is unchanged")
    void jitterVaries() {
        Set<Long> samples = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            samples.add(RetryPolicy.jitteredDelayMillis(RetryTier.T30M));
        }

        assertThat(samples).hasSizeGreaterThan(100);
    }

    @Test
    @DisplayName("each tier's queue-level TTL covers the top of its jitter range")
    void queueTtlCoversJitter() {
        // A queue TTL below the jitter maximum would silently expire messages early, firing retries
        // ahead of schedule; this is the backstop, not the schedule.
        for (RetryTier tier : RetryTier.values()) {
            assertThat(tier.maxTtl().toMillis())
                    .as("%s", tier)
                    .isGreaterThanOrEqualTo(Math.round(tier.nominalDelay().toMillis() * 1.2));
        }
    }

    @Test
    @DisplayName("the ladder is strictly increasing")
    void delaysIncrease() {
        RetryTier[] tiers = RetryTier.values();
        for (int i = 1; i < tiers.length; i++) {
            assertThat(tiers[i].nominalDelay())
                    .isGreaterThan(tiers[i - 1].nominalDelay());
        }
    }

    @Test
    @DisplayName("the full schedule spans a little under five hours")
    void totalWindow() {
        long totalMillis = 0;
        for (int attempt = 1; attempt < RetryPolicy.MAX_ATTEMPTS; attempt++) {
            Optional<RetryTier> tier = RetryPolicy.tierAfter(attempt);
            totalMillis += tier.orElseThrow().nominalDelay().toMillis();
        }

        assertThat(totalMillis).isEqualTo(
                (5 + 30) * 1000L + (2 + 10 + 30) * 60_000L + (1 + 3) * 3_600_000L);
    }
}

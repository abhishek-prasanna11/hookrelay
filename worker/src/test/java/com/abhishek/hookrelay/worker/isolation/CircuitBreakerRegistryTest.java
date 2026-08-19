package com.abhishek.hookrelay.worker.isolation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CircuitBreakerRegistryTest {

    /** A clock the test moves by hand, so cooldown behaviour is tested without sleeping. */
    private static final class MutableClock extends Clock {
        private long millis = 1_700_000_000_000L;

        void advance(long by) {
            millis += by;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static final int THRESHOLD = 3;
    private static final long COOLDOWN_MS = 30_000;

    private MutableClock clock;
    private CircuitBreakerRegistry breakers;
    private UUID endpoint;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        breakers = new CircuitBreakerRegistry(THRESHOLD, COOLDOWN_MS, clock);
        endpoint = UUID.randomUUID();
    }

    private void fail(int times) {
        for (int i = 0; i < times; i++) {
            assertThat(breakers.tryPermit(endpoint)).isTrue();
            breakers.recordFailure(endpoint);
        }
    }

    @Test
    @DisplayName("an unknown endpoint starts closed and permits traffic")
    void startsClosed() {
        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);
        assertThat(breakers.tryPermit(endpoint)).isTrue();
    }

    @Test
    @DisplayName("the breaker opens on the Nth consecutive failure, not before")
    void opensAtThreshold() {
        fail(THRESHOLD - 1);
        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);

        fail(1);
        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.OPEN);
        assertThat(breakers.tryPermit(endpoint)).isFalse();
    }

    @Test
    @DisplayName("failures must be consecutive — a success in between resets the count")
    void successResetsTheCount() {
        fail(THRESHOLD - 1);
        breakers.recordSuccess(endpoint);
        fail(THRESHOLD - 1);

        // An endpoint that fails occasionally and recovers is not down, and must not be shut off.
        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);
    }

    @Test
    @DisplayName("while open, no traffic passes until the cooldown elapses")
    void staysOpenForTheCooldown() {
        fail(THRESHOLD);

        clock.advance(COOLDOWN_MS - 1);
        assertThat(breakers.tryPermit(endpoint)).isFalse();
    }

    @Test
    @DisplayName("after the cooldown exactly one probe is allowed through")
    void allowsExactlyOneProbe() {
        fail(THRESHOLD);
        clock.advance(COOLDOWN_MS);

        assertThat(breakers.tryPermit(endpoint)).as("the probe").isTrue();
        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.HALF_OPEN);

        // This is what makes it a breaker rather than a mute button: recovery costs one request, not
        // a flood aimed at a service that has only just come back.
        assertThat(breakers.tryPermit(endpoint)).as("everything else").isFalse();
        assertThat(breakers.tryPermit(endpoint)).isFalse();
    }

    @Test
    @DisplayName("a successful probe closes the breaker")
    void successfulProbeCloses() {
        fail(THRESHOLD);
        clock.advance(COOLDOWN_MS);
        breakers.tryPermit(endpoint);

        breakers.recordSuccess(endpoint);

        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);
        assertThat(breakers.tryPermit(endpoint)).isTrue();
    }

    @Test
    @DisplayName("a failed probe reopens the breaker with a fresh cooldown")
    void failedProbeReopens() {
        fail(THRESHOLD);
        clock.advance(COOLDOWN_MS);
        breakers.tryPermit(endpoint);

        breakers.recordFailure(endpoint);

        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.OPEN);
        assertThat(breakers.tryPermit(endpoint)).isFalse();

        clock.advance(COOLDOWN_MS - 1);
        assertThat(breakers.tryPermit(endpoint)).as("cooldown restarted, not resumed").isFalse();

        clock.advance(1);
        assertThat(breakers.tryPermit(endpoint)).isTrue();
    }

    @Test
    @DisplayName("breakers are independent per endpoint")
    void breakersAreIndependent() {
        UUID other = UUID.randomUUID();
        fail(THRESHOLD);

        assertThat(breakers.stateOf(endpoint)).isEqualTo(CircuitBreakerRegistry.State.OPEN);
        assertThat(breakers.stateOf(other)).isEqualTo(CircuitBreakerRegistry.State.CLOSED);
        assertThat(breakers.tryPermit(other)).isTrue();
    }
}

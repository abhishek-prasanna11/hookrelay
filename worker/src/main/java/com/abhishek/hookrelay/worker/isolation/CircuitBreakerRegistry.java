package com.abhishek.hookrelay.worker.isolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A per-endpoint circuit breaker, so a worker stops paying the full timeout for an endpoint it
 * already knows is down.
 *
 * <pre>
 *        ┌──────────┐  N consecutive failures   ┌────────┐
 *        │  CLOSED  │ ─────────────────────────►│  OPEN  │
 *        └──────────┘                           └───┬────┘
 *             ▲                        cooldown     │
 *             │ probe succeeds              elapses │
 *             │                                     ▼
 *             │                              ┌─────────────┐
 *             └──────────────────────────────│  HALF_OPEN  │
 *                     probe fails            │ (one probe) │
 *                                            └─────────────┘
 * </pre>
 *
 * <p>{@code HALF_OPEN} is what makes this a breaker rather than a mute button: after the cooldown
 * exactly <em>one</em> delivery is let through. Success closes the breaker, failure reopens it with a
 * fresh cooldown, so recovery is automatic and costs one request instead of a flood.
 *
 * <p>State is worker-local and in-memory. With <em>W</em> workers it takes {@code W × threshold}
 * failures to shut an endpoint off everywhere. A shared breaker needs Redis and a distributed state
 * machine — dramatically more complexity for a modest gain, hence BLUEPRINT.md §32.
 */
@Component
public class CircuitBreakerRegistry {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerRegistry.class);

    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private static final class Breaker {
        int consecutiveFailures;
        long openedAtMillis;
        boolean probeInFlight;
        State state = State.CLOSED;
    }

    private final Map<UUID, Breaker> byEndpoint = new ConcurrentHashMap<>();
    private final int failureThreshold;
    private final long cooldownMillis;
    private final Clock clock;

    // Explicit, because the test-seam constructor below means Spring cannot choose on its own.
    @Autowired
    public CircuitBreakerRegistry(
            @Value("${hookrelay.circuit-breaker.failure-threshold}") int failureThreshold,
            @Value("${hookrelay.circuit-breaker.cooldown-ms}") long cooldownMillis) {
        this(failureThreshold, cooldownMillis, Clock.systemUTC());
    }

    /** Test seam: lets a test move the clock rather than sleep through the cooldown. */
    CircuitBreakerRegistry(int failureThreshold, long cooldownMillis, Clock clock) {
        this.failureThreshold = failureThreshold;
        this.cooldownMillis = cooldownMillis;
        this.clock = clock;
    }

    /**
     * @return true if this delivery may contact the endpoint. Exactly one caller gets true while the
     *         breaker is half-open — the probe.
     */
    public boolean tryPermit(UUID endpointId) {
        Breaker breaker = byEndpoint.computeIfAbsent(endpointId, id -> new Breaker());
        synchronized (breaker) {
            switch (breaker.state) {
                case CLOSED:
                    return true;
                case OPEN:
                    if (clock.millis() - breaker.openedAtMillis < cooldownMillis) {
                        return false;
                    }
                    breaker.state = State.HALF_OPEN;
                    breaker.probeInFlight = true;
                    log.info("circuit breaker for endpoint {} half-open, probing", endpointId);
                    return true;
                case HALF_OPEN:
                    // Only the probe proceeds; everything else fails fast until it reports back.
                    if (breaker.probeInFlight) {
                        return false;
                    }
                    breaker.probeInFlight = true;
                    return true;
                default:
                    return true;
            }
        }
    }

    public void recordSuccess(UUID endpointId) {
        Breaker breaker = byEndpoint.computeIfAbsent(endpointId, id -> new Breaker());
        synchronized (breaker) {
            // Consecutive, not cumulative: an endpoint that fails occasionally and recovers should
            // never trip. Only an unbroken run of failures indicates it is actually down.
            breaker.consecutiveFailures = 0;
            breaker.probeInFlight = false;
            if (breaker.state != State.CLOSED) {
                log.info("circuit breaker for endpoint {} closed", endpointId);
            }
            breaker.state = State.CLOSED;
        }
    }

    public void recordFailure(UUID endpointId) {
        Breaker breaker = byEndpoint.computeIfAbsent(endpointId, id -> new Breaker());
        synchronized (breaker) {
            breaker.probeInFlight = false;
            if (breaker.state == State.HALF_OPEN) {
                breaker.state = State.OPEN;
                breaker.openedAtMillis = clock.millis();
                log.info("circuit breaker for endpoint {} reopened after a failed probe", endpointId);
                return;
            }
            breaker.consecutiveFailures++;
            if (breaker.state == State.CLOSED && breaker.consecutiveFailures >= failureThreshold) {
                breaker.state = State.OPEN;
                breaker.openedAtMillis = clock.millis();
                log.warn("circuit breaker for endpoint {} opened after {} consecutive failures",
                        endpointId, breaker.consecutiveFailures);
            }
        }
    }

    public State stateOf(UUID endpointId) {
        Breaker breaker = byEndpoint.get(endpointId);
        if (breaker == null) {
            return State.CLOSED;
        }
        synchronized (breaker) {
            return breaker.state;
        }
    }

    /**
     * How many endpoints are in each state.
     *
     * <p>Exposed this way rather than per endpoint because a metric labelled by endpoint id creates
     * one time series per customer — see docs/phase06-observability.md §1.3.
     */
    public Map<State, Integer> countByState() {
        Map<State, Integer> counts = new java.util.EnumMap<>(State.class);
        for (Breaker breaker : byEndpoint.values()) {
            State state;
            synchronized (breaker) {
                state = breaker.state;
            }
            counts.merge(state, 1, Integer::sum);
        }
        return counts;
    }

    /** Test seam. */
    void reset() {
        byEndpoint.clear();
    }
}

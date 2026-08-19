package com.abhishek.hookrelay.worker.delivery;

import com.abhishek.hookrelay.common.domain.DeliveryStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The worker's database writes, as plain SQL.
 *
 * <p>Deliberately not JPA. Each of these is a single statement on the per-attempt hot path, and the
 * important one needs {@code UPDATE ... RETURNING}, which the entity lifecycle only gets in the way
 * of expressing.
 */
@Component
public class DeliveryStore {

    private final JdbcTemplate jdbc;

    public DeliveryStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims the next attempt on a delivery, atomically.
     *
     * <p>One statement does three jobs that would otherwise be a check-then-act race between two
     * workers handling the same redelivered message:
     *
     * <ul>
     *   <li>{@code AND status <> 'SUCCEEDED'} refuses the claim if the delivery already succeeded.
     *       An empty result means "someone already did this" — acknowledge and skip.</li>
     *   <li>The {@code UPDATE} takes a row lock for its duration, so a concurrent worker on the same
     *       row waits and then sees the updated state rather than reading a stale one.</li>
     *   <li>{@code RETURNING attempt_count} yields an attempt number that is unique per increment
     *       even under concurrency, which is what keeps {@code delivery_attempts}'s
     *       {@code UNIQUE (delivery_id, attempt_no)} a safety net rather than a source of spurious
     *       failures.</li>
     * </ul>
     *
     * @return the claimed attempt number, or empty if the delivery has already succeeded or does
     *         not exist
     */
    @Transactional
    public Optional<Integer> claimAttempt(UUID deliveryId) {
        List<Integer> claimed = jdbc.queryForList("""
                UPDATE deliveries
                   SET status = 'IN_FLIGHT',
                       attempt_count = attempt_count + 1,
                       updated_at = now()
                 WHERE id = ?
                   AND status <> 'SUCCEEDED'
                RETURNING attempt_count
                """, Integer.class, deliveryId);
        return claimed.isEmpty() ? Optional.empty() : Optional.of(claimed.get(0));
    }

    /**
     * Records the attempt and moves the delivery to its resulting state, in one transaction, so an
     * attempt row can never exist without the matching status change.
     */
    @Transactional
    public void recordAttempt(UUID deliveryId, int attemptNo, OffsetDateTime startedAt,
                              AttemptOutcome outcome, DeliveryStatus newStatus) {
        jdbc.update("""
                INSERT INTO delivery_attempts
                    (delivery_id, attempt_no, started_at, duration_ms, response_status, error_class, response_body)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                deliveryId,
                attemptNo,
                startedAt,
                outcome.durationMs(),
                outcome.responseStatus(),
                outcome.errorClass(),
                outcome.responseBody());

        jdbc.update("""
                UPDATE deliveries
                   SET status = ?,
                       last_error = ?,
                       updated_at = now()
                 WHERE id = ?
                """,
                newStatus.name(),
                describeError(outcome),
                deliveryId);
    }

    /** Marks a delivery dead without an HTTP attempt — used when its endpoint or event is gone. */
    @Transactional
    public void markDeadWithoutAttempt(UUID deliveryId, String reason) {
        jdbc.update("""
                UPDATE deliveries
                   SET status = 'DEAD',
                       last_error = ?,
                       updated_at = now()
                 WHERE id = ?
                """, reason, deliveryId);
    }

    private static String describeError(AttemptOutcome outcome) {
        if (outcome.succeeded()) {
            return null;
        }
        if (outcome.responseStatus() != null) {
            return "HTTP " + outcome.responseStatus();
        }
        return outcome.errorClass();
    }
}

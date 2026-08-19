package com.abhishek.hookrelay.api.service;

import java.sql.SQLException;
import java.util.Locale;

/**
 * Identifies <em>which</em> database constraint a failed write violated.
 *
 * <p>This distinction matters. The ingest path deliberately lets PostgreSQL reject duplicate
 * idempotency keys instead of checking for them first (see docs/phase01-ingest.md section 1.6), so
 * it has to catch the resulting exception. But catching every integrity violation and treating it
 * as "duplicate key" would misreport a foreign-key failure, a CHECK failure, or the
 * {@code deliveries_event_endpoint_uq} safety net as a successful deduplication — turning a real
 * bug into a silent 200.
 *
 * <p>The cause chain is walked rather than inspected at one level because the exception arrives
 * wrapped differently depending on whether it surfaced through a Spring Data repository, through
 * {@code EntityManager.flush()}, or at commit.
 */
public final class Constraints {

    /** PostgreSQL SQLSTATE for unique_violation. */
    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    private Constraints() {
    }

    public static boolean isViolationOf(Throwable throwable, String constraintName) {
        for (Throwable c = throwable; c != null; c = c.getCause()) {

            if (c instanceof org.hibernate.exception.ConstraintViolationException hibernateViolation) {
                String name = hibernateViolation.getConstraintName();
                if (name != null && name.equalsIgnoreCase(constraintName)) {
                    return true;
                }
            }

            // Fallback for drivers or paths where Hibernate could not extract the constraint name.
            if (c instanceof SQLException sqlException
                    && SQLSTATE_UNIQUE_VIOLATION.equals(sqlException.getSQLState())) {
                String message = sqlException.getMessage();
                if (message != null
                        && message.toLowerCase(Locale.ROOT).contains(constraintName.toLowerCase(Locale.ROOT))) {
                    return true;
                }
            }

            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }
}

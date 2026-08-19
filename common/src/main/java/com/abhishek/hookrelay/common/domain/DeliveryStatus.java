package com.abhishek.hookrelay.common.domain;

/**
 * Lifecycle of a single delivery — one event's obligation to one endpoint.
 *
 * <pre>
 *                      ┌──────────────► SUCCEEDED   (2xx received)
 *                      │
 *   PENDING ──► IN_FLIGHT
 *      ▲               │
 *      │               ├──────────────► DEAD        (attempts exhausted, or permanent failure)
 *      │               │
 *      └───── FAILED ◄─┘                            (retryable failure, awaiting next attempt)
 * </pre>
 *
 * <p>{@code SUCCEEDED} and {@code DEAD} are terminal. Blueprint section 1 requires that every
 * accepted delivery eventually reaches one of them — never silently disappears.
 *
 * <p>Phase 1 only ever creates rows in {@code PENDING}; the rest of the machine arrives in phases
 * 3 and 4.
 */
public enum DeliveryStatus {
    PENDING,
    IN_FLIGHT,
    SUCCEEDED,
    FAILED,
    DEAD
}

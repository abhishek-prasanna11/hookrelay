package com.abhishek.hookrelay.common.repo;

import com.abhishek.hookrelay.common.domain.Event;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface EventRepository extends JpaRepository<Event, UUID> {

    /**
     * Reads back the row that won an idempotency race. Called only after a unique violation on
     * {@code events_idempotency_uq}, in a fresh transaction — the transaction that hit the
     * violation is aborted and cannot run further statements.
     */
    Optional<Event> findByTenantIdAndIdempotencyKey(UUID tenantId, String idempotencyKey);

    Optional<Event> findByIdAndTenantId(UUID id, UUID tenantId);
}

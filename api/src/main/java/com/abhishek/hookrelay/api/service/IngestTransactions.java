package com.abhishek.hookrelay.api.service;

import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.Endpoint;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.domain.OutboxEvent;
import com.abhishek.hookrelay.common.repo.EndpointRepository;
import com.abhishek.hookrelay.common.util.Uuid7;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The one transactional unit of ingest, kept in its own bean.
 *
 * <p>It is separate from {@link IngestService} for a mundane but load-bearing reason: Spring's
 * {@code @Transactional} works by proxying the bean, so a call from one method of a bean to another
 * method of the <em>same</em> bean bypasses the proxy entirely and runs with no transaction at all.
 * {@code IngestService} needs to call this transactionally and then, if it fails, read in a
 * <em>different</em> transaction — which is only possible across a bean boundary.
 *
 * <p>Annotated {@code @Repository} so Spring's persistence-exception translation applies to the
 * raw {@link EntityManager} use here, converting Hibernate exceptions into the
 * {@code DataAccessException} hierarchy.
 */
@Repository
public class IngestTransactions {

    @PersistenceContext
    private EntityManager entityManager;

    private final EndpointRepository endpoints;

    public IngestTransactions(EndpointRepository endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Persists the event and one delivery per matching endpoint, atomically.
     *
     * <p>The whole point of the single transaction: an event that is visible but is missing one of
     * its delivery obligations would be silently under-delivered forever, with nothing recording
     * that an endpoint was ever owed anything.
     *
     * @throws org.springframework.dao.DataIntegrityViolationException if the idempotency key
     *         already exists — the caller is expected to catch this and resolve the duplicate.
     */
    @Transactional
    public IngestResult createEventAndFanOut(UUID tenantId, String idempotencyKey, String eventType,
                                             JsonNode payload, String requestHash) {
        OffsetDateTime now = OffsetDateTime.now();
        UUID eventId = Uuid7.generate();

        entityManager.persist(new Event(eventId, tenantId, eventType, payload, idempotencyKey, requestHash, now));

        // Flushed before fan-out so a duplicate idempotency key is rejected immediately, rather
        // than after we have already done N delivery inserts that will be rolled back anyway.
        entityManager.flush();

        List<Endpoint> matching = endpoints.findMatching(tenantId, eventType);

        List<UUID> deliveryIds = new ArrayList<>(matching.size());
        for (Endpoint endpoint : matching) {
            UUID deliveryId = Uuid7.generate();
            deliveryIds.add(deliveryId);
            entityManager.persist(new Delivery(deliveryId, eventId, endpoint.getId(), now));
        }
        // Deliveries are flushed before the outbox rows that reference them. Hibernate would
        // probably order these correctly on its own, but "probably" is not what a foreign key
        // deserves — being explicit costs one extra round trip and removes the question.
        entityManager.flush();

        for (UUID deliveryId : deliveryIds) {
            entityManager.persist(new OutboxEvent(Uuid7.generate(), deliveryId, now));
        }
        entityManager.flush();

        return new IngestResult(eventId, matching.size(), true);
    }
}

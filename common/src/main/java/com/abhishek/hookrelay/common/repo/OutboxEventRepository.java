package com.abhishek.hookrelay.common.repo;

import com.abhishek.hookrelay.common.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    Optional<OutboxEvent> findByDeliveryId(UUID deliveryId);

    /**
     * Claims a batch of unpublished rows for this poller.
     *
     * <p>{@code FOR UPDATE} locks the returned rows for the duration of the transaction.
     * {@code SKIP LOCKED} is what makes this usable from more than one replica: instead of waiting
     * for rows another poller already holds — which would serialise every replica behind the
     * slowest one — it silently returns the next unlocked rows instead. That turns an ordinary
     * table into a work queue several processes can drain in parallel, with no external lock
     * service and no leader election.
     */
    @Query(value = """
            SELECT * FROM outbox_events
             WHERE published_at IS NULL
             ORDER BY created_at
             LIMIT :batchSize
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> lockUnpublishedBatch(@Param("batchSize") int batchSize);

    @Modifying
    @Query(value = "UPDATE outbox_events SET published_at = now() WHERE id IN (:ids)",
            nativeQuery = true)
    int markPublished(@Param("ids") List<UUID> ids);

    @Modifying
    @Query(value = """
            UPDATE outbox_events
               SET attempt_count = attempt_count + 1,
                   last_error = :error
             WHERE id IN (:ids)
            """, nativeQuery = true)
    int recordPublishFailure(@Param("ids") List<UUID> ids, @Param("error") String error);

    /**
     * Removes rows whose message the broker confirmed long enough ago that they are no longer
     * useful for diagnosis. Without this the outbox grows forever — a table that is only ever
     * inserted into and updated is a slow-motion outage.
     */
    @Modifying
    @Query(value = """
            DELETE FROM outbox_events
             WHERE published_at IS NOT NULL
               AND published_at < :cutoff
            """, nativeQuery = true)
    int purgePublishedBefore(@Param("cutoff") OffsetDateTime cutoff);

    long countByPublishedAtIsNull();
}

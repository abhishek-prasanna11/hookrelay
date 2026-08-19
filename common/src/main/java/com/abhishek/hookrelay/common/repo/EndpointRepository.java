package com.abhishek.hookrelay.common.repo;

import com.abhishek.hookrelay.common.domain.Endpoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EndpointRepository extends JpaRepository<Endpoint, UUID> {

    Optional<Endpoint> findByIdAndTenantId(UUID id, UUID tenantId);

    List<Endpoint> findByTenantIdOrderByCreatedAtDesc(UUID tenantId);

    /**
     * The fan-out query: every active endpoint belonging to this tenant that subscribes to this
     * event type.
     *
     * <p>Uses the array containment operator {@code @>} rather than {@code = ANY(event_types)}
     * because only {@code @>} can use the GIN index on {@code event_types}. The explicit
     * {@code CAST} is needed because the JDBC parameter arrives untyped and PostgreSQL cannot infer
     * the array element type otherwise.
     *
     * <p>Matching is exact string equality. Wildcards such as {@code payment.*} are deliberately
     * out of scope for phase 1 — see docs/phase01-ingest.md section 3.3.
     */
    @Query(value = """
            SELECT * FROM endpoints
             WHERE tenant_id = :tenantId
               AND active
               AND event_types @> ARRAY[CAST(:eventType AS text)]
             ORDER BY id
            """, nativeQuery = true)
    List<Endpoint> findMatching(@Param("tenantId") UUID tenantId, @Param("eventType") String eventType);
}

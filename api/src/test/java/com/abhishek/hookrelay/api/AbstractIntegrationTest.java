package com.abhishek.hookrelay.api;

import com.abhishek.hookrelay.common.messaging.RabbitTopology;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;

import java.util.List;
import java.util.UUID;

/**
 * Base for integration tests.
 *
 * <p>Runs against a real PostgreSQL and a real RabbitMQ rather than in-memory substitutes, because
 * the behaviours these phases most need to prove are exactly the ones an emulation gets wrong:
 * {@code jsonb}, {@code text[]} containment, {@code FOR UPDATE SKIP LOCKED}, the precise unique
 * violation surfaced by the driver, publisher confirms, and real transaction and locking semantics
 * under concurrency.
 *
 * <p>Both containers are singletons started once for the whole JVM rather than {@code @Container}
 * fields, so every test class shares them instead of paying container startup per class.
 *
 * <p>The outbox publisher is <strong>disabled by default</strong> so tests can drive it explicitly
 * and assert deterministically. {@link OutboxRecoveryTest} re-enables it to exercise the real timer.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "hookrelay.outbox.publisher.enabled=false",
        // Keeps registration from doing a DNS lookup per test, and lets loopback URLs register.
        // EndpointSsrfTest turns it back off to prove the guard is wired in.
        "hookrelay.security.allow-private-destinations=true",
        "hookrelay.outbox.purge.enabled=false",
        "hookrelay.outbox.publisher.confirm-timeout-ms=1500"
})
public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

    static {
        POSTGRES.start();
        RABBIT.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected RabbitAdmin rabbitAdmin;

    protected UUID tenant;

    @BeforeEach
    void resetPerTestState() {
        tenant = UUID.randomUUID();

        // A fresh tenant id isolates tenant-scoped tables, but two things here have no tenant: the
        // outbox and the queue. The publisher deliberately claims *every* unpublished row regardless
        // of tenant, so rows left behind by an earlier test would be published into this test's
        // queue-depth assertions. Both are therefore cleared explicitly.
        jdbc.update("DELETE FROM outbox_events");
        rabbitAdmin.purgeQueue(RabbitTopology.DELIVERIES_QUEUE, false);
    }

    // ---- request helpers ----------------------------------------------------------------------

    protected HttpHeaders headers(String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Tenant-Id", tenant.toString());
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return headers;
    }

    protected ResponseEntity<JsonNode> postEvent(String idempotencyKey, String eventType, String payloadJson) {
        String body = """
                {"event_type": "%s", "payload": %s}
                """.formatted(eventType, payloadJson);
        return rest.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>(body, headers(idempotencyKey)), JsonNode.class);
    }

    protected ResponseEntity<JsonNode> postRawEvent(String idempotencyKey, String rawBody) {
        return rest.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>(rawBody, headers(idempotencyKey)), JsonNode.class);
    }

    protected UUID registerEndpoint(String url, List<String> eventTypes) {
        String types = eventTypes.stream().map(t -> "\"" + t + "\"").reduce((a, b) -> a + "," + b).orElse("");
        String body = """
                {"url": "%s", "event_types": [%s]}
                """.formatted(url, types);
        ResponseEntity<JsonNode> response = rest.exchange("/v1/endpoints", HttpMethod.POST,
                new HttpEntity<>(body, headers(null)), JsonNode.class);
        if (response.getStatusCode().value() != 201) {
            throw new IllegalStateException("endpoint registration failed: " + response.getBody());
        }
        return UUID.fromString(response.getBody().get("id").asText());
    }

    protected void deactivateEndpoint(UUID endpointId) {
        rest.exchange("/v1/endpoints/" + endpointId, HttpMethod.DELETE,
                new HttpEntity<>(headers(null)), Void.class);
    }

    // ---- database assertions ------------------------------------------------------------------

    protected int countEvents() {
        return jdbc.queryForObject("SELECT count(*) FROM events WHERE tenant_id = ?", Integer.class, tenant);
    }

    protected int countDeliveriesForEvent(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM deliveries WHERE event_id = ?", Integer.class, eventId);
    }

    protected List<String> deliveryStatusesForEvent(UUID eventId) {
        return jdbc.queryForList("SELECT status FROM deliveries WHERE event_id = ?", String.class, eventId);
    }

    protected List<UUID> deliveryEndpointIdsForEvent(UUID eventId) {
        return jdbc.queryForList("SELECT endpoint_id FROM deliveries WHERE event_id = ? ORDER BY endpoint_id",
                UUID.class, eventId);
    }

    // ---- outbox assertions --------------------------------------------------------------------

    protected int countOutboxRowsForTenant() {
        return jdbc.queryForObject("""
                SELECT count(*) FROM outbox_events o
                  JOIN deliveries d ON d.id = o.delivery_id
                  JOIN events e     ON e.id = d.event_id
                 WHERE e.tenant_id = ?
                """, Integer.class, tenant);
    }

    protected int countUnpublishedOutboxRowsForTenant() {
        return jdbc.queryForObject("""
                SELECT count(*) FROM outbox_events o
                  JOIN deliveries d ON d.id = o.delivery_id
                  JOIN events e     ON e.id = d.event_id
                 WHERE e.tenant_id = ? AND o.published_at IS NULL
                """, Integer.class, tenant);
    }

    protected List<UUID> outboxDeliveryIdsForTenant() {
        return jdbc.queryForList("""
                SELECT o.delivery_id FROM outbox_events o
                  JOIN deliveries d ON d.id = o.delivery_id
                  JOIN events e     ON e.id = d.event_id
                 WHERE e.tenant_id = ?
                """, UUID.class, tenant);
    }

    // ---- queue assertions ---------------------------------------------------------------------

    protected int queueDepth() {
        QueueInformation info = rabbitAdmin.getQueueInfo(RabbitTopology.DELIVERIES_QUEUE);
        return info == null ? 0 : info.getMessageCount();
    }

    // ---- response helpers ---------------------------------------------------------------------

    protected static UUID eventIdOf(ResponseEntity<JsonNode> response) {
        return UUID.fromString(response.getBody().get("event_id").asText());
    }

    protected static int deliveriesCreatedOf(ResponseEntity<JsonNode> response) {
        return response.getBody().get("deliveries_created").asInt();
    }
}

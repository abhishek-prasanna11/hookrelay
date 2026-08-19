package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.util.Uuid7;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Base for worker integration tests.
 *
 * <p>The worker module has no dependency on the API, so tests seed {@code endpoints},
 * {@code events} and {@code deliveries} directly. That is deliberate rather than a shortcut: the
 * worker's contract is with the database and the queue, and exercising it without the API proves
 * there is no hidden coupling between the two services.
 *
 * <p>Flyway runs here (test scope only) because the tests need a schema; in production the API owns
 * migrations and the worker only validates against them.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "spring.flyway.enabled=true",
        // The test receiver runs on loopback, which is exactly what SsrfGuard exists to refuse.
        "hookrelay.security.allow-private-destinations=true",
        "hookrelay.delivery.connect-timeout-ms=2000",
        "hookrelay.delivery.request-timeout-ms=1500"
})
public abstract class AbstractWorkerIntegrationTest {

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
    protected JdbcTemplate jdbc;

    protected TestWebhookReceiver receiver;
    protected UUID tenant;

    @BeforeEach
    void startReceiver() throws IOException {
        tenant = UUID.randomUUID();
        receiver = new TestWebhookReceiver();
    }

    @AfterEach
    void stopReceiver() {
        if (receiver != null) {
            receiver.close();
        }
    }

    // ---- seeding ------------------------------------------------------------------------------

    protected UUID insertEndpoint(String url, String secret) {
        UUID id = Uuid7.generate();
        jdbc.update("""
                INSERT INTO endpoints (id, tenant_id, url, secret, event_types, active, max_concurrency)
                VALUES (?, ?, ?, ?, ARRAY['payment.succeeded']::text[], true, 5)
                """, id, tenant, url, secret);
        return id;
    }

    protected UUID insertEvent(String eventType, String payloadJson) {
        UUID id = Uuid7.generate();
        jdbc.update("""
                INSERT INTO events (id, tenant_id, event_type, payload, idempotency_key, request_hash)
                VALUES (?, ?, ?, ?::jsonb, ?, 'test-hash')
                """, id, tenant, eventType, payloadJson, "key-" + id);
        return id;
    }

    protected UUID insertDelivery(UUID eventId, UUID endpointId) {
        UUID id = Uuid7.generate();
        jdbc.update("""
                INSERT INTO deliveries (id, event_id, endpoint_id, status, attempt_count)
                VALUES (?, ?, ?, 'PENDING', 0)
                """, id, eventId, endpointId);
        return id;
    }

    /** Seeds an endpoint pointing at the test receiver plus an event and a delivery for it. */
    protected UUID seedDelivery(String secret) {
        UUID endpointId = insertEndpoint(receiver.url(), secret);
        UUID eventId = insertEvent("payment.succeeded", "{\"order_id\": \"A-1\", \"amount\": 4200}");
        return insertDelivery(eventId, endpointId);
    }

    protected UUID seedDeliveryTo(String url, String secret) {
        UUID endpointId = insertEndpoint(url, secret);
        UUID eventId = insertEvent("payment.succeeded", "{\"order_id\": \"A-1\"}");
        return insertDelivery(eventId, endpointId);
    }

    // ---- assertions ---------------------------------------------------------------------------

    protected String deliveryStatus(UUID deliveryId) {
        return jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId);
    }

    protected int deliveryAttemptCount(UUID deliveryId) {
        return jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, deliveryId);
    }

    protected String deliveryLastError(UUID deliveryId) {
        return jdbc.queryForObject("SELECT last_error FROM deliveries WHERE id = ?",
                String.class, deliveryId);
    }

    protected List<Map<String, Object>> attempts(UUID deliveryId) {
        return jdbc.queryForList(
                "SELECT * FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_no", deliveryId);
    }
}

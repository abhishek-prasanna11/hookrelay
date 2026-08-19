package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.worker.delivery.DeliveryProcessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SSRF guard with enforcement on — the production configuration.
 *
 * <p>Every other worker test sets {@code allow-private-destinations=true} because the embedded test
 * receiver lives on loopback, which is precisely what the guard exists to refuse. This class turns
 * it back off, so the guard is exercised as it actually ships rather than only unit-tested in
 * isolation.
 */
@TestPropertySource(properties = "hookrelay.security.allow-private-destinations=false")
class SsrfEnforcedDeliveryTest extends AbstractWorkerIntegrationTest {

    private static final String SECRET = "ssrf-secret-0123456789";

    @Autowired
    DeliveryProcessor processor;

    @Test
    @DisplayName("a loopback destination is refused at delivery time, with no request made")
    void loopbackIsRefusedAtDeliveryTime() {
        // The endpoint row is seeded directly, standing in for one registered before the guard was
        // enabled — or one whose DNS changed after registration.
        UUID deliveryId = seedDelivery(SECRET);

        processor.process(deliveryId);

        assertThat(receiver.requestCount())
                .as("the worker must not contact an internal address")
                .isZero();
        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(attempts(deliveryId).get(0).get("error_class"))
                .isEqualTo(DeliveryProcessor.ERROR_SSRF_BLOCKED);
    }

    @Test
    @DisplayName("the cloud metadata service is refused")
    void metadataServiceIsRefused() {
        UUID deliveryId = seedDeliveryTo(
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/", SECRET);

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(attempts(deliveryId).get(0).get("error_class"))
                .isEqualTo(DeliveryProcessor.ERROR_SSRF_BLOCKED);
        assertThat(deliveryLastError(deliveryId)).isEqualTo(DeliveryProcessor.ERROR_SSRF_BLOCKED);
    }

    @Test
    @DisplayName("a private-range destination is refused")
    void privateRangeIsRefused() {
        UUID deliveryId = seedDeliveryTo("http://10.0.0.5:8080/hook", SECRET);

        processor.process(deliveryId);

        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(attempts(deliveryId).get(0).get("error_class"))
                .isEqualTo(DeliveryProcessor.ERROR_SSRF_BLOCKED);
    }

    @Test
    @DisplayName("being blocked is permanent — it is not retried up the ladder")
    void blockedDestinationIsNotRetried() {
        UUID deliveryId = seedDeliveryTo("http://127.0.0.1:9999/hook", SECRET);

        processor.process(deliveryId);

        // Repeating a request that will be refused for the same reason every time is pure waste.
        assertThat(deliveryStatus(deliveryId)).isEqualTo("DEAD");
        assertThat(deliveryAttemptCount(deliveryId)).isEqualTo(1);
    }
}

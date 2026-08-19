package com.abhishek.hookrelay.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Registration-time SSRF rejection, with enforcement on.
 *
 * <p>The rest of the API suite allows private destinations so that tests can register loopback URLs
 * without a DNS lookup per call. This class turns enforcement back on to prove the guard is actually
 * wired into {@code POST /v1/endpoints} rather than merely present in the codebase.
 */
@TestPropertySource(properties = "hookrelay.security.allow-private-destinations=false")
class EndpointSsrfTest extends AbstractIntegrationTest {

    private ResponseEntity<JsonNode> register(String url) {
        String body = """
                {"url": "%s", "event_types": ["payment.succeeded"]}
                """.formatted(url);
        return rest.exchange("/v1/endpoints", HttpMethod.POST,
                new HttpEntity<>(body, headers(null)), JsonNode.class);
    }

    @Test
    @DisplayName("a loopback url cannot be registered")
    void rejectsLoopback() {
        ResponseEntity<JsonNode> response = register("http://127.0.0.1:9000/hook");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("invalid_url");
        assertThat(response.getBody().get("message").asText()).contains("loopback");
    }

    @Test
    @DisplayName("the cloud metadata address cannot be registered")
    void rejectsMetadataService() {
        ResponseEntity<JsonNode> response = register("http://169.254.169.254/latest/meta-data/");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("message").asText()).contains("link-local");
    }

    @Test
    @DisplayName("a private-range url cannot be registered")
    void rejectsPrivateRange() {
        assertThat(register("http://10.1.2.3/hook").getStatusCode().value()).isEqualTo(400);
        assertThat(register("http://192.168.0.1/hook").getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("a url with userinfo cannot be registered")
    void rejectsUserinfo() {
        ResponseEntity<JsonNode> response = register("http://internal-host@example.com/hook");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("message").asText()).contains("userinfo");
    }

    @Test
    @DisplayName("a host that does not resolve is still accepted — DNS may be temporarily broken")
    void allowsUnresolvableHostAtRegistration() {
        // Delivery-time validation is the real gate. Refusing registration here would make endpoint
        // creation depend on the resolver's mood, and would not add protection: an attacker controls
        // what the name resolves to later regardless.
        ResponseEntity<JsonNode> response = register("https://does-not-exist.invalid/hook");

        assertThat(response.getStatusCode().value()).isEqualTo(201);
    }
}

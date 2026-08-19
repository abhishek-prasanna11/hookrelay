package com.abhishek.hookrelay.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointApiTest extends AbstractIntegrationTest {

    private ResponseEntity<JsonNode> register(String body) {
        return rest.exchange("/v1/endpoints", HttpMethod.POST,
                new HttpEntity<>(body, headers(null)), JsonNode.class);
    }

    @Test
    @DisplayName("registration generates a secret and returns it exactly once")
    void registrationGeneratesSecretReturnedOnlyOnce() {
        ResponseEntity<JsonNode> created = register("""
                {"url": "https://a.example.com/hook", "event_types": ["payment.succeeded"]}""");

        assertThat(created.getStatusCode().value()).isEqualTo(201);
        String secret = created.getBody().get("secret").asText();
        assertThat(secret).hasSize(64);   // 32 random bytes, hex encoded

        UUID id = UUID.fromString(created.getBody().get("id").asText());
        ResponseEntity<JsonNode> fetched = rest.exchange("/v1/endpoints/" + id, HttpMethod.GET,
                new HttpEntity<>(headers(null)), JsonNode.class);

        assertThat(fetched.getStatusCode().value()).isEqualTo(200);
        assertThat(fetched.getBody().has("secret"))
                .as("a signing key must never be readable after creation")
                .isFalse();
    }

    @Test
    @DisplayName("event_types are trimmed, deduplicated and sorted")
    void eventTypesAreNormalised() {
        ResponseEntity<JsonNode> created = register("""
                {"url": "https://a.example.com/hook",
                 "event_types": ["  user.created ", "payment.succeeded", "user.created"]}""");

        JsonNode types = created.getBody().get("event_types");
        assertThat(types).hasSize(2);
        assertThat(types.get(0).asText()).isEqualTo("payment.succeeded");
        assertThat(types.get(1).asText()).isEqualTo("user.created");
    }

    @Test
    @DisplayName("max_concurrency defaults to 5")
    void maxConcurrencyDefaults() {
        ResponseEntity<JsonNode> created = register("""
                {"url": "https://a.example.com/hook", "event_types": ["x"]}""");

        assertThat(created.getBody().get("max_concurrency").asInt()).isEqualTo(5);
    }

    @Test
    @DisplayName("a non-http scheme is rejected")
    void nonHttpSchemeRejected() {
        ResponseEntity<JsonNode> response = register("""
                {"url": "file:///etc/passwd", "event_types": ["x"]}""");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("invalid_url");
    }

    @Test
    @DisplayName("a relative url is rejected")
    void relativeUrlRejected() {
        ResponseEntity<JsonNode> response = register("""
                {"url": "/hook", "event_types": ["x"]}""");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("empty event_types is rejected")
    void emptyEventTypesRejected() {
        ResponseEntity<JsonNode> response = register("""
                {"url": "https://a.example.com/hook", "event_types": []}""");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("missing_event_types");
    }

    @Test
    @DisplayName("max_concurrency must be positive")
    void nonPositiveMaxConcurrencyRejected() {
        ResponseEntity<JsonNode> response = register("""
                {"url": "https://a.example.com/hook", "event_types": ["x"], "max_concurrency": 0}""");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("invalid_max_concurrency");
    }

    @Test
    @DisplayName("one tenant cannot read another tenant's endpoint")
    void endpointsAreTenantScoped() {
        UUID id = registerEndpoint("https://a.example.com/hook", List.of("x"));

        tenant = UUID.randomUUID();   // act as a different tenant
        ResponseEntity<JsonNode> response = rest.exchange("/v1/endpoints/" + id, HttpMethod.GET,
                new HttpEntity<>(headers(null)), JsonNode.class);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    @DisplayName("deactivation is a soft delete — the endpoint is still readable")
    void deactivationIsSoft() {
        UUID id = registerEndpoint("https://a.example.com/hook", List.of("x"));
        deactivateEndpoint(id);

        ResponseEntity<JsonNode> response = rest.exchange("/v1/endpoints/" + id, HttpMethod.GET,
                new HttpEntity<>(headers(null)), JsonNode.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody().get("active").asBoolean()).isFalse();
    }

    @Test
    @DisplayName("a missing X-Tenant-Id is rejected")
    void missingTenantHeaderRejected() {
        ResponseEntity<JsonNode> response = rest.exchange("/v1/endpoints", HttpMethod.GET,
                HttpEntity.EMPTY, JsonNode.class);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error").asText()).isEqualTo("missing_header");
    }
}

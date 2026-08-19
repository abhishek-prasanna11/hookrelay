package com.abhishek.hookrelay.api.web;

import com.abhishek.hookrelay.api.service.EndpointService;
import com.abhishek.hookrelay.api.web.dto.EndpointResponse;
import com.abhishek.hookrelay.api.web.dto.RegisterEndpointRequest;
import com.abhishek.hookrelay.common.domain.Endpoint;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * Endpoint registration.
 *
 * <p>{@code X-Tenant-Id} is taken at face value: there is no authentication in phase 1, so anyone
 * who can reach this API can claim any tenant. That is a deliberate scope decision recorded in
 * docs/phase01-ingest.md section 4.5, not an oversight — the project's engineering stories are
 * about delivery reliability. An API-key filter resolving key to tenant is the minimal fix.
 */
@RestController
@RequestMapping("/v1/endpoints")
public class EndpointController {

    private final EndpointService endpoints;

    public EndpointController(EndpointService endpoints) {
        this.endpoints = endpoints;
    }

    @PostMapping
    public ResponseEntity<EndpointResponse> register(
            @RequestHeader("X-Tenant-Id") UUID tenantId,
            @RequestBody RegisterEndpointRequest request) {

        Endpoint endpoint = endpoints.register(
                tenantId,
                request.url(),
                request.eventTypes(),
                request.maxConcurrency(),
                request.secret());

        // The secret is returned exactly once, here.
        return ResponseEntity.status(HttpStatus.CREATED).body(EndpointResponse.withSecret(endpoint));
    }

    @GetMapping
    public List<EndpointResponse> list(@RequestHeader("X-Tenant-Id") UUID tenantId) {
        return endpoints.list(tenantId).stream().map(EndpointResponse::withoutSecret).toList();
    }

    @GetMapping("/{id}")
    public EndpointResponse get(@RequestHeader("X-Tenant-Id") UUID tenantId,
                                @PathVariable UUID id) {
        return EndpointResponse.withoutSecret(endpoints.get(tenantId, id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deactivate(@RequestHeader("X-Tenant-Id") UUID tenantId,
                                           @PathVariable UUID id) {
        endpoints.deactivate(tenantId, id);
        return ResponseEntity.noContent().build();
    }
}

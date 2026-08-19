package com.abhishek.hookrelay.api.service;

import com.abhishek.hookrelay.api.error.ApiException;
import com.abhishek.hookrelay.common.domain.Endpoint;
import com.abhishek.hookrelay.common.net.DestinationPolicy;
import com.abhishek.hookrelay.common.net.SsrfGuard;
import com.abhishek.hookrelay.common.repo.EndpointRepository;
import com.abhishek.hookrelay.common.util.Uuid7;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
public class EndpointService {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SECRET_BYTES = 32;
    private static final int DEFAULT_MAX_CONCURRENCY = 5;

    private final EndpointRepository endpoints;
    private final DestinationPolicy destinations;

    public EndpointService(EndpointRepository endpoints, DestinationPolicy destinations) {
        this.endpoints = endpoints;
        this.destinations = destinations;
    }

    @Transactional
    public Endpoint register(UUID tenantId, String url, List<String> eventTypes,
                             Integer maxConcurrency, String providedSecret) {
        validateUrl(url);

        List<String> normalisedTypes = normaliseEventTypes(eventTypes);
        int concurrency = maxConcurrency == null ? DEFAULT_MAX_CONCURRENCY : maxConcurrency;
        if (concurrency <= 0) {
            throw ApiException.badRequest("invalid_max_concurrency", "max_concurrency must be positive");
        }

        String secret = (providedSecret == null || providedSecret.isBlank())
                ? generateSecret()
                : providedSecret;

        Endpoint endpoint = new Endpoint(Uuid7.generate(), tenantId, url, secret,
                normalisedTypes, concurrency, OffsetDateTime.now());
        return endpoints.save(endpoint);
    }

    @Transactional(readOnly = true)
    public List<Endpoint> list(UUID tenantId) {
        return endpoints.findByTenantIdOrderByCreatedAtDesc(tenantId);
    }

    @Transactional(readOnly = true)
    public Endpoint get(UUID tenantId, UUID endpointId) {
        return endpoints.findByIdAndTenantId(endpointId, tenantId)
                .orElseThrow(() -> ApiException.notFound("endpoint_not_found",
                        "no endpoint " + endpointId + " for this tenant"));
    }

    /**
     * Soft delete. Blueprint section 8: deactivating an endpoint stops it matching <em>future</em>
     * events but deliberately does not cancel deliveries already created for it, which remain valid
     * obligations. Hard-deleting the row would also orphan the delivery rows that reference it.
     */
    @Transactional
    public void deactivate(UUID tenantId, UUID endpointId) {
        Endpoint endpoint = get(tenantId, endpointId);
        endpoint.deactivate(OffsetDateTime.now());
        endpoints.save(endpoint);
    }

    private static List<String> normaliseEventTypes(List<String> eventTypes) {
        if (eventTypes == null || eventTypes.isEmpty()) {
            throw ApiException.badRequest("missing_event_types", "event_types must not be empty");
        }
        List<String> normalised = eventTypes.stream()
                .filter(t -> t != null && !t.isBlank())
                .map(String::trim)
                .distinct()
                .sorted()
                .toList();
        if (normalised.isEmpty()) {
            throw ApiException.badRequest("missing_event_types", "event_types must not be empty");
        }
        return normalised;
    }

    /**
     * Rejects what can be proven bad at registration time.
     *
     * <p>A host that fails to resolve is <em>allowed</em> here: DNS is permitted to be temporarily
     * broken, and refusing registration for it would make endpoint creation depend on the resolver's
     * mood. The delivery-time check is the real gate — it runs on a fresh lookup, which is what
     * defeats an attacker who changes DNS after registering a public address.
     */
    private void validateUrl(String url) {
        SsrfGuard.Result result = destinations.check(url, true);
        if (!result.allowed()) {
            throw ApiException.badRequest("invalid_url", result.reason());
        }
    }

    private static String generateSecret() {
        byte[] bytes = new byte[SECRET_BYTES];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}

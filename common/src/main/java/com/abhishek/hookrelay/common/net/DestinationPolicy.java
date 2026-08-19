package com.abhishek.hookrelay.common.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * Applies {@link SsrfGuard} according to configuration, so the API and the worker cannot disagree
 * about what a valid destination is.
 *
 * <p>{@code hookrelay.security.allow-private-destinations} exists because every local setup needs
 * it: a developer running the reference receiver on {@code http://127.0.0.1:9000/hook}, and the test
 * suite pointing at an embedded server on loopback, are both destinations the guard is specifically
 * designed to refuse. Turning it on also skips DNS resolution, which keeps tests offline-safe.
 *
 * <p>It defaults to <strong>false</strong> and logs loudly when enabled. Setting it in production
 * removes the only thing standing between a customer-supplied URL and the cluster's internal
 * network.
 */
@Component
public class DestinationPolicy {

    private static final Logger log = LoggerFactory.getLogger(DestinationPolicy.class);

    private final boolean allowPrivateDestinations;

    public DestinationPolicy(
            @Value("${hookrelay.security.allow-private-destinations:false}") boolean allowPrivateDestinations) {
        this.allowPrivateDestinations = allowPrivateDestinations;
    }

    @PostConstruct
    void warnIfDisabled() {
        if (allowPrivateDestinations) {
            log.warn("hookrelay.security.allow-private-destinations=true — webhook destinations "
                    + "resolving to loopback, private or link-local addresses will be ALLOWED. "
                    + "This is for local development only.");
        }
    }

    /**
     * @param allowUnresolvable true at registration, where DNS is permitted to be temporarily broken
     *                          and the delivery-time check is the real gate; false at delivery.
     */
    public SsrfGuard.Result check(String url, boolean allowUnresolvable) {
        if (allowPrivateDestinations) {
            return SsrfGuard.checkUrlSyntax(url);
        }
        return SsrfGuard.checkDestination(url, allowUnresolvable);
    }

    public boolean isEnforcing() {
        return !allowPrivateDestinations;
    }
}

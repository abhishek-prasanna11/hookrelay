package com.abhishek.hookrelay.api.web.dto;

import java.util.List;

/**
 * @param secret         optional. When omitted a 256-bit secret is generated and returned once, in
 *                       the creation response only.
 * @param maxConcurrency optional, defaults to 5. The per-endpoint in-flight cap the worker enforces
 *                       from phase 5 onward.
 */
public record RegisterEndpointRequest(
        String url,
        List<String> eventTypes,
        Integer maxConcurrency,
        String secret
) {
}

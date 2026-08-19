package com.abhishek.hookrelay.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The worker's {@link ObjectMapper}.
 *
 * <p>Spring Boot's {@code JacksonAutoConfiguration} is conditional on {@code Jackson2ObjectMapperBuilder},
 * which lives in {@code spring-web} — and the worker has no web stack, so no mapper is
 * auto-configured. It is declared explicitly here rather than in {@code common} on purpose: a
 * user-defined {@code ObjectMapper} bean makes the auto-configuration back off, so putting one in
 * the shared module would silently discard the API's {@code spring.jackson} settings and change its
 * wire format from snake_case.
 *
 * <p>A plain mapper is exactly what the webhook envelope needs. {@code WebhookEnvelope} names every
 * field with {@code @JsonProperty}, so its serialization does not depend on any naming strategy —
 * which matters because those bytes are what gets signed.
 */
@Configuration
public class WorkerJsonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

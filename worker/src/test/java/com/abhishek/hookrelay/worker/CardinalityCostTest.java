package com.abhishek.hookrelay.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what a high-cardinality label actually costs, rather than asserting that it is bad.
 *
 * <p>BLUEPRINT.md §23 forbids {@code endpoint_id} as a metric label. That rule is easy to state and
 * easy to quietly break, because the label is genuinely useful and the damage is invisible until the
 * metrics system falls over. This measures the real scrape payload from a real
 * {@link PrometheusMeterRegistry}: same counter, same traffic, with and without the label.
 */
class CardinalityCostTest {

    private static final int ENDPOINTS = 50;
    private static final String[] RESULTS = {"success", "retryable", "permanent"};

    private record Scrape(int series, int bytes) {
    }

    /** Renders the registry exactly as Prometheus would scrape it. */
    private static Scrape scrape(PrometheusMeterRegistry registry, String metricName) {
        String body = registry.scrape();
        int series = 0;
        for (String line : body.split("\n")) {
            if (line.startsWith(metricName) && !line.startsWith("# ")) {
                series++;
            }
        }
        return new Scrape(series, body.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    @DisplayName("labelling a counter by endpoint id multiplies its series by the endpoint count")
    void measureCardinalityCost() {
        PrometheusMeterRegistry bounded = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        PrometheusMeterRegistry unbounded = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        for (int i = 0; i < ENDPOINTS; i++) {
            String endpointId = UUID.randomUUID().toString();
            for (String result : RESULTS) {
                Counter.builder("hookrelay_deliveries_total")
                        .tag("result", result)
                        .register(bounded)
                        .increment();

                Counter.builder("hookrelay_deliveries_total")
                        .tag("result", result)
                        .tag("endpoint_id", endpointId)
                        .register(unbounded)
                        .increment();
            }
        }

        Scrape withoutLabel = scrape(bounded, "hookrelay_deliveries_total");
        Scrape withLabel = scrape(unbounded, "hookrelay_deliveries_total");

        int bytesPerSeries = withLabel.bytes() / Math.max(withLabel.series(), 1);
        long seriesAt10k = (long) RESULTS.length * 10_000;
        long bytesAt10k = seriesAt10k * bytesPerSeries;

        System.out.println("=== CARDINALITY COST ===");
        System.out.printf("endpoints simulated            = %d%n", ENDPOINTS);
        System.out.printf("series  {result}               = %d%n", withoutLabel.series());
        System.out.printf("series  {result, endpoint_id}  = %d%n", withLabel.series());
        System.out.printf("scrape bytes {result}              = %d%n", withoutLabel.bytes());
        System.out.printf("scrape bytes {result, endpoint_id} = %d%n", withLabel.bytes());
        System.out.printf("bytes per series               = %d%n", bytesPerSeries);
        System.out.printf("extrapolated to 10,000 endpoints: %d series, %.1f MB per scrape%n",
                seriesAt10k, bytesAt10k / (1024.0 * 1024.0));
        System.out.println("========================");

        assertThat(withoutLabel.series())
                .as("bounded: one series per result, regardless of endpoint count")
                .isEqualTo(RESULTS.length);
        assertThat(withLabel.series())
                .as("unbounded: one series per result PER ENDPOINT")
                .isEqualTo(RESULTS.length * ENDPOINTS);
        assertThat(withLabel.bytes()).isGreaterThan(withoutLabel.bytes() * 10);
    }
}

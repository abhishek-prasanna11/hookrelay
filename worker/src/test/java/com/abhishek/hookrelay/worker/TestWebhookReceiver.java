package com.abhishek.hookrelay.worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A real HTTP server standing in for a customer endpoint.
 *
 * <p>Deliberately built on the JDK's own server rather than a mocking library: the point of this
 * phase is that a genuine signed request crosses a socket and arrives intact, which a mock would
 * assert nothing about. It records exactly what arrived so tests can verify the signature
 * independently, over the bytes actually received.
 */
public class TestWebhookReceiver implements AutoCloseable {

    public record Received(Map<String, String> headers, byte[] body, long receivedAtMillis) {
        public String header(String name) {
            return headers.get(name.toLowerCase());
        }

        public String bodyAsString() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private final HttpServer server;
    private final java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(16);
    private final List<Received> received = Collections.synchronizedList(new ArrayList<>());

    private volatile int responseStatus = 200;
    private volatile byte[] responseBody = "ok".getBytes(StandardCharsets.UTF_8);
    private volatile long delayMs = 0;
    private volatile String locationHeader = null;

    public TestWebhookReceiver() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // An explicit pool, because HttpServer's default executor is SINGLE-THREADED. Left as the
        // default, a receiver told to delay 2s serialises every request, so a measurement of
        // "how long did the worker pool stay blocked" would really be measuring the test server's
        // own concurrency limit.
        server.setExecutor(executor);
        server.createContext("/hook", this::handle);
        server.createContext("/redirected", exchange -> {
            received.add(readExchange(exchange));
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });
        server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        received.add(readExchange(exchange));

        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (locationHeader != null) {
            exchange.getResponseHeaders().add("Location", locationHeader);
        }
        byte[] body = responseBody;
        exchange.sendResponseHeaders(responseStatus, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static Received readExchange(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((k, v) -> {
            if (!v.isEmpty()) {
                headers.put(k.toLowerCase(), v.get(0));
            }
        });
        byte[] body;
        try (InputStream in = exchange.getRequestBody()) {
            body = in.readAllBytes();
        }
        return new Received(headers, body, System.currentTimeMillis());
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/hook";
    }

    public String redirectTargetUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/redirected";
    }

    public List<Received> received() {
        return List.copyOf(received);
    }

    public int requestCount() {
        return received.size();
    }

    public void reset() {
        received.clear();
        responseStatus = 200;
        responseBody = "ok".getBytes(StandardCharsets.UTF_8);
        delayMs = 0;
        locationHeader = null;
    }

    public TestWebhookReceiver respondWith(int status) {
        this.responseStatus = status;
        return this;
    }

    public TestWebhookReceiver respondWithBody(byte[] body) {
        this.responseBody = body;
        return this;
    }

    public TestWebhookReceiver delay(long millis) {
        this.delayMs = millis;
        return this;
    }

    public TestWebhookReceiver redirectTo(String location) {
        this.responseStatus = 302;
        this.locationHeader = location;
        return this;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}

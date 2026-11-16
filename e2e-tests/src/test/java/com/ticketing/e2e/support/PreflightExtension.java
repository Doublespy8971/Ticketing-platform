package com.ticketing.e2e.support;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JUnit 5 {@link BeforeAllCallback} extension that verifies all platform dependencies
 * before running an E2E test class, and FAILS (never skips) with a diagnostic error
 * if any component is unreachable.
 */
public final class PreflightExtension implements BeforeAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        E2EConfig config = new E2EConfig();
        Stack stack = new Stack(config);

        // 1. Docker CLI check
        stack.verifyDockerCliAvailable();

        // 2. HTTP services check (gateway + 4 downstream services)
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        Map<String, String> endpoints = new LinkedHashMap<>();
        endpoints.put("gateway-service (:8080)", config.gatewayBaseUrl() + "/actuator/health");
        endpoints.put("inventory-service (:8081)", config.inventoryBaseUrl() + "/actuator/health");
        endpoints.put("order-service (:8082)", config.orderBaseUrl() + "/actuator/health");
        endpoints.put("payment-service (:8083)", config.paymentBaseUrl() + "/actuator/health");
        endpoints.put("notification-service (:8084)", config.notificationBaseUrl() + "/health/readiness");

        for (Map.Entry<String, String> entry : endpoints.entrySet()) {
            checkHttpService(client, entry.getKey(), entry.getValue());
        }

        // 3. Three PostgreSQL databases check
        Db db = new Db(config);
        db.checkConnectivity();

        // 4. Redis check on host port
        checkRedis(config.redisHost(), config.redisPort());

        // 5. Kafka check on host port
        try (KafkaProbe kafka = new KafkaProbe(config)) {
            kafka.checkConnectivity();
        }
    }

    private static void checkHttpService(HttpClient client, String name, String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(4))
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() != 200 || resp.body() == null || !resp.body().contains("\"UP\"")) {
                throw new AssertionError("Preflight FAILED: " + name + " at " + url
                        + " returned HTTP " + resp.statusCode() + " body=" + resp.body()
                        + ". Run 'make stack-up' first.");
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError("Preflight FAILED: " + name + " is unreachable at " + url
                    + " (" + e.getMessage() + "). Run 'make stack-up' first.", e);
        }
    }

    private static void checkRedis(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = reader.readLine();
            if (line == null || !line.startsWith("+PONG")) {
                throw new AssertionError("Preflight FAILED: Redis at " + host + ":" + port
                        + " did not respond with +PONG (got: " + line + "). Run 'make stack-up' first.");
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError("Preflight FAILED: Redis is unreachable at " + host + ":" + port
                    + " (" + e.getMessage() + "). Run 'make stack-up' first.", e);
        }
    }
}

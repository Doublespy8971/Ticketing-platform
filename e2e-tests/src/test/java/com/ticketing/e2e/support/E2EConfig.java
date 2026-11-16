package com.ticketing.e2e.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads host ports and connection configuration from {@code infra/.env}
 * (falling back to {@code infra/.env.example} and documented local defaults).
 */
public final class E2EConfig {

    private final Map<String, String> env;

    public E2EConfig() {
        this.env = loadEnv();
    }

    public static E2EConfig load() {
        return new E2EConfig();
    }

    public Path workspaceRoot() {
        if (Files.isDirectory(Path.of("e2e-tests"))) {
            return Path.of(".").toAbsolutePath().normalize();
        }
        return Path.of("..").toAbsolutePath().normalize();
    }

    private static Map<String, String> loadEnv() {
        Map<String, String> map = new HashMap<>();
        List<Path> candidates = List.of(
                Path.of("infra/.env"),
                Path.of("../infra/.env"),
                Path.of("infra/.env.example"),
                Path.of("../infra/.env.example")
        );
        for (Path candidate : candidates) {
            if (Files.isRegularFile(candidate)) {
                try {
                    for (String line : Files.readAllLines(candidate)) {
                        String trimmed = line.trim();
                        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                            continue;
                        }
                        int eq = trimmed.indexOf('=');
                        if (eq > 0) {
                            String key = trimmed.substring(0, eq).trim();
                            String val = trimmed.substring(eq + 1).trim();
                            map.putIfAbsent(key, val);
                        }
                    }
                } catch (IOException ignored) {
                }
            }
        }
        return map;
    }

    private String get(String key, String defaultValue) {
        String sysProp = System.getProperty(key);
        if (sysProp != null && !sysProp.isBlank()) {
            return sysProp.trim();
        }
        String sysEnv = System.getenv(key);
        if (sysEnv != null && !sysEnv.isBlank()) {
            return sysEnv.trim();
        }
        return env.getOrDefault(key, defaultValue);
    }

    public String gatewayBaseUrl() {
        return get("GATEWAY_BASE_URL", "http://localhost:8080");
    }

    public String inventoryBaseUrl() {
        return get("INVENTORY_BASE_URL", "http://localhost:8081");
    }

    public String orderBaseUrl() {
        return get("ORDER_BASE_URL", "http://localhost:8082");
    }

    public String paymentBaseUrl() {
        return get("PAYMENT_BASE_URL", "http://localhost:8083");
    }

    public String notificationBaseUrl() {
        return get("NOTIFICATION_BASE_URL", "http://localhost:8084");
    }

    public String postgresUser() {
        return get("POSTGRES_USER", "ticketing");
    }

    public String postgresPassword() {
        return get("POSTGRES_PASSWORD", "ticketing_secret");
    }

    public int postgresInventoryPort() {
        return Integer.parseInt(get("POSTGRES_INVENTORY_PORT", "5433"));
    }

    public int postgresOrderPort() {
        return Integer.parseInt(get("POSTGRES_ORDER_PORT", "5434"));
    }

    public int postgresPaymentPort() {
        return Integer.parseInt(get("POSTGRES_PAYMENT_PORT", "5435"));
    }

    public String inventoryJdbcUrl() {
        return "jdbc:postgresql://localhost:" + postgresInventoryPort() + "/inventory_db";
    }

    public String orderJdbcUrl() {
        return "jdbc:postgresql://localhost:" + postgresOrderPort() + "/order_db";
    }

    public String paymentJdbcUrl() {
        return "jdbc:postgresql://localhost:" + postgresPaymentPort() + "/payment_db";
    }

    public String redisHost() {
        return get("REDIS_HOST", "localhost");
    }

    public int redisPort() {
        return Integer.parseInt(get("REDIS_PORT", "6379"));
    }

    public int kafkaHostPort() {
        return Integer.parseInt(get("KAFKA_HOST_PORT", "29092"));
    }

    public String kafkaBootstrapServers() {
        return "localhost:" + kafkaHostPort();
    }

    public String jwtSecret() {
        return get("JWT_SECRET", "dev-only-hs256-secret-key-minimum-32-bytes-long!");
    }
}

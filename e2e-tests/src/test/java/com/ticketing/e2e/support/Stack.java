package com.ticketing.e2e.support;

import org.awaitility.Awaitility;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Controls Docker containers from {@code infra/docker-compose.yml} and {@code infra/docker-compose.full.yml}
 * via the {@code docker} CLI with explicit timeouts on every process invocation.
 */
public final class Stack {

    public static final String GATEWAY = "gateway-service";
    public static final String INVENTORY = "inventory-service";
    public static final String ORDER = "order-service";
    public static final String PAYMENT = "payment-service";
    public static final String NOTIFICATION = "notification-service";
    public static final String KAFKA = "kafka";
    public static final String REDIS = "redis";
    public static final String POSTGRES_INVENTORY = "postgres-inventory";
    public static final String POSTGRES_ORDER = "postgres-order";
    public static final String POSTGRES_PAYMENT = "postgres-payment";

    public static final List<String> ALL_CONTAINERS = List.of(
            KAFKA,
            REDIS,
            POSTGRES_INVENTORY,
            POSTGRES_ORDER,
            POSTGRES_PAYMENT,
            INVENTORY,
            ORDER,
            PAYMENT,
            NOTIFICATION,
            GATEWAY
    );

    public static final Map<String, List<String>> SERVICE_CONSUMER_GROUPS = Map.of(
            INVENTORY, List.of(KafkaProbe.CG_INVENTORY_PAYMENT),
            ORDER, List.of(KafkaProbe.CG_ORDER_INVENTORY, KafkaProbe.CG_ORDER_PAYMENT),
            PAYMENT, List.of(KafkaProbe.CG_PAYMENT_ORDER),
            NOTIFICATION, List.of(KafkaProbe.CG_NOTIFICATION_ORDER)
    );

    private static final Duration DEFAULT_CMD_TIMEOUT = Duration.ofSeconds(30);

    private final E2EConfig config;
    private final HttpClient httpClient;
    private volatile KafkaProbe kafkaProbe;
    private final Set<String> touchedContainers = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedContainers = ConcurrentHashMap.newKeySet();

    public Stack(E2EConfig config) {
        this(config, null);
    }

    public Stack(E2EConfig config, KafkaProbe kafkaProbe) {
        this.config = config;
        this.kafkaProbe = kafkaProbe;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    public void setKafkaProbe(KafkaProbe kafkaProbe) {
        this.kafkaProbe = kafkaProbe;
    }

    public record CommandResult(int exitCode, String stdout, String stderr) {}

    public void verifyDockerCliAvailable() {
        CommandResult res = exec(Duration.ofSeconds(10), "docker", "version", "--format", "{{.Server.Version}}");
        if (res.exitCode() != 0 || res.stdout().isBlank()) {
            throw new AssertionError("Docker CLI or daemon is not reachable (exit=" + res.exitCode()
                    + ", stderr=" + res.stderr() + "). Ensure Docker Desktop is running.");
        }
    }

    public void stop(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "stop", "-t", "5", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to stop container " + containerName + ": " + res.stderr());
        }
    }

    public void start(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "start", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to start container " + containerName + ": " + res.stderr());
        }
    }

    public void kill(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "kill", "-s", "KILL", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to kill -9 container " + containerName + ": " + res.stderr());
        }
    }

    public void restart(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "restart", "-t", "5", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to restart container " + containerName + ": " + res.stderr());
        }
    }

    public void pause(String containerName) {
        touchedContainers.add(containerName);
        pausedContainers.add(containerName);
        CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "pause", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to pause container " + containerName + ": " + res.stderr());
        }
    }

    public void unpause(String containerName) {
        pausedContainers.remove(containerName);
        unpauseIfPaused(containerName);
    }

    private void unpauseIfPaused(String containerName) {
        String state = inspectHealthStatus(containerName);
        String[] parts = state.split("\\|");
        if (parts.length >= 2 && "true".equalsIgnoreCase(parts[1])) {
            CommandResult res = exec(DEFAULT_CMD_TIMEOUT, "docker", "unpause", containerName);
            if (res.exitCode() != 0 && !res.stderr().toLowerCase().contains("is not paused")) {
                throw new IllegalStateException("Failed to unpause container " + containerName + ": " + res.stderr());
            }
        }
    }

    public String inspectHealthStatus(String containerName) {
        CommandResult res = exec(
                Duration.ofSeconds(10),
                "docker", "inspect",
                "--format", "{{.State.Status}}|{{.State.Paused}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
                containerName
        );
        if (res.exitCode() != 0) {
            return "missing (" + res.stderr().trim() + ")";
        }
        return res.stdout().trim();
    }

    public boolean isContainerHealthy(String containerName) {
        String state = inspectHealthStatus(containerName);
        String[] parts = state.split("\\|");
        if (parts.length < 3) {
            return false;
        }
        boolean running = "running".equalsIgnoreCase(parts[0]);
        boolean paused = "true".equalsIgnoreCase(parts[1]);
        String health = parts[2];
        boolean dockerHealthy = "healthy".equalsIgnoreCase(health) || "none".equalsIgnoreCase(health);
        if (!running || paused || !dockerHealthy) {
            return false;
        }
        if (!checkHttpHealthIfApplicable(containerName)) {
            return false;
        }
        List<String> consumerGroups = SERVICE_CONSUMER_GROUPS.get(containerName);
        if (consumerGroups != null && kafkaProbe != null) {
            String kafkaState = inspectHealthStatus(KAFKA);
            if (kafkaState.startsWith("running|false|")) {
                if (!kafkaProbe.areConsumerGroupsStableWithMember(consumerGroups)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkHttpHealthIfApplicable(String containerName) {
        Map<String, String> healthUrls = Map.of(
                GATEWAY, config.gatewayBaseUrl() + "/actuator/health",
                INVENTORY, config.inventoryBaseUrl() + "/actuator/health",
                ORDER, config.orderBaseUrl() + "/actuator/health",
                PAYMENT, config.paymentBaseUrl() + "/actuator/health",
                NOTIFICATION, config.notificationBaseUrl() + "/health/readiness"
        );
        String url = healthUrls.get(containerName);
        if (url == null) {
            return true;
        }
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 && resp.body() != null && resp.body().contains("\"UP\"");
        } catch (Exception e) {
            return false;
        }
    }

    public void waitUntilHealthy(String containerName, Duration timeout) {
        Awaitility.await("Container '" + containerName + "' to become healthy (including consumer group Stable check if applicable)")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(400))
                .until(() -> isContainerHealthy(containerName));
        touchedContainers.remove(containerName);
    }

    /**
     * Restores all paused, stopped, or killed containers and waits until every container is healthy.
     * Never swallows errors: collects all errors across all containers, reports all of them, and then fails.
     */
    public void restoreAndWaitAllHealthy(Duration timeout) {
        List<String> errors = new ArrayList<>();
        List<Throwable> causes = new ArrayList<>();

        for (String paused : new ArrayList<>(pausedContainers)) {
            try {
                unpause(paused);
            } catch (Throwable t) {
                errors.add("Failed to unpause " + paused + ": " + t.getMessage());
                causes.add(t);
            }
        }

        for (String container : ALL_CONTAINERS) {
            try {
                String state = inspectHealthStatus(container);
                if (!state.startsWith("running|false|")) {
                    unpauseIfPaused(container);
                    start(container);
                }
            } catch (Throwable t) {
                errors.add("Failed to restore/start " + container + ": " + t.getMessage());
                causes.add(t);
            }
        }

        for (String container : ALL_CONTAINERS) {
            try {
                waitUntilHealthy(container, timeout);
            } catch (Throwable t) {
                errors.add("Container " + container + " did not become healthy within " + timeout.toSeconds() + "s: " + t.getMessage());
                causes.add(t);
            }
        }

        if (!errors.isEmpty()) {
            IllegalStateException ex = new IllegalStateException(
                    "Stack restore failed with " + errors.size() + " error(s):\n  - " + String.join("\n  - ", errors),
                    causes.isEmpty() ? null : causes.get(0)
            );
            for (int i = 1; i < causes.size(); i++) {
                ex.addSuppressed(causes.get(i));
            }
            throw ex;
        }
        touchedContainers.clear();
    }

    /**
     * Invoked by the JVM shutdown hook registered in {@code @BeforeAll} if the test process is
     * terminated before normal teardown completes. Restores every container stopped, killed, or paused.
     */
    public void restoreTouchedContainersOnShutdown() {
        if (pausedContainers.isEmpty() && touchedContainers.isEmpty()) {
            return;
        }
        try {
            restoreAndWaitAllHealthy(Duration.ofSeconds(45));
        } catch (Throwable t) {
            System.err.println("[Stack Shutdown Hook] Error while restoring containers: " + t.getMessage());
            throw t;
        }
    }

    public CommandResult exec(Duration timeout, String... command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);
        try {
            Process proc = pb.start();
            byte[] stdoutBytes;
            byte[] stderrBytes;
            boolean finished = proc.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                proc.destroyForcibly();
                throw new IllegalStateException("Command timed out after " + timeout.toSeconds()
                        + "s: " + String.join(" ", command));
            }
            stdoutBytes = proc.getInputStream().readAllBytes();
            stderrBytes = proc.getErrorStream().readAllBytes();
            return new CommandResult(
                    proc.exitValue(),
                    new String(stdoutBytes, StandardCharsets.UTF_8),
                    new String(stderrBytes, StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            return new CommandResult(127, "", e.getMessage() != null ? e.getMessage() : e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running command: " + String.join(" ", command), e);
        }
    }
}

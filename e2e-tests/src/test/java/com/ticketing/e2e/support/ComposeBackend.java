package com.ticketing.e2e.support;

import org.awaitility.Awaitility;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Platform backend targeting Docker Compose via the {@code docker} CLI.
 * Preserves byte-for-byte command and argument equivalence with the original Stack implementation.
 */
public final class ComposeBackend implements PlatformBackend {

    private static final Duration DEFAULT_CMD_TIMEOUT = Duration.ofSeconds(30);

    private final E2EConfig config;
    private final ProcessRunner runner;
    private final HttpClient httpClient;
    private volatile KafkaProbe kafkaProbe;
    private final Set<String> touchedContainers = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedContainers = ConcurrentHashMap.newKeySet();

    public ComposeBackend(E2EConfig config, KafkaProbe kafkaProbe) {
        this(config, kafkaProbe, new SystemProcessRunner());
    }

    public ComposeBackend(E2EConfig config, KafkaProbe kafkaProbe, ProcessRunner runner) {
        this.config = config;
        this.kafkaProbe = kafkaProbe;
        this.runner = runner;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    @Override
    public void setKafkaProbe(KafkaProbe kafkaProbe) {
        this.kafkaProbe = kafkaProbe;
    }

    @Override
    public String backendDescription() {
        return "compose";
    }

    @Override
    public List<String> listWorkloads() {
        return Stack.ALL_CONTAINERS;
    }

    @Override
    public void verifyPreflight() {
        verifyDockerCliAvailable();
    }

    public void verifyDockerCliAvailable() {
        CommandResult res = runner.exec(Duration.ofSeconds(10), "docker", "version", "--format", "{{.Server.Version}}");
        if (res.exitCode() != 0 || res.stdout().isBlank()) {
            throw new AssertionError("Docker CLI or daemon is not reachable (exit=" + res.exitCode()
                    + ", stderr=" + res.stderr() + "). Ensure Docker Desktop is running.");
        }
    }

    @Override
    public void stop(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "stop", "-t", "5", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to stop container " + containerName + ": " + res.stderr());
        }
    }

    @Override
    public void start(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "start", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to start container " + containerName + ": " + res.stderr());
        }
    }

    @Override
    public void kill(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "kill", "-s", "KILL", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to kill -9 container " + containerName + ": " + res.stderr());
        }
    }

    @Override
    public void restart(String containerName) {
        touchedContainers.add(containerName);
        if (pausedContainers.remove(containerName)) {
            unpauseIfPaused(containerName);
        }
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "restart", "-t", "5", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to restart container " + containerName + ": " + res.stderr());
        }
    }

    @Override
    public void pause(String containerName) {
        touchedContainers.add(containerName);
        pausedContainers.add(containerName);
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "pause", containerName);
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to pause container " + containerName + ": " + res.stderr());
        }
    }

    @Override
    public void unpause(String containerName) {
        pausedContainers.remove(containerName);
        unpauseIfPaused(containerName);
    }

    private void unpauseIfPaused(String containerName) {
        String state = inspectHealthStatus(containerName);
        String[] parts = state.split("\\|");
        if (parts.length >= 2 && "true".equalsIgnoreCase(parts[1])) {
            CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, "docker", "unpause", containerName);
            if (res.exitCode() != 0 && !res.stderr().toLowerCase().contains("is not paused")) {
                throw new IllegalStateException("Failed to unpause container " + containerName + ": " + res.stderr());
            }
        }
    }

    @Override
    public String inspectHealthStatus(String containerName) {
        CommandResult res = runner.exec(
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

    @Override
    public boolean isHealthy(String containerName) {
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
        List<String> consumerGroups = Stack.SERVICE_CONSUMER_GROUPS.get(containerName);
        if (consumerGroups != null && kafkaProbe != null) {
            String kafkaState = inspectHealthStatus(Stack.KAFKA);
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
                Stack.GATEWAY, config.gatewayBaseUrl() + "/actuator/health",
                Stack.INVENTORY, config.inventoryBaseUrl() + "/actuator/health",
                Stack.ORDER, config.orderBaseUrl() + "/actuator/health",
                Stack.PAYMENT, config.paymentBaseUrl() + "/actuator/health",
                Stack.NOTIFICATION, config.notificationBaseUrl() + "/health/readiness"
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

    @Override
    public void waitUntilHealthy(String containerName, Duration timeout) {
        Awaitility.await("Container '" + containerName + "' to become healthy (including consumer group Stable check if applicable)")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(400))
                .until(() -> isHealthy(containerName));
        touchedContainers.remove(containerName);
    }

    @Override
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

        for (String container : Stack.ALL_CONTAINERS) {
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

        for (String container : Stack.ALL_CONTAINERS) {
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

    @Override
    public void restoreTouchedOnShutdown() {
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
}

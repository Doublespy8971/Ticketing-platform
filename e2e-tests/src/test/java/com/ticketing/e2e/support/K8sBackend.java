package com.ticketing.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Platform backend targeting Kubernetes via the {@code kubectl} CLI.
 */
public final class K8sBackend implements PlatformBackend {

    private static final Duration DEFAULT_CMD_TIMEOUT = Duration.ofSeconds(30);

    private static final Map<String, String> WORKLOAD_KINDS = Map.of(
            Stack.GATEWAY, "deployment",
            Stack.INVENTORY, "deployment",
            Stack.ORDER, "deployment",
            Stack.PAYMENT, "deployment",
            Stack.NOTIFICATION, "deployment",
            Stack.KAFKA, "statefulset",
            Stack.REDIS, "statefulset",
            Stack.POSTGRES_INVENTORY, "statefulset",
            Stack.POSTGRES_ORDER, "statefulset",
            Stack.POSTGRES_PAYMENT, "statefulset"
    );

    private static final Map<String, Integer> DEFAULT_REPLICAS = Map.of(
            Stack.GATEWAY, 1,
            Stack.INVENTORY, 2,
            Stack.ORDER, 1,
            Stack.PAYMENT, 2,
            Stack.NOTIFICATION, 1,
            Stack.KAFKA, 1,
            Stack.REDIS, 1,
            Stack.POSTGRES_INVENTORY, 1,
            Stack.POSTGRES_ORDER, 1,
            Stack.POSTGRES_PAYMENT, 1
    );

    private final E2EConfig config;
    private final ProcessRunner runner;
    private final PortChecker portChecker;
    private final String namespace;
    private final String context;
    private final ObjectMapper objectMapper;
    private final Path replicaStateFile;
    private final HttpClient httpClient;
    private volatile KafkaProbe kafkaProbe;

    private final Map<String, Integer> rememberedReplicas = new ConcurrentHashMap<>();
    private final Map<String, WorkloadInfo> workloadCache = new ConcurrentHashMap<>();

    public record WorkloadInfo(String kind, String selector, int defaultReplicas) {}

    public K8sBackend(E2EConfig config, KafkaProbe kafkaProbe) {
        this(config, kafkaProbe, new SystemProcessRunner(), new SocketPortChecker());
    }

    public K8sBackend(E2EConfig config, KafkaProbe kafkaProbe, ProcessRunner runner, PortChecker portChecker) {
        this.config = config;
        this.kafkaProbe = kafkaProbe;
        this.runner = runner;
        this.portChecker = portChecker;
        this.namespace = config.k8sNamespace();
        this.context = config.k8sContext();
        this.objectMapper = new ObjectMapper();
        this.replicaStateFile = config.workspaceRoot().resolve("e2e-tests/target/k8s-replicas.json");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        loadPersistedReplicas();
    }

    @Override
    public void setKafkaProbe(KafkaProbe kafkaProbe) {
        this.kafkaProbe = kafkaProbe;
    }

    @Override
    public List<String> listWorkloads() {
        return Stack.ALL_CONTAINERS;
    }

    public List<String> buildKubectl(String... args) {
        List<String> cmd = new ArrayList<>();
        cmd.add("kubectl");
        if (context != null && !context.isBlank()) {
            cmd.add("--context");
            cmd.add(context.trim());
        }
        cmd.add("-n");
        cmd.add(namespace);
        cmd.addAll(List.of(args));
        return cmd;
    }

    private List<String> buildKubectlNoNamespace(String... args) {
        List<String> cmd = new ArrayList<>();
        cmd.add("kubectl");
        if (context != null && !context.isBlank()) {
            cmd.add("--context");
            cmd.add(context.trim());
        }
        cmd.addAll(List.of(args));
        return cmd;
    }

    public WorkloadInfo getWorkloadInfo(String workload) {
        return workloadCache.computeIfAbsent(workload, this::discoverWorkload);
    }

    private WorkloadInfo discoverWorkload(String workload) {
        String kind = WORKLOAD_KINDS.get(workload);
        if (kind == null) {
            kind = "deployment";
        }
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", kind, workload, "-o", "json"));
        if (res.exitCode() != 0 && WORKLOAD_KINDS.get(workload) == null) {
            String altKind = "deployment".equalsIgnoreCase(kind) ? "statefulset" : "deployment";
            CommandResult altRes = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", altKind, workload, "-o", "json"));
            if (altRes.exitCode() == 0) {
                kind = altKind;
                res = altRes;
            }
        }
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to discover workload " + workload + " in namespace " + namespace
                    + ": " + res.stderr());
        }

        try {
            JsonNode root = objectMapper.readTree(res.stdout());
            JsonNode matchLabels = root.path("spec").path("selector").path("matchLabels");
            List<String> selectorParts = new ArrayList<>();
            matchLabels.fields().forEachRemaining(entry ->
                    selectorParts.add(entry.getKey() + "=" + entry.getValue().asText()));
            Collections.sort(selectorParts);
            String selector = String.join(",", selectorParts);
            if (selector.isBlank()) {
                selector = "app.kubernetes.io/name=" + workload;
            }

            int replicas = root.path("spec").path("replicas").asInt(0);
            if (replicas <= 0) {
                replicas = parseLastAppliedReplicas(root);
            }
            if (replicas <= 0) {
                replicas = DEFAULT_REPLICAS.getOrDefault(workload, 1);
            }

            return new WorkloadInfo(kind, selector, replicas);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse JSON specification for workload " + workload, e);
        }
    }

    private int parseLastAppliedReplicas(JsonNode root) {
        String lastApplied = root.path("metadata").path("annotations")
                .path("kubectl.kubernetes.io/last-applied-configuration").asText("");
        if (!lastApplied.isBlank()) {
            try {
                JsonNode appliedRoot = objectMapper.readTree(lastApplied);
                return appliedRoot.path("spec").path("replicas").asInt(0);
            } catch (IOException ignored) {
            }
        }
        return 0;
    }

    private int getCurrentDesiredReplicas(String kind, String workload) {
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", kind, workload, "-o", "jsonpath={.spec.replicas}"));
        if (res.exitCode() == 0 && !res.stdout().isBlank()) {
            try {
                return Integer.parseInt(res.stdout().trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return 0;
    }

    private synchronized void persistRememberedReplicas() {
        try {
            if (rememberedReplicas.isEmpty()) {
                if (Files.exists(replicaStateFile)) {
                    Files.deleteIfExists(replicaStateFile);
                }
                return;
            }
            if (replicaStateFile.getParent() != null) {
                Files.createDirectories(replicaStateFile.getParent());
            }
            objectMapper.writeValue(replicaStateFile.toFile(), rememberedReplicas);
        } catch (IOException e) {
            System.err.println("[K8sBackend] Warning: could not persist replica state: " + e.getMessage());
        }
    }

    private synchronized void loadPersistedReplicas() {
        if (!Files.isRegularFile(replicaStateFile)) {
            return;
        }
        try {
            JsonNode root = objectMapper.readTree(replicaStateFile.toFile());
            root.fields().forEachRemaining(entry -> {
                if (entry.getValue().isInt()) {
                    rememberedReplicas.putIfAbsent(entry.getKey(), entry.getValue().asInt());
                }
            });
        } catch (IOException e) {
            System.err.println("[K8sBackend] Warning: could not load persisted replica state: " + e.getMessage());
        }
    }

    public Map<String, Integer> getRememberedReplicas() {
        return Collections.unmodifiableMap(rememberedReplicas);
    }

    @Override
    public void kill(String workload) {
        WorkloadInfo info = getWorkloadInfo(workload);
        // Delete all pods of the workload forcefully with zero grace period using selector
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("delete", "pod", "-l", info.selector(), "--grace-period=0", "--force"));
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to kill pods for workload " + workload + ": " + res.stderr());
        }
    }

    @Override
    public void stop(String workload) {
        WorkloadInfo info = getWorkloadInfo(workload);
        // Remember replica count if not already remembered (do not overwrite on repeated stop)
        if (!rememberedReplicas.containsKey(workload)) {
            int current = getCurrentDesiredReplicas(info.kind(), workload);
            int toRemember = current > 0 ? current : info.defaultReplicas();
            rememberedReplicas.put(workload, toRemember);
            persistRememberedReplicas();
        }

        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("scale", info.kind(), workload, "--replicas=0"));
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to scale workload " + workload + " to 0: " + res.stderr());
        }

        // Wait until no pods exist for this workload's selector
        Awaitility.await("All pods of workload '" + workload + "' to terminate")
                .atMost(DEFAULT_CMD_TIMEOUT)
                .pollInterval(Duration.ofMillis(300))
                .until(() -> {
                    CommandResult podsRes = runner.exec(Duration.ofSeconds(10), buildKubectl("get", "pods", "-l", info.selector(), "--no-headers"));
                    return podsRes.exitCode() == 0 && podsRes.stdout().trim().isEmpty();
                });
    }

    @Override
    public void start(String workload) {
        WorkloadInfo info = getWorkloadInfo(workload);
        Integer remembered = rememberedReplicas.remove(workload);
        persistRememberedReplicas();
        int targetReplicas = (remembered != null && remembered > 0) ? remembered : info.defaultReplicas();

        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("scale", info.kind(), workload, "--replicas=" + targetReplicas));
        if (res.exitCode() != 0) {
            throw new IllegalStateException("Failed to scale workload " + workload + " to " + targetReplicas + ": " + res.stderr());
        }
    }

    @Override
    public void restart(String workload) {
        stop(workload);
        start(workload);
    }

    @Override
    public void pause(String workload) {
        // Kubernetes has no native container pause primitive without host cgroup freezer access.
        // Implemented as stop (scale to 0) to suspend consumption/processing during the scenario.
        stop(workload);
    }

    @Override
    public void unpause(String workload) {
        // Implemented as start (scale back to desired replicas and wait for readiness).
        start(workload);
    }

    private Integer getHostPortForWorkload(String workload) {
        return switch (workload) {
            case Stack.KAFKA -> config.kafkaHostPort();
            case Stack.REDIS -> config.redisPort();
            case Stack.POSTGRES_INVENTORY -> config.postgresInventoryPort();
            case Stack.POSTGRES_ORDER -> config.postgresOrderPort();
            case Stack.POSTGRES_PAYMENT -> config.postgresPaymentPort();
            default -> null;
        };
    }

    @Override
    public void waitUntilHealthy(String workload, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        WorkloadInfo info = getWorkloadInfo(workload);

        // 1. Await rollout completion for all replicas
        Duration remaining = Duration.between(Instant.now(), deadline);
        if (remaining.isNegative() || remaining.isZero()) remaining = Duration.ofSeconds(1);
        CommandResult rollout = runner.exec(remaining, buildKubectl("rollout", "status", info.kind(), workload, "--timeout=" + remaining.toSeconds() + "s"));
        if (rollout.exitCode() != 0) {
            throw new IllegalStateException("Rollout for workload " + workload + " did not complete within "
                    + timeout.toSeconds() + "s: " + rollout.stderr());
        }

        // 2. If workload consumes Kafka, verify consumer group is Stable with active members
        List<String> cgs = Stack.SERVICE_CONSUMER_GROUPS.get(workload);
        if (cgs != null && kafkaProbe != null) {
            remaining = Duration.between(Instant.now(), deadline);
            if (remaining.isNegative() || remaining.isZero()) remaining = Duration.ofSeconds(1);
            Awaitility.await("Consumer groups for " + workload + " to become Stable with member")
                    .atMost(remaining)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> kafkaProbe.areConsumerGroupsStableWithMember(cgs));
        }

        // 3. For infra workloads, verify the supervised host port accepts TCP connections
        Integer hostPort = getHostPortForWorkload(workload);
        if (hostPort != null) {
            remaining = Duration.between(Instant.now(), deadline);
            if (remaining.isNegative() || remaining.isZero()) remaining = Duration.ofSeconds(1);
            try {
                Awaitility.await("Host port " + hostPort + " for " + workload + " to accept TCP connection")
                        .atMost(remaining)
                        .pollInterval(Duration.ofMillis(400))
                        .until(() -> portChecker.isPortOpen("127.0.0.1", hostPort, Duration.ofMillis(500)));
            } catch (org.awaitility.core.ConditionTimeoutException ex) {
                throw new IllegalStateException("Port " + hostPort + " for " + workload + " did not accept TCP connections within "
                        + timeout.toSeconds() + "s. Check 'make k8s-forward'.", ex);
            }
        }
    }

    @Override
    public boolean isHealthy(String workload) {
        WorkloadInfo info = getWorkloadInfo(workload);
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", info.kind(), workload, "-o", "json"));
        if (res.exitCode() != 0) {
            return false;
        }
        try {
            JsonNode root = objectMapper.readTree(res.stdout());
            int spec = root.path("spec").path("replicas").asInt(0);
            int ready = "deployment".equalsIgnoreCase(info.kind())
                    ? root.path("status").path("availableReplicas").asInt(0)
                    : root.path("status").path("readyReplicas").asInt(0);
            if (spec <= 0 || ready < spec) {
                return false;
            }
        } catch (IOException e) {
            return false;
        }

        List<String> cgs = Stack.SERVICE_CONSUMER_GROUPS.get(workload);
        if (cgs != null && kafkaProbe != null) {
            if (!kafkaProbe.areConsumerGroupsStableWithMember(cgs)) {
                return false;
            }
        }

        Integer hostPort = getHostPortForWorkload(workload);
        if (hostPort != null && !portChecker.isPortOpen("127.0.0.1", hostPort, Duration.ofMillis(400))) {
            return false;
        }
        return true;
    }

    @Override
    public String inspectHealthStatus(String workload) {
        WorkloadInfo info = getWorkloadInfo(workload);
        CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", info.kind(), workload, "-o", "json"));
        if (res.exitCode() != 0) {
            return "missing (" + res.stderr().trim() + ")";
        }
        try {
            JsonNode root = objectMapper.readTree(res.stdout());
            int spec = root.path("spec").path("replicas").asInt(0);
            int ready = "deployment".equalsIgnoreCase(info.kind())
                    ? root.path("status").path("availableReplicas").asInt(0)
                    : root.path("status").path("readyReplicas").asInt(0);
            return (ready >= spec && spec > 0) ? "running|false|healthy" : "not-ready|" + ready + "/" + spec;
        } catch (IOException e) {
            return "error (" + e.getMessage() + ")";
        }
    }

    @Override
    public void restoreAndWaitAllHealthy(Duration timeout) {
        List<String> errors = new ArrayList<>();
        List<Throwable> causes = new ArrayList<>();

        loadPersistedReplicas();
        Map<String, Integer> toRestore = new HashMap<>(rememberedReplicas);
        for (Map.Entry<String, Integer> entry : toRestore.entrySet()) {
            String workload = entry.getKey();
            int count = entry.getValue();
            try {
                WorkloadInfo info = getWorkloadInfo(workload);
                CommandResult res = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("scale", info.kind(), workload, "--replicas=" + count));
                if (res.exitCode() != 0) {
                    errors.add("Failed to scale " + workload + " to " + count + ": " + res.stderr());
                } else {
                    rememberedReplicas.remove(workload);
                }
            } catch (Throwable t) {
                errors.add("Failed to restore " + workload + ": " + t.getMessage());
                causes.add(t);
            }
        }
        persistRememberedReplicas();

        for (String workload : Stack.ALL_CONTAINERS) {
            try {
                waitUntilHealthy(workload, timeout);
            } catch (Throwable t) {
                errors.add("Workload " + workload + " did not become healthy within " + timeout.toSeconds() + "s: " + t.getMessage());
                causes.add(t);
            }
        }

        if (!errors.isEmpty()) {
            IllegalStateException ex = new IllegalStateException(
                    "K8s stack restore failed with " + errors.size() + " error(s):\n  - " + String.join("\n  - ", errors),
                    causes.isEmpty() ? null : causes.get(0)
            );
            for (int i = 1; i < causes.size(); i++) {
                ex.addSuppressed(causes.get(i));
            }
            throw ex;
        }
    }

    @Override
    public void restoreTouchedOnShutdown() {
        if (rememberedReplicas.isEmpty() && !Files.isRegularFile(replicaStateFile)) {
            return;
        }
        try {
            restoreAndWaitAllHealthy(Duration.ofSeconds(45));
        } catch (Throwable t) {
            System.err.println("[K8sBackend Shutdown Hook] Error while restoring workloads: " + t.getMessage());
            throw t;
        }
    }

    @Override
    public void verifyPreflight() {
        // 1. kubectl CLI on PATH
        CommandResult versionRes = runner.exec(Duration.ofSeconds(15), "kubectl", "version", "--client");
        if (versionRes.exitCode() != 0) {
            throw new AssertionError("Preflight FAILED: kubectl CLI check - kubectl is not available on PATH (exit="
                    + versionRes.exitCode() + ", stderr=" + versionRes.stderr() + "). Install kubectl and ensure it is in PATH.");
        }

        // 2. Reachable cluster
        CommandResult clusterRes = runner.exec(Duration.ofSeconds(20), buildKubectlNoNamespace("cluster-info"));
        if (clusterRes.exitCode() != 0) {
            throw new AssertionError("Preflight FAILED: Reachable cluster check - cannot reach Kubernetes cluster (exit="
                    + clusterRes.exitCode() + ", stderr=" + clusterRes.stderr() + "). Run 'make k8s-up' first.");
        }

        // 3. Namespace exists
        CommandResult nsRes = runner.exec(Duration.ofSeconds(15), buildKubectlNoNamespace("get", "namespace", namespace));
        if (nsRes.exitCode() != 0) {
            throw new AssertionError("Preflight FAILED: Namespace check - namespace '" + namespace + "' does not exist (exit="
                    + nsRes.exitCode() + ", stderr=" + nsRes.stderr() + "). Run 'make k8s-up' first.");
        }

        // 4. Every workload ready - query all workloads in namespace
        CommandResult allRes = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", "deployment,statefulset", "-o", "json"));
        if (allRes.exitCode() == 0 && !allRes.stdout().isBlank()) {
            try {
                JsonNode root = objectMapper.readTree(allRes.stdout());
                Map<String, JsonNode> itemsByName = new HashMap<>();
                for (JsonNode item : root.path("items")) {
                    itemsByName.put(item.path("metadata").path("name").asText(), item);
                }
                for (String workload : Stack.ALL_CONTAINERS) {
                    JsonNode item = itemsByName.get(workload);
                    if (item == null) {
                        throw new AssertionError("Preflight FAILED: Workload readiness check - workload '" + workload + "' not found in namespace '"
                                + namespace + "'. Run 'make k8s-up' first.");
                    }
                    String kind = item.path("kind").asText("Deployment");
                    int specReplicas = item.path("spec").path("replicas").asInt(0);
                    int readyReplicas = "deployment".equalsIgnoreCase(kind)
                            ? item.path("status").path("availableReplicas").asInt(0)
                            : item.path("status").path("readyReplicas").asInt(0);
                    if (specReplicas <= 0 || readyReplicas < specReplicas) {
                        throw new AssertionError("Preflight FAILED: Workload readiness check - workload '" + workload
                                + "' is not ready (ready=" + readyReplicas + "/" + specReplicas + "). Run 'make k8s-up' first.");
                    }
                }
                return;
            } catch (IOException ignored) {
            }
        }

        // Fall back to per-workload query if batch get was not supported or failed
        for (String workload : Stack.ALL_CONTAINERS) {
            WorkloadInfo info = getWorkloadInfo(workload);
            CommandResult getRes = runner.exec(DEFAULT_CMD_TIMEOUT, buildKubectl("get", info.kind(), workload, "-o", "json"));
            if (getRes.exitCode() != 0) {
                throw new AssertionError("Preflight FAILED: Workload readiness check - workload '" + workload + "' not found in namespace '"
                        + namespace + "': " + getRes.stderr() + ". Run 'make k8s-up' first.");
            }
            try {
                JsonNode root = objectMapper.readTree(getRes.stdout());
                int specReplicas = root.path("spec").path("replicas").asInt(0);
                int readyReplicas = "deployment".equalsIgnoreCase(info.kind())
                        ? root.path("status").path("availableReplicas").asInt(0)
                        : root.path("status").path("readyReplicas").asInt(0);
                if (specReplicas <= 0 || readyReplicas < specReplicas) {
                    throw new AssertionError("Preflight FAILED: Workload readiness check - workload '" + workload
                            + "' is not ready (ready=" + readyReplicas + "/" + specReplicas + "). Run 'make k8s-up' first.");
                }
            } catch (IOException e) {
                throw new AssertionError("Preflight FAILED: Workload readiness check - unable to parse JSON for workload '" + workload + "': " + e.getMessage());
            }
        }
    }

    @Override
    public String backendDescription() {
        try {
            CommandResult vRes = runner.exec(Duration.ofSeconds(15), buildKubectlNoNamespace("version", "-o", "json"));
            String k8sVersion = "k8s";
            if (vRes.exitCode() == 0 && !vRes.stdout().isBlank()) {
                JsonNode vJson = objectMapper.readTree(vRes.stdout());
                String gitVer = vJson.path("serverVersion").path("gitVersion").asText("");
                if (!gitVer.isBlank()) {
                    k8sVersion = gitVer;
                }
            }
            CommandResult nodesRes = runner.exec(Duration.ofSeconds(15), buildKubectlNoNamespace("get", "nodes", "--no-headers"));
            int nodeCount = 0;
            if (nodesRes.exitCode() == 0 && !nodesRes.stdout().isBlank()) {
                String[] lines = nodesRes.stdout().split("\n");
                for (String line : lines) {
                    if (!line.trim().isEmpty()) {
                        nodeCount++;
                    }
                }
            }
            if (nodeCount > 0) {
                return "k8s (" + k8sVersion + ", " + nodeCount + " nodes)";
            }
            return "k8s (" + k8sVersion + ")";
        } catch (Exception e) {
            return "k8s";
        }
    }
}

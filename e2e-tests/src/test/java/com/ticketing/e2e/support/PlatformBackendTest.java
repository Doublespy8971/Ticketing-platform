package com.ticketing.e2e.support;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Offline unit tests for {@link ComposeBackend} and {@link K8sBackend}.
 * Runs without Docker daemon or Kubernetes cluster via fake process runners and port checkers.
 */
class PlatformBackendTest {

    private static final List<String> TEST_METHODS = List.of(
            "composeBackendReceivesExactDockerCommands",
            "k8sBackendExactKubectlArgumentsForOperations",
            "k8sBackendReplicaBookkeepingAndSecondStopRetention",
            "k8sBackendRestoreAfterExceptionRestoresAll",
            "k8sBackendPortWaitLogicSuccessAndTimeout",
            "k8sBackendPreflightDiagnosticFailures"
    );

    @BeforeAll
    static void printMethodsStart() {
        System.out.println("PlatformBackendTest methods (start): " + TEST_METHODS);
    }

    @AfterAll
    static void printMethodsEnd() {
        System.out.println("PlatformBackendTest methods (end): " + TEST_METHODS);
    }

    private static class RecordingProcessRunner implements ProcessRunner {
        final List<List<String>> executedCommands = Collections.synchronizedList(new ArrayList<>());
        java.util.function.Function<List<String>, CommandResult> handler = cmd -> new CommandResult(0, "", "");

        @Override
        public CommandResult exec(Duration timeout, List<String> command) {
            executedCommands.add(new ArrayList<>(command));
            return handler.apply(command);
        }
    }

    @Test
    void composeBackendReceivesExactDockerCommands() {
        RecordingProcessRunner runner = new RecordingProcessRunner();
        runner.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("docker inspect")) {
                // Return running and healthy
                return new CommandResult(0, "running|false|healthy\n", "");
            }
            if (full.contains("docker version")) {
                return new CommandResult(0, "27.0.3\n", "");
            }
            return new CommandResult(0, "ok\n", "");
        };

        E2EConfig config = E2EConfig.load();
        ComposeBackend backend = new ComposeBackend(config, null, runner);

        backend.verifyPreflight();
        backend.stop("payment-service");
        backend.start("payment-service");
        backend.kill("payment-service");
        backend.restart("payment-service");
        backend.pause("payment-service");
        backend.unpause("payment-service");
        backend.inspectHealthStatus("payment-service");

        List<List<String>> recorded = runner.executedCommands;
        assertThat(recorded).isNotEmpty();

        assertThat(recorded.get(0)).containsExactly("docker", "version", "--format", "{{.Server.Version}}");
        assertThat(recorded.get(1)).containsExactly("docker", "stop", "-t", "5", "payment-service");
        assertThat(recorded.get(2)).containsExactly("docker", "start", "payment-service");
        assertThat(recorded.get(3)).containsExactly("docker", "kill", "-s", "KILL", "payment-service");
        assertThat(recorded.get(4)).containsExactly("docker", "restart", "-t", "5", "payment-service");
        assertThat(recorded.get(5)).containsExactly("docker", "pause", "payment-service");
        // unpause inspects state first
        assertThat(recorded.get(6)).contains("docker", "inspect", "--format", "{{.State.Status}}|{{.State.Paused}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", "payment-service");
        // then inspectHealthStatus calls inspect
        assertThat(recorded.get(7)).contains("docker", "inspect", "--format", "{{.State.Status}}|{{.State.Paused}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", "payment-service");
    }

    @Test
    void k8sBackendExactKubectlArgumentsForOperations() {
        RecordingProcessRunner runner = new RecordingProcessRunner();
        runner.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("get deployment payment-service -o json")) {
                return new CommandResult(0, """
                        {
                          "kind": "Deployment",
                          "spec": {
                            "replicas": 2,
                            "selector": {
                              "matchLabels": {
                                "app.kubernetes.io/name": "payment-service"
                              }
                            }
                          }
                        }
                        """, "");
            }
            if (full.contains("get deployment payment-service -o jsonpath={.spec.replicas}")) {
                return new CommandResult(0, "2", "");
            }
            if (full.contains("get pods -l app.kubernetes.io/name=payment-service --no-headers")) {
                return new CommandResult(0, "", ""); // no pods left -> terminated
            }
            if (full.contains("rollout status deployment payment-service")) {
                return new CommandResult(0, "deployment \"payment-service\" successfully rolled out\n", "");
            }
            return new CommandResult(0, "", "");
        };

        E2EConfig config = E2EConfig.load();
        K8sBackend backend = new K8sBackend(config, null, runner, (h, p, t) -> true);

        // 1. kill
        backend.kill("payment-service");
        List<String> killCmd = runner.executedCommands.get(runner.executedCommands.size() - 1);
        assertThat(killCmd).containsExactly("kubectl", "-n", "ticketing", "delete", "pod", "-l", "app.kubernetes.io/name=payment-service", "--grace-period=0", "--force");

        // 2. stop
        backend.stop("payment-service");
        boolean foundScale0 = runner.executedCommands.stream()
                .anyMatch(c -> c.equals(List.of("kubectl", "-n", "ticketing", "scale", "deployment", "payment-service", "--replicas=0")));
        assertThat(foundScale0).isTrue();

        // 3. start
        backend.start("payment-service");
        boolean foundScale2 = runner.executedCommands.stream()
                .anyMatch(c -> c.equals(List.of("kubectl", "-n", "ticketing", "scale", "deployment", "payment-service", "--replicas=2")));
        assertThat(foundScale2).isTrue();

        // 4. waitUntilHealthy
        backend.waitUntilHealthy("payment-service", Duration.ofSeconds(60));
        boolean foundRollout = runner.executedCommands.stream()
                .anyMatch(c -> c.contains("rollout") && c.contains("status") && c.contains("deployment") && c.contains("payment-service"));
        assertThat(foundRollout).isTrue();
    }

    @Test
    void k8sBackendReplicaBookkeepingAndSecondStopRetention(@TempDir Path tempDir) {
        RecordingProcessRunner runner = new RecordingProcessRunner();
        AtomicInteger currentReplicas = new AtomicInteger(2);

        runner.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("get deployment payment-service -o json")) {
                return new CommandResult(0, """
                        {
                          "kind": "Deployment",
                          "spec": {
                            "replicas": 2,
                            "selector": {
                              "matchLabels": { "app.kubernetes.io/name": "payment-service" }
                            }
                          }
                        }
                        """, "");
            }
            if (full.contains("get deployment payment-service -o jsonpath={.spec.replicas}")) {
                return new CommandResult(0, String.valueOf(currentReplicas.get()), "");
            }
            if (full.contains("scale deployment payment-service --replicas=0")) {
                currentReplicas.set(0);
                return new CommandResult(0, "scaled\n", "");
            }
            if (full.contains("scale deployment payment-service --replicas=2")) {
                currentReplicas.set(2);
                return new CommandResult(0, "scaled\n", "");
            }
            if (full.contains("get pods -l app.kubernetes.io/name=payment-service --no-headers")) {
                return new CommandResult(0, "", "");
            }
            if (full.contains("rollout status deployment payment-service")) {
                return new CommandResult(0, "success\n", "");
            }
            return new CommandResult(0, "", "");
        };

        E2EConfig config = E2EConfig.load();
        K8sBackend backend = new K8sBackend(config, null, runner, (h, p, t) -> true);

        // Initial stop remembers 2
        backend.stop("payment-service");
        assertThat(backend.getRememberedReplicas()).containsEntry("payment-service", 2);

        // A second stop while current desired replicas is 0 MUST NOT overwrite remembered with 0
        backend.stop("payment-service");
        assertThat(backend.getRememberedReplicas()).containsEntry("payment-service", 2);

        // Start restores 2
        backend.start("payment-service");
        assertThat(backend.getRememberedReplicas()).doesNotContainKey("payment-service");
    }

    @Test
    void k8sBackendRestoreAfterExceptionRestoresAll() {
        RecordingProcessRunner runner = new RecordingProcessRunner();
        runner.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("get deployment") || full.contains("get statefulset")) {
                return new CommandResult(0, """
                        {
                          "kind": "Deployment",
                          "spec": {
                            "replicas": 2,
                            "selector": { "matchLabels": { "app.kubernetes.io/name": "test-workload" } }
                          }
                        }
                        """, "");
            }
            if (full.contains("scale")) {
                return new CommandResult(0, "scaled\n", "");
            }
            if (full.contains("rollout status")) {
                return new CommandResult(0, "success\n", "");
            }
            return new CommandResult(0, "", "");
        };

        E2EConfig config = E2EConfig.load();
        K8sBackend backend = new K8sBackend(config, null, runner, (h, p, t) -> true);

        // Simulate stopped workload
        backend.stop("inventory-service");
        assertThat(backend.getRememberedReplicas()).containsKey("inventory-service");

        // Now restore all
        backend.restoreAndWaitAllHealthy(Duration.ofSeconds(5));
        assertThat(backend.getRememberedReplicas()).isEmpty();
    }

    @Test
    void k8sBackendPortWaitLogicSuccessAndTimeout() {
        RecordingProcessRunner runner = new RecordingProcessRunner();
        runner.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("get statefulset kafka -o json")) {
                return new CommandResult(0, """
                        {
                          "kind": "StatefulSet",
                          "spec": {
                            "replicas": 1,
                            "selector": { "matchLabels": { "app.kubernetes.io/name": "kafka" } }
                          }
                        }
                        """, "");
            }
            if (full.contains("rollout status statefulset kafka")) {
                return new CommandResult(0, "success\n", "");
            }
            return new CommandResult(0, "", "");
        };

        E2EConfig config = E2EConfig.load();

        // 1. Success after 2 attempts
        AtomicInteger attempts = new AtomicInteger(0);
        PortChecker retryingChecker = (h, p, t) -> attempts.incrementAndGet() >= 2;
        K8sBackend backendSuccess = new K8sBackend(config, null, runner, retryingChecker);
        backendSuccess.waitUntilHealthy("kafka", Duration.ofSeconds(5));
        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);

        // 2. Timeout failure with message mentioning make k8s-forward
        PortChecker failingChecker = (h, p, t) -> false;
        K8sBackend backendFailure = new K8sBackend(config, null, runner, failingChecker);
        assertThatThrownBy(() -> backendFailure.waitUntilHealthy("kafka", Duration.ofMillis(800)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Port 29092 for kafka did not accept TCP connections")
                .hasMessageContaining("Check 'make k8s-forward'");
    }

    @Test
    void k8sBackendPreflightDiagnosticFailures() {
        E2EConfig config = E2EConfig.load();

        // Case 1: kubectl CLI missing
        RecordingProcessRunner noKubectl = new RecordingProcessRunner();
        noKubectl.handler = cmd -> new CommandResult(127, "", "kubectl: command not found");
        K8sBackend backendNoKubectl = new K8sBackend(config, null, noKubectl, (h, p, t) -> true);
        assertThatThrownBy(backendNoKubectl::verifyPreflight)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Preflight FAILED: kubectl CLI check");

        // Case 2: Cluster unreachable
        RecordingProcessRunner clusterUnreachable = new RecordingProcessRunner();
        clusterUnreachable.handler = cmd -> {
            if (String.join(" ", cmd).contains("kubectl version --client")) {
                return new CommandResult(0, "Client Version: v1.31.0\n", "");
            }
            return new CommandResult(1, "", "Unable to connect to the server");
        };
        K8sBackend backendClusterDown = new K8sBackend(config, null, clusterUnreachable, (h, p, t) -> true);
        assertThatThrownBy(backendClusterDown::verifyPreflight)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Preflight FAILED: Reachable cluster check");

        // Case 3: Namespace missing
        RecordingProcessRunner nsMissing = new RecordingProcessRunner();
        nsMissing.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("kubectl version --client")) {
                return new CommandResult(0, "Client Version: v1.31.0\n", "");
            }
            if (full.contains("cluster-info")) {
                return new CommandResult(0, "Kubernetes control plane is running\n", "");
            }
            if (full.contains("get namespace ticketing")) {
                return new CommandResult(1, "", "Error from server (NotFound): namespaces \"ticketing\" not found");
            }
            return new CommandResult(0, "", "");
        };
        K8sBackend backendNsMissing = new K8sBackend(config, null, nsMissing, (h, p, t) -> true);
        assertThatThrownBy(backendNsMissing::verifyPreflight)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Preflight FAILED: Namespace check");

        // Case 4: Workload not ready
        RecordingProcessRunner notReady = new RecordingProcessRunner();
        notReady.handler = cmd -> {
            String full = String.join(" ", cmd);
            if (full.contains("kubectl version --client")) {
                return new CommandResult(0, "Client Version: v1.31.0\n", "");
            }
            if (full.contains("cluster-info") || full.contains("get namespace ticketing")) {
                return new CommandResult(0, "ok\n", "");
            }
            if (full.contains("get deployment,statefulset -o json")) {
                return new CommandResult(0, """
                        {
                          "items": [
                            {
                              "kind": "Deployment",
                              "metadata": { "name": "gateway-service" },
                              "spec": { "replicas": 1 },
                              "status": { "availableReplicas": 0 }
                            }
                          ]
                        }
                        """, "");
            }
            if (full.contains("get deployment gateway-service -o json")) {
                return new CommandResult(0, """
                        {
                          "kind": "Deployment",
                          "spec": { "replicas": 1, "selector": { "matchLabels": { "app.kubernetes.io/name": "gateway-service" } } },
                          "status": { "availableReplicas": 0 }
                        }
                        """, "");
            }
            return new CommandResult(0, "", "");
        };
        K8sBackend backendNotReady = new K8sBackend(config, null, notReady, (h, p, t) -> true);
        assertThatThrownBy(backendNotReady::verifyPreflight)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Preflight FAILED: Workload readiness check");
    }
}

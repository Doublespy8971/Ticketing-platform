package com.ticketing.e2e.support;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * High-level control harness for the platform topology, delegating to either
 * {@link ComposeBackend} (default) or {@link K8sBackend} based on {@link E2EConfig#backend()}.
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

    private final E2EConfig config;
    private final PlatformBackend backend;
    private volatile KafkaProbe kafkaProbe;
    private final ProcessRunner processRunner;

    public Stack(E2EConfig config) {
        this(config, null);
    }

    public Stack(E2EConfig config, KafkaProbe kafkaProbe) {
        this(config, kafkaProbe, createDefaultBackend(config, kafkaProbe));
    }

    public Stack(E2EConfig config, KafkaProbe kafkaProbe, PlatformBackend backend) {
        this.config = config;
        this.kafkaProbe = kafkaProbe;
        this.backend = backend;
        this.processRunner = new SystemProcessRunner();
        if (kafkaProbe != null) {
            this.backend.setKafkaProbe(kafkaProbe);
        }
    }

    public static PlatformBackend createDefaultBackend(E2EConfig config, KafkaProbe kafkaProbe) {
        if (config.backend() == E2EConfig.BackendType.K8S) {
            return new K8sBackend(config, kafkaProbe);
        }
        return new ComposeBackend(config, kafkaProbe);
    }

    public PlatformBackend getBackend() {
        return backend;
    }

    public void setKafkaProbe(KafkaProbe kafkaProbe) {
        this.kafkaProbe = kafkaProbe;
        if (backend != null) {
            backend.setKafkaProbe(kafkaProbe);
        }
    }

    public void verifyDockerCliAvailable() {
        backend.verifyPreflight();
    }

    public void stop(String containerName) {
        backend.stop(containerName);
    }

    public void start(String containerName) {
        backend.start(containerName);
    }

    public void kill(String containerName) {
        backend.kill(containerName);
    }

    public void restart(String containerName) {
        backend.restart(containerName);
    }

    public void pause(String containerName) {
        backend.pause(containerName);
    }

    public void unpause(String containerName) {
        backend.unpause(containerName);
    }

    public String inspectHealthStatus(String containerName) {
        return backend.inspectHealthStatus(containerName);
    }

    public boolean isContainerHealthy(String containerName) {
        return backend.isHealthy(containerName);
    }

    public void waitUntilHealthy(String containerName, Duration timeout) {
        backend.waitUntilHealthy(containerName, timeout);
    }

    public void restoreAndWaitAllHealthy(Duration timeout) {
        backend.restoreAndWaitAllHealthy(timeout);
    }

    public void restoreTouchedContainersOnShutdown() {
        backend.restoreTouchedOnShutdown();
    }

    public CommandResult exec(Duration timeout, String... command) {
        return processRunner.exec(timeout, command);
    }
}

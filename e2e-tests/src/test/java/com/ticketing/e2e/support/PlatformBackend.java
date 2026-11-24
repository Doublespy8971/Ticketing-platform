package com.ticketing.e2e.support;

import java.time.Duration;
import java.util.List;

/**
 * Common abstraction for operations the E2E test harness performs on the running platform
 * (Docker Compose or Kubernetes).
 */
public interface PlatformBackend {

    void kill(String workload);

    void stop(String workload);

    void start(String workload);

    void restart(String workload);

    void pause(String workload);

    void unpause(String workload);

    void waitUntilHealthy(String workload, Duration timeout);

    void restoreAndWaitAllHealthy(Duration timeout);

    void restoreTouchedOnShutdown();

    void verifyPreflight();

    List<String> listWorkloads();

    String inspectHealthStatus(String workload);

    boolean isHealthy(String workload);

    void setKafkaProbe(KafkaProbe kafkaProbe);

    String backendDescription();
}

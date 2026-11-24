package com.ticketing.e2e;

import com.ticketing.e2e.support.CommandResult;
import com.ticketing.e2e.support.E2EConfig;
import com.ticketing.e2e.support.ProcessRunner;
import com.ticketing.e2e.support.ScenarioReportRecorder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic unit tests for {@link ScenarioReportRecorder}.
 */
class ScenarioReportRecorderTest {

    private static final List<String> TEST_METHODS = List.of(
            "failingScenarioRendersFailAndNeverContainsVerified",
            "unrunScenariosRenderNotRunOnRowsAndPartialOnGuaranteesAndZeroRunRendersNotRun",
            "fullyPassingSuiteRendersVerifiedPublishesDocsAndReconcilesAllTotalsAndDeadLetters",
            "reportHeaderIncludesBackendAndFailoverBudget"
    );

    @BeforeAll
    static void printMethodsStart() {
        System.out.println("ScenarioReportRecorderTest methods (start): " + TEST_METHODS);
    }

    @AfterAll
    static void printMethodsEnd() {
        System.out.println("ScenarioReportRecorderTest methods (end): " + TEST_METHODS);
    }

    private static Map<String, Boolean> allTrueInvariants() {
        Map<String, Boolean> map = new LinkedHashMap<>();
        for (String id : List.of("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8")) {
            map.put(id, true);
        }
        return map;
    }

    private static ScenarioReportRecorder.ScenarioRecord makeRecord(
            String id,
            String status,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            int reservationsCommitted,
            int ordersCreated,
            int paymentsSucceeded,
            int paymentsFailed,
            int refunds,
            long notificationsConfirmed,
            long notificationsCancelled,
            long deadLetters,
            long expectedDeadLetters,
            Map<String, Boolean> invariants
    ) {
        return new ScenarioReportRecorder.ScenarioRecord(
                id,
                "Scenario " + id,
                "unit",
                status,
                250L,
                10,
                holdsAttempted,
                holdsAccepted,
                holdsRejected,
                holdsRateLimited,
                holdsFailedOrLost,
                reservationsCommitted,
                Map.of("CONFIRMED", ordersCreated),
                ordersCreated,
                Map.of("CONFIRMED", ordersCreated),
                paymentsSucceeded,
                paymentsFailed,
                refunds,
                notificationsConfirmed,
                notificationsCancelled,
                deadLetters,
                expectedDeadLetters,
                invariants,
                "unit-test"
        );
    }

    private static String withPublishFlag(boolean publish, ThrowingRunnable action) throws Exception {
        String previous = System.getProperty("e2e.publishReport");
        System.setProperty("e2e.publishReport", Boolean.toString(publish));
        try {
            action.run();
        } finally {
            if (previous == null) {
                System.clearProperty("e2e.publishReport");
            } else {
                System.setProperty("e2e.publishReport", previous);
            }
        }
        return previous;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void failingScenarioRendersFailAndNeverContainsVerified(@TempDir Path tempDir) throws Exception {
        ScenarioReportRecorder recorder = new ScenarioReportRecorder();
        for (int i = 1; i <= 12; i++) {
            recorder.record(makeRecord(
                    "S" + i, "PASS",
                    10, 5, 4, 1, 0,
                    5, 5, 5, 0, 0,
                    5L, 0L, 0L, 0L,
                    allTrueInvariants()
            ));
        }
        Map<String, Boolean> failInvariants = allTrueInvariants();
        failInvariants.put("I1", false);
        recorder.record(makeRecord(
                "S13", "FAIL",
                5, 2, 2, 0, 1,
                2, 2, 2, 0, 0,
                2L, 0L, 0L, 0L,
                failInvariants
        ));

        withPublishFlag(true, () -> recorder.writeReports(tempDir));

        Path targetReport = tempDir.resolve("e2e-tests/target/e2e-report.md");
        Path docsReport = tempDir.resolve("docs/e2e-report.md");

        assertThat(targetReport).exists();
        assertThat(docsReport).doesNotExist();

        String markdown = Files.readString(targetReport, StandardCharsets.UTF_8);
        assertThat(markdown)
                .contains("**FAIL**")
                .doesNotContain("VERIFIED");
    }

    @Test
    void unrunScenariosRenderNotRunOnRowsAndPartialOnGuaranteesAndZeroRunRendersNotRun(@TempDir Path tempDir) throws Exception {
        // Case 1: Zero scenarios executed
        Path emptyRoot = Files.createDirectories(tempDir.resolve("empty"));
        ScenarioReportRecorder emptyRecorder = new ScenarioReportRecorder();
        withPublishFlag(true, () -> emptyRecorder.writeReports(emptyRoot));

        Path emptyTarget = emptyRoot.resolve("e2e-tests/target/e2e-report.md");
        Path emptyDocs = emptyRoot.resolve("docs/e2e-report.md");
        assertThat(emptyTarget).exists();
        assertThat(emptyDocs).doesNotExist();

        String emptyMarkdown = Files.readString(emptyTarget, StandardCharsets.UTF_8);
        assertThat(emptyMarkdown)
                .contains("**NOT RUN**")
                .doesNotContain("VERIFIED")
                .doesNotContain("**FAIL**");

        // Case 2: Subset (S1..S3) executed
        Path partialRoot = Files.createDirectories(tempDir.resolve("partial"));
        ScenarioReportRecorder partialRecorder = new ScenarioReportRecorder();
        for (int i = 1; i <= 3; i++) {
            partialRecorder.record(makeRecord(
                    "S" + i, "PASS",
                    6, 3, 3, 0, 0,
                    3, 3, 3, 0, 0,
                    3L, 0L, 0L, 0L,
                    allTrueInvariants()
            ));
        }
        withPublishFlag(true, () -> partialRecorder.writeReports(partialRoot));

        Path partialTarget = partialRoot.resolve("e2e-tests/target/e2e-report.md");
        Path partialDocs = partialRoot.resolve("docs/e2e-report.md");
        assertThat(partialTarget).exists();
        assertThat(partialDocs).doesNotExist();

        String partialMarkdown = Files.readString(partialTarget, StandardCharsets.UTF_8);
        assertThat(partialMarkdown)
                .contains("**PARTIAL**")
                .contains("**NOT RUN**")
                .doesNotContain("VERIFIED");
    }

    @Test
    void fullyPassingSuiteRendersVerifiedPublishesDocsAndReconcilesAllTotalsAndDeadLetters(@TempDir Path tempDir) throws Exception {
        ScenarioReportRecorder recorder = new ScenarioReportRecorder();

        // Record S1..S12 with known metrics + S13 with distinct metrics to verify totals
        int totalAttempted = 0;
        int totalAccepted = 0;
        int totalRejected = 0;
        int totalRateLimited = 0;
        int totalFailedOrLost = 0;
        int totalReservations = 0;
        int totalOrders = 0;
        int totalPaymentsSucceeded = 0;
        int totalPaymentsFailed = 0;
        int totalRefunds = 0;
        long totalNotifConfirmed = 0L;
        long totalNotifCancelled = 0L;
        long totalDlt = 0L;
        long totalExpectedDlt = 0L;

        for (int i = 1; i <= 13; i++) {
            int attempted = 10 + i;
            int accepted = 4 + (i % 3);
            int rejected = 3;
            int rateLimited = 1;
            int failedOrLost = (i == 10) ? 2 : 0; // simulate crash-lost HTTP hold responses
            int committed = (i == 10) ? accepted + 1 : accepted; // crash reconciliation recovers 1
            int orders = committed;
            int payOk = (i == 7) ? 0 : orders;
            int payFail = (i == 7) ? 2 : 0;
            int refunds = (i == 6) ? 1 : 0;
            long notifOk = payOk;
            long notifCancel = (i == 6 || i == 7) ? 1L : 0L;
            long dlt = (i == 9) ? 2L : 0L;
            long expectedDlt = (i == 9) ? 1L : 0L;

            recorder.record(makeRecord(
                    "S" + i, "PASS",
                    attempted, accepted, rejected, rateLimited, failedOrLost,
                    committed, orders, payOk, payFail, refunds,
                    notifOk, notifCancel, dlt, expectedDlt,
                    allTrueInvariants()
            ));

            if (i <= 12) {
                totalAttempted += attempted;
                totalAccepted += accepted;
                totalRejected += rejected;
                totalRateLimited += rateLimited;
                totalFailedOrLost += failedOrLost;
                totalReservations += committed;
                totalOrders += orders;
                totalPaymentsSucceeded += payOk;
                totalPaymentsFailed += payFail;
                totalRefunds += refunds;
                totalNotifConfirmed += notifOk;
                totalNotifCancelled += notifCancel;
                totalDlt += dlt;
                totalExpectedDlt += expectedDlt;
            }
        }

        withPublishFlag(true, () -> recorder.writeReports(tempDir));

        Path targetReport = tempDir.resolve("e2e-tests/target/e2e-report.md");
        Path docsReport = tempDir.resolve("docs/e2e-report.md");

        assertThat(targetReport).exists();
        assertThat(docsReport).exists();

        String markdown = Files.readString(targetReport, StandardCharsets.UTF_8);
        String docsMarkdown = Files.readString(docsReport, StandardCharsets.UTF_8);
        assertThat(docsMarkdown).isEqualTo(markdown);

        assertThat(markdown)
                .contains("**VERIFIED** (13/13 scenarios passed, 0 violations)")
                .doesNotContain("**FAIL**")
                .doesNotContain("**PARTIAL**")
                .doesNotContain("**NOT RUN**");

        for (String inv : List.of("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8")) {
            assertThat(markdown).contains("| **" + inv + "** |");
        }

        // Verify reconciled S1-S12 summary totals appear in markdown
        assertThat(markdown)
                .contains("| Scenarios in Catalog | 13 (Passed: 13, Failed: 0, Disabled: 0, Not Run: 0) |")
                .contains("| HTTP Seat Holds (`Attempted / Accepted 201 / Conflict 409 / Rate-Limited 429 / Error or Lost`) | "
                        + totalAttempted + " / " + totalAccepted + " / " + totalRejected + " / " + totalRateLimited + " / " + totalFailedOrLost + " |")
                .contains("| Database Reservations Committed (`inventory_db`) | " + totalReservations
                        + " (CONFIRMED=" + totalReservations + ") (reconciled: " + totalAccepted
                        + " HTTP `201`s received + " + (totalReservations - totalAccepted)
                        + " committed in DB whose HTTP response was lost to crash = " + totalReservations + " DB reservations) |")
                .contains("| Database Orders Created (`order_db`) | " + totalOrders + " (CONFIRMED=" + totalOrders + ") |")
                .contains("| Payments (`Succeeded / Failed / Refunded`) | "
                        + totalPaymentsSucceeded + " / " + totalPaymentsFailed + " / " + totalRefunds + " |")
                .contains("| Notifications Delivered (`order.confirmed / order.cancelled`) | "
                        + totalNotifConfirmed + " / " + totalNotifCancelled + " |")
                .contains("| Dead-Letter Messages (`Actual / Expected / Unexpected`) | "
                        + totalDlt + " / " + totalExpectedDlt + " / " + (totalDlt - totalExpectedDlt) + " |");
    }

    @Test
    void reportHeaderIncludesBackendAndFailoverBudget(@TempDir Path tempDir) throws Exception {
        // Compose default
        ScenarioReportRecorder composeRecorder = new ScenarioReportRecorder();
        composeRecorder.writeReports(tempDir);
        Path reportFile = tempDir.resolve("e2e-tests/target/e2e-report.md");
        String composeMd = Files.readString(reportFile, StandardCharsets.UTF_8);
        assertThat(composeMd)
                .contains("- **Backend**: `compose`")
                .contains("- **Failover Budget**: `60s`");

        // Custom process runner for K8s
        ProcessRunner fakeK8sRunner = (timeout, command) -> {
            String cmdStr = String.join(" ", command);
            if (cmdStr.contains("version -o json")) {
                return new CommandResult(0, "{\"serverVersion\":{\"gitVersion\":\"v1.35.5+k3s1\"}}", "");
            }
            if (cmdStr.contains("get nodes --no-headers")) {
                return new CommandResult(0, "node-1 Ready\nnode-2 Ready\nnode-3 Ready\n", "");
            }
            return new CommandResult(0, "", "");
        };

        String prevBackend = System.getProperty("E2E_BACKEND");
        String prevBudget = System.getProperty("E2E_FAILOVER_BUDGET_SECONDS");
        System.setProperty("E2E_BACKEND", "k8s");
        System.setProperty("E2E_FAILOVER_BUDGET_SECONDS", "180");
        try {
            E2EConfig k8sConfig = E2EConfig.load();
            ScenarioReportRecorder k8sRecorder = new ScenarioReportRecorder(k8sConfig, fakeK8sRunner);
            k8sRecorder.writeReports(tempDir);
            String k8sMd = Files.readString(reportFile, StandardCharsets.UTF_8);
            assertThat(k8sMd)
                    .contains("- **Backend**: `k8s (v1.35.5+k3s1, 3 nodes)`")
                    .contains("- **Failover Budget**: `180s`");
        } finally {
            if (prevBackend == null) System.clearProperty("E2E_BACKEND");
            else System.setProperty("E2E_BACKEND", prevBackend);
            if (prevBudget == null) System.clearProperty("E2E_FAILOVER_BUDGET_SECONDS");
            else System.setProperty("E2E_FAILOVER_BUDGET_SECONDS", prevBudget);
        }
    }
}

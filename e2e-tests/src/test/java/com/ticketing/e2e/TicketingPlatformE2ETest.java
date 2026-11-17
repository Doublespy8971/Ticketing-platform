package com.ticketing.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ticketing.e2e.support.Api;
import com.ticketing.e2e.support.Db;
import com.ticketing.e2e.support.E2EConfig;
import com.ticketing.e2e.support.Fixtures;
import com.ticketing.e2e.support.InvariantAuditor;
import com.ticketing.e2e.support.KafkaProbe;
import com.ticketing.e2e.support.Notifications;
import com.ticketing.e2e.support.PreflightExtension;
import com.ticketing.e2e.support.Quiescence;
import com.ticketing.e2e.support.ScenarioReportRecorder;
import com.ticketing.e2e.support.Stack;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-service end-to-end verification suite exercising the real containerized
 * Ticketing Platform through {@code gateway-service} ({@code http://localhost:8080})
 * and proving the global distributed invariants I1–I8.
 */
@ExtendWith(PreflightExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TicketingPlatformE2ETest {

    private static final Set<String> TERMINAL_ORDER_STATUSES = Set.of(
            "CONFIRMED",
            "CANCELLED_EXPIRED",
            "CANCELLED_PAYMENT_FAILED",
            "CANCELLED_REFUNDED"
    );

    private static E2EConfig config;
    private static Stack stack;
    private static Api api;
    private static Db db;
    private static KafkaProbe kafkaProbe;
    private static Notifications notifications;
    private static Quiescence quiescence;
    private static InvariantAuditor auditor;
    private static ObjectMapper objectMapper;
    private static ScenarioReportRecorder reportRecorder;
    private static Thread shutdownHook;

    private static KafkaProbe.DltSnapshot suiteStartDltSnapshot;
    private static Notifications.NotificationSnapshot suiteStartNotifications;
    private static Map<TopicPartition, Long> suiteStartOrderEventOffsets;
    private static final Set<UUID> baselineOrderOutboxEventIds = ConcurrentHashMap.newKeySet();
    private static final Set<UUID> suiteEventIds = ConcurrentHashMap.newKeySet();
    private static final Map<UUID, Long> declaredRefundPendingOrders = new ConcurrentHashMap<>();
    private static long totalExpectedDeadLettersAcrossSuite = 0L;
    private static long totalExpectedDuplicateNotificationsAcrossSuite = 0L;

    @BeforeAll
    static void beforeAll() {
        printTestMethods("START");
        config = E2EConfig.load();
        kafkaProbe = new KafkaProbe(config);
        stack = new Stack(config, kafkaProbe);
        api = new Api(config);
        db = new Db(config);
        notifications = new Notifications(config);
        quiescence = new Quiescence(db, kafkaProbe);
        auditor = new InvariantAuditor(db, kafkaProbe, notifications);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        reportRecorder = new ScenarioReportRecorder();

        shutdownHook = new Thread(() -> {
            if (stack != null) {
                stack.restoreTouchedContainersOnShutdown();
            }
        }, "e2e-stack-restore-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // Load any previously registered S11 REFUND_PENDING orders that still exist in order_db
        loadPersistedRefundPendingRegistry();

        // Fail fast if the stack contains active HELD reservations or non-terminal orders from earlier runs
        verifyCleanStackBeforeSuite();

        // Ensure complete quiescence before capturing suite baselines using database clock
        Instant dbNow = db.currentTimestampInInventoryDb();
        quiescence.markScenarioStart(dbNow);
        quiescence.awaitForEvents(List.of(), Duration.ofSeconds(60), true);

        // Record baseline outbox eventIds present before this suite run
        baselineOrderOutboxEventIds.clear();
        for (Db.OutboxRow row : db.findOrderOutboxByAggregateIds(null)) {
            if ("order.confirmed".equals(row.eventType()) || "order.cancelled".equals(row.eventType())) {
                baselineOrderOutboxEventIds.add(row.eventId());
            }
        }

        suiteStartDltSnapshot = kafkaProbe.snapshotDltOffsets();
        suiteStartNotifications = notifications.snapshot();
        suiteStartOrderEventOffsets = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        totalExpectedDeadLettersAcrossSuite = 0L;
        totalExpectedDuplicateNotificationsAcrossSuite = 0L;
    }

    @AfterAll
    static void afterAll() {
        Throwable restoreError = null;
        try {
            if (stack != null) {
                stack.restoreAndWaitAllHealthy(Duration.ofSeconds(60));
            }
        } catch (Throwable t) {
            restoreError = t;
        } finally {
            try {
                if (reportRecorder != null && config != null) {
                    recordDisabledScenariosIfAny();
                    reportRecorder.writeReports(config.workspaceRoot());
                }
            } catch (Exception e) {
                System.err.println("Error writing E2E reports in @AfterAll: " + e.getMessage());
                if (restoreError == null) {
                    restoreError = e;
                }
            }
            if (shutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                }
            }
            if (kafkaProbe != null) {
                kafkaProbe.close();
            }
            printTestMethods("END");
        }
        if (restoreError != null) {
            if (restoreError instanceof RuntimeException re) {
                throw re;
            }
            if (restoreError instanceof Error err) {
                throw err;
            }
            throw new IllegalStateException("Teardown failed during @AfterAll", restoreError);
        }
    }

    private static void verifyCleanStackBeforeSuite() {
        List<Db.ReservationRow> activeHeldReservations = db.findReservations(null).stream()
                .filter(r -> "HELD".equals(r.status()))
                .toList();

        List<Db.OrderRow> nonTerminalOrders = db.findOrders(null).stream()
                .filter(o -> !TERMINAL_ORDER_STATUSES.contains(o.status()))
                .filter(o -> !("REFUND_PENDING".equals(o.status()) && declaredRefundPendingOrders.containsKey(o.orderId())))
                .toList();

        if (!activeHeldReservations.isEmpty() || !nonTerminalOrders.isEmpty()) {
            throw new AssertionError(String.format(
                    "Dirty stack detected before E2E suite start! Found %d active HELD reservation(s) and %d non-terminal order(s) from earlier runs.%n"
                            + "  Active HELD reservations: %s%n"
                            + "  Non-terminal orders: %s%n"
                            + "Please run 'make stack-reset' to reset the stack before running the E2E suite.",
                    activeHeldReservations.size(),
                    nonTerminalOrders.size(),
                    activeHeldReservations,
                    nonTerminalOrders
            ));
        }
    }

    private static Path refundPendingRegistryFile() {
        return config.workspaceRoot().resolve("e2e-tests/target/s11-allowed-refund-pending.txt");
    }

    private static void loadPersistedRefundPendingRegistry() {
        declaredRefundPendingOrders.clear();
        Path file = refundPendingRegistryFile();
        if (!Files.isRegularFile(file)) {
            return;
        }
        try {
            Map<UUID, Db.OrderRow> existingOrders = db.findOrders(null).stream()
                    .collect(Collectors.toMap(Db.OrderRow::orderId, o -> o, (a, b) -> a));
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;
                String[] parts = trimmed.split(":");
                UUID orderId = UUID.fromString(parts[0]);
                long dltCount = parts.length > 1 ? Long.parseLong(parts[1]) : 1L;
                Db.OrderRow existing = existingOrders.get(orderId);
                if (existing != null && "REFUND_PENDING".equals(existing.status())) {
                    declaredRefundPendingOrders.put(orderId, dltCount);
                    quiescence.registerAllowedRefundPendingOrder(orderId);
                }
            }
        } catch (Exception ignored) {
        }
    }

    private static void registerExpectedRefundPendingOrder(UUID orderId, long declaredDeadLetters) {
        declaredRefundPendingOrders.put(orderId, declaredDeadLetters);
        quiescence.registerAllowedRefundPendingOrder(orderId);
        try {
            Path file = refundPendingRegistryFile();
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            String line = orderId + ":" + declaredDeadLetters + System.lineSeparator();
            Files.writeString(file, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
        }
    }

    private static void printTestMethods(String phase) {
        List<String> names = Arrays.stream(TicketingPlatformE2ETest.class.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class))
                .sorted(Comparator.comparingInt(m -> m.isAnnotationPresent(Order.class)
                        ? m.getAnnotation(Order.class).value() : Integer.MAX_VALUE))
                .map(Method::getName)
                .toList();
        System.out.println("=== [TicketingPlatformE2ETest " + phase + "] Test methods (" + names.size() + "): " + names + " ===");
    }

    private static void recordDisabledScenariosIfAny() {
        for (Method m : TicketingPlatformE2ETest.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(Test.class) && m.isAnnotationPresent(org.junit.jupiter.api.Disabled.class)) {
                String reason = m.getAnnotation(org.junit.jupiter.api.Disabled.class).value();
                String displayName = m.isAnnotationPresent(DisplayName.class)
                        ? m.getAnnotation(DisplayName.class).value()
                        : m.getName();
                String tag = m.isAnnotationPresent(Tag.class)
                        ? m.getAnnotation(Tag.class).value()
                        : "chaos";
                String id = m.getName().toUpperCase().split("_")[0];
                if (!reportRecorder.hasRecord(id)) {
                    reportRecorder.record(new ScenarioReportRecorder.ScenarioRecord(
                            id, displayName, tag, "DISABLED", 0L,
                            0, 0, 0, 0, 0, 0,
                            0, Map.of(), 0, Map.of(),
                            0, 0, 0, 0L, 0L, 0L, 0L, Map.of(),
                            "Disabled due to genuine product bug: " + reason
                    ));
                }
            }
        }
    }

    private static Random logSeed(String scenarioId, long seed) {
        System.out.printf("[E2E-SEED] Scenario %s initialized with fixed random seed=%d (0x%08X)%n",
                scenarioId, seed, seed);
        return new Random(seed);
    }

    private UUID awaitOrderMaterialized(Fixtures.UserFixture user, UUID reservationId, Duration timeout) {
        Awaitility.await("Order " + reservationId + " materialized in AWAITING_PAYMENT")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(150))
                .until(() -> {
                    Api.ApiResponse resp = api.getOrder(user.bearerToken(), reservationId);
                    return resp.status() == 200
                            && resp.json() != null
                            && ("AWAITING_PAYMENT".equals(resp.json().path("status").asText())
                                || "RESERVED".equals(resp.json().path("status").asText()));
                });
        return reservationId;
    }

    private void awaitOrderStatusViaGateway(Fixtures.UserFixture user, UUID orderId, String expectedStatus, Duration timeout) {
        Awaitility.await("Order " + orderId + " reaches status " + expectedStatus + " via gateway")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    Api.ApiResponse resp = api.getOrder(user.bearerToken(), orderId);
                    return resp.status() == 200
                            && resp.json() != null
                            && expectedStatus.equals(resp.json().path("status").asText());
                });
    }

    private InvariantAuditor.AuditResult finishAndAuditScenario(
            String scenarioId,
            String scenarioName,
            String tag,
            Instant scenarioStart,
            UUID eventId,
            int usersCount,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            Map<TopicPartition, Long> orderEventsStartOffsets,
            KafkaProbe.DltSnapshot dltBefore,
            long expectedDeadLetters,
            Notifications.NotificationSnapshot notifBefore,
            String notes) {
        return finishAndAuditScenario(
                scenarioId, scenarioName, tag, scenarioStart, eventId,
                usersCount, holdsAttempted, holdsAccepted, holdsRejected, holdsRateLimited, 0,
                orderEventsStartOffsets, dltBefore, expectedDeadLetters, notifBefore,
                0L, false, notes
        );
    }

    private InvariantAuditor.AuditResult finishAndAuditScenario(
            String scenarioId,
            String scenarioName,
            String tag,
            Instant scenarioStart,
            UUID eventId,
            int usersCount,
            int holdsAttempted,
            int holdsAccepted,
            int holdsRejected,
            int holdsRateLimited,
            int holdsFailedOrLost,
            Map<TopicPartition, Long> orderEventsStartOffsets,
            KafkaProbe.DltSnapshot dltBefore,
            long expectedDeadLetters,
            Notifications.NotificationSnapshot notifBefore,
            long expectedDuplicateNotifications,
            boolean allowRefundPendingResting,
            String notes) {

        quiescence.markScenarioStart(scenarioStart);
        quiescence.await(eventId, Duration.ofSeconds(60), allowRefundPendingResting);
        Notifications.NotificationSnapshot notifAfter = notifications.snapshot();
        Notifications.NotificationSnapshot notifDiff = Notifications.diff(notifBefore, notifAfter);
        KafkaProbe.DltDiff dltDiff = kafkaProbe.diffDltOffsets(dltBefore);

        InvariantAuditor.ScenarioContext ctx = new InvariantAuditor.ScenarioContext(
                eventId,
                scenarioStart,
                kafkaProbe,
                orderEventsStartOffsets,
                dltBefore,
                expectedDeadLetters,
                notifBefore,
                notifAfter,
                expectedDuplicateNotifications,
                declaredRefundPendingOrders.keySet()
        );
        InvariantAuditor.AuditResult auditResult = auditor.auditScenario(ctx);
        long durationMs = Duration.between(scenarioStart, db.currentTimestampInInventoryDb()).toMillis();

        reportRecorder.record(ScenarioReportRecorder.fromEventAudit(
                scenarioId,
                scenarioName,
                tag,
                auditResult.allPassed() ? "PASS" : "FAIL",
                Math.max(0L, durationMs),
                usersCount,
                holdsAttempted,
                holdsAccepted,
                holdsRejected,
                holdsRateLimited,
                holdsFailedOrLost,
                eventId,
                db,
                notifDiff,
                dltDiff.totalDelta(),
                expectedDeadLetters,
                auditResult,
                notes
        ));

        auditResult.assertAllPassed();
        totalExpectedDeadLettersAcrossSuite += expectedDeadLetters;
        totalExpectedDuplicateNotificationsAcrossSuite += expectedDuplicateNotifications;
        return auditResult;
    }

    // =========================================================================
    // S1 — Happy path through the gateway
    // =========================================================================
    @Test
    @Order(1)
    @Tag("fast")
    @DisplayName("S1: Happy path through the gateway — hold 2 seats, checkout tok_success, verify SOLD/AVAILABLE filters and notification")
    void s1_happyPathThroughGateway() {
        logSeed("S1", 0x510001L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 6, 15_000L, "USD");
        suiteEventIds.add(event.eventId());

        Fixtures.UserFixture buyer = Fixtures.newUser(api);
        List<UUID> targetSeats = List.of(event.seatIds().get(0), event.seatIds().get(1));

        Api.ApiResponse holdResp = api.holdSeats(buyer.bearerToken(), event.eventId(), targetSeats, UUID.randomUUID());
        assertThat(holdResp.status()).isEqualTo(201);
        UUID reservationId = UUID.fromString(holdResp.json().get("reservationId").asText());

        UUID orderId = awaitOrderMaterialized(buyer, reservationId, Duration.ofSeconds(15));

        Api.ApiResponse checkoutResp = api.checkout(buyer.bearerToken(), orderId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(checkoutResp.status()).isEqualTo(202);

        awaitOrderStatusViaGateway(buyer, orderId, "CONFIRMED", Duration.ofSeconds(20));

        // Verify GET /api/v1/events/{eventId}/seats (unfiltered + filtered by SOLD and AVAILABLE)
        Api.ApiResponse allSeatsResp = api.getSeats(event.eventId(), null, null, 200, null);
        assertThat(allSeatsResp.status()).isEqualTo(200);
        assertThat(allSeatsResp.headerValue("Cache-Control")).contains("max-age=1");
        JsonNode allItems = allSeatsResp.json().get("items");
        assertThat(allItems).hasSize(6);

        Set<UUID> soldIdsFromAll = new HashSet<>();
        Set<UUID> availIdsFromAll = new HashSet<>();
        for (JsonNode item : allItems) {
            UUID sid = UUID.fromString(item.get("seatId").asText());
            String st = item.get("status").asText();
            if ("SOLD".equals(st)) {
                soldIdsFromAll.add(sid);
            } else if ("AVAILABLE".equals(st)) {
                availIdsFromAll.add(sid);
            }
        }
        assertThat(soldIdsFromAll).containsExactlyInAnyOrderElementsOf(targetSeats);
        assertThat(availIdsFromAll).hasSize(4);

        Api.ApiResponse soldFilterResp = api.getSeats(event.eventId(), null, "SOLD", 200, null);
        assertThat(soldFilterResp.status()).isEqualTo(200);
        List<UUID> soldFilterIds = new ArrayList<>();
        soldFilterResp.json().get("items").forEach(n -> soldFilterIds.add(UUID.fromString(n.get("seatId").asText())));
        assertThat(soldFilterIds).containsExactlyInAnyOrderElementsOf(targetSeats);

        Api.ApiResponse availFilterResp = api.getSeats(event.eventId(), null, "AVAILABLE", 200, null);
        assertThat(availFilterResp.status()).isEqualTo(200);
        List<UUID> availFilterIds = new ArrayList<>();
        availFilterResp.json().get("items").forEach(n -> availFilterIds.add(UUID.fromString(n.get("seatId").asText())));
        assertThat(availFilterIds).containsExactlyInAnyOrderElementsOf(availIdsFromAll);

        // Wait for notification-service counter to increment
        Awaitility.await("notification_events_total{event_type=order.confirmed,outcome=sent} increments by 1")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> notifications.diff(notifBefore).sent("order.confirmed") == 1L);

        finishAndAuditScenario(
                "S1", "Happy path through the gateway", "fast",
                start, event.eventId(),
                1, 1, 1, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "2 seats held and confirmed; public seat availability filters verified."
        );
    }

    // =========================================================================
    // S2 — Declined payment
    // =========================================================================
    @Test
    @Order(2)
    @Tag("fast")
    @DisplayName("S2: Declined payment — tok_decline cancels order, releases seat back to AVAILABLE, and second buyer purchases it")
    void s2_declinedPaymentReleasesSeatForSecondBuyer() {
        logSeed("S2", 0x520002L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 4, 12_000L, "USD");
        suiteEventIds.add(event.eventId());
        UUID contestedSeatId = event.seatIds().get(0);

        Fixtures.UserFixture user1 = Fixtures.newUser(api);
        Fixtures.UserFixture user2 = Fixtures.newUser(api);

        // User 1 holds seat and checks out with tok_decline
        Api.ApiResponse hold1 = api.holdSeats(user1.bearerToken(), event.eventId(), List.of(contestedSeatId), UUID.randomUUID());
        assertThat(hold1.status()).isEqualTo(201);
        UUID order1Id = UUID.fromString(hold1.json().get("reservationId").asText());
        awaitOrderMaterialized(user1, order1Id, Duration.ofSeconds(15));

        Api.ApiResponse checkout1 = api.checkout(user1.bearerToken(), order1Id, UUID.randomUUID(), Fixtures.TOK_DECLINE);
        assertThat(checkout1.status()).isEqualTo(202);

        awaitOrderStatusViaGateway(user1, order1Id, "CANCELLED_PAYMENT_FAILED", Duration.ofSeconds(20));

        // Verify seat is AVAILABLE again via public endpoint
        Awaitility.await("Seat returns to AVAILABLE after declined payment")
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    Api.ApiResponse seatsResp = api.getSeats(event.eventId(), null, "AVAILABLE", 200, null);
                    if (seatsResp.status() != 200 || seatsResp.json() == null) return false;
                    for (JsonNode n : seatsResp.json().get("items")) {
                        if (contestedSeatId.toString().equals(n.get("seatId").asText())) {
                            return true;
                        }
                    }
                    return false;
                });

        // User 2 holds and buys that same seat with tok_success
        Api.ApiResponse hold2 = api.holdSeats(user2.bearerToken(), event.eventId(), List.of(contestedSeatId), UUID.randomUUID());
        assertThat(hold2.status()).isEqualTo(201);
        UUID order2Id = UUID.fromString(hold2.json().get("reservationId").asText());
        awaitOrderMaterialized(user2, order2Id, Duration.ofSeconds(15));

        Api.ApiResponse checkout2 = api.checkout(user2.bearerToken(), order2Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(checkout2.status()).isEqualTo(202);

        awaitOrderStatusViaGateway(user2, order2Id, "CONFIRMED", Duration.ofSeconds(20));

        // Verify notifications: 1 cancelled, 1 confirmed
        Awaitility.await("Both cancelled and confirmed notifications sent")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    Notifications.NotificationSnapshot diff = notifications.diff(notifBefore);
                    return diff.sent("order.cancelled") == 1L && diff.sent("order.confirmed") == 1L;
                });

        List<Db.PaymentRow> p1 = db.findPaymentsByOrderIds(Set.of(order1Id));
        List<Db.PaymentRow> p2 = db.findPaymentsByOrderIds(Set.of(order2Id));
        assertThat(p1).hasSize(1);
        assertThat(p1.get(0).status()).isEqualTo("FAILED");
        assertThat(p2).hasSize(1);
        assertThat(p2.get(0).status()).isEqualTo("SUCCEEDED");

        finishAndAuditScenario(
                "S2", "Declined payment and seat re-purchase", "fast",
                start, event.eventId(),
                2, 2, 2, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "User 1 declined (FAILED), seat released to AVAILABLE and bought by User 2 (CONFIRMED)."
        );
    }

    // =========================================================================
    // S3 — Hold expiry
    // =========================================================================
    @Test
    @Order(3)
    @Tag("fast")
    @DisplayName("S3: Hold expiry — unpaid hold expires via forceReservationExpiry, cancels order, emits notification, and frees seat")
    void s3_holdExpiryReleasesSeatAndCancelsOrder() {
        logSeed("S3", 0x530003L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 3, 10_000L, "USD");
        suiteEventIds.add(event.eventId());
        UUID seatId = event.seatIds().get(0);

        Fixtures.UserFixture user1 = Fixtures.newUser(api);
        Fixtures.UserFixture user2 = Fixtures.newUser(api);

        Api.ApiResponse hold1 = api.holdSeats(user1.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
        assertThat(hold1.status()).isEqualTo(201);
        UUID res1Id = UUID.fromString(hold1.json().get("reservationId").asText());
        awaitOrderMaterialized(user1, res1Id, Duration.ofSeconds(15));

        // Force expiry in inventory_db
        db.forceReservationExpiry(res1Id);

        // Wait for expiry sweeper to transition reservation -> EXPIRED and order -> CANCELLED_EXPIRED
        awaitOrderStatusViaGateway(user1, res1Id, "CANCELLED_EXPIRED", Duration.ofSeconds(25));

        assertThat(db.findPaymentsByOrderIds(Set.of(res1Id))).isEmpty();

        Awaitility.await("Cancellation notification sent for expired hold")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> notifications.diff(notifBefore).sent("order.cancelled") == 1L);

        // Verify seat is AVAILABLE and User 2 can now hold and complete purchase on that seat
        Api.ApiResponse hold2 = api.holdSeats(user2.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
        assertThat(hold2.status()).isEqualTo(201);
        UUID res2Id = UUID.fromString(hold2.json().get("reservationId").asText());
        awaitOrderMaterialized(user2, res2Id, Duration.ofSeconds(15));
        Api.ApiResponse checkout2 = api.checkout(user2.bearerToken(), res2Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(checkout2.status()).isEqualTo(202);
        awaitOrderStatusViaGateway(user2, res2Id, "CONFIRMED", Duration.ofSeconds(20));

        finishAndAuditScenario(
                "S3", "Hold expiry and seat reclamation", "fast",
                start, event.eventId(),
                2, 2, 2, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "Expired reservation produced 0 payments, 1 order.cancelled notification, and freed seat for User 2."
        );
    }

    // =========================================================================
    // S4 — Late payment (the core saga compensation)
    // =========================================================================
    @Test
    @Order(4)
    @Tag("chaos")
    @DisplayName("S4: Late payment saga compensation — payment-service stopped while hold expires and seat sells to Buyer 2, then late charge is refunded")
    void s4_latePaymentCompensationAndRefund() {
        logSeed("S4", 0x540004L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 1, 20_000L, "USD");
        suiteEventIds.add(event.eventId());
        UUID seatId = event.seatIds().get(0);

        Fixtures.UserFixture buyer1 = Fixtures.newUser(api);
        Fixtures.UserFixture buyer2 = Fixtures.newUser(api);

        try {
            // 1. Buyer 1 holds the single seat and order reaches AWAITING_PAYMENT
            Api.ApiResponse hold1 = api.holdSeats(buyer1.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
            assertThat(hold1.status()).isEqualTo(201);
            UUID order1Id = UUID.fromString(hold1.json().get("reservationId").asText());
            awaitOrderMaterialized(buyer1, order1Id, Duration.ofSeconds(15));

            // 2. Stop payment-service before checkout so order.payment.requested is guaranteed to sit in Kafka unconsumed
            stack.stop("payment-service");
            Api.ApiResponse checkout1 = api.checkout(buyer1.bearerToken(), order1Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
            assertThat(checkout1.status()).isEqualTo(202);

            Awaitility.await("order.payment.requested published to Kafka while payment-service is stopped")
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> db.countUnpublishedOutbox("order_db", event.eventId()) == 0
                            && kafkaProbe.totalConsumerGroupLag("payment-service-order-cg") >= 1L);

            // Prove payment request is unconsumed before forcing expiry: lag > 0 and no payment row exists
            assertThat(kafkaProbe.totalConsumerGroupLag("payment-service-order-cg")).isGreaterThanOrEqualTo(1L);
            assertThat(db.findPaymentsByOrderIds(Set.of(order1Id)))
                    .as("No payment row may exist for order1 while payment-service is stopped before forcing expiry")
                    .isEmpty();

            // 3. Force-expire Buyer 1's reservation while payment-service is stopped
            db.forceReservationExpiry(order1Id);

            Awaitility.await("Buyer 1 reservation is EXPIRED, order is CANCELLED_EXPIRED, and seat is AVAILABLE")
                    .atMost(Duration.ofSeconds(25))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> {
                        List<Db.ReservationRow> resList = db.findReservations(event.eventId());
                        List<Db.OrderRow> ordList = db.findOrders(event.eventId());
                        List<Db.SeatRow> seatList = db.findSeats(event.eventId());
                        boolean resExpired = resList.stream()
                                .anyMatch(r -> r.reservationId().equals(order1Id) && "EXPIRED".equals(r.status()));
                        boolean ordCancelled = ordList.stream()
                                .anyMatch(o -> o.orderId().equals(order1Id) && "CANCELLED_EXPIRED".equals(o.status()));
                        boolean seatAvail = seatList.stream()
                                .anyMatch(s -> s.seatId().equals(seatId) && "AVAILABLE".equals(s.status()));
                        return resExpired && ordCancelled && seatAvail;
                    });

            // 4. Buyer 2 holds that same seat while payment-service is still stopped
            Api.ApiResponse hold2 = api.holdSeats(buyer2.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
            assertThat(hold2.status()).isEqualTo(201);
            UUID order2Id = UUID.fromString(hold2.json().get("reservationId").asText());
            awaitOrderMaterialized(buyer2, order2Id, Duration.ofSeconds(15));

            // 5. Buyer 2 submits checkout (queued in Kafka behind Buyer 1's payment request)
            Api.ApiResponse checkout2 = api.checkout(buyer2.bearerToken(), order2Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
            assertThat(checkout2.status()).isEqualTo(202);

            // 6. Start payment-service and wait for both sagas to reach quiescence
            stack.start("payment-service");
            stack.waitUntilHealthy("payment-service", Duration.ofSeconds(60));

            awaitOrderStatusViaGateway(buyer2, order2Id, "CONFIRMED", Duration.ofSeconds(30));
            awaitOrderStatusViaGateway(buyer1, order1Id, "CANCELLED_REFUNDED", Duration.ofSeconds(30));

            // 7. Verify Buyer 1's payment was created and transitioned to REFUNDED with net charge 0
            List<Db.PaymentRow> buyer1Payments = db.findPaymentsByOrderIds(Set.of(order1Id));
            assertThat(buyer1Payments).hasSize(1);
            Db.PaymentRow p1 = buyer1Payments.get(0);
            assertThat(p1.status()).isEqualTo("REFUNDED");
            assertThat(p1.amountCents()).isEqualTo(20_000L);

            List<Db.OutboxRow> order1Outbox = db.findOrderOutboxByAggregateIds(Set.of(order1Id));
            assertThat(order1Outbox.stream().filter(r -> "order.refund.requested".equals(r.eventType())).count())
                    .isEqualTo(1L);

            List<Db.OutboxRow> pay1Outbox = db.findPaymentOutboxByAggregateIds(Set.of(order1Id));
            List<Db.OutboxRow> refundedEvents = pay1Outbox.stream()
                    .filter(r -> "payment.refunded".equals(r.eventType()))
                    .toList();
            assertThat(refundedEvents).hasSize(1);
            JsonNode refundPayload = objectMapper.readTree(refundedEvents.get(0).payloadJson());
            long refundedCents = refundPayload.has("payload")
                    ? refundPayload.get("payload").get("amountCents").asLong()
                    : refundPayload.get("amountCents").asLong();
            assertThat(refundedCents).isEqualTo(p1.amountCents()); // Net charge == 0

            // 8. Verify Buyer 2 owns the seat in SOLD / CONFIRMED
            List<Db.SeatRow> finalSeats = db.findSeats(event.eventId());
            assertThat(finalSeats).hasSize(1);
            assertThat(finalSeats.get(0).status()).isEqualTo("SOLD");
            List<Db.ReservationItemRow> resItems = db.findReservationItems(event.eventId());
            assertThat(resItems.stream()
                    .filter(i -> "CONFIRMED".equals(i.status()))
                    .map(Db.ReservationItemRow::reservationId)
                    .toList())
                    .containsExactly(order2Id);
        } catch (Exception e) {
            throw new RuntimeException("S4 failed", e);
        } finally {
            stack.restoreAndWaitAllHealthy(Duration.ofSeconds(60));
        }

        finishAndAuditScenario(
                "S4", "Late payment saga compensation", "chaos",
                start, event.eventId(),
                2, 2, 2, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "Buyer 1 late charge automatically compensated (CANCELLED_EXPIRED -> REFUND_PENDING -> CANCELLED_REFUNDED); Buyer 2 CONFIRMED."
        );
    }

    // =========================================================================
    // S5 — No oversell under load
    // =========================================================================
    @Test
    @Order(5)
    @Tag("fast")
    @DisplayName("S5: No oversell under load — 200 users across 64 threads contend for 20 seats, 5 declined holds re-bought in wave 2")
    void s5_noOversellUnderHighContentionLoad() throws Exception {
        Random rnd = logSeed("S5", 0x550005L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        int totalSeats = 20;
        int wave1UsersCount = 200;
        // createOnSaleEvent mints 1 admin token via POST /api/v1/auth/token; we mint 4 user tokens via gateway
        // (total 5 tokens via gateway endpoint) and sign the remaining tokens locally so the token endpoint's
        // rate limit is never consumed by test setup.
        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, totalSeats, 9_500L, "USD");
        suiteEventIds.add(event.eventId());
        List<UUID> allSeatIds = event.seatIds();

        List<Fixtures.UserFixture> wave1Users = new ArrayList<>(wave1UsersCount);
        List<List<UUID>> wave1SeatChoices = new ArrayList<>(wave1UsersCount);
        for (int i = 0; i < wave1UsersCount; i++) {
            if (i < 4) {
                wave1Users.add(Fixtures.newUser(api));
            } else {
                wave1Users.add(Fixtures.newUserLocal(api));
            }
            int count = (i < 20) ? 1 : (1 + rnd.nextInt(2));
            if (i < 20) {
                // Guarantee every one of the 20 seats is targeted by at least one single-seat request
                wave1SeatChoices.add(List.of(allSeatIds.get(i)));
            } else if (count == 1) {
                wave1SeatChoices.add(List.of(allSeatIds.get(rnd.nextInt(totalSeats))));
            } else {
                int idx1 = rnd.nextInt(totalSeats);
                int idx2 = (idx1 + 1 + rnd.nextInt(totalSeats - 1)) % totalSeats;
                wave1SeatChoices.add(List.of(allSeatIds.get(idx1), allSeatIds.get(idx2)));
            }
        }

        record HoldAttemptResult(Fixtures.UserFixture user, List<UUID> seats, Api.ApiResponse response) {}

        ExecutorService pool = Executors.newFixedThreadPool(64);
        int holdsAttemptedTotal = wave1UsersCount;
        int holds201Count = 0;
        int holds409Count = 0;
        int holds429Count = 0;
        int holdsErrorCount = 0;
        int totalSimulatedUsers = wave1UsersCount;

        try {
            CountDownLatch readyLatch = new CountDownLatch(64);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Callable<HoldAttemptResult>> tasks = new ArrayList<>(wave1UsersCount);
            for (int i = 0; i < wave1UsersCount; i++) {
                final Fixtures.UserFixture u = wave1Users.get(i);
                final List<UUID> requestedSeats = wave1SeatChoices.get(i);
                tasks.add(() -> {
                    readyLatch.countDown();
                    startLatch.await(10, TimeUnit.SECONDS);
                    Api.ApiResponse resp = api.holdSeats(u.bearerToken(), event.eventId(), requestedSeats, UUID.randomUUID());
                    return new HoldAttemptResult(u, requestedSeats, resp);
                });
            }
            List<Future<HoldAttemptResult>> futures = new ArrayList<>(wave1UsersCount);
            for (Callable<HoldAttemptResult> task : tasks) {
                futures.add(pool.submit(task));
            }
            readyLatch.await(10, TimeUnit.SECONDS);
            startLatch.countDown();

            List<HoldAttemptResult> wave1Results = new ArrayList<>(wave1UsersCount);
            for (Future<HoldAttemptResult> f : futures) {
                wave1Results.add(f.get(30, TimeUnit.SECONDS));
            }

            for (HoldAttemptResult r : wave1Results) {
                int st = r.response().status();
                if (st == 201) holds201Count++;
                else if (st == 409) holds409Count++;
                else if (st == 429) holds429Count++;
                else holdsErrorCount++;
            }

            int holds201 = 0;
            int holds409 = 0;
            int holds429 = 0;
            List<HoldAttemptResult> wave1Winners = new ArrayList<>();

            for (HoldAttemptResult r : wave1Results) {
                int status = r.response().status();
                assertThat(status)
                        .as("Every hold HTTP status must be in {201, 409, 429} and never 5xx, got %d (%s)",
                                status, r.response().body())
                        .isIn(201, 409, 429);
                if (status == 429) {
                    assertThat(r.response().headerValue("Retry-After"))
                            .as("429 response must include Retry-After header")
                            .isNotBlank();
                    holds429++;
                } else if (status == 409) {
                    holds409++;
                } else if (status == 201) {
                    holds201++;
                    wave1Winners.add(r);
                }
            }

            assertThat(wave1Winners.size())
                    .as("Accepted holds in wave 1 must be <= 20 and >= 6")
                    .isBetween(6, totalSeats);

            // Wait for all wave 1 winners' orders to materialize in AWAITING_PAYMENT
            List<UUID> wave1OrderIds = new ArrayList<>(wave1Winners.size());
            for (HoldAttemptResult winner : wave1Winners) {
                UUID resId = UUID.fromString(winner.response().json().get("reservationId").asText());
                awaitOrderMaterialized(winner.user(), resId, Duration.ofSeconds(20));
                wave1OrderIds.add(resId);
            }

            // Concurrently checkout all wave 1 winners: first 5 use tok_decline, the rest use tok_success
            int declineCount = 5;
            List<UUID> declinedOrderIds = new ArrayList<>(declineCount);
            List<UUID> releasedSeatsFromDeclines = new ArrayList<>();
            List<Future<Api.ApiResponse>> checkoutFutures = new ArrayList<>(wave1Winners.size());

            for (int i = 0; i < wave1Winners.size(); i++) {
                final HoldAttemptResult winner = wave1Winners.get(i);
                final UUID orderId = wave1OrderIds.get(i);
                final boolean shouldDecline = i < declineCount;
                if (shouldDecline) {
                    declinedOrderIds.add(orderId);
                    releasedSeatsFromDeclines.addAll(winner.seats());
                }
                final String token = shouldDecline ? Fixtures.TOK_DECLINE : Fixtures.TOK_SUCCESS;
                checkoutFutures.add(pool.submit(() ->
                        api.checkout(winner.user().bearerToken(), orderId, UUID.randomUUID(), token)));
            }

            for (Future<Api.ApiResponse> cf : checkoutFutures) {
                Api.ApiResponse cResp = cf.get(20, TimeUnit.SECONDS);
                assertThat(cResp.status())
                        .as("Checkout HTTP status must be 202 (never 5xx)")
                        .isEqualTo(202);
            }

            // Wait for the 5 declined orders to reach CANCELLED_PAYMENT_FAILED and release their seats
            for (int i = 0; i < declineCount; i++) {
                awaitOrderStatusViaGateway(wave1Winners.get(i).user(), declinedOrderIds.get(i),
                        "CANCELLED_PAYMENT_FAILED", Duration.ofSeconds(25));
            }

            // Wait until all released seats from those 5 declined holds are AVAILABLE in inventory_db
            Awaitility.await("Seats from 5 declined holds return to AVAILABLE")
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> {
                        Map<UUID, String> statusBySeat = db.findSeats(event.eventId()).stream()
                                .collect(Collectors.toMap(Db.SeatRow::seatId, Db.SeatRow::status));
                        return releasedSeatsFromDeclines.stream()
                                .allMatch(sid -> "AVAILABLE".equals(statusBySeat.get(sid)));
                    });

            // Second wave: new users hold and buy every released seat (1 seat per user)
            int wave2Accepted = 0;
            totalSimulatedUsers += releasedSeatsFromDeclines.size();
            holdsAttemptedTotal += releasedSeatsFromDeclines.size();
            for (UUID releasedSeatId : releasedSeatsFromDeclines) {
                Fixtures.UserFixture wave2User = Fixtures.newUserLocal(api);
                Api.ApiResponse w2Hold = api.holdSeats(
                        wave2User.bearerToken(), event.eventId(), List.of(releasedSeatId), UUID.randomUUID());
                assertThat(w2Hold.status()).isEqualTo(201);
                wave2Accepted++;
                holds201Count++;
                UUID w2OrderId = UUID.fromString(w2Hold.json().get("reservationId").asText());
                awaitOrderMaterialized(wave2User, w2OrderId, Duration.ofSeconds(15));
                Api.ApiResponse w2Checkout = api.checkout(
                        wave2User.bearerToken(), w2OrderId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
                assertThat(w2Checkout.status()).isEqualTo(202);
            }

            quiescence.await(event.eventId(), Duration.ofSeconds(60));

            int total201s = holds201 + wave2Accepted;
            List<Db.ReservationRow> allReservations = db.findReservations(event.eventId());
            List<Db.ReservationRow> confirmedReservations = allReservations.stream()
                    .filter(r -> "CONFIRMED".equals(r.status()))
                    .toList();
            List<Db.OrderRow> confirmedOrders = db.findOrders(event.eventId()).stream()
                    .filter(o -> "CONFIRMED".equals(o.status()))
                    .toList();
            List<Db.SeatRow> soldSeats = db.findSeats(event.eventId()).stream()
                    .filter(s -> "SOLD".equals(s.status()))
                    .toList();

            assertThat(allReservations).hasSize(total201s);
            assertThat(confirmedReservations).hasSize(total201s - declineCount);
            assertThat(confirmedOrders).hasSize(confirmedReservations.size());

            long confirmedOrderItemsCount = db.findOrderItemsByOrderIds(
                    confirmedOrders.stream().map(Db.OrderRow::orderId).collect(Collectors.toSet())).size();
            assertThat(soldSeats).hasSize((int) confirmedOrderItemsCount);

            finishAndAuditScenario(
                    "S5", "No oversell under load (200 users, 64 threads, 20 seats)", "fast",
                    start, event.eventId(),
                    totalSimulatedUsers,
                    holdsAttemptedTotal,
                    total201s,
                    holds409,
                    holds429,
                    orderOffsetsBefore, dltBefore, 0L, notifBefore,
                    String.format("Wave 1: %d accepted (201), %d rejected (409), %d rate-limited (429); 5 declined & re-bought in Wave 2 (%d seats).",
                            holds201, holds409, holds429, wave2Accepted)
            );
        } catch (Throwable t) {
            // If S5 fails (e.g. INV-001 where holds under 64-thread contention return 5xx),
            // expire any HELD reservations created in Wave 1 so subsequent scenarios and S13 reach
            // terminal quiescence cleanly, record S5 as FAIL in the report, and rethrow the failure.
            try {
                cleanupHeldReservationsOnScenarioFailure(event.eventId(), start);
                Notifications.NotificationSnapshot notifAfter = notifications.snapshot();
                Notifications.NotificationSnapshot notifDiff = Notifications.diff(notifBefore, notifAfter);
                KafkaProbe.DltDiff dltDiff = kafkaProbe.diffDltOffsets(dltBefore);
                InvariantAuditor.ScenarioContext ctx = new InvariantAuditor.ScenarioContext(
                        event.eventId(), start, kafkaProbe, orderOffsetsBefore,
                        dltBefore, 0L, notifBefore, notifAfter, 0L, declaredRefundPendingOrders.keySet()
                );
                InvariantAuditor.AuditResult auditResult = auditor.auditScenario(ctx);
                long durationMs = Duration.between(start, db.currentTimestampInInventoryDb()).toMillis();
                String failMsg = t.getMessage() != null ? t.getMessage().replaceAll("\\s+", " ").trim() : t.toString();
                reportRecorder.record(ScenarioReportRecorder.fromEventAudit(
                        "S5",
                        "No oversell under load (200 users, 64 threads, 20 seats)",
                        "fast",
                        "FAIL",
                        Math.max(0L, durationMs),
                        totalSimulatedUsers,
                        holdsAttemptedTotal,
                        holds201Count,
                        holds409Count,
                        holds429Count,
                        holdsErrorCount,
                        event.eventId(),
                        db,
                        notifDiff,
                        dltDiff.totalDelta(),
                        0L,
                        auditResult,
                        "Scenario failed assertion (INV-001): " + failMsg
                ));
            } catch (Exception cleanupEx) {
                System.err.println("Warning during S5 failure cleanup/recording: " + cleanupEx.getMessage());
            }
            throw t;
        } finally {
            pool.shutdownNow();
        }
    }

    private void cleanupHeldReservationsOnScenarioFailure(UUID eventId, Instant scenarioStart) {
        List<Db.ReservationRow> reservations = db.findReservations(eventId);
        List<UUID> heldIds = reservations.stream()
                .filter(r -> "HELD".equals(r.status()))
                .map(Db.ReservationRow::reservationId)
                .toList();
        if (!heldIds.isEmpty()) {
            // Wait for orders of those HELD reservations to materialize before expiring them
            Awaitility.await("Materialize orders for HELD reservations before failure cleanup")
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> {
                        Set<UUID> existingOrderIds = db.findOrders(eventId).stream()
                                .map(Db.OrderRow::orderId)
                                .collect(Collectors.toSet());
                        return existingOrderIds.containsAll(heldIds);
                    });
            for (UUID resId : heldIds) {
                db.forceReservationExpiry(resId);
            }
        }
        quiescence.markScenarioStart(scenarioStart);
        quiescence.await(eventId, Duration.ofSeconds(30));
    }

    // =========================================================================
    // S6 — Duplicate and replayed events
    // =========================================================================
    @Test
    @Order(6)
    @Tag("fast")
    @DisplayName("S6: Duplicate and replayed events — replaying payment.succeeded, inventory.seat.reserved, order.payment.requested, and order.confirmed causes zero side effects")
    void s6_duplicateAndReplayedEventsAreIdempotent() throws Exception {
        logSeed("S6", 0x560006L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        Map<TopicPartition, Long> payOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_PAYMENT_EVENTS);
        Map<TopicPartition, Long> invOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_INVENTORY_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 2, 14_000L, "USD");
        suiteEventIds.add(event.eventId());
        Fixtures.UserFixture buyer = Fixtures.newUser(api);

        Api.ApiResponse holdResp = api.holdSeats(buyer.bearerToken(), event.eventId(), List.of(event.seatIds().get(0)), UUID.randomUUID());
        assertThat(holdResp.status()).isEqualTo(201);
        UUID orderId = UUID.fromString(holdResp.json().get("reservationId").asText());
        awaitOrderMaterialized(buyer, orderId, Duration.ofSeconds(15));

        Api.ApiResponse checkoutResp = api.checkout(buyer.bearerToken(), orderId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(checkoutResp.status()).isEqualTo(202);
        awaitOrderStatusViaGateway(buyer, orderId, "CONFIRMED", Duration.ofSeconds(20));
        quiescence.await(event.eventId(), Duration.ofSeconds(30));

        // Snapshot outbox counts, payments table, and notifications after happy-path order is CONFIRMED
        int invOutboxCountBefore = db.findInventoryOutboxByAggregateIds(Set.of(orderId)).size();
        int ordOutboxCountBefore = db.findOrderOutboxByAggregateIds(Set.of(orderId)).size();
        int payOutboxCountBefore = db.findPaymentOutboxByAggregateIds(Set.of(orderId)).size();
        List<Db.PaymentRow> paymentsBeforeReplays = db.findPaymentsByOrderIds(Set.of(orderId));
        assertThat(paymentsBeforeReplays).hasSize(1);
        Notifications.NotificationSnapshot notifAfterConfirm = notifications.snapshot();

        // Find the original payment.succeeded, inventory.seat.reserved, order.payment.requested, and order.confirmed records from Kafka
        KafkaProbe.ConsumedEvent origPaySucceeded = kafkaProbe.readSince(KafkaProbe.TOPIC_PAYMENT_EVENTS, payOffsetsBefore)
                .stream()
                .filter(e -> orderId.toString().equals(e.key()) && "payment.succeeded".equals(e.eventType()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Original payment.succeeded not found on payment.events"));

        KafkaProbe.ConsumedEvent origSeatReserved = kafkaProbe.readSince(KafkaProbe.TOPIC_INVENTORY_EVENTS, invOffsetsBefore)
                .stream()
                .filter(e -> orderId.toString().equals(e.key()) && "inventory.seat.reserved".equals(e.eventType()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Original inventory.seat.reserved not found on inventory.events"));

        List<KafkaProbe.ConsumedEvent> orderEvents = kafkaProbe.readSince(KafkaProbe.TOPIC_ORDER_EVENTS, orderOffsetsBefore);
        KafkaProbe.ConsumedEvent origPaymentRequested = orderEvents.stream()
                .filter(e -> orderId.toString().equals(e.key()) && "order.payment.requested".equals(e.eventType()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Original order.payment.requested not found on order.events"));

        KafkaProbe.ConsumedEvent origOrderConfirmed = orderEvents.stream()
                .filter(e -> orderId.toString().equals(e.key()) && "order.confirmed".equals(e.eventType()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Original order.confirmed not found on order.events"));

        // (a) Produce exact copy of already-processed payment.succeeded (same eventId)
        kafkaProbe.produceRaw(
                KafkaProbe.TOPIC_PAYMENT_EVENTS,
                orderId.toString(),
                origPaySucceeded.rawValue(),
                Map.of(
                        "event-type", "payment.succeeded",
                        "event-id", origPaySucceeded.eventId().toString(),
                        "schema-version", "1",
                        "X-Correlation-Id", UUID.randomUUID().toString()
                )
        );

        // (b) Produce second payment.succeeded for the same order with a NEW eventId
        UUID newEventId = UUID.randomUUID();
        ObjectNode modifiedEnvelope = (ObjectNode) objectMapper.readTree(origPaySucceeded.rawValue());
        modifiedEnvelope.put("eventId", newEventId.toString());
        kafkaProbe.produceRaw(
                KafkaProbe.TOPIC_PAYMENT_EVENTS,
                orderId.toString(),
                objectMapper.writeValueAsString(modifiedEnvelope),
                Map.of(
                        "event-type", "payment.succeeded",
                        "event-id", newEventId.toString(),
                        "schema-version", "1",
                        "X-Correlation-Id", UUID.randomUUID().toString()
                )
        );

        // (c) Produce exact copy of already-processed inventory.seat.reserved (same eventId)
        kafkaProbe.produceRaw(
                KafkaProbe.TOPIC_INVENTORY_EVENTS,
                orderId.toString(),
                origSeatReserved.rawValue(),
                Map.of(
                        "event-type", "inventory.seat.reserved",
                        "event-id", origSeatReserved.eventId().toString(),
                        "schema-version", "1",
                        "X-Correlation-Id", UUID.randomUUID().toString()
                )
        );

        // (d) Replay order.payment.requested onto order.events with key orderId
        kafkaProbe.produceRaw(
                KafkaProbe.TOPIC_ORDER_EVENTS,
                orderId.toString(),
                origPaymentRequested.rawValue(),
                Map.of(
                        "event-type", "order.payment.requested",
                        "event-id", origPaymentRequested.eventId().toString(),
                        "schema-version", "1",
                        "X-Correlation-Id", UUID.randomUUID().toString()
                )
        );

        // (e) Replay order.confirmed onto order.events with key orderId
        kafkaProbe.produceRaw(
                KafkaProbe.TOPIC_ORDER_EVENTS,
                orderId.toString(),
                origOrderConfirmed.rawValue(),
                Map.of(
                        "event-type", "order.confirmed",
                        "event-id", origOrderConfirmed.eventId().toString(),
                        "schema-version", "1",
                        "X-Correlation-Id", UUID.randomUUID().toString()
                )
        );

        // Wait for all consumer groups to consume the replayed messages and reach lag 0,
        // and for notification-service to record the duplicate order.confirmed event
        quiescence.await(event.eventId(), Duration.ofSeconds(30));
        Awaitility.await("notification-service duplicate counter increments by 1 for replayed order.confirmed")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> notifications.diff(notifAfterConfirm).duplicate("order.confirmed") == 1L);

        // Assert no state change, no new outbox rows anywhere, payments table unchanged,
        // notification duplicate counter +1 while sent counter unchanged (+0), and 0 DLT growth
        assertThat(db.findInventoryOutboxByAggregateIds(Set.of(orderId))).hasSize(invOutboxCountBefore);
        assertThat(db.findOrderOutboxByAggregateIds(Set.of(orderId))).hasSize(ordOutboxCountBefore);
        assertThat(db.findPaymentOutboxByAggregateIds(Set.of(orderId))).hasSize(payOutboxCountBefore);
        assertThat(db.findPaymentsByOrderIds(Set.of(orderId))).isEqualTo(paymentsBeforeReplays);

        Notifications.NotificationSnapshot notifAfterReplays = notifications.snapshot();
        Notifications.NotificationSnapshot replayNotifDiff = Notifications.diff(notifAfterConfirm, notifAfterReplays);
        assertThat(replayNotifDiff.totalSent()).isEqualTo(0L);
        assertThat(replayNotifDiff.duplicate("order.confirmed")).isEqualTo(1L);
        assertThat(kafkaProbe.diffDltOffsets(dltBefore).totalDelta()).isEqualTo(0L);

        finishAndAuditScenario(
                "S6", "Duplicate and replayed Kafka events", "fast",
                start, event.eventId(),
                1, 1, 1, 0, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                1L, false,
                "Replayed payment.succeeded (same & new eventId), inventory.seat.reserved, order.payment.requested, and order.confirmed: 0 new outbox rows, payments unchanged, notification duplicate=+1, sent=+0, 0 DLT."
        );
    }

    // =========================================================================
    // S7 — Crash recovery (SIGKILL payment-service, inventory-service, order-service)
    // =========================================================================
    @Test
    @Order(7)
    @Tag("chaos")
    @DisplayName("S7: Crash recovery — kill -9 payment-service with payment in flight, then kill -9 inventory-service with holds in flight and order-service with checkouts in flight")
    void s7_crashRecoveryUnderSigkill() throws Exception {
        Random rnd = logSeed("S7", 0x570007L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 16, 11_000L, "USD");
        suiteEventIds.add(event.eventId());

        int holdsAttempted = 0;
        int holdsAccepted = 0;
        int holdsRejected = 0;
        int holdsRateLimited = 0;
        int holdsFailedOrLost = 0;

        ExecutorService pool = Executors.newFixedThreadPool(24);
        try {
            // Part 1: Verify payment is in flight (no payment row yet and lag > 0) before SIGKILL on payment-service
            Fixtures.UserFixture user0 = Fixtures.newUser(api);
            Map<UUID, Fixtures.UserFixture> usersById = new ConcurrentHashMap<>();
            usersById.put(user0.userId(), user0);

            holdsAttempted++;
            Api.ApiResponse hold0 = api.holdSeats(
                    user0.bearerToken(), event.eventId(), List.of(event.seatIds().get(0)), UUID.randomUUID());
            assertThat(hold0.status()).isEqualTo(201);
            holdsAccepted++;
            UUID order0Id = UUID.fromString(hold0.json().get("reservationId").asText());
            awaitOrderMaterialized(user0, order0Id, Duration.ofSeconds(15));

            // Pause payment-service so the checkout event is published to order.events and guaranteed in-flight before SIGKILL
            stack.pause("payment-service");
            Api.ApiResponse checkout0 = api.checkout(user0.bearerToken(), order0Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
            assertThat(checkout0.status()).isEqualTo(202);

            Awaitility.await("order.payment.requested published with consumer lag > 0 before SIGKILL")
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> db.countUnpublishedOutbox("order_db", event.eventId()) == 0
                            && kafkaProbe.totalConsumerGroupLag("payment-service-order-cg") >= 1L);

            assertThat(db.findPaymentsByOrderIds(Set.of(order0Id)))
                    .as("Payment for order0 must still be in flight (no payment row yet) before SIGKILL")
                    .isEmpty();
            assertThat(kafkaProbe.totalConsumerGroupLag("payment-service-order-cg"))
                    .as("payment-service consumer group lag must be > 0 before SIGKILL")
                    .isGreaterThan(0L);

            stack.kill("payment-service");
            stack.start("payment-service");
            stack.waitUntilHealthy("payment-service", Duration.ofSeconds(60));

            awaitOrderStatusViaGateway(user0, order0Id, "CONFIRMED", Duration.ofSeconds(30));
            assertThat(db.findPaymentsByOrderIds(Set.of(order0Id))).hasSize(1);

            // Part 2a: Kill -9 inventory-service while hold requests are actually in flight (using a latch)
            int trafficUsersCount = 45;
            List<Fixtures.UserFixture> trafficUsers = new ArrayList<>(trafficUsersCount);
            List<UUID> targetSeats = new ArrayList<>(trafficUsersCount);
            for (int i = 0; i < trafficUsersCount; i++) {
                Fixtures.UserFixture u = Fixtures.newUserLocal(api);
                trafficUsers.add(u);
                usersById.put(u.userId(), u);
                // Seat index 1..15
                UUID seatId = event.seatIds().get(1 + (rnd.nextInt(15)));
                targetSeats.add(seatId);
            }

            int latchThreshold = 8;
            CountDownLatch holdsDispatchedLatch = new CountDownLatch(latchThreshold);
            CountDownLatch secondWaveGate = new CountDownLatch(1);
            AtomicInteger completedHolds = new AtomicInteger(0);
            List<Future<Api.ApiResponse>> holdFutures = new ArrayList<>(trafficUsersCount);

            for (int i = 0; i < trafficUsersCount; i++) {
                final int idx = i;
                final Fixtures.UserFixture u = trafficUsers.get(i);
                final UUID seatId = targetSeats.get(i);
                holdFutures.add(pool.submit(() -> {
                    try {
                        if (idx >= 16) {
                            secondWaveGate.await(5, TimeUnit.SECONDS);
                        }
                        holdsDispatchedLatch.countDown();
                        return api.holdSeats(u.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
                    } finally {
                        completedHolds.incrementAndGet();
                    }
                }));
            }

            // Wait until N hold requests have been dispatched and assert that requests are still in flight before SIGKILL
            assertThat(holdsDispatchedLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(completedHolds.get())
                    .as("Hold requests must still be in flight when inventory-service is killed")
                    .isLessThan(trafficUsersCount);
            secondWaveGate.countDown();

            stack.kill("inventory-service");
            stack.start("inventory-service");
            stack.waitUntilHealthy("inventory-service", Duration.ofSeconds(60));

            List<UUID> received201ReservationIds = new ArrayList<>();
            received201ReservationIds.add(order0Id);

            for (Future<Api.ApiResponse> f : holdFutures) {
                holdsAttempted++;
                Api.ApiResponse resp = f.get(30, TimeUnit.SECONDS);
                int status = resp.status();
                // During the declared SIGKILL outage window, gateway 502/503/504 or local transportError (status 0) is expected, never 500
                if (resp.transportError()) {
                    assertThat(status).isEqualTo(0);
                    holdsFailedOrLost++;
                } else {
                    assertThat(status)
                            .as("Unexpected HTTP status during inventory-service crash recovery: %d body=%s", status, resp.body())
                            .isIn(201, 409, 429, 502, 503, 504);
                    if (status == 201) {
                        holdsAccepted++;
                        received201ReservationIds.add(UUID.fromString(resp.json().get("reservationId").asText()));
                    } else if (status == 409) {
                        holdsRejected++;
                    } else if (status == 429) {
                        holdsRateLimited++;
                    } else {
                        holdsFailedOrLost++;
                    }
                }
            }

            // Ensure we have at least 6 HELD reservations so Part 2b has multiple checkouts in flight when order-service is killed
            List<Db.SeatRow> availableAfterHoldKill = db.findSeats(event.eventId()).stream()
                    .filter(s -> "AVAILABLE".equals(s.status()))
                    .toList();
            int currentHeldCount = (int) db.findReservations(event.eventId()).stream()
                    .filter(r -> "HELD".equals(r.status()))
                    .count();
            for (int i = 0; currentHeldCount < 6 && i < availableAfterHoldKill.size(); i++) {
                Fixtures.UserFixture extraUser = trafficUsers.get(i);
                holdsAttempted++;
                Api.ApiResponse extraHold = api.holdSeats(
                        extraUser.bearerToken(), event.eventId(), List.of(availableAfterHoldKill.get(i).seatId()), UUID.randomUUID());
                if (extraHold.status() == 201) {
                    holdsAccepted++;
                    currentHeldCount++;
                    received201ReservationIds.add(UUID.fromString(extraHold.json().get("reservationId").asText()));
                } else if (extraHold.status() == 409) {
                    holdsRejected++;
                }
            }

            // Reconcile 201 responses with reservation rows in inventory_db:
            // Every HTTP 201 must exist in inventory_db; any additional reservation row in inventory_db is a hold
            // that committed right before SIGKILL dropped its HTTP response.
            List<Db.ReservationRow> allReservationsNow = db.findReservations(event.eventId());
            Set<UUID> dbReservationIds = allReservationsNow.stream()
                    .map(Db.ReservationRow::reservationId)
                    .collect(Collectors.toSet());
            assertThat(dbReservationIds)
                    .as("Every HTTP 201 reservationId must exist in inventory_db.reservations")
                    .containsAll(received201ReservationIds);
            int crashLostHttp201s = dbReservationIds.size() - received201ReservationIds.size();
            assertThat(crashLostHttp201s).isGreaterThanOrEqualTo(0);

            // Wait for all HELD reservations to materialize in order_db
            List<Db.ReservationRow> heldReservations = allReservationsNow.stream()
                    .filter(r -> "HELD".equals(r.status()))
                    .toList();
            for (Db.ReservationRow held : heldReservations) {
                Fixtures.UserFixture owner = usersById.get(held.userId());
                assertThat(owner).isNotNull();
                awaitOrderMaterialized(owner, held.reservationId(), Duration.ofSeconds(30));
            }

            // Part 2b: Kill -9 order-service while checkout requests are actually in flight (using a latch)
            int checkoutCount = heldReservations.size();
            CountDownLatch checkoutsDispatchedLatch = new CountDownLatch(Math.min(2, checkoutCount));
            CountDownLatch checkoutSecondWaveGate = new CountDownLatch(1);
            AtomicInteger completedCheckouts = new AtomicInteger(0);
            List<Future<Api.ApiResponse>> checkoutFutures = new ArrayList<>(checkoutCount);

            for (int i = 0; i < checkoutCount; i++) {
                final int idx = i;
                final Db.ReservationRow held = heldReservations.get(i);
                final Fixtures.UserFixture owner = usersById.get(held.userId());
                checkoutFutures.add(pool.submit(() -> {
                    try {
                        if (idx >= 3) {
                            checkoutSecondWaveGate.await(5, TimeUnit.SECONDS);
                        }
                        checkoutsDispatchedLatch.countDown();
                        return api.checkout(owner.bearerToken(), held.reservationId(), UUID.randomUUID(), Fixtures.TOK_SUCCESS);
                    } finally {
                        completedCheckouts.incrementAndGet();
                    }
                }));
            }

            assertThat(checkoutsDispatchedLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(completedCheckouts.get())
                    .as("Checkout requests must still be in flight when order-service is killed")
                    .isLessThan(checkoutCount);
            checkoutSecondWaveGate.countDown();

            stack.kill("order-service");
            stack.start("order-service");
            stack.waitUntilHealthy("order-service", Duration.ofSeconds(60));

            for (Future<Api.ApiResponse> cf : checkoutFutures) {
                Api.ApiResponse cResp = cf.get(30, TimeUnit.SECONDS);
                if (cResp.transportError()) {
                    assertThat(cResp.status()).isEqualTo(0);
                } else {
                    assertThat(cResp.status())
                            .as("Unexpected HTTP status during order-service crash recovery: %d body=%s", cResp.status(), cResp.body())
                            .isIn(202, 409, 429, 502, 503, 504);
                }
            }

            // Retry checkout for any HELD reservation whose checkout failed during order-service SIGKILL
            for (Db.ReservationRow held : heldReservations) {
                Fixtures.UserFixture owner = usersById.get(held.userId());
                Db.OrderRow currentOrder = db.findOrderById(held.reservationId()).orElseThrow();
                if ("RESERVED".equals(currentOrder.status())) {
                    Api.ApiResponse retryCheckout = api.checkout(
                            owner.bearerToken(), held.reservationId(), UUID.randomUUID(), Fixtures.TOK_SUCCESS);
                    assertThat(retryCheckout.status()).isIn(202, 409);
                }
            }

            for (Db.ReservationRow held : heldReservations) {
                Fixtures.UserFixture owner = usersById.get(held.userId());
                awaitOrderStatusViaGateway(owner, held.reservationId(), "CONFIRMED", Duration.ofSeconds(30));
            }
        } finally {
            pool.shutdownNow();
            stack.restoreAndWaitAllHealthy(Duration.ofSeconds(60));
        }

        int finalDbReservations = db.findReservations(event.eventId()).size();
        int lostHttp201Count = finalDbReservations - holdsAccepted;
        String reconciliationNote = String.format(
                "SIGKILL on payment-service (in-flight payment), inventory-service (in-flight holds), and order-service (in-flight checkouts) recovered cleanly; HTTP 201s=%d, DB reservations=%d (%d committed in DB whose HTTP response was lost to SIGKILL).",
                holdsAccepted, finalDbReservations, lostHttp201Count
        );

        finishAndAuditScenario(
                "S7", "Crash recovery (SIGKILL payment, inventory, and order services)", "chaos",
                start, event.eventId(),
                46, holdsAttempted, holdsAccepted, holdsRejected, holdsRateLimited, holdsFailedOrLost,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                0L, false,
                reconciliationNote
        );
    }

    // =========================================================================
    // S8 — Broker outage (stop kafka for 15 seconds while 30 orders in flight)
    // =========================================================================
    @Test
    @Order(8)
    @Tag("chaos")
    @DisplayName("S8: Broker outage — stop kafka for 15s while 30 orders are in flight, then start kafka and verify full outbox drain")
    void s8_kafkaBrokerOutageOutboxResilience() {
        logSeed("S8", 0x580008L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        int orderCount = 30;
        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, orderCount, 13_000L, "USD");
        suiteEventIds.add(event.eventId());

        List<Fixtures.UserFixture> users = new ArrayList<>(orderCount);
        for (int i = 0; i < orderCount; i++) {
            users.add(Fixtures.newUserLocal(api));
        }

        List<UUID> firstHalfOrders = new ArrayList<>(15);
        List<UUID> secondHalfOrders = new ArrayList<>(15);

        try {
            // 1. First 15 users hold 1 seat each and reach AWAITING_PAYMENT before Kafka stops
            for (int i = 0; i < 15; i++) {
                Api.ApiResponse holdResp = api.holdSeats(
                        users.get(i).bearerToken(), event.eventId(), List.of(event.seatIds().get(i)), UUID.randomUUID());
                assertThat(holdResp.status()).isEqualTo(201);
                UUID resId = UUID.fromString(holdResp.json().get("reservationId").asText());
                firstHalfOrders.add(resId);
            }
            for (int i = 0; i < 15; i++) {
                awaitOrderMaterialized(users.get(i), firstHalfOrders.get(i), Duration.ofSeconds(20));
            }

            // 2. Stop Kafka for 15 seconds while all 30 orders are in flight
            stack.stop("kafka");
            Instant kafkaStoppedAt = Instant.now();

            // While Kafka is down:
            // - First 15 orders execute checkout (buffering order.payment.requested in order_db.outbox_events)
            for (int i = 0; i < 15; i++) {
                Api.ApiResponse checkoutResp = api.checkout(
                        users.get(i).bearerToken(), firstHalfOrders.get(i), UUID.randomUUID(), Fixtures.TOK_SUCCESS);
                assertThat(checkoutResp.status()).isEqualTo(202);
            }
            // - Second 15 users hold seats 15..29 (buffering inventory.seat.reserved in inventory_db.outbox_events)
            for (int i = 15; i < 30; i++) {
                Api.ApiResponse holdResp = api.holdSeats(
                        users.get(i).bearerToken(), event.eventId(), List.of(event.seatIds().get(i)), UUID.randomUUID());
                assertThat(holdResp.status()).isEqualTo(201);
                UUID resId = UUID.fromString(holdResp.json().get("reservationId").asText());
                secondHalfOrders.add(resId);
            }

            // Verify outbox rows are buffered unpublished while Kafka is down, and wait full 15s outage window
            assertThat(db.countUnpublishedOutbox("inventory_db", event.eventId())).isGreaterThanOrEqualTo(15);
            assertThat(db.countUnpublishedOutbox("order_db", event.eventId())).isGreaterThanOrEqualTo(15);

            Awaitility.await("15-second Kafka broker outage window")
                    .atMost(Duration.ofSeconds(20))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> Duration.between(kafkaStoppedAt, Instant.now()).toMillis() >= 15_000L);

            // 3. Start Kafka and wait until healthy
            stack.start("kafka");
            stack.waitUntilHealthy("kafka", Duration.ofSeconds(60));

            // 4. As inventory_db.outbox_events drains, the second 15 orders materialize; check them out
            for (int i = 0; i < 15; i++) {
                Fixtures.UserFixture u = users.get(15 + i);
                UUID orderId = secondHalfOrders.get(i);
                awaitOrderMaterialized(u, orderId, Duration.ofSeconds(45));
                Api.ApiResponse checkoutResp = api.checkout(
                        u.bearerToken(), orderId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
                assertThat(checkoutResp.status()).isEqualTo(202);
            }

            // 5. Wait for all 30 orders to reach CONFIRMED
            for (int i = 0; i < 15; i++) {
                awaitOrderStatusViaGateway(users.get(i), firstHalfOrders.get(i), "CONFIRMED", Duration.ofSeconds(45));
                awaitOrderStatusViaGateway(users.get(15 + i), secondHalfOrders.get(i), "CONFIRMED", Duration.ofSeconds(45));
            }
        } finally {
            stack.restoreAndWaitAllHealthy(Duration.ofSeconds(60));
        }

        finishAndAuditScenario(
                "S8", "Kafka broker 15s outage with 30 in-flight orders", "chaos",
                start, event.eventId(),
                orderCount, orderCount, orderCount, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "30 in-flight orders buffered in inventory_db and order_db outboxes during 15s Kafka stop; all 30 reached CONFIRMED after broker recovery."
        );
    }

    // =========================================================================
    // S9 — Idempotent checkout through the gateway
    // =========================================================================
    @Test
    @Order(9)
    @Tag("fast")
    @DisplayName("S9: Idempotent checkout through the gateway — concurrent same Idempotency-Key and concurrent different keys create only 1 payment each")
    void s9_idempotentCheckoutThroughGateway() throws Exception {
        logSeed("S9", 0x590009L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 2, 16_000L, "USD");
        suiteEventIds.add(event.eventId());

        Fixtures.UserFixture user1 = Fixtures.newUser(api);
        Fixtures.UserFixture user2 = Fixtures.newUser(api);

        // Order 1: two simultaneous checkout requests with the SAME Idempotency-Key and body
        Api.ApiResponse hold1 = api.holdSeats(user1.bearerToken(), event.eventId(), List.of(event.seatIds().get(0)), UUID.randomUUID());
        assertThat(hold1.status()).isEqualTo(201);
        UUID order1Id = UUID.fromString(hold1.json().get("reservationId").asText());
        awaitOrderMaterialized(user1, order1Id, Duration.ofSeconds(15));

        // Order 2: two simultaneous checkout requests with DIFFERENT Idempotency-Keys
        Api.ApiResponse hold2 = api.holdSeats(user2.bearerToken(), event.eventId(), List.of(event.seatIds().get(1)), UUID.randomUUID());
        assertThat(hold2.status()).isEqualTo(201);
        UUID order2Id = UUID.fromString(hold2.json().get("reservationId").asText());
        awaitOrderMaterialized(user2, order2Id, Duration.ofSeconds(15));

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            // Case A: Same Idempotency-Key and same body on Order 1
            UUID sharedKey = UUID.randomUUID();
            CountDownLatch latchA = new CountDownLatch(1);
            Future<Api.ApiResponse> sameKey1 = pool.submit(() -> {
                latchA.await(5, TimeUnit.SECONDS);
                return api.checkout(user1.bearerToken(), order1Id, sharedKey, Fixtures.TOK_SUCCESS);
            });
            Future<Api.ApiResponse> sameKey2 = pool.submit(() -> {
                latchA.await(5, TimeUnit.SECONDS);
                return api.checkout(user1.bearerToken(), order1Id, sharedKey, Fixtures.TOK_SUCCESS);
            });
            latchA.countDown();

            Api.ApiResponse respA1 = sameKey1.get(15, TimeUnit.SECONDS);
            Api.ApiResponse respA2 = sameKey2.get(15, TimeUnit.SECONDS);

            // At least one must be 202; if both are 202, at least one must carry X-Idempotent-Replay: true;
            // otherwise one is 202 and one is 409 with errorCode IDEMPOTENCY_REQUEST_IN_PROGRESS and Retry-After: 1
            List<Integer> statusesA = List.of(respA1.status(), respA2.status());
            assertThat(statusesA).contains(202);
            for (Api.ApiResponse r : List.of(respA1, respA2)) {
                assertThat(r.status()).isIn(202, 409);
                if (r.status() == 409) {
                    assertThat(r.headerValue("Retry-After")).isEqualTo("1");
                    assertThat(r.errorCode()).isEqualTo("IDEMPOTENCY_REQUEST_IN_PROGRESS");
                }
            }
            if (respA1.status() == 202 && respA2.status() == 202) {
                assertThat(List.of(respA1.headerValue("X-Idempotent-Replay"), respA2.headerValue("X-Idempotent-Replay")))
                        .as("When two identical-key requests both return 202, at least one must carry X-Idempotent-Replay: true")
                        .contains("true");
            } else {
                assertThat(statusesA).containsExactlyInAnyOrder(202, 409);
            }

            // Subsequent replay with the same Idempotency-Key and same body returns 202 with X-Idempotent-Replay: true
            Api.ApiResponse replayResp = api.checkout(user1.bearerToken(), order1Id, sharedKey, Fixtures.TOK_SUCCESS);
            assertThat(replayResp.status()).isEqualTo(202);
            assertThat(replayResp.headerValue("X-Idempotent-Replay")).isEqualTo("true");

            // Reusing the same Idempotency-Key with a DIFFERENT body returns 422 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH
            Api.ApiResponse mismatchResp = api.checkout(user1.bearerToken(), order1Id, sharedKey, Fixtures.TOK_DECLINE);
            assertThat(mismatchResp.status()).isEqualTo(422);
            assertThat(mismatchResp.errorCode()).isEqualTo("IDEMPOTENCY_KEY_PAYLOAD_MISMATCH");

            // Case B: Different Idempotency-Keys simultaneously on Order 2
            CountDownLatch latchB = new CountDownLatch(1);
            Future<Api.ApiResponse> diffKey1 = pool.submit(() -> {
                latchB.await(5, TimeUnit.SECONDS);
                return api.checkout(user2.bearerToken(), order2Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
            });
            Future<Api.ApiResponse> diffKey2 = pool.submit(() -> {
                latchB.await(5, TimeUnit.SECONDS);
                return api.checkout(user2.bearerToken(), order2Id, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
            });
            latchB.countDown();

            Api.ApiResponse respB1 = diffKey1.get(15, TimeUnit.SECONDS);
            Api.ApiResponse respB2 = diffKey2.get(15, TimeUnit.SECONDS);

            // Exactly one wins the row lock (202) and the other sees PAYMENT_PENDING (409 INVALID_ORDER_STATE)
            assertThat(List.of(respB1.status(), respB2.status()))
                    .containsExactlyInAnyOrder(202, 409);
            Api.ApiResponse conflictB = respB1.status() == 409 ? respB1 : respB2;
            assertThat(conflictB.errorCode()).isEqualTo("INVALID_ORDER_STATE");

            awaitOrderStatusViaGateway(user1, order1Id, "CONFIRMED", Duration.ofSeconds(20));
            awaitOrderStatusViaGateway(user2, order2Id, "CONFIRMED", Duration.ofSeconds(20));

            assertThat(db.findPaymentsByOrderIds(Set.of(order1Id))).hasSize(1);
            assertThat(db.findPaymentsByOrderIds(Set.of(order2Id))).hasSize(1);
        } finally {
            pool.shutdownNow();
        }

        finishAndAuditScenario(
                "S9", "Idempotent checkout through the gateway", "fast",
                start, event.eventId(),
                2, 2, 2, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "Same-key concurrent + replay returned 202 (X-Idempotent-Replay: true), payload mismatch returned 422, different-key concurrent returned [202, 409 INVALID_ORDER_STATE]; 1 payment per order."
        );
    }

    // =========================================================================
    // S10 — Technical provider error (tok_error)
    // =========================================================================
    @Test
    @Order(10)
    @Tag("fast")
    @DisplayName("S10: Technical provider error — tok_error retries via retry topic and reaches CONFIRMED with 1 charge and 0 DLT")
    void s10_technicalProviderErrorRetriesAndSucceeds() {
        logSeed("S10", 0x5A0010L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        String retryTopic = "order.events-payment-service-order-cg-retry-0";
        Map<TopicPartition, Long> retryOffsetsBefore = kafkaProbe.endOffsets(retryTopic);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 2, 18_000L, "USD");
        suiteEventIds.add(event.eventId());
        Fixtures.UserFixture buyer = Fixtures.newUser(api);

        Api.ApiResponse holdResp = api.holdSeats(buyer.bearerToken(), event.eventId(), List.of(event.seatIds().get(0)), UUID.randomUUID());
        assertThat(holdResp.status()).isEqualTo(201);
        UUID orderId = UUID.fromString(holdResp.json().get("reservationId").asText());
        awaitOrderMaterialized(buyer, orderId, Duration.ofSeconds(15));

        Api.ApiResponse checkoutResp = api.checkout(buyer.bearerToken(), orderId, UUID.randomUUID(), Fixtures.TOK_ERROR);
        assertThat(checkoutResp.status()).isEqualTo(202);

        awaitOrderStatusViaGateway(buyer, orderId, "CONFIRMED", Duration.ofSeconds(30));

        // Verify 1 effective charge in payment_db
        List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(Set.of(orderId));
        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).status()).isEqualTo("SUCCEEDED");

        // Verify retry topic record by key (count by key equal to 1)
        long retryRecordCountForOrder = kafkaProbe.countByKey(
                retryTopic, retryOffsetsBefore, orderId.toString(), "order.payment.requested");
        assertThat(retryRecordCountForOrder)
                .as("Expected retry topic %s to contain exactly 1 record with key=%s", retryTopic, orderId)
                .isEqualTo(1L);

        // Verify DLT unchanged
        assertThat(kafkaProbe.diffDltOffsets(dltBefore).totalDelta()).isEqualTo(0L);

        finishAndAuditScenario(
                "S10", "Technical provider error (tok_error) retry recovery", "fast",
                start, event.eventId(),
                1, 1, 1, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "tok_error triggered retry topic (" + retryTopic + " countByKey=" + retryRecordCountForOrder + "), succeeded on retry with 1 charge and 0 DLT."
        );
    }

    // =========================================================================
    // S11 — Refund declined (tok_refund_decline)
    // =========================================================================
    @Test
    @Order(11)
    @Tag("chaos")
    @DisplayName("S11: Refund declined — late payment with tok_refund_decline keeps payment SUCCEEDED, order REFUND_PENDING, and routes to DLT")
    void s11_refundDeclinedRoutesToDeadLetterTopic() {
        logSeed("S11", 0x5B0011L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 1, 22_000L, "USD");
        suiteEventIds.add(event.eventId());
        UUID seatId = event.seatIds().get(0);
        Fixtures.UserFixture buyer = Fixtures.newUser(api);

        try {
            Api.ApiResponse hold = api.holdSeats(buyer.bearerToken(), event.eventId(), List.of(seatId), UUID.randomUUID());
            assertThat(hold.status()).isEqualTo(201);
            UUID orderId = UUID.fromString(hold.json().get("reservationId").asText());
            awaitOrderMaterialized(buyer, orderId, Duration.ofSeconds(15));

            stack.stop("payment-service");
            Api.ApiResponse checkout = api.checkout(buyer.bearerToken(), orderId, UUID.randomUUID(), Fixtures.TOK_REFUND_DECLINE);
            assertThat(checkout.status()).isEqualTo(202);

            Awaitility.await("order.payment.requested published while payment-service stopped")
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> db.countUnpublishedOutbox("order_db", event.eventId()) == 0
                            && kafkaProbe.totalConsumerGroupLag("payment-service-order-cg") >= 1L);

            assertThat(db.findPaymentsByOrderIds(Set.of(orderId))).isEmpty();

            db.forceReservationExpiry(orderId);
            awaitOrderStatusViaGateway(buyer, orderId, "CANCELLED_EXPIRED", Duration.ofSeconds(25));

            // Register orderId as expected to rest in REFUND_PENDING with 1 declared dead-letter record
            registerExpectedRefundPendingOrder(orderId, 1L);

            stack.start("payment-service");
            stack.waitUntilHealthy("payment-service", Duration.ofSeconds(60));

            // Wait for the declined refund to route to order.events-payment-service-order-cg-dlt and reach quiescence
            Awaitility.await("order.refund.requested routed to order.events-payment-service-order-cg-dlt")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> kafkaProbe.diffDltOffsets(dltBefore)
                            .deltaByTopic().getOrDefault("order.events-payment-service-order-cg-dlt", 0L) == 1L);

            quiescence.await(event.eventId(), Duration.ofSeconds(60), true);

            // Expected per S11 specification:
            // - payment stays SUCCEEDED
            // - order stays REFUND_PENDING
            // - event lands in order.events-payment-service-order-cg-dlt (1 DLT record)
            // - no payment.refunded is emitted
            List<Db.PaymentRow> payments = db.findPaymentsByOrderIds(Set.of(orderId));
            assertThat(payments).hasSize(1);
            assertThat(payments.get(0).status())
                    .as("Payment with tok_refund_decline must remain SUCCEEDED when refund is declined")
                    .isEqualTo("SUCCEEDED");

            List<Db.OrderRow> orders = db.findOrders(event.eventId());
            assertThat(orders).hasSize(1);
            assertThat(orders.get(0).status())
                    .as("Order must remain REFUND_PENDING when refund is declined")
                    .isEqualTo("REFUND_PENDING");

            List<Db.OutboxRow> payOutbox = db.findPaymentOutboxByAggregateIds(Set.of(orderId));
            assertThat(payOutbox.stream().filter(r -> "payment.refunded".equals(r.eventType())).count())
                    .as("No payment.refunded event may be emitted when refund is declined")
                    .isEqualTo(0L);

            KafkaProbe.DltDiff dltDiff = kafkaProbe.diffDltOffsets(dltBefore);
            assertThat(dltDiff.deltaByTopic().getOrDefault("order.events-payment-service-order-cg-dlt", 0L))
                    .as("Expected 1 record in order.events-payment-service-order-cg-dlt")
                    .isEqualTo(1L);
        } finally {
            stack.restoreAndWaitAllHealthy(Duration.ofSeconds(60));
        }

        finishAndAuditScenario(
                "S11", "Refund declined (tok_refund_decline) routes to DLT", "chaos",
                start, event.eventId(),
                1, 1, 1, 0, 0, 0,
                orderOffsetsBefore, dltBefore, 1L, notifBefore,
                0L, true,
                "Late payment with tok_refund_decline kept payment SUCCEEDED, order REFUND_PENDING, emitted 0 payment.refunded, and routed 1 order.refund.requested to DLT."
        );
    }

    // =========================================================================
    // S12 — Gateway protection
    // =========================================================================
    @Test
    @Order(12)
    @Tag("fast")
    @DisplayName("S12: Gateway protection — RBAC 403 on admin routes, spoofed X-User-Roles and X-User-Id neutralization, cross-user order isolation, and per-user rate limit isolation")
    void s12_gatewayProtectionRbacHeaderSpoofingAndRateLimitIsolation() {
        logSeed("S12", 0x5C0012L);
        Instant start = db.currentTimestampInInventoryDb();
        Map<TopicPartition, Long> orderOffsetsBefore = kafkaProbe.endOffsets(KafkaProbe.TOPIC_ORDER_EVENTS);
        KafkaProbe.DltSnapshot dltBefore = kafkaProbe.snapshotDltOffsets();
        Notifications.NotificationSnapshot notifBefore = notifications.snapshot();

        Fixtures.EventFixture event = Fixtures.createOnSaleEvent(api, 2, 12_500L, "USD");
        suiteEventIds.add(event.eventId());

        Fixtures.UserFixture userA = Fixtures.newUser(api);
        Fixtures.UserFixture userB = Fixtures.newUser(api);
        Fixtures.UserFixture userC = Fixtures.newUser(api);

        // 1. User token rejected on admin routes (403 Forbidden), including when spoofing X-User-Roles: ADMIN
        Api.ApiResponse forbiddenCreate = api.createEvent(
                userA.bearerToken(), "Unauthorized Event", "Arena", Instant.now(), Instant.now().plusSeconds(3600));
        assertThat(forbiddenCreate.status()).isEqualTo(403);

        Api.ApiResponse forbiddenStatus = api.setEventStatus(userA.bearerToken(), event.eventId(), "CANCELLED");
        assertThat(forbiddenStatus.status()).isEqualTo(403);

        Api.ApiResponse spoofedAdminRoleCreate = api.createEventWithHeaders(
                userA.bearerToken(), "Spoofed Role Event", "Arena", Instant.now(), Instant.now().plusSeconds(3600),
                Map.of("X-User-Roles", "ADMIN"));
        assertThat(spoofedAdminRoleCreate.status()).isEqualTo(403);
        assertThat(spoofedAdminRoleCreate.errorCode()).isEqualTo("FORBIDDEN");

        Api.ApiResponse spoofedAdminRoleStatus = api.setEventStatusWithHeaders(
                userA.bearerToken(), event.eventId(), "CANCELLED",
                Map.of("X-User-Roles", "ADMIN"));
        assertThat(spoofedAdminRoleStatus.status()).isEqualTo(403);
        assertThat(spoofedAdminRoleStatus.errorCode()).isEqualTo("FORBIDDEN");

        // 2. Ownership check: User B cannot read or check out User A's order (with or without spoofed X-User-Id)
        Api.ApiResponse holdA = api.holdSeats(userA.bearerToken(), event.eventId(), List.of(event.seatIds().get(0)), UUID.randomUUID());
        assertThat(holdA.status()).isEqualTo(201);
        UUID orderAId = UUID.fromString(holdA.json().get("reservationId").asText());
        awaitOrderMaterialized(userA, orderAId, Duration.ofSeconds(15));

        Api.ApiResponse crossUserRead = api.getOrder(userB.bearerToken(), orderAId);
        assertThat(crossUserRead.status()).isEqualTo(403);
        assertThat(crossUserRead.errorCode()).isEqualTo("FORBIDDEN");

        Api.ApiResponse spoofedRead = api.getOrderWithHeaders(
                userB.bearerToken(), orderAId, Map.of("X-User-Id", userA.userId().toString()));
        assertThat(spoofedRead.status()).isEqualTo(403);
        assertThat(spoofedRead.errorCode()).isEqualTo("FORBIDDEN");

        UUID unknownOrderId = UUID.randomUUID();
        Api.ApiResponse unknownOrderCheckout = api.checkout(
                userB.bearerToken(), unknownOrderId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        Api.ApiResponse crossUserCheckout = api.checkout(
                userB.bearerToken(), orderAId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        Api.ApiResponse spoofedCheckout = api.checkoutWithSpoofedHeaders(
                userB.bearerToken(), orderAId, UUID.randomUUID(), Fixtures.TOK_SUCCESS,
                Map.of("X-User-Id", userA.userId().toString()));

        // All return 409 ORDER_NOT_READY without leaking whether the order exists for another user
        assertThat(crossUserCheckout.status()).isEqualTo(409);
        assertThat(crossUserCheckout.errorCode()).isEqualTo("ORDER_NOT_READY");
        assertThat(spoofedCheckout.status()).isEqualTo(unknownOrderCheckout.status()).isEqualTo(409);
        assertThat(spoofedCheckout.json().path("errorCode").asText())
                .isEqualTo(unknownOrderCheckout.json().path("errorCode").asText())
                .isEqualTo("ORDER_NOT_READY");

        // User A's order is untouched and User A can check it out normally
        Api.ApiResponse validCheckoutA = api.checkout(userA.bearerToken(), orderAId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(validCheckoutA.status()).isEqualTo(202);
        awaitOrderStatusViaGateway(userA, orderAId, "CONFIRMED", Duration.ofSeconds(20));

        // 3. Burst of requests from User C hits 429 with Retry-After while User B's request at the same moment succeeds
        int rateLimitedCount = 0;
        for (int i = 0; i < 20; i++) {
            Api.ApiResponse burstResp = api.getOrder(userC.bearerToken(), orderAId);
            if (burstResp.status() == 429) {
                rateLimitedCount++;
                assertThat(burstResp.headerValue("Retry-After")).isNotBlank();
            }
        }
        assertThat(rateLimitedCount)
                .as("Expected User C burst of 20 requests (capacity=10) to trigger 429")
                .isGreaterThanOrEqualTo(1);

        // Immediately verify User B is unaffected by User C's rate limit and can hold & buy the second seat
        Api.ApiResponse holdB = api.holdSeats(userB.bearerToken(), event.eventId(), List.of(event.seatIds().get(1)), UUID.randomUUID());
        assertThat(holdB.status()).isEqualTo(201);
        UUID orderBId = UUID.fromString(holdB.json().get("reservationId").asText());
        awaitOrderMaterialized(userB, orderBId, Duration.ofSeconds(15));
        Api.ApiResponse checkoutB = api.checkout(userB.bearerToken(), orderBId, UUID.randomUUID(), Fixtures.TOK_SUCCESS);
        assertThat(checkoutB.status()).isEqualTo(202);
        awaitOrderStatusViaGateway(userB, orderBId, "CONFIRMED", Duration.ofSeconds(20));

        finishAndAuditScenario(
                "S12", "Gateway protection (RBAC, header spoofing, rate-limit isolation)", "fast",
                start, event.eventId(),
                3, 2, 2, 0, 0,
                orderOffsetsBefore, dltBefore, 0L, notifBefore,
                "RBAC & spoofed X-User-Roles: ADMIN returned 403, User B read of User A order returned 403, User B checkout of User A order returned 409 ORDER_NOT_READY, User C GET burst hit 429 (" + rateLimitedCount + "x) while User B hold+checkout succeeded."
        );
    }

    // =========================================================================
    // S13 — Final global audit across all databases, Kafka consumer groups, and DLTs
    // =========================================================================
    @Test
    @Order(13)
    @Tag("fast")
    @Tag("chaos")
    @DisplayName("S13: Final global audit — verify I1–I8 across all rows of inventory_db, order_db, and payment_db, zero lag across all 13 Kafka consumer groups, exact DLT delta, and exact notification counter equality")
    void s13_finalGlobalAuditAcrossAllDatabases() {
        logSeed("S13", 0x5D0013L);
        Instant start = db.currentTimestampInInventoryDb();

        // 1. Wait for complete global quiescence across ALL events in inventory_db, order_db, payment_db, and Kafka
        List<UUID> allEventIdsInDb = db.findEvents(null).stream().map(Db.EventRow::eventId).toList();
        quiescence.awaitForEvents(allEventIdsInDb, Duration.ofSeconds(60), true);

        // 2. Assert 0 consumer group lag everywhere
        Map<String, Long> allLags = kafkaProbe.allConsumerGroupLags();
        for (Map.Entry<String, Long> entry : allLags.entrySet()) {
            assertThat(entry.getValue())
                    .as("Expected 0 consumer group lag for %s", entry.getKey())
                    .isEqualTo(0L);
        }

        // 3. Assert total DLT growth across the whole run equals sum of declared expected dead letters
        KafkaProbe.DltDiff globalDltDiff = kafkaProbe.diffDltOffsets(suiteStartDltSnapshot);
        assertThat(globalDltDiff.totalDelta())
                .as("Global dead-letter growth must equal declared expected dead letters (%d), got %s",
                        totalExpectedDeadLettersAcrossSuite, globalDltDiff.deltaByTopic())
                .isEqualTo(totalExpectedDeadLettersAcrossSuite);

        // 4. Run InvariantAuditor over ALL rows of all three databases (eventIds = null)
        Notifications.NotificationSnapshot suiteEndNotifications = notifications.snapshot();
        Notifications.NotificationSnapshot globalNotifDiff = Notifications.diff(suiteStartNotifications, suiteEndNotifications);

        InvariantAuditor.AuditResult fullGlobalAudit = auditor.auditGlobalAllRows(
                kafkaProbe,
                suiteStartOrderEventOffsets,
                suiteStartDltSnapshot,
                totalExpectedDeadLettersAcrossSuite,
                suiteStartNotifications,
                suiteEndNotifications,
                totalExpectedDuplicateNotificationsAcrossSuite,
                declaredRefundPendingOrders.keySet(),
                baselineOrderOutboxEventIds
        );

        long durationMs = Duration.between(start, db.currentTimestampInInventoryDb()).toMillis();

        reportRecorder.record(ScenarioReportRecorder.fromEventsAudit(
                "S13",
                "Final global audit across all rows of all three databases, Kafka consumer groups, DLTs, and notifications",
                "fast,chaos",
                fullGlobalAudit.allPassed() ? "PASS" : "FAIL",
                Math.max(0L, durationMs),
                0,
                0,
                0,
                0,
                0,
                0,
                null,
                db,
                globalNotifDiff,
                globalDltDiff.totalDelta(),
                totalExpectedDeadLettersAcrossSuite,
                fullGlobalAudit,
                "Audited all rows across inventory_db, order_db, and payment_db, all 13 Kafka consumer groups (lag=0), DLT delta, and exact notification-service counter equality."
        ));

        fullGlobalAudit.assertAllPassed();
    }
}

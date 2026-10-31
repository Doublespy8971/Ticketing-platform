package com.ticketing.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.ReservationExpiredPayload;
import com.ticketing.common.events.SeatConfirmedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.inventory.api.ReservationController;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.api.dto.ReservationResponse;
import com.ticketing.inventory.config.ExpirySweeperConfig;
import com.ticketing.inventory.config.ExpirySweeperProperties;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.InventoryRedisProperties;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mockito;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ExpirySweeperIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("inventory_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.1-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "25");
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("ticketing.sweeper.enabled", () -> "false");
    }

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private RedisSeatGate redisSeatGate;

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private ExpirySweeper expirySweeper;

    @Autowired
    private ExpirySweeperProperties sweeperProperties;

    @Autowired
    private InventoryRedisProperties redisProperties;

    @Autowired
    @Qualifier("holdInventoryScript")
    private DefaultRedisScript<String> holdInventoryScript;

    @Autowired
    @Qualifier("releaseInventoryScript")
    private DefaultRedisScript<Long> releaseInventoryScript;

    @Autowired
    @Qualifier("confirmInventoryScript")
    private DefaultRedisScript<String> confirmInventoryScript;

    @Autowired
    private Clock clock;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectProvider<MeterRegistry> meterRegistryProvider;

    @BeforeEach
    void cleanState() {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate.execute(
                "TRUNCATE TABLE idempotency_keys, processed_events, outbox_events, reservation_items, reservations, seats, events CASCADE");
        stringRedisTemplate.execute((RedisCallback<Void>) (RedisConnection connection) -> {
            connection.serverCommands().flushAll();
            return null;
        });
        expirySweeper.setAfterReservationLockHook(null);
        expirySweeper.setAfterExpireCommitBeforeRedisReleaseHook(null);
        expirySweeper.invalidateOverdueHoldGaugeCache();
    }

    @AfterEach
    void resetHooks() {
        expirySweeper.setAfterReservationLockHook(null);
        expirySweeper.setAfterExpireCommitBeforeRedisReleaseHook(null);
    }

    // -------------------------------------------------------------------------
    // 1. Dedicated scheduler bean & Micrometer gauge / metrics verification
    // -------------------------------------------------------------------------

    @Test
    void dedicatedSingleThreadSchedulerBeanAndMetricsAreConfigured() {
        assertThat(applicationContext.containsBean(ExpirySweeperConfig.SCHEDULER_BEAN_NAME)).isTrue();
        ThreadPoolTaskScheduler scheduler = applicationContext.getBean(
                ExpirySweeperConfig.SCHEDULER_BEAN_NAME, ThreadPoolTaskScheduler.class);
        assertThat(scheduler.getThreadNamePrefix()).isEqualTo("expiry-sweeper-");
        assertThat(scheduler.getScheduledThreadPoolExecutor().getCorePoolSize()).isEqualTo(1);
        assertThat(sweeperProperties.getIntervalMs()).isEqualTo(1000L);
        assertThat(sweeperProperties.getBatchSize()).isEqualTo(500);
    }

    // -------------------------------------------------------------------------
    // 2. Expired HELD reservation becomes EXPIRED, items EXPIRED, seats AVAILABLE
    //    with version incremented, 1 reservation.expired outbox row, Redis deleted
    // -------------------------------------------------------------------------

    @Test
    void expiredHeldReservationTransitionsToExpiredReleasesSeatsWritesOutboxAndDeletesRedisKeys() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "SEC-A", "1", "101", 15000L, "HELD", 1L),
                seatRecord(seat2, eventId, "SEC-A", "1", "102", 15000L, "HELD", 1L)
        ));
        seedReservationInDbAndRedis(reservationId, eventId, userId, List.of(seat1, seat2), "HELD", -30);

        expirySweeper.invalidateOverdueHoldGaugeCache();
        Gauge overdueGauge = meterRegistry.find(ExpirySweeper.METRIC_OLDEST_OVERDUE_HOLD).gauge();
        assertThat(overdueGauge).isNotNull();
        double overdueBeforeSweep = overdueGauge.value();
        assertThat(overdueBeforeSweep).isGreaterThanOrEqualTo(25.0);

        double ticketingExpiredBefore = counterValue(ExpirySweeper.METRIC_TICKETING_RESERVATIONS_EXPIRED);
        long sweepTimerCountBefore = timerCount(ExpirySweeper.METRIC_SWEEP_DURATION);

        int expiredCount = expirySweeper.sweep();
        assertThat(expiredCount).isEqualTo(1);

        // Sweeper updates gauge at end of sweep; no more overdue holds, so gauge is now 0.0
        assertThat(overdueGauge.value()).isEqualTo(0.0);

        // 1. Reservation row is EXPIRED with expired_at from DB clock
        ReservationRecord updatedRes = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(updatedRes.status()).isEqualTo("EXPIRED");
        assertThat(updatedRes.expiredAt()).isNotNull();
        assertThat(updatedRes.expiredAt()).isAfter(updatedRes.expiresAt());

        // 2. Reservation items are EXPIRED
        List<ReservationItemRecord> items = reservationRepository.findItemsByReservationId(reservationId);
        assertThat(items).hasSize(2);
        assertThat(items).allSatisfy(item -> assertThat(item.status()).isEqualTo("EXPIRED"));

        // 3. Seats are AVAILABLE with version incremented from 1 -> 2
        SeatRecord s1After = seatRepository.findById(seat1).orElseThrow();
        SeatRecord s2After = seatRepository.findById(seat2).orElseThrow();
        assertThat(s1After.status()).isEqualTo("AVAILABLE");
        assertThat(s1After.version()).isEqualTo(2L);
        assertThat(s2After.status()).isEqualTo("AVAILABLE");
        assertThat(s2After.version()).isEqualTo(2L);

        // 4. Exactly one inventory.reservation.expired outbox row with correct key and payload
        List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList("SELECT * FROM outbox_events");
        assertThat(outboxRows).hasSize(1);
        Map<String, Object> outboxRow = outboxRows.getFirst();
        assertThat(outboxRow.get("aggregate_type")).isEqualTo("RESERVATION");
        assertThat(outboxRow.get("aggregate_id")).isEqualTo(reservationId);
        assertThat(outboxRow.get("topic")).isEqualTo(Topics.INVENTORY_EVENTS);
        assertThat(outboxRow.get("event_type")).isEqualTo(EventTypes.INVENTORY_RESERVATION_EXPIRED);
        assertThat(outboxRow.get("schema_version")).isEqualTo(1);
        assertThat(outboxRow.get("correlation_id")).isNotNull();
        assertThat(outboxRow.get("published_at")).isNull();

        ReservationExpiredPayload payload = objectMapper.readValue(
                outboxRow.get("payload").toString(), ReservationExpiredPayload.class);
        assertThat(payload.reservationId()).isEqualTo(reservationId);
        assertThat(payload.orderId()).isEqualTo(reservationId);
        assertThat(payload.userId()).isEqualTo(userId);
        assertThat(payload.eventId()).isEqualTo(eventId);
        assertThat(payload.seatIds()).containsExactlyInAnyOrder(seat1, seat2);
        assertThat(payload.expiredAt()).isEqualTo(updatedRes.expiredAt());

        // 5. Redis seat keys are deleted
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat1))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat2))).isFalse();

        // 6. Metrics incremented
        assertThat(counterValue(ExpirySweeper.METRIC_TICKETING_RESERVATIONS_EXPIRED))
                .isEqualTo(ticketingExpiredBefore + 1.0);
        assertThat(timerCount(ExpirySweeper.METRIC_SWEEP_DURATION))
                .isEqualTo(sweepTimerCountBefore + 1L);
    }

    // -------------------------------------------------------------------------
    // 3. A reservation that has not yet expired is untouched;
    //    CONFIRMED and RELEASED reservations are untouched
    // -------------------------------------------------------------------------

    @Test
    void nonExpiredHeldAndConfirmedAndReleasedReservationsAreUntouched() {
        UUID eventId = UUID.randomUUID();
        UUID activeSeat = UUID.randomUUID();
        UUID confirmedSeat = UUID.randomUUID();
        UUID releasedSeat = UUID.randomUUID();

        UUID activeResId = UUID.randomUUID();
        UUID confirmedResId = UUID.randomUUID();
        UUID releasedResId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(activeSeat, eventId, "SEC-A", "1", "1", 10000L, "HELD", 1L),
                seatRecord(confirmedSeat, eventId, "SEC-A", "1", "2", 10000L, "SOLD", 2L),
                seatRecord(releasedSeat, eventId, "SEC-A", "1", "3", 10000L, "AVAILABLE", 2L)
        ));

        // 1. HELD reservation with expires_at in the future (+300s)
        seedReservationInDbAndRedis(activeResId, eventId, UUID.randomUUID(), List.of(activeSeat), "HELD", 300);

        // 2. CONFIRMED reservation with expires_at in the past (-60s)
        seedReservationInDbAndRedis(confirmedResId, eventId, UUID.randomUUID(), List.of(confirmedSeat), "CONFIRMED", -60);
        stringRedisTemplate.opsForValue().set(RedisKeys.seatKey(eventId, confirmedSeat), RedisKeys.SOLD_MARKER);

        // 3. RELEASED reservation with expires_at in the past (-60s)
        seedReservationInDbAndRedis(releasedResId, eventId, UUID.randomUUID(), List.of(releasedSeat), "RELEASED", -60);
        stringRedisTemplate.delete(RedisKeys.seatKey(eventId, releasedSeat));

        int expiredCount = expirySweeper.sweep();
        assertThat(expiredCount).isZero();

        // Also call expireReservation directly on each to verify re-check guard
        assertThat(expirySweeper.expireReservation(activeResId)).isFalse();
        assertThat(expirySweeper.expireReservation(confirmedResId)).isFalse();
        assertThat(expirySweeper.expireReservation(releasedResId)).isFalse();

        assertThat(reservationRepository.findById(activeResId).orElseThrow().status()).isEqualTo("HELD");
        assertThat(reservationRepository.findById(confirmedResId).orElseThrow().status()).isEqualTo("CONFIRMED");
        assertThat(reservationRepository.findById(releasedResId).orElseThrow().status()).isEqualTo("RELEASED");

        assertThat(seatRepository.findById(activeSeat).orElseThrow().status()).isEqualTo("HELD");
        assertThat(seatRepository.findById(activeSeat).orElseThrow().version()).isEqualTo(1L);
        assertThat(seatRepository.findById(confirmedSeat).orElseThrow().status()).isEqualTo("SOLD");
        assertThat(seatRepository.findById(confirmedSeat).orElseThrow().version()).isEqualTo(2L);
        assertThat(seatRepository.findById(releasedSeat).orElseThrow().status()).isEqualTo("AVAILABLE");
        assertThat(seatRepository.findById(releasedSeat).orElseThrow().version()).isEqualTo(2L);

        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, activeSeat)))
                .isEqualTo(activeResId.toString());
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, confirmedSeat)))
                .isEqualTo(RedisKeys.SOLD_MARKER);

        Integer outboxCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Integer.class);
        assertThat(outboxCount).isZero();
    }

    // -------------------------------------------------------------------------
    // 4. After expiry the same seats can be held again through the hold endpoint
    // -------------------------------------------------------------------------

    @Test
    void afterExpirySameSeatsCanBeHeldAgainThroughHoldEndpoint() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID user1 = UUID.randomUUID();
        UUID user2 = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "FLOOR", "1", "10", 20000L, "AVAILABLE", 0L),
                seatRecord(seat2, eventId, "FLOOR", "1", "11", 20000L, "AVAILABLE", 0L)
        ));

        CreateReservationRequest holdRequest = new CreateReservationRequest(eventId, List.of(seat1, seat2));

        // 1. User 1 holds seat1 and seat2 via POST /api/v1/reservations
        MvcResult firstHoldResult = mockMvc.perform(post("/api/v1/reservations")
                        .header(CorrelationConstants.USER_ID_HEADER, user1.toString())
                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(holdRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("HELD")))
                .andReturn();

        ReservationResponse firstResponse = objectMapper.readValue(
                firstHoldResult.getResponse().getContentAsString(), ReservationResponse.class);
        UUID firstReservationId = firstResponse.reservationId();

        // User 2 cannot hold while User 1's hold is active
        mockMvc.perform(post("/api/v1/reservations")
                        .header(CorrelationConstants.USER_ID_HEADER, user2.toString())
                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(holdRequest)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));

        // 2. Move firstReservationId's expires_at into the past via SQL and run sweep
        jdbcTemplate.update("""
                UPDATE reservations
                SET created_at = NOW() - INTERVAL '610 seconds',
                    expires_at = NOW() - INTERVAL '10 seconds'
                WHERE reservation_id = ?
                """, firstReservationId);

        assertThat(expirySweeper.sweep()).isEqualTo(1);
        assertThat(reservationRepository.findById(firstReservationId).orElseThrow().status()).isEqualTo("EXPIRED");

        // 3. User 2 now holds the exact same seats through POST /api/v1/reservations and gets 201 Created
        MvcResult secondHoldResult = mockMvc.perform(post("/api/v1/reservations")
                        .header(CorrelationConstants.USER_ID_HEADER, user2.toString())
                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(holdRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("HELD")))
                .andExpect(jsonPath("$.userId", is(user2.toString())))
                .andReturn();

        ReservationResponse secondResponse = objectMapper.readValue(
                secondHoldResult.getResponse().getContentAsString(), ReservationResponse.class);
        UUID secondReservationId = secondResponse.reservationId();
        assertThat(secondReservationId).isNotEqualTo(firstReservationId);

        // Seat versions progressed: 0 -> 1 (hold 1) -> 2 (expired) -> 3 (hold 2)
        assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("HELD");
        assertThat(seatRepository.findById(seat1).orElseThrow().version()).isEqualTo(3L);
        assertThat(seatRepository.findById(seat2).orElseThrow().status()).isEqualTo("HELD");
        assertThat(seatRepository.findById(seat2).orElseThrow().version()).isEqualTo(3L);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1)))
                .isEqualTo(secondReservationId.toString());
    }

    // -------------------------------------------------------------------------
    // 5. Two sweeper instances running concurrently over 200 expired reservations
    //    produce exactly one outbox row per reservation and no failures
    // -------------------------------------------------------------------------

    @Test
    void twoSweeperInstancesRunningConcurrentlyOver200ExpiredReservationsProduceOneOutboxRowEachAndNoFailures()
            throws Exception {
        int totalReservations = 200;
        UUID eventId = UUID.randomUUID();

        List<SeatRecord> seats = new ArrayList<>(totalReservations);
        List<UUID> seatIds = new ArrayList<>(totalReservations);
        for (int i = 0; i < totalReservations; i++) {
            UUID seatId = UUID.randomUUID();
            seatIds.add(seatId);
            seats.add(seatRecord(seatId, eventId, "SEC-CONC", "1", String.valueOf(i + 1), 10000L, "HELD", 1L));
        }
        seedEventAndSeats(eventId, seats);

        List<UUID> reservationIds = new ArrayList<>(totalReservations);
        for (int i = 0; i < totalReservations; i++) {
            UUID resId = UUID.randomUUID();
            reservationIds.add(resId);
            seedReservationInDbAndRedis(resId, eventId, UUID.randomUUID(), List.of(seatIds.get(i)), "HELD", -(totalReservations - i));
        }

        ExpirySweeper sweeper1 = new ExpirySweeper(
                reservationRepository, seatRepository, redisSeatGate, outboxWriter,
                transactionManager, sweeperProperties, meterRegistryProvider);
        ExpirySweeper sweeper2 = new ExpirySweeper(
                reservationRepository, seatRepository, redisSeatGate, outboxWriter,
                transactionManager, sweeperProperties, meterRegistryProvider);

        double failuresBefore = counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Integer> f1 = pool.submit(() -> {
                ready.countDown();
                start.await();
                int count = 0;
                while (true) {
                    int c = sweeper1.sweep();
                    count += c;
                    if (c == 0) {
                        break;
                    }
                }
                return count;
            });
            Future<Integer> f2 = pool.submit(() -> {
                ready.countDown();
                start.await();
                int count = 0;
                while (true) {
                    int c = sweeper2.sweep();
                    count += c;
                    if (c == 0) {
                        break;
                    }
                }
                return count;
            });

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            int expiredBy1 = f1.get(30, TimeUnit.SECONDS);
            int expiredBy2 = f2.get(30, TimeUnit.SECONDS);
            assertThat(expiredBy1 + expiredBy2).isEqualTo(totalReservations);
        } finally {
            pool.shutdownNow();
        }

        // Zero sweep failures
        assertThat(counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES)).isEqualTo(failuresBefore);

        // All 200 reservations are EXPIRED
        Integer expiredResCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservations WHERE status = 'EXPIRED' AND expired_at IS NOT NULL",
                Integer.class);
        assertThat(expiredResCount).isEqualTo(totalReservations);

        // All 200 reservation_items are EXPIRED
        Integer expiredItemCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservation_items WHERE status = 'EXPIRED'",
                Integer.class);
        assertThat(expiredItemCount).isEqualTo(totalReservations);

        // All 200 seats are AVAILABLE with version = 2
        Integer availableSeatCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM seats WHERE event_id = ? AND status = 'AVAILABLE' AND version = 2",
                Integer.class, eventId);
        assertThat(availableSeatCount).isEqualTo(totalReservations);

        // Exactly 1 outbox row per reservation (200 total, 200 distinct aggregate_id)
        Integer totalOutboxRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE event_type = ?",
                Integer.class, EventTypes.INVENTORY_RESERVATION_EXPIRED);
        Integer distinctOutboxAggregates = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT aggregate_id) FROM outbox_events WHERE event_type = ?",
                Integer.class, EventTypes.INVENTORY_RESERVATION_EXPIRED);
        assertThat(totalOutboxRows).isEqualTo(totalReservations);
        assertThat(distinctOutboxAggregates).isEqualTo(totalReservations);

        // All Redis seat keys deleted
        for (UUID seatId : seatIds) {
            assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seatId))).isFalse();
        }
    }

    // -------------------------------------------------------------------------
    // 6. Concurrent confirm and expire on the same reservation ends with
    //    exactly one winner and consistent seats, items and outbox rows
    // -------------------------------------------------------------------------

    @Test
    void concurrentConfirmAndExpireOnSameReservationEndsWithExactlyOneWinnerAndConsistentState() throws Exception {
        int trials = 20;
        UUID eventId = UUID.randomUUID();

        List<UUID> seatIds = new ArrayList<>(trials);
        List<SeatRecord> seats = new ArrayList<>(trials);
        for (int i = 0; i < trials; i++) {
            UUID seatId = UUID.randomUUID();
            seatIds.add(seatId);
            seats.add(seatRecord(seatId, eventId, "SEC-RACE", "1", String.valueOf(i + 1), 12000L, "HELD", 1L));
        }
        seedEventAndSeats(eventId, seats);

        List<UUID> reservationIds = new ArrayList<>(trials);
        for (int i = 0; i < trials; i++) {
            UUID resId = UUID.randomUUID();
            reservationIds.add(resId);
            seedReservationInDbAndRedis(resId, eventId, UUID.randomUUID(), List.of(seatIds.get(i)), "HELD", -5);
        }

        TransactionTemplate requiresNewTx = new TransactionTemplate(transactionManager);
        requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < trials; i++) {
                final UUID resId = reservationIds.get(i);
                final UUID seatId = seatIds.get(i);
                final UUID paymentId = UUID.randomUUID();
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);

                // Deterministically exercise both lock orderings on the first two iterations,
                // and run unthrottled races on the remaining iterations.
                final int trialIndex = i;
                CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
                if (trialIndex == 0) {
                    // Trial 0: expire acquires the row lock first; confirm starts while expire holds the lock
                    expirySweeper.setAfterReservationLockHook(lockAcquiredLatch::countDown);
                } else {
                    expirySweeper.setAfterReservationLockHook(null);
                }

                Future<Boolean> expireFuture = pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    if (trialIndex == 1) {
                        lockAcquiredLatch.await(5, TimeUnit.SECONDS);
                    }
                    return expirySweeper.expireReservation(resId);
                });

                Future<Boolean> confirmFuture = pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    if (trialIndex == 0) {
                        lockAcquiredLatch.await(5, TimeUnit.SECONDS);
                    }
                    Optional<ReservationRepository.ConfirmedReservation> confirmedOpt =
                            requiresNewTx.execute(status -> {
                                Optional<ReservationRepository.ConfirmedReservation> c =
                                        reservationRepository.confirmIfHeld(resId);
                                if (trialIndex == 1) {
                                    lockAcquiredLatch.countDown();
                                }
                                if (c.isPresent()) {
                                    ReservationRepository.ConfirmedReservation cr = c.get();
                                    outboxWriter.append(
                                            resId,
                                            EventTypes.INVENTORY_SEAT_CONFIRMED,
                                            new SeatConfirmedPayload(
                                                    resId,
                                                    resId,
                                                    paymentId,
                                                    cr.eventId(),
                                                    cr.seatIds(),
                                                    cr.confirmedAt()
                                            )
                                    );
                                }
                                return c;
                            });
                    if (confirmedOpt != null && confirmedOpt.isPresent()) {
                        redisSeatGate.confirm(eventId, List.of(seatId), resId);
                        return true;
                    }
                    return false;
                });

                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();

                boolean expireWon = expireFuture.get(15, TimeUnit.SECONDS);
                boolean confirmWon = confirmFuture.get(15, TimeUnit.SECONDS);

                // Strictly one winner (XOR)
                assertThat(expireWon ^ confirmWon)
                        .as("Trial %d for reservation %s: expireWon=%s, confirmWon=%s", i, resId, expireWon, confirmWon)
                        .isTrue();
                if (trialIndex == 0) {
                    assertThat(expireWon).isTrue();
                } else if (trialIndex == 1) {
                    assertThat(confirmWon).isTrue();
                }

                ReservationRecord finalRes = reservationRepository.findById(resId).orElseThrow();
                List<ReservationItemRecord> finalItems = reservationRepository.findItemsByReservationId(resId);
                SeatRecord finalSeat = seatRepository.findById(seatId).orElseThrow();
                List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList(
                        "SELECT * FROM outbox_events WHERE aggregate_id = ?", resId);

                assertThat(outboxRows).hasSize(1);
                assertThat(finalSeat.version()).isEqualTo(2L);

                if (expireWon) {
                    assertThat(finalRes.status()).isEqualTo("EXPIRED");
                    assertThat(finalRes.expiredAt()).isNotNull();
                    assertThat(finalRes.confirmedAt()).isNull();
                    assertThat(finalItems).allSatisfy(item -> assertThat(item.status()).isEqualTo("EXPIRED"));
                    assertThat(finalSeat.status()).isEqualTo("AVAILABLE");
                    assertThat(outboxRows.getFirst().get("event_type"))
                            .isEqualTo(EventTypes.INVENTORY_RESERVATION_EXPIRED);
                    assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seatId))).isFalse();
                } else {
                    assertThat(finalRes.status()).isEqualTo("CONFIRMED");
                    assertThat(finalRes.confirmedAt()).isNotNull();
                    assertThat(finalRes.expiredAt()).isNull();
                    assertThat(finalItems).allSatisfy(item -> assertThat(item.status()).isEqualTo("CONFIRMED"));
                    assertThat(finalSeat.status()).isEqualTo("SOLD");
                    assertThat(outboxRows.getFirst().get("event_type"))
                            .isEqualTo(EventTypes.INVENTORY_SEAT_CONFIRMED);
                    assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId)))
                            .isEqualTo(RedisKeys.SOLD_MARKER);
                    assertThat(stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seatId))).isEqualTo(-1L);
                }
            }
        } finally {
            expirySweeper.setAfterReservationLockHook(null);
            pool.shutdownNow();
        }
    }

    // -------------------------------------------------------------------------
    // 7. A Redis outage during release still leaves PostgreSQL expired
    //    and does not throw out of the sweep
    // -------------------------------------------------------------------------

    @Test
    void redisOutageDuringReleaseStillLeavesPostgreSqlExpiredAndDoesNotThrowOutOfSweep() {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seatId, eventId, "SEC-OUTAGE", "1", "1", 10000L, "HELD", 1L)
        ));
        seedReservationInDbAndRedis(reservationId, eventId, UUID.randomUUID(), List.of(seatId), "HELD", -20);

        // Build a RedisSeatGate pointing to an unreachable Redis port to simulate a Redis outage during release
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(100))
                .build();
        LettuceConnectionFactory brokenFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("localhost", 1), clientConfig);
        brokenFactory.afterPropertiesSet();
        try {
            StringRedisTemplate brokenRedisTemplate = new StringRedisTemplate(brokenFactory);
            RedisSeatGate brokenGate = new RedisSeatGate(
                    brokenRedisTemplate,
                    holdInventoryScript,
                    releaseInventoryScript,
                    confirmInventoryScript,
                    redisProperties,
                    clock,
                    meterRegistryProvider
            );

            ExpirySweeper sweeperWithBrokenRedis = new ExpirySweeper(
                    reservationRepository,
                    seatRepository,
                    brokenGate,
                    outboxWriter,
                    transactionManager,
                    sweeperProperties,
                    meterRegistryProvider
            );

            double failuresBefore = counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES);

            // Must not throw even though Redis release fails
            int expiredCount = sweeperWithBrokenRedis.sweep();
            assertThat(expiredCount).isEqualTo(1);
            assertThat(counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES)).isEqualTo(failuresBefore);

            // PostgreSQL state is committed as EXPIRED with outbox row
            ReservationRecord resAfter = reservationRepository.findById(reservationId).orElseThrow();
            assertThat(resAfter.status()).isEqualTo("EXPIRED");
            assertThat(resAfter.expiredAt()).isNotNull();
            assertThat(reservationRepository.findItemsByReservationId(reservationId))
                    .allSatisfy(item -> assertThat(item.status()).isEqualTo("EXPIRED"));
            SeatRecord seatAfter = seatRepository.findById(seatId).orElseThrow();
            assertThat(seatAfter.status()).isEqualTo("AVAILABLE");
            assertThat(seatAfter.version()).isEqualTo(2L);

            Integer outboxCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                    Integer.class, reservationId, EventTypes.INVENTORY_RESERVATION_EXPIRED);
            assertThat(outboxCount).isEqualTo(1);
        } finally {
            brokenFactory.destroy();
        }
    }

    // -------------------------------------------------------------------------
    // 8. One reservation whose seat update count mismatches rolls back only
    //    itself while the others in the batch succeed
    // -------------------------------------------------------------------------

    @Test
    void oneReservationWithSeatUpdateCountMismatchRollsBackOnlyItselfWhileOthersInBatchSucceed() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2a = UUID.randomUUID();
        UUID seat2b = UUID.randomUUID();
        UUID seat3 = UUID.randomUUID();

        UUID res1 = UUID.randomUUID();
        UUID res2Bad = UUID.randomUUID();
        UUID res3 = UUID.randomUUID();

        // seat2b is already AVAILABLE in seats table (mismatch with res2Bad's HELD item for seat2b!)
        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "SEC-B", "1", "1", 10000L, "HELD", 1L),
                seatRecord(seat2a, eventId, "SEC-B", "1", "2", 10000L, "HELD", 1L),
                seatRecord(seat2b, eventId, "SEC-B", "1", "3", 10000L, "AVAILABLE", 5L),
                seatRecord(seat3, eventId, "SEC-B", "1", "4", 10000L, "HELD", 1L)
        ));

        seedReservationInDbAndRedis(res1, eventId, UUID.randomUUID(), List.of(seat1), "HELD", -30);
        seedReservationInDbAndRedis(res2Bad, eventId, UUID.randomUUID(), List.of(seat2a, seat2b), "HELD", -20);
        seedReservationInDbAndRedis(res3, eventId, UUID.randomUUID(), List.of(seat3), "HELD", -10);

        double failuresBefore = counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES);

        int expiredCount = expirySweeper.sweep();
        assertThat(expiredCount).isEqualTo(2);
        assertThat(counterValue(ExpirySweeper.METRIC_SWEEP_FAILURES)).isEqualTo(failuresBefore + 1.0);

        // res1 and res3 succeeded
        assertThat(reservationRepository.findById(res1).orElseThrow().status()).isEqualTo("EXPIRED");
        assertThat(reservationRepository.findById(res3).orElseThrow().status()).isEqualTo("EXPIRED");
        assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("AVAILABLE");
        assertThat(seatRepository.findById(seat1).orElseThrow().version()).isEqualTo(2L);
        assertThat(seatRepository.findById(seat3).orElseThrow().status()).isEqualTo("AVAILABLE");
        assertThat(seatRepository.findById(seat3).orElseThrow().version()).isEqualTo(2L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat1))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seat3))).isFalse();

        // res2Bad rolled back completely: reservation, items, seat2a, Redis, and outbox are all unchanged
        ReservationRecord badResAfter = reservationRepository.findById(res2Bad).orElseThrow();
        assertThat(badResAfter.status()).isEqualTo("HELD");
        assertThat(badResAfter.expiredAt()).isNull();
        assertThat(reservationRepository.findItemsByReservationId(res2Bad))
                .hasSize(2)
                .allSatisfy(item -> assertThat(item.status()).isEqualTo("HELD"));
        SeatRecord seat2aAfter = seatRepository.findById(seat2a).orElseThrow();
        assertThat(seat2aAfter.status()).isEqualTo("HELD");
        assertThat(seat2aAfter.version()).isEqualTo(1L);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2a)))
                .isEqualTo(res2Bad.toString());

        Integer res2OutboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?",
                Integer.class, res2Bad);
        assertThat(res2OutboxCount).isZero();

        Integer totalOutboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE event_type = ?",
                Integer.class, EventTypes.INVENTORY_RESERVATION_EXPIRED);
        assertThat(totalOutboxCount).isEqualTo(2);
    }

    // -------------------------------------------------------------------------
    // 9. The batch limit of 500 is respected across multiple sweeps
    // -------------------------------------------------------------------------

    @Test
    void batchLimitOf500IsRespectedAcrossMultipleSweeps() {
        int totalExpired = 520;
        UUID eventId = UUID.randomUUID();

        List<SeatRecord> seats = new ArrayList<>(totalExpired);
        List<UUID> seatIds = new ArrayList<>(totalExpired);
        for (int i = 0; i < totalExpired; i++) {
            UUID seatId = UUID.randomUUID();
            seatIds.add(seatId);
            seats.add(seatRecord(seatId, eventId, "SEC-500", "1", String.valueOf(i + 1), 10000L, "HELD", 1L));
        }
        seedEventAndSeats(eventId, seats);

        List<UUID> reservationIds = new ArrayList<>(totalExpired);
        for (int i = 0; i < totalExpired; i++) {
            UUID resId = UUID.randomUUID();
            reservationIds.add(resId);
            // Earliest 500 have expiresOffsetSeconds -525..-26; latest 20 have -25..-6
            seedReservationInDbOnPgOnly(resId, eventId, UUID.randomUUID(), List.of(seatIds.get(i)), "HELD", -(525 - i));
        }

        // First sweep processes exactly 500 (the 500 with earliest expires_at)
        int firstSweepCount = expirySweeper.sweep();
        assertThat(firstSweepCount).isEqualTo(500);

        Integer expiredAfterFirst = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservations WHERE status = 'EXPIRED'", Integer.class);
        Integer heldAfterFirst = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservations WHERE status = 'HELD'", Integer.class);
        assertThat(expiredAfterFirst).isEqualTo(500);
        assertThat(heldAfterFirst).isEqualTo(20);

        // Verify the 20 remaining HELD reservations are the ones with the latest expires_at (indices 500..519)
        for (int i = 500; i < totalExpired; i++) {
            assertThat(reservationRepository.findById(reservationIds.get(i)).orElseThrow().status())
                    .isEqualTo("HELD");
        }

        // Second sweep processes the remaining 20
        int secondSweepCount = expirySweeper.sweep();
        assertThat(secondSweepCount).isEqualTo(20);

        Integer expiredAfterSecond = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservations WHERE status = 'EXPIRED'", Integer.class);
        Integer heldAfterSecond = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reservations WHERE status = 'HELD'", Integer.class);
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE event_type = ?",
                Integer.class, EventTypes.INVENTORY_RESERVATION_EXPIRED);
        assertThat(expiredAfterSecond).isEqualTo(520);
        assertThat(heldAfterSecond).isZero();
        assertThat(outboxCount).isEqualTo(520);

        // Third sweep is a no-op
        assertThat(expirySweeper.sweep()).isZero();
    }

    // -------------------------------------------------------------------------
    // 10. Sweeping an event with a different reservation holding a seat in Redis
    //     never releases the other reservation's key
    // -------------------------------------------------------------------------

    @Test
    void sweepingEventWithDifferentReservationHoldingSeatInRedisNeverReleasesOtherReservationsKey() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID expiredResId = UUID.randomUUID();
        UUID otherResId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "SEC-TOKEN", "1", "1", 10000L, "HELD", 1L),
                seatRecord(seat2, eventId, "SEC-TOKEN", "1", "2", 10000L, "HELD", 1L)
        ));

        // expiredResId is expired in PostgreSQL on seat1
        seedReservationInDbAndRedis(expiredResId, eventId, UUID.randomUUID(), List.of(seat1), "HELD", -15);
        // otherResId is active in PostgreSQL on seat2
        seedReservationInDbAndRedis(otherResId, eventId, UUID.randomUUID(), List.of(seat2), "HELD", 300);

        // Overwrite seat1's Redis key with otherResId's token to verify compare-and-delete token check
        stringRedisTemplate.opsForValue().set(
                RedisKeys.seatKey(eventId, seat1),
                otherResId.toString(),
                Duration.ofSeconds(660)
        );

        int expiredCount = expirySweeper.sweep();
        assertThat(expiredCount).isEqualTo(1);

        // expiredResId is expired in PostgreSQL
        assertThat(reservationRepository.findById(expiredResId).orElseThrow().status()).isEqualTo("EXPIRED");
        // otherResId is still HELD in PostgreSQL
        assertThat(reservationRepository.findById(otherResId).orElseThrow().status()).isEqualTo("HELD");

        // Neither seat1 (holding otherResId's token) nor seat2 (holding otherResId's token) was deleted in Redis!
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1)))
                .isEqualTo(otherResId.toString());
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2)))
                .isEqualTo(otherResId.toString());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void seedEventAndSeats(UUID eventId, List<SeatRecord> seats) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant dropTime = now.minus(2, ChronoUnit.HOURS);
        Instant eventTime = now.plus(7, ChronoUnit.DAYS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Test Event", "Test Arena", eventTime, dropTime, "ON_SALE", now, now));
            seatRepository.bulkInsert(seats);
        });

        stringRedisTemplate.opsForValue().set(
                RedisKeys.eventDropTsKey(eventId),
                String.valueOf(dropTime.toEpochMilli()),
                Duration.ofSeconds(RedisKeys.EVENT_DROP_TS_TTL_SECONDS)
        );
    }

    private SeatRecord seatRecord(
            UUID seatId,
            UUID eventId,
            String section,
            String row,
            String number,
            long priceCents,
            String status,
            long version) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        return new SeatRecord(seatId, eventId, section, row, number, priceCents, "USD", status, version, now, now);
    }

    private void seedReservationInDbAndRedis(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            List<UUID> seatIds,
            String status,
            long expiresOffsetSeconds) {
        seedReservationInDbOnPgOnly(reservationId, eventId, userId, seatIds, status, expiresOffsetSeconds);
        for (UUID seatId : seatIds) {
            stringRedisTemplate.opsForValue().set(
                    RedisKeys.seatKey(eventId, seatId),
                    reservationId.toString(),
                    Duration.ofSeconds(660)
            );
        }
    }

    private void seedReservationInDbOnPgOnly(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            List<UUID> seatIds,
            String status,
            long expiresOffsetSeconds) {
        long createdOffsetSeconds = expiresOffsetSeconds - 600L;
        transactionTemplate.executeWithoutResult(tx -> {
            jdbcTemplate.update("""
                    INSERT INTO reservations (
                        reservation_id, event_id, user_id, status, total_amount_cents, currency,
                        created_at, expires_at, updated_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, 'USD',
                        NOW() + (? * INTERVAL '1 second'),
                        NOW() + (? * INTERVAL '1 second'),
                        NOW()
                    )
                    """,
                    new Object[]{
                            reservationId,
                            eventId,
                            userId,
                            status,
                            10000L * seatIds.size(),
                            createdOffsetSeconds,
                            expiresOffsetSeconds
                    },
                    new int[]{
                            Types.OTHER,
                            Types.OTHER,
                            Types.OTHER,
                            Types.VARCHAR,
                            Types.BIGINT,
                            Types.BIGINT,
                            Types.BIGINT
                    }
            );

            for (int i = 0; i < seatIds.size(); i++) {
                jdbcTemplate.update("""
                        INSERT INTO reservation_items (
                            reservation_item_id, reservation_id, event_id, seat_id,
                            section_code, row_label, seat_number, price_cents, status,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'SEC', '1', ?, 10000, ?, NOW(), NOW())
                        """,
                        new Object[]{
                                UUID.randomUUID(),
                                reservationId,
                                eventId,
                                seatIds.get(i),
                                String.valueOf(i + 1),
                                status
                        },
                        new int[]{
                                Types.OTHER,
                                Types.OTHER,
                                Types.OTHER,
                                Types.OTHER,
                                Types.VARCHAR,
                                Types.VARCHAR
                        }
                );
            }
        });
    }

    @Test
    void lockHookTestWhileTransactionHoldsRowLockExpireReservationReturnsSkippedWithoutBlockingAndLaterSweepExpiresIt()
            throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "SEC-A", "1", "101", 15000L, "HELD", 1L)
        ));
        seedReservationInDbAndRedis(reservationId, eventId, userId, List.of(seat1), "HELD", -30);

        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        ExecutorService lockHolderPool = Executors.newSingleThreadExecutor();
        ExecutorService sweeperPool = Executors.newSingleThreadExecutor();

        try {
            // Thread 1: Acquire exclusive row lock on the reservation
            Future<?> lockHolderFuture = lockHolderPool.submit(() -> {
                transactionTemplate.executeWithoutResult(status -> {
                    jdbcTemplate.queryForList(
                            "SELECT reservation_id FROM reservations WHERE reservation_id = ? FOR UPDATE",
                            reservationId);
                    lockAcquired.countDown();
                    try {
                        assertThat(releaseLock.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            assertThat(lockAcquired.await(5, TimeUnit.SECONDS)).isTrue();

            // Thread 2: expireReservation on another thread must return skipped (false) within 2 seconds
            long startNanos = System.nanoTime();
            Future<Boolean> expireFuture = sweeperPool.submit(() -> expirySweeper.expireReservation(reservationId));
            Boolean expired = expireFuture.get(2, TimeUnit.SECONDS);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertThat(expired).isFalse();
            assertThat(elapsedMillis).isLessThan(2000L);

            // Reservation is still HELD
            ReservationRecord stillHeld = reservationRepository.findById(reservationId).orElseThrow();
            assertThat(stillHeld.status()).isEqualTo("HELD");

            // Release the lock
            releaseLock.countDown();
            lockHolderFuture.get(5, TimeUnit.SECONDS);

            // Later sweep expires it
            int swept = expirySweeper.sweep();
            assertThat(swept).isEqualTo(1);

            ReservationRecord expiredRes = reservationRepository.findById(reservationId).orElseThrow();
            assertThat(expiredRes.status()).isEqualTo("EXPIRED");
            assertThat(expiredRes.expiredAt()).isNotNull();
            assertThat(expiredRes.expiredAt().compareTo(expiredRes.expiresAt())).isGreaterThanOrEqualTo(0);

            SeatRecord seatAfter = seatRepository.findById(seat1).orElseThrow();
            assertThat(seatAfter.status()).isEqualTo("AVAILABLE");
            assertThat(seatAfter.version()).isEqualTo(2L);
        } finally {
            releaseLock.countDown();
            lockHolderPool.shutdownNow();
            sweeperPool.shutdownNow();
        }
    }

    @Test
    void scheduledBatchTestDrainsBacklogAndRespectsMaxPagesPerTick() {
        boolean originalEnabled = sweeperProperties.isEnabled();
        int originalBatchSize = sweeperProperties.getBatchSize();
        int originalMaxPages = sweeperProperties.getMaxPagesPerTick();
        sweeperProperties.setEnabled(true);
        try {
            // Part 1: 1200 expired reservations are all expired by a single call of scheduledSweep()
            UUID eventId1 = UUID.randomUUID();
            transactionTemplate.executeWithoutResult(status -> {
                eventRepository.insert(new EventRecord(
                        eventId1, "Big Event 1", "Venue",
                        Instant.now().plus(7, ChronoUnit.DAYS), Instant.now().minus(2, ChronoUnit.HOURS),
                        "ON_SALE", Instant.now(), Instant.now()));
            });
            bulkSeedExpiredReservations(eventId1, 1200);

            int expiredTotal = expirySweeper.scheduledSweep();
            assertThat(expiredTotal).isEqualTo(1200);

            Integer expiredInDb = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM reservations WHERE status = 'EXPIRED'", Integer.class);
            assertThat(expiredInDb).isEqualTo(1200);

            // Part 2: with batch size 10 and max pages 3, one tick expires exactly 30
            sweeperProperties.setBatchSize(10);
            sweeperProperties.setMaxPagesPerTick(3);

            UUID eventId2 = UUID.randomUUID();
            transactionTemplate.executeWithoutResult(status -> {
                eventRepository.insert(new EventRecord(
                        eventId2, "Small Event 2", "Venue",
                        Instant.now().plus(7, ChronoUnit.DAYS), Instant.now().minus(2, ChronoUnit.HOURS),
                        "ON_SALE", Instant.now(), Instant.now()));
            });
            bulkSeedExpiredReservations(eventId2, 50);

            int expiredTick = expirySweeper.scheduledSweep();
            assertThat(expiredTick).isEqualTo(30);

            // 30 expired from event2, leaving 20 HELD
            Integer heldRemaining = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM reservations WHERE event_id = ? AND status = 'HELD'",
                    Integer.class, eventId2);
            assertThat(heldRemaining).isEqualTo(20);
        } finally {
            sweeperProperties.setEnabled(originalEnabled);
            sweeperProperties.setBatchSize(originalBatchSize);
            sweeperProperties.setMaxPagesPerTick(originalMaxPages);
        }
    }

    @Test
    void metricsTestOnlyTicketingExpiredCounterExistsAndGaugeReadsFromMemoryWithoutQueryingRepository() {
        // 1. Assert only ticketing_reservations_expired_total exists and increments
        assertThat(meterRegistry.find("inventory_expired_reservations_total").counter()).isNull();
        Counter ticketingCounter = meterRegistry.find(ExpirySweeper.METRIC_TICKETING_RESERVATIONS_EXPIRED).counter();
        assertThat(ticketingCounter).isNotNull();

        double beforeCount = ticketingCounter.count();
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seatId, eventId, "SEC-M", "1", "1", 10000L, "HELD", 1L)
        ));
        seedReservationInDbAndRedis(reservationId, eventId, userId, List.of(seatId), "HELD", -10);

        int swept = expirySweeper.sweep();
        assertThat(swept).isEqualTo(1);
        assertThat(ticketingCounter.count()).isEqualTo(beforeCount + 1.0);

        // 2. Mockito spy on repository: verify gauge reads never query repository
        ReservationRepository rawRepo = new ReservationRepository(jdbcTemplate);
        ReservationRepository spyRepo = Mockito.spy(rawRepo);
        SimpleMeterRegistry testRegistry = new SimpleMeterRegistry();
        ExpirySweeperProperties testProps = new ExpirySweeperProperties();
        ExpirySweeper sweeperWithSpy = new ExpirySweeper(
                spyRepo, seatRepository, redisSeatGate, outboxWriter,
                transactionManager, testProps,
                new SimpleObjectProvider<>(testRegistry)
        );

        Gauge gauge = testRegistry.find(ExpirySweeper.METRIC_OLDEST_OVERDUE_HOLD).gauge();
        assertThat(gauge).isNotNull();

        // Reading gauge from registry must NEVER query repository
        verify(spyRepo, never()).queryOldestOverdueHoldSeconds();
        double gaugeValBefore = gauge.value();
        verify(spyRepo, never()).queryOldestOverdueHoldSeconds();
        assertThat(gaugeValBefore).isEqualTo(0.0);

        // After sweep, gauge is updated by sweep and returns a number
        sweeperWithSpy.sweep();
        verify(spyRepo, times(1)).queryOldestOverdueHoldSeconds();
        double gaugeValAfter = gauge.value();
        assertThat(gaugeValAfter).isGreaterThanOrEqualTo(0.0);
        // Reading again still does not invoke repository
        verify(spyRepo, times(1)).queryOldestOverdueHoldSeconds();

        // Idempotent registration: calling registerMetrics again keeps exactly 1 gauge
        sweeperWithSpy.registerMetrics(testRegistry);
        assertThat(testRegistry.find(ExpirySweeper.METRIC_OLDEST_OVERDUE_HOLD).gauges()).hasSize(1);
    }

    @Test
    void clockTestAssertingExpiredAtIsGreaterThanOrEqualToExpiresAtForSweptReservations() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        seedEventAndSeats(eventId, List.of(
                seatRecord(seat1, eventId, "SEC-CLK", "1", "101", 15000L, "HELD", 1L)
        ));
        seedReservationInDbAndRedis(reservationId, eventId, userId, List.of(seat1), "HELD", -10);

        int swept = expirySweeper.sweep();
        assertThat(swept).isEqualTo(1);

        ReservationRecord res = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(res.status()).isEqualTo("EXPIRED");
        assertThat(res.expiredAt()).isNotNull();
        assertThat(res.expiresAt()).isNotNull();
        assertThat(res.expiredAt().compareTo(res.expiresAt())).isGreaterThanOrEqualTo(0);
    }

    private List<UUID> bulkSeedExpiredReservations(UUID eventId, int count) {
        List<UUID> reservationIds = new ArrayList<>(count);
        List<UUID> seatIds = new ArrayList<>(count);
        List<SeatRecord> seats = new ArrayList<>(count);
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        for (int i = 0; i < count; i++) {
            UUID seatId = UUID.randomUUID();
            UUID resId = UUID.randomUUID();
            seatIds.add(seatId);
            reservationIds.add(resId);
            seats.add(new SeatRecord(seatId, eventId, "SEC-BLK", "1", String.valueOf(i + 1), 10000L, "USD", "HELD", 1L, now, now));
        }

        transactionTemplate.executeWithoutResult(status -> {
            seatRepository.bulkInsert(seats);

            jdbcTemplate.batchUpdate("""
                    INSERT INTO reservations (
                        reservation_id, event_id, user_id, status, total_amount_cents, currency,
                        created_at, expires_at, updated_at
                    ) VALUES (?, ?, ?, 'HELD', 10000, 'USD',
                              clock_timestamp() - INTERVAL '1 hour',
                              clock_timestamp() - (? * INTERVAL '1 millisecond' + INTERVAL '10 seconds'),
                              clock_timestamp())
                    """,
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setObject(1, reservationIds.get(i), Types.OTHER);
                            ps.setObject(2, eventId, Types.OTHER);
                            ps.setObject(3, UUID.randomUUID(), Types.OTHER);
                            ps.setLong(4, (count - i) * 10L);
                        }

                        @Override
                        public int getBatchSize() {
                            return count;
                        }
                    });

            jdbcTemplate.batchUpdate("""
                    INSERT INTO reservation_items (
                        reservation_item_id, reservation_id, event_id, seat_id, section_code,
                        row_label, seat_number, price_cents, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'SEC-BLK', '1', ?, 10000, 'HELD',
                              clock_timestamp() - INTERVAL '1 hour',
                              clock_timestamp())
                    """,
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setObject(1, UUID.randomUUID(), Types.OTHER);
                            ps.setObject(2, reservationIds.get(i), Types.OTHER);
                            ps.setObject(3, eventId, Types.OTHER);
                            ps.setObject(4, seatIds.get(i), Types.OTHER);
                            ps.setString(5, String.valueOf(i + 1));
                        }

                        @Override
                        public int getBatchSize() {
                            return count;
                        }
                    });
        });

        return reservationIds;
    }

    private static class SimpleObjectProvider<T> implements ObjectProvider<T> {
        private final T instance;
        SimpleObjectProvider(T instance) { this.instance = instance; }
        @Override public T getObject(Object... args) { return instance; }
        @Override public T getIfAvailable() { return instance; }
        @Override public T getIfUnique() { return instance; }
        @Override public T getObject() { return instance; }
    }

    private double counterValue(String name) {
        Counter counter = meterRegistry.find(name).counter();
        return counter != null ? counter.count() : 0.0;
    }

    private long timerCount(String name) {
        Timer timer = meterRegistry.find(name).timer();
        return timer != null ? timer.count() : 0L;
    }
}


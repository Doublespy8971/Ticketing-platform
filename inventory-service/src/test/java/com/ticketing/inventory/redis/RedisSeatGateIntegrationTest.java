package com.ticketing.inventory.redis;

import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
class RedisSeatGateIntegrationTest {

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
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisSeatGate redisSeatGate;

    @Autowired
    private RedisRewarmService redisRewarmService;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

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
    private InventoryRedisProperties redisProperties;

    @BeforeEach
    void cleanState() {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate.execute("TRUNCATE TABLE reservation_items, reservations, seats, events CASCADE");
        stringRedisTemplate.execute((RedisCallback<Void>) (RedisConnection connection) -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    void eventNotCachedAndEventNotOpenReturnDocumentedOutcomes() {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        // 1. Event drop_ts key is not cached in Redis
        HoldOutcome notCached = redisSeatGate.hold(eventId, List.of(seatId), reservationId, nowMs);
        assertThat(notCached).isEqualTo(HoldOutcome.EVENT_NOT_CACHED);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId))).isNull();

        // 2. Event drop_ts is in the future (nowMs < dropMs)
        cacheEventDropTime(eventId, nowMs + 60_000L);
        HoldOutcome notOpen = redisSeatGate.hold(eventId, List.of(seatId), reservationId, nowMs);
        assertThat(notOpen).isEqualTo(HoldOutcome.EVENT_NOT_OPEN);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId))).isNull();
    }

    @Test
    void seatTtlAfterHoldIsBetween655And660SecondsAndSecondReservationCannotTakeHeldSeat() {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID reservation1 = UUID.randomUUID();
        UUID reservation2 = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        cacheEventDropTime(eventId, nowMs - 1_000L);

        HoldOutcome firstHold = redisSeatGate.hold(eventId, List.of(seatId), reservation1, nowMs);
        assertThat(firstHold).isEqualTo(HoldOutcome.HELD);

        String seatKey = RedisKeys.seatKey(eventId, seatId);
        assertThat(stringRedisTemplate.opsForValue().get(seatKey)).isEqualTo(reservation1.toString());

        Long ttlSeconds = stringRedisTemplate.getExpire(seatKey, TimeUnit.SECONDS);
        assertThat(ttlSeconds).isNotNull().isBetween(655L, 660L);

        // Second reservation cannot take the held seat
        HoldOutcome secondHold = redisSeatGate.hold(eventId, List.of(seatId), reservation2, nowMs);
        assertThat(secondHold).isEqualTo(HoldOutcome.SEAT_TAKEN);
        assertThat(stringRedisTemplate.opsForValue().get(seatKey)).isEqualTo(reservation1.toString());
    }

    @Test
    void multiSeatHoldIsAllOrNothing() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID seat3 = UUID.randomUUID();
        UUID res1 = UUID.randomUUID();
        UUID res2 = UUID.randomUUID();
        UUID res3 = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        cacheEventDropTime(eventId, nowMs - 1_000L);

        // Hold seat2 first with res1
        assertThat(redisSeatGate.hold(eventId, List.of(seat2), res1, nowMs)).isEqualTo(HoldOutcome.HELD);

        // Attempt to hold [seat1, seat2, seat3] with res2 -> must fail and write nothing for seat1 or seat3
        HoldOutcome multiOutcome = redisSeatGate.hold(eventId, List.of(seat1, seat2, seat3), res2, nowMs);
        assertThat(multiOutcome).isEqualTo(HoldOutcome.SEAT_TAKEN);

        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isNull();
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat3))).isNull();
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2))).isEqualTo(res1.toString());

        // Holding [seat1, seat3] with res3 succeeds atomically
        HoldOutcome freeOutcome = redisSeatGate.hold(eventId, List.of(seat3, seat1), res3, nowMs);
        assertThat(freeOutcome).isEqualTo(HoldOutcome.HELD);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isEqualTo(res3.toString());
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat3))).isEqualTo(res3.toString());
    }

    @Test
    void twoHundredConcurrentThreadsRacingForSameSeatProduceExactlyOneWinner() throws Exception {
        int threadCount = 200;
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        cacheEventDropTime(eventId, nowMs - 5_000L);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        Map<UUID, HoldOutcome> outcomes = new ConcurrentHashMap<>();
        List<Future<?>> futures = new ArrayList<>(threadCount);

        try {
            for (int i = 0; i < threadCount; i++) {
                UUID reservationId = UUID.randomUUID();
                futures.add(pool.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    HoldOutcome outcome = redisSeatGate.hold(eventId, List.of(seatId), reservationId, nowMs);
                    outcomes.put(reservationId, outcome);
                }));
            }

            assertThat(readyLatch.await(10, TimeUnit.SECONDS)).isTrue();
            startGate.countDown();

            for (Future<?> future : futures) {
                future.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(outcomes).hasSize(threadCount);
        List<UUID> winners = outcomes.entrySet().stream()
                .filter(e -> e.getValue() == HoldOutcome.HELD)
                .map(Map.Entry::getKey)
                .toList();
        long rejectedCount = outcomes.values().stream()
                .filter(o -> o == HoldOutcome.SEAT_TAKEN)
                .count();

        assertThat(winners).hasSize(1);
        assertThat(rejectedCount).isEqualTo(threadCount - 1);

        String storedHolder = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId));
        assertThat(storedHolder).isEqualTo(winners.getFirst().toString());
    }

    @Test
    void releaseWithWrongTokenDoesNotFreeSeatAndReleaseWithRightTokenDoes() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID ownerReservationId = UUID.randomUUID();
        UUID wrongReservationId = UUID.randomUUID();
        UUID nextReservationId = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        cacheEventDropTime(eventId, nowMs - 1_000L);
        assertThat(redisSeatGate.hold(eventId, List.of(seat1, seat2), ownerReservationId, nowMs))
                .isEqualTo(HoldOutcome.HELD);

        // Wrong token must not release any seat
        long wrongReleased = redisSeatGate.release(eventId, List.of(seat1, seat2), wrongReservationId);
        assertThat(wrongReleased).isZero();
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1)))
                .isEqualTo(ownerReservationId.toString());
        assertThat(redisSeatGate.hold(eventId, List.of(seat1), nextReservationId, nowMs))
                .isEqualTo(HoldOutcome.SEAT_TAKEN);

        // Right token frees both seats
        long rightReleased = redisSeatGate.release(eventId, List.of(seat1, seat2), ownerReservationId);
        assertThat(rightReleased).isEqualTo(2L);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isNull();
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2))).isNull();

        // Seat can now be held by a new reservation
        assertThat(redisSeatGate.hold(eventId, List.of(seat1), nextReservationId, nowMs))
                .isEqualTo(HoldOutcome.HELD);
    }

    @Test
    void confirmLeavesKeyWithTtlMinusOneAndBlocksFutureReleaseOrHold() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        long nowMs = System.currentTimeMillis();

        cacheEventDropTime(eventId, nowMs - 1_000L);
        assertThat(redisSeatGate.hold(eventId, List.of(seat1, seat2), reservationId, nowMs))
                .isEqualTo(HoldOutcome.HELD);

        ConfirmOutcome confirmOutcome = redisSeatGate.confirm(eventId, List.of(seat1, seat2), reservationId);
        assertThat(confirmOutcome).isEqualTo(ConfirmOutcome.CONFIRMED);

        for (UUID seatId : List.of(seat1, seat2)) {
            String key = RedisKeys.seatKey(eventId, seatId);
            assertThat(stringRedisTemplate.opsForValue().get(key)).isEqualTo(RedisKeys.SOLD_MARKER);
            assertThat(stringRedisTemplate.getExpire(key)).isEqualTo(-1L);
        }

        // Release cannot delete SOLD marker and new hold is rejected
        assertThat(redisSeatGate.release(eventId, List.of(seat1, seat2), reservationId)).isZero();
        assertThat(redisSeatGate.hold(eventId, List.of(seat1), UUID.randomUUID(), nowMs))
                .isEqualTo(HoldOutcome.SEAT_TAKEN);
    }

    @Test
    void afterFlushAllRewarmRestoresHeldSeatsWithCorrectRemainingTtlAndSoldSeatsWithNoTtl() {
        UUID eventId = UUID.randomUUID();
        UUID heldSeatId = UUID.randomUUID();
        UUID soldSeatId = UUID.randomUUID();
        UUID availableSeatId = UUID.randomUUID();
        UUID activeReservationId = UUID.randomUUID();

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant dropTime = now.minus(10, ChronoUnit.MINUTES);
        Instant eventTime = now.plus(7, ChronoUnit.DAYS);
        Instant expiresAt = now.plus(300, ChronoUnit.SECONDS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Flash Sale Concert", "Madison Square Garden",
                    eventTime, dropTime, "ON_SALE", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(heldSeatId, eventId, "SEC-A", "1", "101", 15000L, "USD", "HELD", 1L, now, now),
                    new SeatRecord(soldSeatId, eventId, "SEC-A", "1", "102", 15000L, "USD", "SOLD", 2L, now, now),
                    new SeatRecord(availableSeatId, eventId, "SEC-A", "1", "103", 15000L, "USD", "AVAILABLE", 0L, now, now)
            ));
            reservationRepository.insert(
                    new ReservationRecord(
                            activeReservationId, eventId, UUID.randomUUID(), "HELD",
                            15000L, "USD", now, expiresAt, null, null, null, now
                    ),
                    List.of(new ReservationItemRecord(
                            UUID.randomUUID(), activeReservationId, eventId, heldSeatId,
                            "SEC-A", "1", "101", 15000L, "HELD", now, now
                    ))
            );
        });

        // Simulate Redis FLUSHALL
        stringRedisTemplate.execute((RedisCallback<Void>) (RedisConnection connection) -> {
            connection.serverCommands().flushAll();
            return null;
        });

        assertThat(redisSeatGate.hold(eventId, List.of(availableSeatId), UUID.randomUUID()))
                .isEqualTo(HoldOutcome.EVENT_NOT_CACHED);

        // Run rewarm on demand
        redisRewarmService.rewarmAll();

        // 1. Event drop_ts is restored
        String eventKey = RedisKeys.eventDropTsKey(eventId);
        assertThat(stringRedisTemplate.opsForValue().get(eventKey))
                .isEqualTo(String.valueOf(dropTime.toEpochMilli()));
        assertThat(stringRedisTemplate.getExpire(eventKey, TimeUnit.SECONDS))
                .isBetween(604790L, 604800L);

        // 2. HELD seat lock is restored with remaining_sec (~300s) + 60s safety margin (~360s)
        String heldKey = RedisKeys.seatKey(eventId, heldSeatId);
        assertThat(stringRedisTemplate.opsForValue().get(heldKey))
                .isEqualTo(activeReservationId.toString());
        Long heldTtl = stringRedisTemplate.getExpire(heldKey, TimeUnit.SECONDS);
        assertThat(heldTtl).isNotNull().isBetween(350L, 361L);

        // 3. SOLD seat marker is restored with no TTL (-1)
        String soldKey = RedisKeys.seatKey(eventId, soldSeatId);
        assertThat(stringRedisTemplate.opsForValue().get(soldKey))
                .isEqualTo(RedisKeys.SOLD_MARKER);
        assertThat(stringRedisTemplate.getExpire(soldKey)).isEqualTo(-1L);

        // 4. New holds on restored HELD or SOLD seats are rejected, while available seat succeeds
        assertThat(redisSeatGate.hold(eventId, List.of(heldSeatId), UUID.randomUUID()))
                .isEqualTo(HoldOutcome.SEAT_TAKEN);
        assertThat(redisSeatGate.hold(eventId, List.of(soldSeatId), UUID.randomUUID()))
                .isEqualTo(HoldOutcome.SEAT_TAKEN);
        assertThat(redisSeatGate.hold(eventId, List.of(availableSeatId), UUID.randomUUID()))
                .isEqualTo(HoldOutcome.HELD);
    }

    @Test
    void connectionFailuresAreWrappedInRedisUnavailableException() {
        RedisStandaloneConfiguration unreachableConfig = new RedisStandaloneConfiguration("127.0.0.1", 1);
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(100))
                .build();
        LettuceConnectionFactory badFactory = new LettuceConnectionFactory(unreachableConfig, clientConfig);
        badFactory.afterPropertiesSet();

        try {
            StringRedisTemplate badTemplate = new StringRedisTemplate(badFactory);
            RedisSeatGate brokenGate = new RedisSeatGate(
                    badTemplate,
                    holdInventoryScript,
                    releaseInventoryScript,
                    confirmInventoryScript,
                    redisProperties,
                    Clock.systemUTC(),
                    null
            );

            UUID eventId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();
            UUID reservationId = UUID.randomUUID();

            assertThatThrownBy(() -> brokenGate.hold(eventId, List.of(seatId), reservationId))
                    .isInstanceOf(RedisUnavailableException.class);
            assertThatThrownBy(() -> brokenGate.release(eventId, List.of(seatId), reservationId))
                    .isInstanceOf(RedisUnavailableException.class);
            assertThatThrownBy(() -> brokenGate.confirm(eventId, List.of(seatId), reservationId))
                    .isInstanceOf(RedisUnavailableException.class);
        } finally {
            badFactory.destroy();
        }
    }

    @Test
    void rewarmWithBatchSizeOneConcurrentHoldGetsEventNotCachedUntilLastSeatCommandWritten() {
        UUID eventId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Ordering Barrier Event", "Venue A",
                    now.plus(1, ChronoUnit.DAYS), now.minus(1, ChronoUnit.HOURS), "ON_SALE", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(seat1, eventId, "SEC", "A", "1", 10000L, "USD", "SOLD", 1L, now, now),
                    new SeatRecord(seat2, eventId, "SEC", "A", "2", 10000L, "USD", "SOLD", 1L, now, now)
            ));
        });

        int originalBatchSize = redisProperties.getPipelineBatchSize();
        redisProperties.setPipelineBatchSize(1);
        AtomicInteger flushedBatches = new AtomicInteger(0);
        UUID probeReservationId = UUID.randomUUID();

        try {
            redisRewarmService.setOnPipelineBatchFlushed(() -> {
                int batchNum = flushedBatches.incrementAndGet();
                // During seat write batches (batch 1 and 2), event:{eventId}:drop_ts has NOT been written!
                if (batchNum <= 2) {
                    HoldOutcome outcome = redisSeatGate.hold(eventId, List.of(seat2), probeReservationId);
                    assertThat(outcome).isEqualTo(HoldOutcome.EVENT_NOT_CACHED);
                }
            });

            boolean rewarmed = redisRewarmService.rewarmEvent(eventId);
            assertThat(rewarmed).isTrue();

            // Total flushed batches: 2 seat batches (1 seat each) + 1 drop_ts batch = 3 batches
            assertThat(flushedBatches.get()).isEqualTo(3);

            // After rewarm completes, event:{eventId}:drop_ts is written and seat2 is SOLD
            HoldOutcome finalOutcome = redisSeatGate.hold(eventId, List.of(seat2), probeReservationId);
            assertThat(finalOutcome).isEqualTo(HoldOutcome.SEAT_TAKEN);
        } finally {
            redisProperties.setPipelineBatchSize(originalBatchSize);
            redisRewarmService.setOnPipelineBatchFlushed(null);
        }
    }

    @Test
    void completedOrCancelledEventsAreNotRewarmed() {
        UUID completedEventId = UUID.randomUUID();
        UUID cancelledEventId = UUID.randomUUID();
        UUID scheduledEventId = UUID.randomUUID();
        UUID completedSeat = UUID.randomUUID();
        UUID cancelledSeat = UUID.randomUUID();
        UUID scheduledSeat = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    completedEventId, "Completed Concert", "Venue C",
                    now.minus(2, ChronoUnit.DAYS), now.minus(3, ChronoUnit.DAYS), "COMPLETED", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(completedSeat, completedEventId, "SEC", "A", "1", 10000L, "USD", "SOLD", 1L, now, now)
            ));

            eventRepository.insert(new EventRecord(
                    cancelledEventId, "Cancelled Concert", "Venue X",
                    now.plus(2, ChronoUnit.DAYS), now.minus(1, ChronoUnit.DAYS), "CANCELLED", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(cancelledSeat, cancelledEventId, "SEC", "A", "1", 10000L, "USD", "SOLD", 1L, now, now)
            ));

            eventRepository.insert(new EventRecord(
                    scheduledEventId, "Scheduled Concert", "Venue S",
                    now.plus(5, ChronoUnit.DAYS), now.minus(1, ChronoUnit.HOURS), "SCHEDULED", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(scheduledSeat, scheduledEventId, "SEC", "A", "1", 10000L, "USD", "SOLD", 1L, now, now)
            ));
        });

        // 1. Direct rewarmEvent calls for COMPLETED and CANCELLED return false and write nothing
        assertThat(redisRewarmService.rewarmEvent(completedEventId)).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(completedEventId))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(completedEventId, completedSeat))).isFalse();

        assertThat(redisRewarmService.rewarmEvent(cancelledEventId)).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(cancelledEventId))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(cancelledEventId, cancelledSeat))).isFalse();

        // 2. rewarmAll processes only SCHEDULED, ON_SALE, SOLD_OUT
        redisRewarmService.rewarmAll();

        assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(completedEventId))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(cancelledEventId))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(scheduledEventId))).isTrue();
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(scheduledEventId, scheduledSeat)))
                .isEqualTo(RedisKeys.SOLD_MARKER);
    }

    @Test
    void soldPagingWorksAcrossMoreThanOnePage() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        int totalSeats = 1050; // exceeds default page size 1000
        List<SeatRecord> seats = new ArrayList<>(totalSeats);
        for (int i = 0; i < totalSeats; i++) {
            seats.add(new SeatRecord(
                    UUID.randomUUID(), eventId, "SEC", "ROW-" + (i / 50), "SEAT-" + (i % 50),
                    5000L, "USD", "SOLD", 1L, now, now
            ));
        }

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Big Stadium Event", "Stadium",
                    now.plus(10, ChronoUnit.DAYS), now.minus(1, ChronoUnit.DAYS), "ON_SALE", now, now
            ));
            seatRepository.bulkInsert(seats);
        });

        boolean rewarmed = redisRewarmService.rewarmEvent(eventId);
        assertThat(rewarmed).isTrue();

        // Verify all 1050 seats were restored as SOLD in Redis
        for (SeatRecord seat : seats) {
            String val = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat.seatId()));
            assertThat(val).isEqualTo(RedisKeys.SOLD_MARKER);
        }
    }

    @Test
    void reservationReleasedBetweenPgReadAndRedisWriteLeavesNoGhostLockAfterPostCheck() {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Ghost Lock Event", "Venue G",
                    now.plus(1, ChronoUnit.DAYS), now.minus(1, ChronoUnit.HOURS), "ON_SALE", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(seatId, eventId, "SEC", "1", "1", 10000L, "USD", "HELD", 1L, now, now)
            ));
            reservationRepository.insert(
                    new ReservationRecord(
                            reservationId, eventId, UUID.randomUUID(), "HELD",
                            10000L, "USD", now, now.plus(300, ChronoUnit.SECONDS), null, null, null, now
                    ),
                    List.of(new ReservationItemRecord(
                            UUID.randomUUID(), reservationId, eventId, seatId,
                            "SEC", "1", "1", 10000L, "HELD", now, now
                    ))
            );
        });

        // Hook: right after HELD locks are read inside REPEATABLE_READ snapshot,
        // simulate a concurrent release in PostgreSQL before Redis writes take place.
        redisRewarmService.setAfterHeldLocksReadHook(() -> {
            transactionTemplate.executeWithoutResult(status -> {
                reservationRepository.updateStatus(reservationId, "RELEASED");
            });
        });

        try {
            boolean rewarmed = redisRewarmService.rewarmEvent(eventId);
            assertThat(rewarmed).isTrue();

            // The post-check detected reservation is now RELEASED and deleted the Redis key!
            String seatKey = RedisKeys.seatKey(eventId, seatId);
            assertThat(stringRedisTemplate.hasKey(seatKey)).isFalse();

            // A new reservation can now acquire the seat!
            UUID newReservationId = UUID.randomUUID();
            HoldOutcome outcome = redisSeatGate.hold(eventId, List.of(seatId), newReservationId);
            assertThat(outcome).isEqualTo(HoldOutcome.HELD);
            assertThat(stringRedisTemplate.opsForValue().get(seatKey)).isEqualTo(newReservationId.toString());
        } finally {
            redisRewarmService.setAfterHeldLocksReadHook(null);
        }
    }

    @Test
    void twoConcurrentRewarmEventCallsForSameEventRunWorkOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Single Flight Event", "Venue SF",
                    now.plus(1, ChronoUnit.DAYS), now.minus(1, ChronoUnit.HOURS), "ON_SALE", now, now
            ));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(seatId, eventId, "SEC", "1", "1", 10000L, "USD", "SOLD", 1L, now, now)
            ));
        });

        redisRewarmService.resetRewarmExecutionCount();

        CountDownLatch thread1Started = new CountDownLatch(1);
        CountDownLatch thread2Entered = new CountDownLatch(1);
        CountDownLatch allowThread1ToProceed = new CountDownLatch(1);

        redisRewarmService.setInsideRewarmHook(() -> {
            thread1Started.countDown();
            try {
                // Wait until thread 2 has entered rewarmEvent(eventId)
                thread2Entered.await(5, TimeUnit.SECONDS);
                allowThread1ToProceed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> f1 = executor.submit(() -> redisRewarmService.rewarmEvent(eventId));

            // Wait until thread 1 is executing inside doRewarmEvent
            assertThat(thread1Started.await(5, TimeUnit.SECONDS)).isTrue();

            // Submit thread 2 for the same eventId
            Future<Boolean> f2 = executor.submit(() -> {
                thread2Entered.countDown();
                return redisRewarmService.rewarmEvent(eventId);
            });

            // Let thread 1 finish
            allowThread1ToProceed.countDown();

            Boolean res1 = f1.get(5, TimeUnit.SECONDS);
            Boolean res2 = f2.get(5, TimeUnit.SECONDS);

            assertThat(res1).isTrue();
            assertThat(res2).isTrue();

            // Exactly one underlying execution ran!
            assertThat(redisRewarmService.getRewarmExecutionCount()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            redisRewarmService.setInsideRewarmHook(null);
        }
    }

    private void cacheEventDropTime(UUID eventId, long dropEpochMs) {
        stringRedisTemplate.opsForValue().set(
                RedisKeys.eventDropTsKey(eventId),
                String.valueOf(dropEpochMs),
                Duration.ofSeconds(RedisKeys.EVENT_DROP_TS_TTL_SECONDS)
        );
    }
}


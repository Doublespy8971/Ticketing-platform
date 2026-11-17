package com.ticketing.inventory.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.redis.RedisRewarmService;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.inventory.service.ReservationService;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.micrometer.core.instrument.MeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ReservationRedisFailureIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16.4-alpine"))
            .withDatabaseName("inventory_test_db")
            .withUsername("ticketing")
            .withPassword("ticketing_secret");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4.1-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "10");
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private RedisRewarmService redisRewarmService;

    @Autowired
    private RedisSeatGate redisSeatGate;

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .cleanDisabled(false)
                .load()
                .clean();
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        stringRedisTemplate.execute((RedisCallback<Void>) (RedisConnection connection) -> {
            connection.serverCommands().flushAll();
            return null;
        });

        resetHooks();
    }

    @AfterEach
    void tearDown() {
        resetHooks();
    }

    private void resetHooks() {
        redisSeatGate.setBeforeHoldHook(null);
        redisSeatGate.setAfterHoldScriptHook(null);
        redisSeatGate.setBeforeReleaseHook(null);
        reservationService.setAfterIdempotencyClaimHook(null);
        reservationService.setAfterRedisHoldHook(null);
        reservationService.setInTransactionHook(null);
        reservationService.setBeforeCommitHook(null);
        reservationService.setAfterCommitHook(null);
    }

    @Test
    void redisConnectionFailureReturns503WithRetryAfterAndLeavesZeroDatabaseRowsThenRestartSucceedsWith201() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        seedOpenEventWithSeat(eventId, seatId);

        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
        String requestJson = objectMapper.writeValueAsString(request);

        // 1. Simulate Redis connection failure before hold
        redisSeatGate.setBeforeHoldHook(() -> {
            throw new RedisConnectionException("Simulated Redis connection failure");
        });

        UUID correlationId1 = UUID.randomUUID();
        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", userId.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .header(CorrelationConstants.HTTP_HEADER, correlationId1.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.status", is(503)))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.SERVICE_UNAVAILABLE.name())))
                .andExpect(jsonPath("$.detail", is("Inventory concurrency gate is temporarily unavailable.")))
                .andExpect(jsonPath("$.instance", is("/api/v1/reservations")))
                .andExpect(jsonPath("$.correlationId", is(correlationId1.toString())));

        // Assert 0 rows in reservations, reservation_items, outbox_events, idempotency_keys
        Integer reservationsCount = jdbcTemplate.queryForObject("SELECT count(*) FROM reservations WHERE event_id = ?::uuid", Integer.class, eventId);
        Integer itemsCount = jdbcTemplate.queryForObject("SELECT count(*) FROM reservation_items WHERE event_id = ?::uuid", Integer.class, eventId);
        Integer outboxCount = jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Integer.class);
        Integer idempotencyCount = jdbcTemplate.queryForObject("SELECT count(*) FROM idempotency_keys WHERE idempotency_key = ?::uuid", Integer.class, idempotencyKey);

        assertThat(reservationsCount).isEqualTo(0);
        assertThat(itemsCount).isEqualTo(0);
        assertThat(outboxCount).isEqualTo(0);
        assertThat(idempotencyCount).isEqualTo(0);

        // Verify metric recorded
        double unavailableMetric = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "hold")
                .tag("cause", "connection")
                .counter()
                .count();
        assertThat(unavailableMetric).isGreaterThanOrEqualTo(1.0);

        // 2. "Restart" Redis: clear simulated connection failure
        redisSeatGate.setBeforeHoldHook(null);

        // Retry the exact same request with the exact same Idempotency-Key
        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", userId.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .header(CorrelationConstants.HTTP_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("HELD")))
                .andExpect(jsonPath("$.userId", is(userId.toString())))
                .andExpect(jsonPath("$.eventId", is(eventId.toString())))
                .andExpect(jsonPath("$.seats[0].seatId", is(seatId.toString())));

        // Verify now 1 row in reservations, reservation_items, outbox_events, idempotency_keys
        reservationsCount = jdbcTemplate.queryForObject("SELECT count(*) FROM reservations WHERE event_id = ?::uuid", Integer.class, eventId);
        itemsCount = jdbcTemplate.queryForObject("SELECT count(*) FROM reservation_items WHERE event_id = ?::uuid", Integer.class, eventId);
        outboxCount = jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Integer.class);
        String idempotencyStatus = jdbcTemplate.queryForObject("SELECT status FROM idempotency_keys WHERE idempotency_key = ?::uuid", String.class, idempotencyKey);
        Integer responseStatus = jdbcTemplate.queryForObject("SELECT response_status FROM idempotency_keys WHERE idempotency_key = ?::uuid", Integer.class, idempotencyKey);

        assertThat(reservationsCount).isEqualTo(1);
        assertThat(itemsCount).isEqualTo(1);
        assertThat(outboxCount).isEqualTo(1);
        assertThat(idempotencyStatus).isEqualTo("COMPLETED");
        assertThat(responseStatus).isEqualTo(201);
    }

    @Test
    void holdTimeoutAfterScriptExecutionExecutesBestEffortReleaseAndAllowsImmediateHoldByAnotherBuyer() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        seedOpenEventWithSeat(eventId, seatId);

        UUID user1 = UUID.randomUUID();
        UUID user2 = UUID.randomUUID();
        UUID idemp1 = UUID.randomUUID();
        UUID idemp2 = UUID.randomUUID();
        CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
        String requestJson = objectMapper.writeValueAsString(request);

        // Inject simulated timeout AFTER the Redis hold script executes
        redisSeatGate.setAfterHoldScriptHook(() -> {
            throw new RedisCommandTimeoutException("Simulated Redis timeout after hold_inventory.lua executed");
        });

        // User 1 requests hold -> script executes, but timeout is triggered -> best-effort release frees seat -> returns 503
        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", user1.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idemp1.toString())
                        .header(CorrelationConstants.HTTP_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.SERVICE_UNAVAILABLE.name())))
                .andExpect(jsonPath("$.detail", is("Inventory concurrency gate is temporarily unavailable.")));

        // Verify seat key in Redis was freed by bestEffortRelease (not orphaned for 660s)
        String seatKey = RedisKeys.seatKey(eventId, seatId);
        assertThat(stringRedisTemplate.opsForValue().get(seatKey)).isNull();

        // Verify timeout metric incremented
        double timeoutMetric = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "hold")
                .tag("cause", "timeout")
                .counter()
                .count();
        assertThat(timeoutMetric).isGreaterThanOrEqualTo(1.0);

        // Reset hook so User 2 can proceed normally
        redisSeatGate.setAfterHoldScriptHook(null);

        // User 2 immediately attempts to hold the same seat -> succeeds with 201 CREATED
        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", user2.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idemp2.toString())
                        .header(CorrelationConstants.HTTP_HEADER, UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("HELD")))
                .andExpect(jsonPath("$.userId", is(user2.toString())))
                .andExpect(jsonPath("$.seats[0].seatId", is(seatId.toString())));

        // Redis key now holds User 2's reservation
        String redisToken = stringRedisTemplate.opsForValue().get(seatKey);
        assertThat(redisToken).isNotNull();
    }

    @Test
    void bestEffortReleaseFailureDuringCompensationStillReturns503WithoutExceptionLeakage() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        seedOpenEventWithSeat(eventId, seatId);

        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
        String requestJson = objectMapper.writeValueAsString(request);

        // Both hold and release simulate connection failure (Redis completely down)
        redisSeatGate.setBeforeHoldHook(() -> {
            throw new RedisConnectionException("Redis down for hold");
        });
        redisSeatGate.setBeforeReleaseHook(() -> {
            throw new RedisConnectionException("Redis down for release");
        });

        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", userId.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.status", is(503)))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.SERVICE_UNAVAILABLE.name())))
                .andExpect(jsonPath("$.detail", is("Inventory concurrency gate is temporarily unavailable.")))
                .andExpect(jsonPath("$.errors").isEmpty());
    }

    @Test
    void nonRedisUnexpectedExceptionProduces500InternalServerError() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        seedOpenEventWithSeat(eventId, seatId);

        UUID userId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
        String requestJson = objectMapper.writeValueAsString(request);

        // Inject unexpected non-Redis runtime exception during transaction
        reservationService.setInTransactionHook(() -> {
            throw new RuntimeException("Simulated unexpected database failure");
        });

        mockMvc.perform(post("/api/v1/reservations")
                        .header("X-User-Id", userId.toString())
                        .header("X-User-Roles", "USER")
                        .header("Idempotency-Key", idempotencyKey.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isInternalServerError())
                .andExpect(header().doesNotExist(HttpHeaders.RETRY_AFTER))
                .andExpect(jsonPath("$.status", is(500)))
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.INTERNAL_SERVER_ERROR.name())))
                .andExpect(jsonPath("$.detail", is("An unexpected error occurred.")));
    }

    @Autowired
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;

    private void seedOpenEventWithSeat(UUID eventId, UUID seatId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant pastDropTime = now.minus(1, ChronoUnit.HOURS);
        Instant futureEventTime = now.plus(30, ChronoUnit.DAYS);

        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId,
                    "Redis Failure Test Event",
                    "Grand Arena",
                    futureEventTime,
                    pastDropTime,
                    "ON_SALE",
                    now,
                    now
            ));

            seatRepository.bulkInsert(List.of(new SeatRecord(
                    seatId,
                    eventId,
                    "SEC-A",
                    "1",
                    "101",
                    10000L,
                    "USD",
                    "AVAILABLE",
                    0L,
                    now,
                    now
            )));
        });

        redisRewarmService.rewarmEvent(eventId);
    }
}

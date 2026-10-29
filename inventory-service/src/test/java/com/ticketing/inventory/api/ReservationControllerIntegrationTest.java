package com.ticketing.inventory.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.ticketing.common.events.SeatReservedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.common.messaging.Topics;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.api.dto.ReservationResponse;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.idempotency.IdempotencyKeyRecord;
import com.ticketing.inventory.idempotency.IdempotencyKeyRepository;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.redis.RedisRewarmService;
import com.ticketing.inventory.service.ReservationService;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ReservationControllerIntegrationTest {

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
                registry.add("spring.datasource.hikari.maximum-pool-size", () -> "30");
                registry.add("spring.data.redis.host", REDIS::getHost);
                registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
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
        private IdempotencyKeyRepository idempotencyKeyRepository;

        @Autowired
        private RedisRewarmService redisRewarmService;

        @Autowired
        private ReservationService reservationService;

        @Autowired
        private MeterRegistry meterRegistry;

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
                reservationService.setAfterIdempotencyClaimHook(null);
                reservationService.setAfterRedisHoldHook(null);
                reservationService.setInTransactionHook(null);
                reservationService.setBeforeCommitHook(null);
                reservationService.setAfterCommitHook(null);
                reservationService.setBeforeCompensationStatusReadHook(null);
                redisRewarmService.setAfterHeldLocksReadHook(null);
                redisRewarmService.setInsideRewarmHook(null);
                redisRewarmService.setOnPipelineBatchFlushed(null);
        }

        @AfterEach
        void resetHooks() {
                reservationService.setAfterIdempotencyClaimHook(null);
                reservationService.setAfterRedisHoldHook(null);
                reservationService.setInTransactionHook(null);
                reservationService.setBeforeCommitHook(null);
                reservationService.setAfterCommitHook(null);
                reservationService.setBeforeCompensationStatusReadHook(null);
                redisRewarmService.setAfterHeldLocksReadHook(null);
                redisRewarmService.setInsideRewarmHook(null);
                redisRewarmService.setOnPipelineBatchFlushed(null);
        }

        // -------------------------------------------------------------------------
        // 1. Happy path: 201 response, DB rows, idempotency row, outbox_events row
        // -------------------------------------------------------------------------

        @Test
        void happyPathCreatesReservationItemsHeldSeatsIdempotencyRowAndOutboxEvent() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey = UUID.randomUUID();
                String correlationId = UUID.randomUUID().toString();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "FLOOR-A", "1", "101", 15000L, "AVAILABLE"),
                                seat(seat2, eventId, "FLOOR-A", "1", "102", 15000L, "AVAILABLE")), true);

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seat1, seat2));

                MvcResult mvcResult = mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .header(CorrelationConstants.HTTP_HEADER, correlationId)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isCreated())
                                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, correlationId))
                                .andExpect(header().doesNotExist(ReservationController.IDEMPOTENT_REPLAY_HEADER))
                                .andExpect(jsonPath("$.eventId", is(eventId.toString())))
                                .andExpect(jsonPath("$.userId", is(userId.toString())))
                                .andExpect(jsonPath("$.status", is("HELD")))
                                .andExpect(jsonPath("$.totalAmountCents", is(30000)))
                                .andExpect(jsonPath("$.currency", is("USD")))
                                .andExpect(jsonPath("$.seats", hasSize(2)))
                                .andExpect(jsonPath("$.seats[0].seatId", is(seat1.toString())))
                                .andExpect(jsonPath("$.seats[0].sectionCode", is("FLOOR-A")))
                                .andExpect(jsonPath("$.seats[0].rowLabel", is("1")))
                                .andExpect(jsonPath("$.seats[0].seatNumber", is("101")))
                                .andExpect(jsonPath("$.seats[0].priceCents", is(15000)))
                                .andExpect(jsonPath("$.seats[1].seatId", is(seat2.toString())))
                                .andExpect(jsonPath("$.seats[1].seatNumber", is("102")))
                                .andReturn();

                ReservationResponse response = objectMapper.readValue(
                                mvcResult.getResponse().getContentAsString(), ReservationResponse.class);

                UUID reservationId = response.reservationId();
                assertThat(reservationId).isNotNull();
                assertThat(response.orderId()).isEqualTo(reservationId);
                // UUIDv7 has version 7
                assertThat(reservationId.version()).isEqualTo(7);
                assertThat(Duration.between(response.reservedAt(), response.expiresAt()))
                                .isEqualTo(Duration.ofSeconds(600));

                // 1. Check reservations row in PostgreSQL
                ReservationRecord savedReservation = reservationRepository.findById(reservationId).orElseThrow();
                assertThat(savedReservation.status()).isEqualTo("HELD");
                assertThat(savedReservation.userId()).isEqualTo(userId);
                assertThat(savedReservation.eventId()).isEqualTo(eventId);
                assertThat(savedReservation.totalAmountCents()).isEqualTo(30000L);
                assertThat(savedReservation.currency()).isEqualTo("USD");
                assertThat(savedReservation.createdAt()).isEqualTo(response.reservedAt());
                assertThat(savedReservation.expiresAt()).isEqualTo(response.expiresAt());

                // 2. Check reservation_items rows in PostgreSQL
                List<ReservationItemRecord> items = reservationRepository.findItemsByReservationId(reservationId);
                assertThat(items).hasSize(2);
                assertThat(items).allSatisfy(item -> {
                        assertThat(item.status()).isEqualTo("HELD");
                        assertThat(item.priceCents()).isEqualTo(15000L);
                        assertThat(item.eventId()).isEqualTo(eventId);
                });

                // 3. Check seats rows in PostgreSQL (status = HELD, version incremented from 0
                // -> 1)
                SeatRecord s1After = seatRepository.findById(seat1).orElseThrow();
                SeatRecord s2After = seatRepository.findById(seat2).orElseThrow();
                assertThat(s1After.status()).isEqualTo("HELD");
                assertThat(s1After.version()).isEqualTo(1L);
                assertThat(s2After.status()).isEqualTo("HELD");
                assertThat(s2After.version()).isEqualTo(1L);

                // 4. Check Redis keys hold the reservationId token with ~660s TTL
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1)))
                                .isEqualTo(reservationId.toString());
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2)))
                                .isEqualTo(reservationId.toString());
                assertThat(stringRedisTemplate.getExpire(RedisKeys.seatKey(eventId, seat1), TimeUnit.SECONDS))
                                .isBetween(650L, 660L);

                // 5. Check idempotency_keys row in PostgreSQL
                IdempotencyKeyRecord idemRecord = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)
                                .orElseThrow();
                assertThat(idemRecord.status()).isEqualTo("COMPLETED");
                assertThat(idemRecord.responseStatus()).isEqualTo(201);
                assertThat(idemRecord.endpoint()).isEqualTo("POST /api/v1/reservations");
                assertThat(idemRecord.requestHash()).hasSize(64);

                // 6. Check outbox_events row in PostgreSQL
                List<Map<String, Object>> outboxRows = jdbcTemplate.queryForList("SELECT * FROM outbox_events");
                assertThat(outboxRows).hasSize(1);
                Map<String, Object> outboxRow = outboxRows.getFirst();
                assertThat(outboxRow.get("aggregate_type")).isEqualTo("RESERVATION");
                assertThat(outboxRow.get("aggregate_id")).isEqualTo(reservationId);
                assertThat(outboxRow.get("event_type")).isEqualTo(EventTypes.INVENTORY_SEAT_RESERVED);
                assertThat(outboxRow.get("topic")).isEqualTo(Topics.INVENTORY_EVENTS);
                assertThat(outboxRow.get("correlation_id")).isEqualTo(UUID.fromString(correlationId));
                assertThat(outboxRow.get("schema_version")).isEqualTo(1);
                assertThat(outboxRow.get("published_at")).isNull();

                String payloadJson = outboxRow.get("payload").toString();
                SeatReservedPayload eventPayload = objectMapper.readValue(payloadJson, SeatReservedPayload.class);
                assertThat(eventPayload.reservationId()).isEqualTo(reservationId);
                assertThat(eventPayload.orderId()).isEqualTo(reservationId);
                assertThat(eventPayload.userId()).isEqualTo(userId);
                assertThat(eventPayload.eventId()).isEqualTo(eventId);
                assertThat(eventPayload.totalAmountCents()).isEqualTo(30000L);
                assertThat(eventPayload.currency()).isEqualTo("USD");
                assertThat(eventPayload.reservedAt()).isEqualTo(response.reservedAt());
                assertThat(eventPayload.expiresAt()).isEqualTo(response.expiresAt());
                assertThat(eventPayload.seats()).hasSize(2);

                // Verify metrics were recorded without event_id tag
                assertThat(meterRegistry.find("inventory_hold_requests_total").tag("outcome", "CREATED").counter())
                                .isNotNull()
                                .satisfies(c -> assertThat(c.count()).isGreaterThanOrEqualTo(1.0));
                assertThat(meterRegistry.find("ticketing_reservations_created_total").counter())
                                .isNotNull()
                                .satisfies(c -> {
                                        assertThat(c.count()).isGreaterThanOrEqualTo(1.0);
                                        assertThat(c.getId().getTag("event_id")).isNull();
                                });
        }

        // -------------------------------------------------------------------------
        // 2. Multi-seat request is all-or-nothing
        // -------------------------------------------------------------------------

        @Test
        void multiSeatRequestIsAllOrNothing() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();
                UUID seat3 = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "SEC-A", "1", "101", 10000L, "AVAILABLE"),
                                seat(seat2, eventId, "SEC-A", "1", "102", 10000L, "AVAILABLE"),
                                seat(seat3, eventId, "SEC-A", "1", "103", 10000L, "AVAILABLE")), true);

                // First user holds seat2
                CreateReservationRequest holdSeat2 = new CreateReservationRequest(eventId, List.of(seat2));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(holdSeat2)))
                                .andExpect(status().isCreated());

                // Second user requests [seat1, seat2, seat3] -> must fail 409
                // SEAT_ALREADY_TAKEN and leave seat1 & seat3 free
                CreateReservationRequest holdThree = new CreateReservationRequest(eventId,
                                List.of(seat1, seat2, seat3));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(holdThree)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.status", is(409)))
                                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));

                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isNull();
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat3))).isNull();
                assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(seatRepository.findById(seat3).orElseThrow().status()).isEqualTo("AVAILABLE");

                // Third user can now hold [seat1, seat3]
                CreateReservationRequest holdOneAndThree = new CreateReservationRequest(eventId, List.of(seat1, seat3));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(holdOneAndThree)))
                                .andExpect(status().isCreated());
        }

        // -------------------------------------------------------------------------
        // 3. 100 concurrent requests for the same seat by different users
        // -------------------------------------------------------------------------

        @Test
        void oneHundredConcurrentRequestsForSameSeatProduceExactlyOneWinnerAndNinetyNine409() throws Exception {
                int concurrency = 100;
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "VIP", "1", "1", 25000L, "AVAILABLE")), true);

                String requestBody = objectMapper
                                .writeValueAsString(new CreateReservationRequest(eventId, List.of(seatId)));

                ExecutorService pool = Executors.newFixedThreadPool(concurrency);
                CountDownLatch ready = new CountDownLatch(concurrency);
                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger status201Count = new AtomicInteger(0);
                AtomicInteger status409SeatTakenCount = new AtomicInteger(0);
                Map<Integer, Integer> otherStatuses = new ConcurrentHashMap<>();
                List<Future<?>> futures = new ArrayList<>(concurrency);

                try {
                        for (int i = 0; i < concurrency; i++) {
                                final String userId = UUID.randomUUID().toString();
                                final String idemKey = UUID.randomUUID().toString();
                                futures.add(pool.submit(() -> {
                                        ready.countDown();
                                        try {
                                                start.await();
                                                MvcResult result = mockMvc.perform(post("/api/v1/reservations")
                                                                .header(CorrelationConstants.USER_ID_HEADER, userId)
                                                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                                                idemKey)
                                                                .contentType(MediaType.APPLICATION_JSON)
                                                                .content(requestBody))
                                                                .andReturn();
                                                int status = result.getResponse().getStatus();
                                                if (status == 201) {
                                                        status201Count.incrementAndGet();
                                                } else if (status == 409) {
                                                        JsonNode body = objectMapper.readTree(
                                                                        result.getResponse().getContentAsString());
                                                        if ("SEAT_ALREADY_TAKEN"
                                                                        .equals(body.path("errorCode").asText())) {
                                                                status409SeatTakenCount.incrementAndGet();
                                                        } else {
                                                                otherStatuses.merge(status, 1, Integer::sum);
                                                        }
                                                } else {
                                                        otherStatuses.merge(status, 1, Integer::sum);
                                                }
                                        } catch (Exception e) {
                                                throw new RuntimeException(e);
                                        }
                                }));
                        }

                        assertThat(ready.await(15, TimeUnit.SECONDS)).isTrue();
                        start.countDown();

                        for (Future<?> f : futures) {
                                f.get(30, TimeUnit.SECONDS);
                        }
                } finally {
                        pool.shutdownNow();
                }

                assertThat(otherStatuses).isEmpty();
                assertThat(status201Count.get()).isEqualTo(1);
                assertThat(status409SeatTakenCount.get()).isEqualTo(99);

                Integer activeItems = jdbcTemplate.queryForObject(
                                "SELECT COUNT(*) FROM reservation_items WHERE event_id = ? AND seat_id = ? AND status IN ('HELD', 'CONFIRMED')",
                                Integer.class, eventId, seatId);
                assertThat(activeItems).isEqualTo(1);

                Integer reservationsCount = jdbcTemplate.queryForObject(
                                "SELECT COUNT(*) FROM reservations WHERE event_id = ?",
                                Integer.class, eventId);
                assertThat(reservationsCount).isEqualTo(1);

                SeatRecord seatAfter = seatRepository.findById(seatId).orElseThrow();
                assertThat(seatAfter.status()).isEqualTo("HELD");
                assertThat(seatAfter.version()).isEqualTo(1L);
        }

        // -------------------------------------------------------------------------
        // 4. Two concurrent requests for overlapping seat sets ([A, B] and [B, C])
        // -------------------------------------------------------------------------

        @Test
        void twoConcurrentRequestsForOverlappingSeatSetsNeverBothSucceed() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatA = UUID.randomUUID();
                UUID seatB = UUID.randomUUID();
                UUID seatC = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatA, eventId, "SEC", "1", "A", 10000L, "AVAILABLE"),
                                seat(seatB, eventId, "SEC", "1", "B", 10000L, "AVAILABLE"),
                                seat(seatC, eventId, "SEC", "1", "C", 10000L, "AVAILABLE")), true);

                String bodyAB = objectMapper
                                .writeValueAsString(new CreateReservationRequest(eventId, List.of(seatA, seatB)));
                String bodyBC = objectMapper
                                .writeValueAsString(new CreateReservationRequest(eventId, List.of(seatB, seatC)));

                ExecutorService pool = Executors.newFixedThreadPool(2);
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);

                try {
                        Future<Integer> f1 = pool.submit(() -> {
                                ready.countDown();
                                start.await();
                                return mockMvc.perform(post("/api/v1/reservations")
                                                .header(CorrelationConstants.USER_ID_HEADER,
                                                                UUID.randomUUID().toString())
                                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                                UUID.randomUUID().toString())
                                                .contentType(MediaType.APPLICATION_JSON)
                                                .content(bodyAB))
                                                .andReturn().getResponse().getStatus();
                        });
                        Future<Integer> f2 = pool.submit(() -> {
                                ready.countDown();
                                start.await();
                                return mockMvc.perform(post("/api/v1/reservations")
                                                .header(CorrelationConstants.USER_ID_HEADER,
                                                                UUID.randomUUID().toString())
                                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                                UUID.randomUUID().toString())
                                                .contentType(MediaType.APPLICATION_JSON)
                                                .content(bodyBC))
                                                .andReturn().getResponse().getStatus();
                        });

                        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                        start.countDown();

                        int status1 = f1.get(10, TimeUnit.SECONDS);
                        int status2 = f2.get(10, TimeUnit.SECONDS);

                        assertThat(List.of(status1, status2)).containsExactlyInAnyOrder(201, 409);

                        Integer reservationsCount = jdbcTemplate.queryForObject(
                                        "SELECT COUNT(*) FROM reservations WHERE event_id = ?", Integer.class, eventId);
                        assertThat(reservationsCount).isEqualTo(1);
                } finally {
                        pool.shutdownNow();
                }
        }

        // -------------------------------------------------------------------------
        // 5. Failure injected inside transaction after Redis hold rolls back DB and
        // releases Redis keys
        // -------------------------------------------------------------------------

        @Test
        void failureInjectedInsideTransactionRollsBackDbAndReleasesRedisKeysSoSubsequentRequestSucceeds()
                        throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "SEC", "1", "1", 12000L, "AVAILABLE"),
                                seat(seat2, eventId, "SEC", "1", "2", 12000L, "AVAILABLE")), true);

                reservationService.setInTransactionHook(() -> {
                        throw new IllegalStateException("Simulated OutboxWriter / mid-transaction failure");
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seat1, seat2));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isInternalServerError())
                                .andExpect(jsonPath("$.errorCode", is("INTERNAL_SERVER_ERROR")));

                // Verify 0 reservations, 0 items, 0 outbox rows, seats still AVAILABLE, and
                // Redis keys released
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations", Integer.class)).isZero();
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservation_items", Integer.class))
                                .isZero();
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Integer.class)).isZero();
                assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(seatRepository.findById(seat2).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isNull();
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat2))).isNull();

                // Remove failure hook: subsequent request for the exact same seats succeeds
                // with 201!
                reservationService.setInTransactionHook(null);

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isCreated())
                                .andExpect(jsonPath("$.status", is("HELD")));
        }

        // -------------------------------------------------------------------------
        // 6. Simulated commit failure rolls back DB and releases Redis keys
        // -------------------------------------------------------------------------

        @Test
        void simulatedCommitFailureRollsBackDbAndReleasesRedisKeys() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 12000L, "AVAILABLE")), true);

                reservationService.setBeforeCommitHook(() -> {
                        throw new RuntimeException(
                                        "Simulated PostgreSQL commit failure in beforeCommit synchronization");
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isInternalServerError());

                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations", Integer.class)).isZero();
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservation_items", Integer.class))
                                .isZero();
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Integer.class)).isZero();
                assertThat(seatRepository.findById(seatId).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId))).isNull();

                // Clear hook and verify seat can be held cleanly
                reservationService.setBeforeCommitHook(null);
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isCreated());
        }

        // -------------------------------------------------------------------------
        // 7. Replay with same Idempotency-Key and same body returns identical 201
        // response
        // -------------------------------------------------------------------------

        @Test
        void replayWithSameIdempotencyKeyAndSameBodyReturnsIdentical201ResponseAndCreatesNoSecondReservation()
                        throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 18000L, "AVAILABLE")), true);

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
                String jsonBody = objectMapper.writeValueAsString(request);

                MvcResult firstCall = mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(jsonBody))
                                .andExpect(status().isCreated())
                                .andExpect(header().doesNotExist(ReservationController.IDEMPOTENT_REPLAY_HEADER))
                                .andReturn();

                MvcResult secondCall = mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(jsonBody))
                                .andExpect(status().isCreated())
                                .andExpect(header().string(ReservationController.IDEMPOTENT_REPLAY_HEADER, "true"))
                                .andReturn();

                ReservationResponse firstResp = objectMapper.readValue(
                                firstCall.getResponse().getContentAsString(), ReservationResponse.class);
                ReservationResponse secondResp = objectMapper.readValue(
                                secondCall.getResponse().getContentAsString(), ReservationResponse.class);

                assertThat(secondResp).isEqualTo(firstResp);
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations", Integer.class))
                                .isEqualTo(1);
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Integer.class))
                                .isEqualTo(1);
        }

        // -------------------------------------------------------------------------
        // 8. Same Idempotency-Key with different body returns 422
        // IDEMPOTENCY_KEY_PAYLOAD_MISMATCH
        // and concurrent IN_PROGRESS returns 409 IDEMPOTENCY_REQUEST_IN_PROGRESS
        // -------------------------------------------------------------------------

        @Test
        void sameIdempotencyKeyWithDifferentBodyReturns422AndConcurrentInProgressReturns409() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "SEC", "1", "1", 10000L, "AVAILABLE"),
                                seat(seat2, eventId, "SEC", "1", "2", 10000L, "AVAILABLE")), true);

                // First call with seat1 succeeds
                CreateReservationRequest req1 = new CreateReservationRequest(eventId, List.of(seat1));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(req1)))
                                .andExpect(status().isCreated());

                // Second call with SAME idempotencyKey but DIFFERENT body (seat2) -> 422
                // IDEMPOTENCY_KEY_PAYLOAD_MISMATCH
                CreateReservationRequest req2 = new CreateReservationRequest(eventId, List.of(seat2));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(req2)))
                                .andExpect(status().isUnprocessableEntity())
                                .andExpect(jsonPath("$.status", is(422)))
                                .andExpect(jsonPath("$.errorCode", is("IDEMPOTENCY_KEY_PAYLOAD_MISMATCH")));

                // Simulate an active IN_PROGRESS claim for seat2 and verify a concurrent
                // duplicate returns 409 with Retry-After: 1
                UUID inProgressKey = UUID.randomUUID();
                CountDownLatch insideTx = new CountDownLatch(1);
                CountDownLatch releaseTx = new CountDownLatch(1);

                reservationService.setInTransactionHook(() -> {
                        insideTx.countDown();
                        try {
                                releaseTx.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                        }
                });

                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                        String req2Json = objectMapper.writeValueAsString(req2);
                        Future<MvcResult> primaryFuture = pool.submit(() -> mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER, inProgressKey.toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(req2Json))
                                        .andReturn());

                        assertThat(insideTx.await(5, TimeUnit.SECONDS)).isTrue();

                        // Concurrent duplicate while primary is IN_PROGRESS -> 409
                        // IDEMPOTENCY_REQUEST_IN_PROGRESS + Retry-After: 1
                        mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER, inProgressKey.toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(req2Json))
                                        .andExpect(status().isConflict())
                                        .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                                        .andExpect(jsonPath("$.status", is(409)))
                                        .andExpect(jsonPath("$.errorCode", is("IDEMPOTENCY_REQUEST_IN_PROGRESS")));

                        releaseTx.countDown();
                        assertThat(primaryFuture.get(10, TimeUnit.SECONDS).getResponse().getStatus()).isEqualTo(201);
                } finally {
                        reservationService.setInTransactionHook(null);
                        pool.shutdownNow();
                }
        }

        // -------------------------------------------------------------------------
        // 9. Event not yet open (drop_time in the future) returns 409 EVENT_NOT_OPEN
        // -------------------------------------------------------------------------

        @Test
        void eventNotYetOpenReturns409EventNotOpen() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

                transactionTemplate.executeWithoutResult(status -> {
                        eventRepository.insert(new EventRecord(
                                        eventId, "Future Drop Concert", "Arena",
                                        now.plus(10, ChronoUnit.DAYS),
                                        now.plus(2, ChronoUnit.HOURS), // dropTime in the future
                                        "SCHEDULED", now, now));
                        seatRepository.bulkInsert(List.of(
                                        seat(seatId, eventId, "SEC", "1", "1", 10000L, "AVAILABLE")));
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.status", is(409)))
                                .andExpect(jsonPath("$.errorCode", is("EVENT_NOT_OPEN")));

                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId))).isNull();
        }

        // -------------------------------------------------------------------------
        // 10. Redis flushed before call leads to EVENT_NOT_CACHED -> rewarm ->
        // successful hold
        // -------------------------------------------------------------------------

        @Test
        void redisFlushedBeforeCallTriggersRewarmAndSucceeds() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID soldSeatId = UUID.randomUUID();
                UUID availableSeatId = UUID.randomUUID();

                // Seed PostgreSQL WITHOUT warming Redis (simulating cold/flushed Redis)
                seedOpenEventWithSeats(eventId, List.of(
                                seat(soldSeatId, eventId, "SEC", "1", "1", 10000L, "SOLD"),
                                seat(availableSeatId, eventId, "SEC", "1", "2", 10000L, "AVAILABLE")), false);

                assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(eventId))).isFalse();

                // 1. Attempting to hold availableSeatId triggers EVENT_NOT_CACHED ->
                // rewarmEvent -> 201 HELD
                CreateReservationRequest availableReq = new CreateReservationRequest(eventId, List.of(availableSeatId));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(availableReq)))
                                .andExpect(status().isCreated())
                                .andExpect(jsonPath("$.status", is("HELD")));

                assertThat(stringRedisTemplate.hasKey(RedisKeys.eventDropTsKey(eventId))).isTrue();
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, soldSeatId)))
                                .isEqualTo(RedisKeys.SOLD_MARKER);

                // 2. Attempting to hold soldSeatId is rejected by Redis because rewarm restored
                // its SOLD marker
                CreateReservationRequest soldReq = new CreateReservationRequest(eventId, List.of(soldSeatId));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(soldReq)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));
        }

        // -------------------------------------------------------------------------
        // 11. Seats already SOLD or HELD in PostgreSQL but missing from Redis are
        // rejected
        // by PostgreSQL and Redis is compensated
        // -------------------------------------------------------------------------

        @Test
        void seatsAlreadySoldOrHeldInPostgresButMissingFromRedisAreRejectedByPostgresAndRedisIsCompensated()
                        throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID soldSeatInPg = UUID.randomUUID();
                UUID heldSeatInPg = UUID.randomUUID();
                UUID existingResId = UUID.randomUUID();
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

                transactionTemplate.executeWithoutResult(status -> {
                        eventRepository.insert(new EventRecord(
                                        eventId, "Drift Test Event", "Stadium",
                                        now.plus(7, ChronoUnit.DAYS),
                                        now.minus(1, ChronoUnit.HOURS),
                                        "ON_SALE", now, now));
                        seatRepository.bulkInsert(List.of(
                                        new SeatRecord(soldSeatInPg, eventId, "SEC", "1", "1", 10000L, "USD", "SOLD",
                                                        1L, now, now),
                                        new SeatRecord(heldSeatInPg, eventId, "SEC", "1", "2", 10000L, "USD", "HELD",
                                                        1L, now, now)));
                        reservationRepository.insert(
                                        new ReservationRecord(
                                                        existingResId, eventId, UUID.randomUUID(), "HELD",
                                                        10000L, "USD", now, now.plus(300, ChronoUnit.SECONDS), null,
                                                        null, null, now),
                                        List.of(new ReservationItemRecord(
                                                        UUID.randomUUID(), existingResId, eventId, heldSeatInPg,
                                                        "SEC", "1", "2", 10000L, "HELD", now, now)));
                });

                // Cache ONLY event:{eventId}:drop_ts in Redis, leaving both seat keys missing
                // from Redis!
                stringRedisTemplate.opsForValue().set(
                                RedisKeys.eventDropTsKey(eventId),
                                String.valueOf(now.minus(1, ChronoUnit.HOURS).toEpochMilli()),
                                Duration.ofSeconds(RedisKeys.EVENT_DROP_TS_TTL_SECONDS));
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, soldSeatInPg))).isNull();
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, heldSeatInPg))).isNull();

                double compensationsBefore = meterRegistry.find("inventory_redis_compensations_total").counter() != null
                                ? meterRegistry.find("inventory_redis_compensations_total").counter().count()
                                : 0.0;

                // 1. Requesting soldSeatInPg: passes Redis hold, rejected by PostgreSQL guarded
                // seat update (status != AVAILABLE),
                // rolls back DB and compensates (releases) Redis key!
                CreateReservationRequest soldRequest = new CreateReservationRequest(eventId, List.of(soldSeatInPg));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(soldRequest)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.status", is(409)))
                                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));

                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, soldSeatInPg))).isNull();

                // 2. Requesting heldSeatInPg: passes Redis hold, rejected by PostgreSQL
                // idx_reservation_items_active_seat
                // (and/or guarded seat update), rolls back DB and compensates (releases) Redis
                // key!
                CreateReservationRequest heldRequest = new CreateReservationRequest(eventId, List.of(heldSeatInPg));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(heldRequest)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.status", is(409)))
                                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));

                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, heldSeatInPg))).isNull();

                double compensationsAfter = meterRegistry.find("inventory_redis_compensations_total").counter().count();
                assertThat(compensationsAfter - compensationsBefore).isEqualTo(2.0);
        }

        // -------------------------------------------------------------------------
        // Additional Header & Bean Validation Tests
        // -------------------------------------------------------------------------

        @Test
        void missingHeaderAndValidationErrorsReturnDocumentedErrorCodes() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                CreateReservationRequest validBody = new CreateReservationRequest(eventId, List.of(seatId));

                // 1. Missing X-User-Id -> 401 UNAUTHORIZED
                mockMvc.perform(post("/api/v1/reservations")
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(validBody)))
                                .andExpect(status().isUnauthorized())
                                .andExpect(jsonPath("$.status", is(401)))
                                .andExpect(jsonPath("$.errorCode", is("UNAUTHORIZED")));

                // 2. Missing Idempotency-Key -> 400 MISSING_IDEMPOTENCY_KEY
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(validBody)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.status", is(400)))
                                .andExpect(jsonPath("$.errorCode", is("MISSING_IDEMPOTENCY_KEY")));

                // 3. More than 6 seats -> 400 VALIDATION_ERROR
                CreateReservationRequest sevenSeats = new CreateReservationRequest(eventId, List.of(
                                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(sevenSeats)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.status", is(400)))
                                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));

                // 4. Duplicate seatIds in request -> 400 VALIDATION_ERROR
                CreateReservationRequest duplicateSeats = new CreateReservationRequest(eventId,
                                List.of(seatId, seatId));
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(duplicateSeats)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.status", is(400)))
                                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));
        }

        // -------------------------------------------------------------------------
        // 12. Exception thrown after database commit leaves reservation HELD in
        // PostgreSQL
        // and Redis seat key still set
        // -------------------------------------------------------------------------

        @Test
        void exceptionThrownAfterDatabaseCommitLeavesReservationHeldAndRedisKeyStillSet() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 12000L, "AVAILABLE")), true);

                reservationService.setAfterCommitHook(() -> {
                        throw new RuntimeException("Simulated network drop after PostgreSQL commit succeeded");
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isInternalServerError());

                // Verify reservation committed as HELD in PostgreSQL and Redis seat key was NOT
                // released
                List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                                "SELECT reservation_id, status FROM reservations WHERE event_id = ?", eventId);
                assertThat(rows).hasSize(1);
                UUID committedReservationId = (UUID) rows.getFirst().get("reservation_id");
                assertThat(rows.getFirst().get("status")).isEqualTo("HELD");
                assertThat(seatRepository.findById(seatId).orElseThrow().status()).isEqualTo("HELD");
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId)))
                                .isEqualTo(committedReservationId.toString());
        }

        // -------------------------------------------------------------------------
        // 13. When post-commit status read also fails, Redis key is not released
        // -------------------------------------------------------------------------

        @Test
        void whenPostCommitStatusReadAlsoFailsRedisKeyIsNotReleased() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 12000L, "AVAILABLE")), true);

                reservationService.setInTransactionHook(() -> {
                        throw new RuntimeException("Simulated transaction error");
                });
                reservationService.setBeforeCompensationStatusReadHook(() -> {
                        throw new RuntimeException("Simulated database connectivity failure during status check");
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isInternalServerError());

                // Because the status check failed, the Redis lock is NOT released (covered by
                // 660s TTL)
                String redisVal = stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId));
                assertThat(redisVal).isNotNull();
                assertThat(UUID.fromString(redisVal)).isNotNull();
        }

        // -------------------------------------------------------------------------
        // 14. Interrupt flag set inside transaction failure hook still deletes Redis
        // key
        // and preserves the interrupt flag
        // -------------------------------------------------------------------------

        @Test
        void interruptFlagSetInsideTransactionFailureHookDeletesRedisKeyAndPreservesFlag() {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 12000L, "AVAILABLE")), true);

                reservationService.setInTransactionHook(() -> {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Simulated failure while thread is interrupted");
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                boolean interruptPreserved = false;
                try {
                        assertThatThrownBy(() -> reservationService.createReservation(userId, idempotencyKey, request))
                                        .isInstanceOf(IllegalStateException.class);
                        interruptPreserved = Thread.interrupted();
                } finally {
                        // Ensure test thread interrupt flag is cleared even on assertion failure
                        Thread.interrupted();
                }

                assertThat(interruptPreserved).isTrue();
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seatId))).isNull();
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations", Integer.class)).isZero();
                assertThat(seatRepository.findById(seatId).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)).isEmpty();
        }

        // -------------------------------------------------------------------------
        // 15. Rewarm with reservation confirmed between PG read and Redis write leaves
        // seat key as SOLD with no TTL
        // -------------------------------------------------------------------------

        @Test
        void rewarmWithReservationConfirmedBetweenPgReadAndRedisWriteLeavesSeatKeyAsSoldWithNoTtl() {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                UUID reservationId = UUID.randomUUID();
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

                transactionTemplate.executeWithoutResult(status -> {
                        eventRepository.insert(new EventRecord(
                                        eventId, "Confirmed During Rewarm Event", "Venue",
                                        now.plus(1, ChronoUnit.DAYS), now.minus(1, ChronoUnit.HOURS), "ON_SALE", now,
                                        now));
                        seatRepository.bulkInsert(List.of(
                                        new SeatRecord(seatId, eventId, "SEC", "1", "1", 10000L, "USD", "HELD", 1L, now,
                                                        now)));
                        reservationRepository.insert(
                                        new ReservationRecord(
                                                        reservationId, eventId, UUID.randomUUID(), "HELD",
                                                        10000L, "USD", now, now.plus(300, ChronoUnit.SECONDS), null,
                                                        null, null, now),
                                        List.of(new ReservationItemRecord(
                                                        UUID.randomUUID(), reservationId, eventId, seatId,
                                                        "SEC", "1", "1", 10000L, "HELD", now, now)));
                });

                redisRewarmService.setAfterHeldLocksReadHook(() -> transactionTemplate.executeWithoutResult(status -> {
                        reservationRepository.updateStatus(reservationId, "CONFIRMED");
                        jdbcTemplate.update("UPDATE seats SET status = 'SOLD', version = version + 1 WHERE seat_id = ?",
                                        seatId);
                }));

                boolean rewarmed = redisRewarmService.rewarmEvent(eventId);
                assertThat(rewarmed).isTrue();

                String seatKey = RedisKeys.seatKey(eventId, seatId);
                assertThat(stringRedisTemplate.opsForValue().get(seatKey)).isEqualTo(RedisKeys.SOLD_MARKER);
                assertThat(stringRedisTemplate.getExpire(seatKey)).isEqualTo(-1L);
        }

        // -------------------------------------------------------------------------
        // 16. Stale IN_PROGRESS row reclaimed by second request while first is paused
        // cannot be completed or deleted by first, and second ends with 201 and
        // COMPLETED
        // -------------------------------------------------------------------------

        @Test
        void staleInProgressRowReclaimedBySecondRequestWhileFirstIsPausedCannotBeCompletedOrDeletedByFirst()
                        throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey1 = UUID.randomUUID();
                UUID idempotencyKey2 = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "SEC", "1", "1", 10000L, "AVAILABLE"),
                                seat(seat2, eventId, "SEC", "1", "2", 10000L, "AVAILABLE")), true);

                // Part A: Request 1 is paused inside transaction right before markCompleted;
                // its claim becomes stale and is
                // reclaimed, causing Request 1's markCompleted to update 0 rows, roll back PG,
                // compensate Redis, and not delete
                // the reclaimed row; Request 2 then completes with 201 and a COMPLETED record.
                AtomicBoolean firstInTx = new AtomicBoolean(true);
                reservationService.setInTransactionHook(() -> {
                        if (firstInTx.compareAndSet(true, false)) {
                                jdbcTemplate.update(
                                                "UPDATE idempotency_keys SET created_at = NOW() - INTERVAL '31 seconds' WHERE user_id = ? AND idempotency_key = ?",
                                                userId, idempotencyKey1);
                                assertThat(idempotencyKeyRepository.reclaimStaleInProgress(userId, idempotencyKey1))
                                                .isPresent();
                        }
                });

                CreateReservationRequest reqSeat1 = new CreateReservationRequest(eventId, List.of(seat1));
                String reqSeat1Json = objectMapper.writeValueAsString(reqSeat1);

                // Request 1 loses its claim at markCompleted -> rolls back reservation,
                // releases Redis lock, and cannot delete reclaimed row
                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey1.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(reqSeat1Json))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.errorCode", is("IDEMPOTENCY_REQUEST_IN_PROGRESS")));

                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations", Integer.class)).isZero();
                assertThat(seatRepository.findById(seat1).orElseThrow().status()).isEqualTo("AVAILABLE");
                assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.seatKey(eventId, seat1))).isNull();
                assertThat(idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey1)).isPresent();

                reservationService.setInTransactionHook(null);

                // Part B: Request 1 is paused at afterIdempotencyClaimHook while Request 2
                // reclaims the stale row and finishes
                // with 201 Created and a COMPLETED record; when Request 1 resumes and fails, it
                // cannot delete Request 2's row.
                CountDownLatch req1PausedAfterClaim = new CountDownLatch(1);
                CountDownLatch allowReq1ToResume = new CountDownLatch(1);
                AtomicBoolean pauseFirstClaim = new AtomicBoolean(true);

                reservationService.setAfterIdempotencyClaimHook(() -> {
                        if (pauseFirstClaim.compareAndSet(true, false)) {
                                req1PausedAfterClaim.countDown();
                                try {
                                        allowReq1ToResume.await(5, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                }
                        }
                });

                CreateReservationRequest reqSeat2 = new CreateReservationRequest(eventId, List.of(seat2));
                String reqSeat2Json = objectMapper.writeValueAsString(reqSeat2);

                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                        Future<MvcResult> req1Future = pool.submit(() -> mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                        idempotencyKey2.toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(reqSeat2Json))
                                        .andReturn());

                        assertThat(req1PausedAfterClaim.await(5, TimeUnit.SECONDS)).isTrue();

                        // Backdate Request 1's IN_PROGRESS row so Request 2 can reclaim it
                        // deterministically
                        jdbcTemplate.update(
                                        "UPDATE idempotency_keys SET created_at = NOW() - INTERVAL '31 seconds' WHERE user_id = ? AND idempotency_key = ?",
                                        userId, idempotencyKey2);

                        // Request 2 reclaims the stale row and completes with 201 Created
                        MvcResult req2Result = mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                        idempotencyKey2.toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(reqSeat2Json))
                                        .andExpect(status().isCreated())
                                        .andReturn();

                        ReservationResponse req2Response = objectMapper.readValue(
                                        req2Result.getResponse().getContentAsString(), ReservationResponse.class);

                        // Resume Request 1: it sees seat2 already taken (409) and its finally block
                        // cannot delete Request 2's COMPLETED row
                        allowReq1ToResume.countDown();
                        MvcResult req1Result = req1Future.get(10, TimeUnit.SECONDS);
                        assertThat(req1Result.getResponse().getStatus()).isEqualTo(409);

                        IdempotencyKeyRecord finalRecord = idempotencyKeyRepository
                                        .findByUserAndKey(userId, idempotencyKey2).orElseThrow();
                        assertThat(finalRecord.status()).isEqualTo("COMPLETED");
                        assertThat(finalRecord.responseStatus()).isEqualTo(201);
                        ReservationResponse storedResponse = objectMapper.readValue(finalRecord.responseBody(),
                                        ReservationResponse.class);
                        assertThat(storedResponse).isEqualTo(req2Response);
                } finally {
                        reservationService.setAfterIdempotencyClaimHook(null);
                        pool.shutdownNow();
                }
        }

        // -------------------------------------------------------------------------
        // 17. 409 rejection leaves no idempotency row and retry with same key
        // re-executes
        // -------------------------------------------------------------------------

        @Test
        void rejection409LeavesNoIdempotencyRowAndRetryWithSameKeyReExecutes() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();
                UUID userId = UUID.randomUUID();
                UUID idempotencyKey = UUID.randomUUID();
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

                // Seed event with drop_time in the future -> 409 EVENT_NOT_OPEN
                transactionTemplate.executeWithoutResult(status -> {
                        eventRepository.insert(new EventRecord(
                                        eventId, "Soon Open Event", "Arena",
                                        now.plus(7, ChronoUnit.DAYS),
                                        now.plus(1, ChronoUnit.HOURS),
                                        "SCHEDULED", now, now));
                        seatRepository.bulkInsert(List.of(
                                        seat(seatId, eventId, "SEC", "1", "1", 10000L, "AVAILABLE")));
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));
                String jsonBody = objectMapper.writeValueAsString(request);

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(jsonBody))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.errorCode", is("EVENT_NOT_OPEN")));

                // Verify 409 rejection left no idempotency_keys row
                assertThat(idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)).isEmpty();

                // Move drop_time to the past and warm Redis
                jdbcTemplate.update(
                                "UPDATE events SET drop_time = ?, status = 'ON_SALE' WHERE event_id = ?",
                                java.sql.Timestamp.from(now.minus(5, ChronoUnit.MINUTES)), eventId);
                redisRewarmService.rewarmEvent(eventId);

                // Retry with the exact same Idempotency-Key -> re-executes and returns 201
                // Created
                MvcResult retryResult = mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(jsonBody))
                                .andExpect(status().isCreated())
                                .andExpect(header().doesNotExist(ReservationController.IDEMPOTENT_REPLAY_HEADER))
                                .andReturn();

                // Replay of the 201 returns the identical stored response with
                // X-Idempotent-Replay: true
                MvcResult replayResult = mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, userId.toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, idempotencyKey.toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(jsonBody))
                                .andExpect(status().isCreated())
                                .andExpect(header().string(ReservationController.IDEMPOTENT_REPLAY_HEADER, "true"))
                                .andReturn();

                assertThat(replayResult.getResponse().getContentAsString())
                                .isEqualTo(retryResult.getResponse().getContentAsString());
        }

        // -------------------------------------------------------------------------
        // 18. Event cancelled during rewarm returns 409 EVENT_NOT_OPEN
        // -------------------------------------------------------------------------

        @Test
        void eventCancelledDuringRewarmReturns409EventNotOpen() throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seatId = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seatId, eventId, "SEC", "1", "1", 10000L, "AVAILABLE")), false);

                redisRewarmService.setInsideRewarmHook(() -> {
                        jdbcTemplate.update("UPDATE events SET status = 'CANCELLED' WHERE event_id = ?", eventId);
                });

                CreateReservationRequest request = new CreateReservationRequest(eventId, List.of(seatId));

                mockMvc.perform(post("/api/v1/reservations")
                                .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                .header(ReservationController.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.status", is(409)))
                                .andExpect(jsonPath("$.errorCode", is("EVENT_NOT_OPEN")));
        }

        // -------------------------------------------------------------------------
        // 19. Two concurrent hold transactions for the same seat with Redis bypassed
        // produce one 201 and one 409 and no deadlock error
        // -------------------------------------------------------------------------

        @Test
        void twoConcurrentHoldTransactionsForSameSeatWithRedisBypassedProduceOne201AndOne409WithoutDeadlock()
                        throws Exception {
                UUID eventId = UUID.randomUUID();
                UUID seat1 = UUID.randomUUID();
                UUID seat2 = UUID.randomUUID();

                seedOpenEventWithSeats(eventId, List.of(
                                seat(seat1, eventId, "SEC", "1", "1", 10000L, "AVAILABLE"),
                                seat(seat2, eventId, "SEC", "1", "2", 10000L, "AVAILABLE")), true);

                CountDownLatch bothPassedRedis = new CountDownLatch(2);
                AtomicBoolean firstClearedRedis = new AtomicBoolean(false);

                reservationService.setAfterRedisHoldHook(() -> {
                        if (firstClearedRedis.compareAndSet(false, true)) {
                                stringRedisTemplate.delete(List.of(
                                                RedisKeys.seatKey(eventId, seat1),
                                                RedisKeys.seatKey(eventId, seat2)));
                        }
                        bothPassedRedis.countDown();
                        try {
                                bothPassedRedis.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                        }
                });

                String reqJson = objectMapper
                                .writeValueAsString(new CreateReservationRequest(eventId, List.of(seat1, seat2)));

                ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                        Future<MvcResult> f1 = pool.submit(() -> mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                        UUID.randomUUID().toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(reqJson))
                                        .andReturn());

                        Future<MvcResult> f2 = pool.submit(() -> mockMvc.perform(post("/api/v1/reservations")
                                        .header(CorrelationConstants.USER_ID_HEADER, UUID.randomUUID().toString())
                                        .header(ReservationController.IDEMPOTENCY_KEY_HEADER,
                                                        UUID.randomUUID().toString())
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(reqJson))
                                        .andReturn());

                        MvcResult r1 = f1.get(10, TimeUnit.SECONDS);
                        MvcResult r2 = f2.get(10, TimeUnit.SECONDS);

                        assertThat(List.of(r1.getResponse().getStatus(), r2.getResponse().getStatus()))
                                        .containsExactlyInAnyOrder(201, 409);

                        MvcResult conflictResult = r1.getResponse().getStatus() == 409 ? r1 : r2;
                        JsonNode conflictBody = objectMapper
                                        .readTree(conflictResult.getResponse().getContentAsString());
                        assertThat(conflictBody.path("errorCode").asText()).isEqualTo("SEAT_ALREADY_TAKEN");

                        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservations WHERE event_id = ?",
                                        Integer.class, eventId))
                                        .isEqualTo(1);
                } finally {
                        reservationService.setAfterRedisHoldHook(null);
                        pool.shutdownNow();
                }
        }

        // -------------------------------------------------------------------------
        // Helpers
        // -------------------------------------------------------------------------

        private void seedOpenEventWithSeats(UUID eventId, List<SeatRecord> seats, boolean warmRedis) {
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                transactionTemplate.executeWithoutResult(status -> {
                        eventRepository.insert(new EventRecord(
                                        eventId,
                                        "Championship Finals",
                                        "Main Arena",
                                        now.plus(7, ChronoUnit.DAYS),
                                        now.minus(5, ChronoUnit.MINUTES),
                                        "ON_SALE",
                                        now,
                                        now));
                        seatRepository.bulkInsert(seats);
                });
                if (warmRedis) {
                        redisRewarmService.rewarmEvent(eventId);
                }
        }

        private SeatRecord seat(
                        UUID seatId,
                        UUID eventId,
                        String section,
                        String row,
                        String seatNumber,
                        long priceCents,
                        String status) {
                Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                long version = "AVAILABLE".equals(status) ? 0L : 1L;
                return new SeatRecord(seatId, eventId, section, row, seatNumber, priceCents, "USD", status, version,
                                now, now);
        }
}

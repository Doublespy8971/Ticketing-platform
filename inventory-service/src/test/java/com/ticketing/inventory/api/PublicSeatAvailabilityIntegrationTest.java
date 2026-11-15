package com.ticketing.inventory.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.error.ErrorCode;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class PublicSeatAvailabilityIntegrationTest {

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
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("ticketing.sweeper.enabled", () -> "false");
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
    private ReservationRepository reservationRepository;

    @Autowired
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;

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
    }

    private EventRecord createTestEvent(String status) {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        EventRecord event = new EventRecord(
                eventId,
                "Seat Availability Concert",
                "Grand Arena",
                now.plus(1, ChronoUnit.DAYS),
                now.minus(1, ChronoUnit.HOURS),
                status != null ? status : "ON_SALE",
                now,
                now
        );
        transactionTemplate.executeWithoutResult(s -> eventRepository.insert(event));
        return event;
    }

    private void insertSeats(List<SeatRecord> seats) {
        transactionTemplate.executeWithoutResult(s -> seatRepository.bulkInsert(seats));
    }

    @Test
    void seatsOfEventOrderedBySectionRowAndSeatNumber() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        // Insert seats intentionally out of order
        List<SeatRecord> seatsToInsert = List.of(
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-B", "02", "10", 12000, "USD", "AVAILABLE", 0, Instant.now(), Instant.now()),
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "02", "01", 10000, "USD", "AVAILABLE", 0, Instant.now(), Instant.now()),
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "01", "02", 10000, "USD", "AVAILABLE", 0, Instant.now(), Instant.now()),
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "01", "01", 10000, "USD", "AVAILABLE", 0, Instant.now(), Instant.now())
        );
        insertSeats(seatsToInsert);

        MvcResult result = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.CACHE_CONTROL, "max-age=1"))
                .andExpect(jsonPath("$.eventId", is(eventId.toString())))
                .andReturn();

        JsonNode root = objectMapper.readTree(result.getResponse().getContentAsString());
        JsonNode items = root.get("items");
        assertThat(items).hasSize(4);

        // Expected sorted order:
        // 1. SEC-A, row 01, seat 01
        // 2. SEC-A, row 01, seat 02
        // 3. SEC-A, row 02, seat 01
        // 4. SEC-B, row 02, seat 10
        assertThat(items.get(0).get("sectionCode").asText()).isEqualTo("SEC-A");
        assertThat(items.get(0).get("rowLabel").asText()).isEqualTo("01");
        assertThat(items.get(0).get("seatNumber").asText()).isEqualTo("01");

        assertThat(items.get(1).get("sectionCode").asText()).isEqualTo("SEC-A");
        assertThat(items.get(1).get("rowLabel").asText()).isEqualTo("01");
        assertThat(items.get(1).get("seatNumber").asText()).isEqualTo("02");

        assertThat(items.get(2).get("sectionCode").asText()).isEqualTo("SEC-A");
        assertThat(items.get(2).get("rowLabel").asText()).isEqualTo("02");
        assertThat(items.get(2).get("seatNumber").asText()).isEqualTo("01");

        assertThat(items.get(3).get("sectionCode").asText()).isEqualTo("SEC-B");
        assertThat(items.get(3).get("rowLabel").asText()).isEqualTo("02");
        assertThat(items.get(3).get("seatNumber").asText()).isEqualTo("10");
    }

    @Test
    void statusFilter() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        List<SeatRecord> seats = List.of(
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "1", "1", 10000, "USD", "AVAILABLE", 0, Instant.now(), Instant.now()),
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "1", "2", 10000, "USD", "HELD", 0, Instant.now(), Instant.now()),
                new SeatRecord(UUID.randomUUID(), eventId, "SEC-A", "1", "3", 10000, "USD", "SOLD", 0, Instant.now(), Instant.now())
        );
        insertSeats(seats);

        // Filter AVAILABLE
        MvcResult availResult = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("status", "AVAILABLE"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode availItems = objectMapper.readTree(availResult.getResponse().getContentAsString()).get("items");
        assertThat(availItems).hasSize(1);
        assertThat(availItems.get(0).get("status").asText()).isEqualTo("AVAILABLE");

        // Filter HELD
        MvcResult heldResult = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("status", "HELD"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode heldItems = objectMapper.readTree(heldResult.getResponse().getContentAsString()).get("items");
        assertThat(heldItems).hasSize(1);
        assertThat(heldItems.get(0).get("status").asText()).isEqualTo("HELD");

        // Filter SOLD
        MvcResult soldResult = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("status", "SOLD"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode soldItems = objectMapper.readTree(soldResult.getResponse().getContentAsString()).get("items");
        assertThat(soldItems).hasSize(1);
        assertThat(soldItems.get(0).get("status").asText()).isEqualTo("SOLD");

        // Filter invalid status -> 400 VALIDATION_ERROR
        mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("status", "NOT_A_STATUS"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.VALIDATION_ERROR.name())));
    }

    @Test
    void limitAndCursorPagingAcrossMoreThanTwoPages() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        // Create 25 seats
        List<SeatRecord> seats = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            seats.add(new SeatRecord(
                    UUID.randomUUID(),
                    eventId,
                    "SEC-A",
                    "01",
                    String.format("%02d", i),
                    10000,
                    "USD",
                    "AVAILABLE",
                    0,
                    Instant.now(),
                    Instant.now()
            ));
        }
        insertSeats(seats);

        List<String> collectedSeatIds = new ArrayList<>();

        // Page 1: limit=10
        MvcResult page1Result = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("limit", "10"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode page1 = objectMapper.readTree(page1Result.getResponse().getContentAsString());
        JsonNode items1 = page1.get("items");
        assertThat(items1).hasSize(10);
        String cursor1 = page1.get("nextCursor").asText();
        assertThat(cursor1).isNotBlank();
        for (JsonNode item : items1) {
            collectedSeatIds.add(item.get("seatId").asText());
        }

        // Page 2: limit=10, cursor=cursor1
        MvcResult page2Result = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("limit", "10")
                        .param("cursor", cursor1))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode page2 = objectMapper.readTree(page2Result.getResponse().getContentAsString());
        JsonNode items2 = page2.get("items");
        assertThat(items2).hasSize(10);
        String cursor2 = page2.get("nextCursor").asText();
        assertThat(cursor2).isNotBlank();
        for (JsonNode item : items2) {
            collectedSeatIds.add(item.get("seatId").asText());
        }

        // Page 3: limit=10, cursor=cursor2
        MvcResult page3Result = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("limit", "10")
                        .param("cursor", cursor2))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode page3 = objectMapper.readTree(page3Result.getResponse().getContentAsString());
        JsonNode items3 = page3.get("items");
        assertThat(items3).hasSize(5);
        assertThat(page3.get("nextCursor").isNull()).isTrue();
        for (JsonNode item : items3) {
            collectedSeatIds.add(item.get("seatId").asText());
        }

        // Assert 25 distinct seats collected without duplicates or gaps
        assertThat(collectedSeatIds).hasSize(25);
        Set<String> uniqueIds = new HashSet<>(collectedSeatIds);
        assertThat(uniqueIds).hasSize(25);
    }

    @Test
    void unknownEventReturns404ResourceNotFound() throws Exception {
        UUID randomEventId = UUID.randomUUID();
        mockMvc.perform(get("/api/v1/events/{eventId}/seats", randomEventId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode", is(ErrorCode.RESOURCE_NOT_FOUND.name())));
    }

    @Test
    void limitAbove1000ClampedTo1000() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .param("limit", "1500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items").isArray());
    }

    @Test
    void responseHasNoUserOrReservationFields() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();
        UUID seatId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        SeatRecord seat = new SeatRecord(
                seatId,
                eventId,
                "SEC-A",
                "01",
                "01",
                15000,
                "USD",
                "HELD",
                0,
                Instant.now(),
                Instant.now()
        );
        insertSeats(List.of(seat));

        Instant now = Instant.now();
        ReservationRecord reservation = new ReservationRecord(
                reservationId,
                eventId,
                userId,
                "HELD",
                15000,
                "USD",
                now,
                now.plus(10, ChronoUnit.MINUTES),
                null,
                null,
                null,
                now
        );
        ReservationItemRecord item = new ReservationItemRecord(
                UUID.randomUUID(),
                reservationId,
                eventId,
                seatId,
                "SEC-A",
                "01",
                "01",
                15000,
                "HELD",
                now,
                now
        );
        transactionTemplate.execute(s -> {
            reservationRepository.insert(reservation, List.of(item));
            return null;
        });

        MvcResult result = mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId))
                .andExpect(status().isOk())
                .andReturn();

        String rawJson = result.getResponse().getContentAsString();
        JsonNode root = objectMapper.readTree(rawJson);

        // Check root and each seat item for absence of user or reservation fields
        Set<String> forbiddenFieldNames = Set.of(
                "userId", "user_id", "userIds",
                "reservationId", "reservation_id",
                "reservationItemId", "reservation_item_id",
                "expiresAt", "expires_at",
                "totalAmountCents", "total_amount_cents"
        );

        assertNoForbiddenFields(root, forbiddenFieldNames);
    }

    private void assertNoForbiddenFields(JsonNode node, Set<String> forbiddenFieldNames) {
        if (node.isObject()) {
            Iterator<String> fieldNames = node.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                assertThat(forbiddenFieldNames)
                        .as("Response must not expose user or reservation field: " + fieldName)
                        .doesNotContain(fieldName);
                assertNoForbiddenFields(node.get(fieldName), forbiddenFieldNames);
            }
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                assertNoForbiddenFields(element, forbiddenFieldNames);
            }
        }
    }

    @Test
    void worksWithoutAnyIdentityHeader() throws Exception {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        // Perform request strictly without Authorization, X-User-Id, X-User-Roles, etc.
        mockMvc.perform(get("/api/v1/events/{eventId}/seats", eventId)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    void explainVerifiesNoSequentialScanOn10000Seats() {
        EventRecord event = createTestEvent("ON_SALE");
        UUID eventId = event.eventId();

        // Populate 10,000 seats for the event
        jdbcTemplate.update("""
                INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status)
                SELECT
                    gen_random_uuid(),
                    ?::uuid,
                    'SEC-' || (i % 10),
                    'ROW-' || (i / 100),
                    'SEAT-' || (i % 100),
                    10000,
                    CASE WHEN i % 3 = 0 THEN 'AVAILABLE' WHEN i % 3 = 1 THEN 'HELD' ELSE 'SOLD' END
                FROM generate_series(1, 10000) AS i
                """, eventId.toString());

        jdbcTemplate.execute("ANALYZE seats");

        List<String> planLines = jdbcTemplate.queryForList("""
                EXPLAIN (FORMAT TEXT)
                SELECT seat_id, event_id, section_code, row_label, seat_number,
                       price_cents, currency, status, version, created_at, updated_at
                FROM seats
                WHERE event_id = ?::uuid
                  AND status = 'AVAILABLE'
                ORDER BY section_code ASC, row_label ASC, seat_number ASC, seat_id ASC
                LIMIT 200
                """, String.class, eventId.toString());

        String fullPlan = String.join("\n", planLines);

        // Verify the plan does NOT contain Seq Scan
        assertThat(fullPlan)
                .as("EXPLAIN plan must not use Seq Scan on seats table. Plan:\n" + fullPlan)
                .doesNotContain("Seq Scan");

        // Verify the plan used the index
        assertThat(fullPlan)
                .as("EXPLAIN plan must use index scan. Plan:\n" + fullPlan)
                .contains("Index Scan");
    }
}

package com.ticketing.inventory.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.inventory.api.dto.BulkCreateSeatsRequest;
import com.ticketing.inventory.api.dto.CreateEventRequest;
import com.ticketing.inventory.api.dto.CreateSeatItemRequest;
import com.ticketing.inventory.api.dto.UpdateEventStatusRequest;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.service.InventoryAdminService;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.support.TransactionTemplate;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class AdminEventControllerIntegrationTest {

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
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private InventoryAdminService adminService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void cleanTables() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        jdbcTemplate.execute("TRUNCATE TABLE reservation_items, reservations, seats, events CASCADE");
        try {
            stringRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
        } catch (Exception ignored) {
        }
    }

    @Test
    void createEventSucceedsForAdmin() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant eventTime = now.plus(30, ChronoUnit.DAYS);
        Instant dropTime = now.plus(5, ChronoUnit.DAYS);

        CreateEventRequest request = new CreateEventRequest(
                "Taylor Swift — The Eras Tour Finale",
                "MetLife Stadium",
                eventTime,
                dropTime
        );

        String correlationId = UUID.randomUUID().toString();

        mockMvc.perform(post("/api/v1/admin/events")
                        .header("X-User-Roles", "ADMIN")
                        .header(CorrelationConstants.HTTP_HEADER, correlationId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, correlationId))
                .andExpect(jsonPath("$.eventId").isNotEmpty())
                .andExpect(jsonPath("$.name", is("Taylor Swift — The Eras Tour Finale")))
                .andExpect(jsonPath("$.venueName", is("MetLife Stadium")))
                .andExpect(jsonPath("$.status", is("SCHEDULED")))
                .andExpect(jsonPath("$.eventTime").isNotEmpty())
                .andExpect(jsonPath("$.dropTime").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty());

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM events", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void createSeatsSucceedsForAdmin() throws Exception {
        UUID eventId = createTestEvent();

        BulkCreateSeatsRequest request = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L),
                new CreateSeatItemRequest("FLOOR-A", "1", "102", 15000L)
        ));

        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN,USER")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(header().exists(CorrelationConstants.HTTP_HEADER))
                .andExpect(jsonPath("$.eventId", is(eventId.toString())))
                .andExpect(jsonPath("$.createdCount", is(2)))
                .andExpect(jsonPath("$.skippedDuplicateCount", is(0)))
                .andExpect(jsonPath("$.seats", hasSize(2)))
                .andExpect(jsonPath("$.seats[0].sectionCode", is("FLOOR-A")))
                .andExpect(jsonPath("$.seats[0].rowLabel", is("1")))
                .andExpect(jsonPath("$.seats[0].seatNumber", is("101")))
                .andExpect(jsonPath("$.seats[0].priceCents", is(15000)))
                .andExpect(jsonPath("$.seats[0].status", is("AVAILABLE")))
                .andExpect(jsonPath("$.seats[1].seatNumber", is("102")));

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM seats WHERE event_id = ?", Integer.class, eventId);
        assertThat(count).isEqualTo(2);
    }

    @Test
    void duplicateSeatRejectionWithinSameRequest() throws Exception {
        UUID eventId = createTestEvent();

        // Two seats with identical sectionCode, rowLabel, seatNumber in the same payload
        BulkCreateSeatsRequest request = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L),
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L)
        ));

        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)))
                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")))
                .andExpect(jsonPath("$.title", is("Seat Already Taken")));

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM seats WHERE event_id = ?", Integer.class, eventId);
        assertThat(count).isZero();
    }

    @Test
    void duplicateSeatRejectionAgainstExistingSeats() throws Exception {
        UUID eventId = createTestEvent();

        // First batch: insert seat 101
        BulkCreateSeatsRequest batch1 = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L)
        ));
        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(batch1)))
                .andExpect(status().isCreated());

        // Second batch: try inserting seat 101 again alongside seat 102
        BulkCreateSeatsRequest batch2 = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L),
                new CreateSeatItemRequest("FLOOR-A", "1", "102", 15000L)
        ));
        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(batch2)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)))
                .andExpect(jsonPath("$.errorCode", is("SEAT_ALREADY_TAKEN")));

        // Verify only 1 seat exists (batch 2 rolled back)
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM seats WHERE event_id = ?", Integer.class, eventId);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void validationErrorsOnCreateEvent() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        // 1. dropTime >= eventTime
        CreateEventRequest invalidTimes = new CreateEventRequest(
                "Event", "Venue", now.plus(1, ChronoUnit.DAYS), now.plus(2, ChronoUnit.DAYS)
        );
        mockMvc.perform(post("/api/v1/admin/events")
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidTimes)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));

        // 2. Blank name
        CreateEventRequest blankName = new CreateEventRequest(
                "", "Venue", now.plus(2, ChronoUnit.DAYS), now.plus(1, ChronoUnit.DAYS)
        );
        mockMvc.perform(post("/api/v1/admin/events")
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blankName)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));
    }

    @Test
    void validationErrorsOnBulkCreateSeats() throws Exception {
        UUID eventId = createTestEvent();

        // 1. Empty seats list
        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seats\":[]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));

        // 2. Non-positive price
        BulkCreateSeatsRequest nonPositivePrice = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("SEC-A", "1", "1", 0L)
        ));
        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(nonPositivePrice)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));

        // 3. Blank section code
        BulkCreateSeatsRequest blankSection = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("   ", "1", "1", 1000L)
        ));
        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blankSection)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));
    }

    @Test
    void forbiddenForNonAdmin() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        CreateEventRequest request = new CreateEventRequest(
                "Concert", "Arena", now.plus(10, ChronoUnit.DAYS), now.plus(1, ChronoUnit.DAYS)
        );

        mockMvc.perform(post("/api/v1/admin/events")
                        .header("X-User-Roles", "USER")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status", is(403)))
                .andExpect(jsonPath("$.errorCode", is("FORBIDDEN")))
                .andExpect(jsonPath("$.title", is("Forbidden")));
    }

    @Test
    void unauthorizedWithoutCredentials() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        CreateEventRequest request = new CreateEventRequest(
                "Concert", "Arena", now.plus(10, ChronoUnit.DAYS), now.plus(1, ChronoUnit.DAYS)
        );

        // No X-User-Roles header
        mockMvc.perform(post("/api/v1/admin/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status", is(401)))
                .andExpect(jsonPath("$.errorCode", is("UNAUTHORIZED")))
                .andExpect(jsonPath("$.title", is("Unauthorized")));
    }

    @Test
    void correlationIdHeaderEchoedOnResponses() throws Exception {
        String sentCorrelationId = "44444444-5555-6666-7777-888888888888";

        // Unauthorized call still echoes the incoming correlation ID
        mockMvc.perform(post("/api/v1/admin/events")
                        .header(CorrelationConstants.HTTP_HEADER, sentCorrelationId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(header().string(CorrelationConstants.HTTP_HEADER, sentCorrelationId))
                .andExpect(jsonPath("$.correlationId", is(sentCorrelationId)));

        // Request without correlation ID gets a new correlation ID generated and echoed
        mockMvc.perform(post("/api/v1/admin/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(header().exists(CorrelationConstants.HTTP_HEADER));
    }

    @Test
    void bulkCreateSeatsReturnsNotFoundWhenEventDoesNotExist() throws Exception {
        UUID nonExistentEventId = UUID.randomUUID();
        BulkCreateSeatsRequest request = new BulkCreateSeatsRequest(List.of(
                new CreateSeatItemRequest("FLOOR-A", "1", "101", 15000L)
        ));

        mockMvc.perform(post("/api/v1/admin/events/{eventId}/seats/bulk", nonExistentEventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status", is(404)))
                .andExpect(jsonPath("$.errorCode", is("RESOURCE_NOT_FOUND")));
    }

    private UUID createTestEvent() {
        return createTestEventWithStatus("SCHEDULED");
    }

    private UUID createTestEventWithStatus(String status) {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        transactionTemplate.executeWithoutResult(s -> eventRepository.insert(new EventRecord(
                eventId,
                "Festival " + status,
                "Park",
                now.plus(14, ChronoUnit.DAYS),
                now.plus(2, ChronoUnit.DAYS),
                status,
                now,
                now
        )));
        return eventId;
    }

    private ResultActions patchStatus(UUID eventId, String status) throws Exception {
        return mockMvc.perform(patch("/api/v1/admin/events/{eventId}/status", eventId)
                .header("X-User-Roles", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new UpdateEventStatusRequest(status))));
    }

    private ResultActions patchStatusRaw(UUID eventId, String rawJson) throws Exception {
        return mockMvc.perform(patch("/api/v1/admin/events/{eventId}/status", eventId)
                .header("X-User-Roles", "ADMIN")
                .contentType(MediaType.APPLICATION_JSON)
                .content(rawJson));
    }

    private String getEventStatus(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT status FROM events WHERE event_id = ?", String.class, eventId);
    }

    @Test
    void updateEventStatus_allAllowedTransitionsWork() throws Exception {
        // 1. SCHEDULED -> ON_SALE
        UUID e1 = createTestEventWithStatus("SCHEDULED");
        patchStatus(e1, "ON_SALE")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("ON_SALE")))
                .andExpect(jsonPath("$.eventId", is(e1.toString())));
        assertThat(getEventStatus(e1)).isEqualTo("ON_SALE");

        // 2. SCHEDULED -> CANCELLED
        UUID e2 = createTestEventWithStatus("SCHEDULED");
        patchStatus(e2, "CANCELLED")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELLED")));
        assertThat(getEventStatus(e2)).isEqualTo("CANCELLED");

        // 3. ON_SALE -> SOLD_OUT
        UUID e3 = createTestEventWithStatus("ON_SALE");
        patchStatus(e3, "SOLD_OUT")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("SOLD_OUT")));
        assertThat(getEventStatus(e3)).isEqualTo("SOLD_OUT");

        // 4. ON_SALE -> CANCELLED
        UUID e4 = createTestEventWithStatus("ON_SALE");
        patchStatus(e4, "CANCELLED")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELLED")));
        assertThat(getEventStatus(e4)).isEqualTo("CANCELLED");

        // 5. ON_SALE -> COMPLETED
        UUID e5 = createTestEventWithStatus("ON_SALE");
        patchStatus(e5, "COMPLETED")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("COMPLETED")));
        assertThat(getEventStatus(e5)).isEqualTo("COMPLETED");

        // 6. SOLD_OUT -> ON_SALE
        UUID e6 = createTestEventWithStatus("SOLD_OUT");
        patchStatus(e6, "ON_SALE")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("ON_SALE")));
        assertThat(getEventStatus(e6)).isEqualTo("ON_SALE");

        // 7. SOLD_OUT -> COMPLETED
        UUID e7 = createTestEventWithStatus("SOLD_OUT");
        patchStatus(e7, "COMPLETED")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("COMPLETED")));
        assertThat(getEventStatus(e7)).isEqualTo("COMPLETED");

        // 8. SOLD_OUT -> CANCELLED
        UUID e8 = createTestEventWithStatus("SOLD_OUT");
        patchStatus(e8, "CANCELLED")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELLED")));
        assertThat(getEventStatus(e8)).isEqualTo("CANCELLED");
    }

    @Test
    void updateEventStatus_allDisallowedTransitionsReturn409() throws Exception {
        // Disallowed transitions from SCHEDULED
        UUID eSched = createTestEventWithStatus("SCHEDULED");
        patchStatus(eSched, "SCHEDULED").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        patchStatus(eSched, "SOLD_OUT").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        patchStatus(eSched, "COMPLETED").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));

        // Disallowed transitions from ON_SALE
        UUID eSale = createTestEventWithStatus("ON_SALE");
        patchStatus(eSale, "ON_SALE").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        patchStatus(eSale, "SCHEDULED").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));

        // Disallowed transitions from SOLD_OUT
        UUID eSold = createTestEventWithStatus("SOLD_OUT");
        patchStatus(eSold, "SOLD_OUT").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        patchStatus(eSold, "SCHEDULED").andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));

        // Disallowed transitions from terminal states CANCELLED and COMPLETED
        UUID eCancelled = createTestEventWithStatus("CANCELLED");
        for (String target : List.of("SCHEDULED", "ON_SALE", "SOLD_OUT", "COMPLETED", "CANCELLED")) {
            patchStatus(eCancelled, target).andExpect(status().isConflict())
                    .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        }

        UUID eCompleted = createTestEventWithStatus("COMPLETED");
        for (String target : List.of("SCHEDULED", "ON_SALE", "SOLD_OUT", "COMPLETED", "CANCELLED")) {
            patchStatus(eCompleted, target).andExpect(status().isConflict())
                    .andExpect(jsonPath("$.errorCode", is("INVALID_STATE_TRANSITION")));
        }
    }

    @Test
    void updateEventStatus_unknownEventReturns404() throws Exception {
        UUID unknownId = UUID.randomUUID();
        patchStatus(unknownId, "ON_SALE")
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status", is(404)))
                .andExpect(jsonPath("$.errorCode", is("RESOURCE_NOT_FOUND")));
    }

    @Test
    void updateEventStatus_invalidStatusValueReturns400() throws Exception {
        UUID eventId = createTestEvent();
        patchStatusRaw(eventId, "{\"status\":\"UNKNOWN_STATUS\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));

        patchStatusRaw(eventId, "{\"status\":\"\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errorCode", is("VALIDATION_ERROR")));
    }

    @Test
    void updateEventStatus_nonAdminReturns403() throws Exception {
        UUID eventId = createTestEvent();
        mockMvc.perform(patch("/api/v1/admin/events/{eventId}/status", eventId)
                        .header("X-User-Roles", "USER")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"ON_SALE\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status", is(403)))
                .andExpect(jsonPath("$.errorCode", is("FORBIDDEN")));

        mockMvc.perform(patch("/api/v1/admin/events/{eventId}/status", eventId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"ON_SALE\"}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status", is(401)))
                .andExpect(jsonPath("$.errorCode", is("UNAUTHORIZED")));
    }

    @Test
    void updateEventStatus_goingOnSaleMakesRedisDropKeyAppearAndCancelledRemovesIt() throws Exception {
        UUID eventId = createTestEventWithStatus("SCHEDULED");
        String redisKey = RedisKeys.eventDropTsKey(eventId);

        stringRedisTemplate.delete(redisKey);
        assertThat(stringRedisTemplate.hasKey(redisKey)).isFalse();

        // 1. Transition SCHEDULED -> ON_SALE brings up the key
        patchStatus(eventId, "ON_SALE").andExpect(status().isOk());
        assertThat(stringRedisTemplate.hasKey(redisKey)).isTrue();
        String dropTs = stringRedisTemplate.opsForValue().get(redisKey);
        assertThat(dropTs).isNotNull();

        // 2. Transition ON_SALE -> CANCELLED deletes the key
        patchStatus(eventId, "CANCELLED").andExpect(status().isOk());
        assertThat(stringRedisTemplate.hasKey(redisKey)).isFalse();
    }

    @Test
    void updateEventStatus_redisFailureAfterCommitStillReturns200() throws Exception {
        UUID eventId = createTestEventWithStatus("SCHEDULED");

        try {
            adminService.setRedisSyncHook(() -> {
                throw new RuntimeException("Simulated Redis network disconnection after commit");
            });

            patchStatus(eventId, "ON_SALE")
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status", is("ON_SALE")));

            assertThat(getEventStatus(eventId)).isEqualTo("ON_SALE");
        } finally {
            adminService.setRedisSyncHook(null);
        }
    }

    @Test
    void patchEventStatusRouteIsMappedAndDoesNotReturn404() throws Exception {
        UUID eventId = createTestEvent();
        mockMvc.perform(patch("/api/v1/admin/events/{eventId}/status", eventId)
                        .header("X-User-Roles", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new UpdateEventStatusRequest("ON_SALE"))))
                .andExpect(status().is(org.hamcrest.Matchers.not(404)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("ON_SALE")));
    }
}


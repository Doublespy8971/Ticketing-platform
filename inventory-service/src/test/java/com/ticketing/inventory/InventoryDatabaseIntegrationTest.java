package com.ticketing.inventory;

import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class InventoryDatabaseIntegrationTest {

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

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbcTemplate;
    private static EventRepository eventRepository;
    private static SeatRepository seatRepository;
    private static ReservationRepository reservationRepository;

    @BeforeAll
    static void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
        jdbcTemplate = new JdbcTemplate(dataSource);

        eventRepository = new EventRepository(jdbcTemplate);
        seatRepository = new SeatRepository(jdbcTemplate);
        reservationRepository = new ReservationRepository(jdbcTemplate);
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void cleanTables() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        jdbcTemplate.execute("TRUNCATE TABLE reservation_items, reservations, seats, events CASCADE");
    }

    @Test
    void allMigrationsApplyOnEmptyDatabaseAndAreIdempotentWhenRunTwice() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();

        MigrateResult secondRun = flyway.migrate();
        assertThat(secondRun.migrationsExecuted).isZero();

        List<String> tableNames = jdbcTemplate.queryForList(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """,
                String.class
        );

        assertThat(tableNames).contains(
                "events",
                "seats",
                "reservations",
                "reservation_items",
                "outbox_events",
                "idempotency_keys",
                "processed_events"
        );
    }

    @Test
    void secondHoldForSameSeatRejectedByPartialUniqueIndexWhileReleasedOrExpiredAllowed() {
        UUID eventId = UUID.randomUUID();
        createEvent(eventId);

        UUID seatId = UUID.randomUUID();
        seatRepository.bulkInsert(List.of(new SeatRecord(
                seatId, eventId, "SEC-A", "1", "101", 15000L, "USD", "AVAILABLE", 0L, null, null
        )));

        UUID res1Id = UUID.randomUUID();
        UUID item1Id = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        ReservationRecord res1 = new ReservationRecord(
                res1Id, eventId, UUID.randomUUID(), "HELD", 15000L, "USD", now, now.plus(10, ChronoUnit.MINUTES), null, null, null, now
        );
        ReservationItemRecord item1 = new ReservationItemRecord(
                item1Id, res1Id, eventId, seatId, "SEC-A", "1", "101", 15000L, "HELD", now, now
        );
        reservationRepository.insert(res1, List.of(item1));

        // Second hold for the same seat in HELD status must fail
        UUID res2Id = UUID.randomUUID();
        UUID item2Id = UUID.randomUUID();
        ReservationRecord res2 = new ReservationRecord(
                res2Id, eventId, UUID.randomUUID(), "HELD", 15000L, "USD", now, now.plus(10, ChronoUnit.MINUTES), null, null, null, now
        );
        ReservationItemRecord item2 = new ReservationItemRecord(
                item2Id, res2Id, eventId, seatId, "SEC-A", "1", "101", 15000L, "HELD", now, now
        );

        assertThatThrownBy(() -> reservationRepository.insert(res2, List.of(item2)))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("idx_reservation_items_active_seat");

        // Transition first item to RELEASED
        jdbcTemplate.update("UPDATE reservation_items SET status = 'RELEASED' WHERE reservation_item_id = ?", item1Id);

        // Now inserting res2 with HELD status on the same seat succeeds
        reservationRepository.insert(res2, List.of(item2));

        // An EXPIRED or RELEASED item for the same seat is also allowed alongside HELD
        UUID res3Id = UUID.randomUUID();
        UUID item3Id = UUID.randomUUID();
        ReservationRecord res3 = new ReservationRecord(
                res3Id, eventId, UUID.randomUUID(), "EXPIRED", 15000L, "USD", now, now.plus(10, ChronoUnit.MINUTES), null, null, null, now
        );
        ReservationItemRecord item3 = new ReservationItemRecord(
                item3Id, res3Id, eventId, seatId, "SEC-A", "1", "101", 15000L, "EXPIRED", now, now
        );
        reservationRepository.insert(res3, List.of(item3));

        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT status FROM reservation_items WHERE seat_id = ? ORDER BY status",
                String.class,
                seatId
        );
        assertThat(statuses).containsExactly("EXPIRED", "HELD", "RELEASED");
    }

    @Test
    void invalidStatusValuesRejectedByCheckConstraints() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        // events status check constraint
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO events (event_id, name, venue_name, event_time, drop_time, status) VALUES (?, 'E', 'V', ?, ?, 'INVALID')",
                new Object[]{eventId, toOdt(now.plus(2, ChronoUnit.DAYS)), toOdt(now.plus(1, ChronoUnit.DAYS))},
                new int[]{Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_events_status");

        // events drop_time < event_time check constraint
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO events (event_id, name, venue_name, event_time, drop_time, status) VALUES (?, 'E', 'V', ?, ?, 'SCHEDULED')",
                new Object[]{eventId, toOdt(now.plus(1, ChronoUnit.DAYS)), toOdt(now.plus(2, ChronoUnit.DAYS))},
                new int[]{Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_events_drop_before_event");

        createEvent(eventId);

        // seats status check constraint
        UUID seatId = UUID.randomUUID();
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, 'A', '1', '1', 1000, 'BAD_STATUS')",
                new Object[]{seatId, eventId},
                new int[]{Types.OTHER, Types.OTHER}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_seats_status");

        // seats price_cents > 0 check constraint
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, 'A', '1', '1', 0, 'AVAILABLE')",
                new Object[]{seatId, eventId},
                new int[]{Types.OTHER, Types.OTHER}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_seats_price_positive");

        seatRepository.bulkInsert(List.of(new SeatRecord(
                seatId, eventId, "SEC-A", "1", "101", 1000L, "USD", "AVAILABLE", 0L, null, null
        )));

        // reservations status check constraint
        UUID resId = UUID.randomUUID();
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at) VALUES (?, ?, ?, 'INVALID', 1000, ?)",
                new Object[]{resId, eventId, UUID.randomUUID(), toOdt(now.plus(10, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_reservations_status");

        // reservations expires_at > created_at check constraint
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, created_at, expires_at) VALUES (?, ?, ?, 'HELD', 1000, ?, ?)",
                new Object[]{resId, eventId, UUID.randomUUID(), toOdt(now), toOdt(now.minus(1, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_reservations_expiry_after_create");

        // reservation_items status check constraint
        jdbcTemplate.update(
                "INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, created_at, expires_at) VALUES (?, ?, ?, 'HELD', 1000, ?, ?)",
                new Object[]{resId, eventId, UUID.randomUUID(), toOdt(now), toOdt(now.plus(10, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE});

        UUID itemId = UUID.randomUUID();
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, ?, ?, 'A', '1', '1', 1000, 'INVALID')",
                new Object[]{itemId, resId, eventId, seatId},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.OTHER}))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_reservation_items_status");
    }

    @Test
    void foreignKeysAreEnforced() {
        UUID nonExistentEventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        // seats -> events FK
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, 'A', '1', '1', 1000, 'AVAILABLE')",
                new Object[]{seatId, nonExistentEventId},
                new int[]{Types.OTHER, Types.OTHER}))
                .isInstanceOf(DataIntegrityViolationException.class);

        UUID eventId = UUID.randomUUID();
        createEvent(eventId);

        // reservations -> events FK
        UUID resId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at) VALUES (?, ?, ?, 'HELD', 1000, ?)",
                new Object[]{resId, nonExistentEventId, UUID.randomUUID(), toOdt(now.plus(10, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class);

        // reservation_items -> reservations, events, seats FKs
        seatRepository.bulkInsert(List.of(new SeatRecord(
                seatId, eventId, "SEC-A", "1", "101", 1000L, "USD", "AVAILABLE", 0L, null, null
        )));
        jdbcTemplate.update(
                "INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at) VALUES (?, ?, ?, 'HELD', 1000, ?)",
                new Object[]{resId, eventId, UUID.randomUUID(), toOdt(now.plus(10, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE});

        UUID itemId = UUID.randomUUID();
        // bad seat_id
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, ?, ?, 'A', '1', '1', 1000, 'HELD')",
                new Object[]{itemId, resId, eventId, UUID.randomUUID()},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.OTHER}))
                .isInstanceOf(DataIntegrityViolationException.class);

        // cascade delete from reservations to reservation_items
        jdbcTemplate.update(
                "INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status) VALUES (?, ?, ?, ?, 'A', '1', '1', 1000, 'HELD')",
                new Object[]{itemId, resId, eventId, seatId},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.OTHER});
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservation_items", Integer.class)).isEqualTo(1);

        jdbcTemplate.update("DELETE FROM reservations WHERE reservation_id = ?", new Object[]{resId}, new int[]{Types.OTHER});
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reservation_items", Integer.class)).isZero();
    }

    @Test
    void repositoriesWorkAsExpected() {
        UUID eventId = UUID.randomUUID();
        Instant eventTime = Instant.now().plus(7, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MICROS);
        Instant dropTime = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MICROS);

        EventRecord event = new EventRecord(
                eventId, "Rock Concert", "Grand Arena", eventTime, dropTime, "SCHEDULED", null, null
        );
        eventRepository.insert(event);

        Optional<EventRecord> loadedEvent = eventRepository.findById(eventId);
        assertThat(loadedEvent).isPresent();
        assertThat(loadedEvent.get().name()).isEqualTo("Rock Concert");
        assertThat(loadedEvent.get().venueName()).isEqualTo("Grand Arena");
        assertThat(loadedEvent.get().status()).isEqualTo("SCHEDULED");

        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        seatRepository.bulkInsert(List.of(
                new SeatRecord(seat1, eventId, "VIP", "A", "1", 25000L, "USD", "AVAILABLE", 0L, null, null),
                new SeatRecord(seat2, eventId, "VIP", "A", "2", 25000L, "USD", "AVAILABLE", 0L, null, null)
        ));

        Optional<SeatRecord> loadedSeat1 = seatRepository.findById(seat1);
        assertThat(loadedSeat1).isPresent();
        assertThat(loadedSeat1.get().sectionCode()).isEqualTo("VIP");
        assertThat(loadedSeat1.get().priceCents()).isEqualTo(25000L);

        UUID resId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant expiresAt = now.plus(10, ChronoUnit.MINUTES);
        ReservationRecord reservation = new ReservationRecord(
                resId, eventId, UUID.randomUUID(), "HELD", 50000L, "USD", now, expiresAt, null, null, null, now
        );
        List<ReservationItemRecord> items = List.of(
                new ReservationItemRecord(UUID.randomUUID(), resId, eventId, seat1, "VIP", "A", "1", 25000L, "HELD", now, now),
                new ReservationItemRecord(UUID.randomUUID(), resId, eventId, seat2, "VIP", "A", "2", 25000L, "HELD", now, now)
        );

        reservationRepository.insert(reservation, items);

        Optional<ReservationRecord> loadedRes = reservationRepository.findById(resId);
        assertThat(loadedRes).isPresent();
        assertThat(loadedRes.get().totalAmountCents()).isEqualTo(50000L);
        assertThat(loadedRes.get().status()).isEqualTo("HELD");

        List<ReservationItemRecord> loadedItems = reservationRepository.findItemsByReservationId(resId);
        assertThat(loadedItems).hasSize(2);
        assertThat(loadedItems).extracting(ReservationItemRecord::seatId).containsExactlyInAnyOrder(seat1, seat2);
    }

    private void createEvent(UUID eventId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        eventRepository.insert(new EventRecord(
                eventId, "Summer Fest", "Stadium", now.plus(7, ChronoUnit.DAYS), now.plus(1, ChronoUnit.DAYS), "SCHEDULED", now, now
        ));
    }

    private static OffsetDateTime toOdt(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}

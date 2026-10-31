package com.ticketing.inventory.service;

import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.RedisKeys;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
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
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class ExpirySweeperScheduledIntegrationTest {

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
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "10");
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        // NOTE: No overrides of ticketing.sweeper properties — tests default configuration!
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
    private EventRepository eventRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

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

    @Test
    void scheduledSweeperAloneExpiresOverdueHeldReservationWithinTenSeconds() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant dropTime = now.minus(2, ChronoUnit.HOURS);
        Instant eventTime = now.plus(7, ChronoUnit.DAYS);

        // 1. Seed event and seat in HELD status
        transactionTemplate.executeWithoutResult(status -> {
            eventRepository.insert(new EventRecord(
                    eventId, "Scheduled Test Event", "Test Arena", eventTime, dropTime, "ON_SALE", now, now));
            seatRepository.bulkInsert(List.of(
                    new SeatRecord(seatId, eventId, "SEC-SCHED", "1", "1", 10000L, "USD", "HELD", 1L, now, now)));
        });

        stringRedisTemplate.opsForValue().set(
                RedisKeys.eventDropTsKey(eventId),
                String.valueOf(dropTime.toEpochMilli()),
                Duration.ofSeconds(RedisKeys.EVENT_DROP_TS_TTL_SECONDS)
        );

        // 2. Insert HELD reservation with created_at 1 hour in the past and expires_at 30 seconds in the past
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update("""
                    INSERT INTO reservations (
                        reservation_id, event_id, user_id, status, total_amount_cents, currency,
                        created_at, expires_at, updated_at
                    ) VALUES (
                        ?, ?, ?, 'HELD', 10000, 'USD',
                        NOW() - INTERVAL '1 hour',
                        NOW() - INTERVAL '30 seconds',
                        NOW() - INTERVAL '1 hour'
                    )
                    """,
                    new Object[]{reservationId, eventId, userId},
                    new int[]{Types.OTHER, Types.OTHER, Types.OTHER}
            );

            jdbcTemplate.update("""
                    INSERT INTO reservation_items (
                        reservation_item_id, reservation_id, event_id, seat_id,
                        section_code, row_label, seat_number, price_cents, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'SEC-SCHED', '1', '1', 10000, 'HELD', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')
                    """,
                    new Object[]{UUID.randomUUID(), reservationId, eventId, seatId},
                    new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.OTHER}
            );
        });

        stringRedisTemplate.opsForValue().set(
                RedisKeys.seatKey(eventId, seatId),
                reservationId.toString(),
                Duration.ofSeconds(660)
        );

        // Verify initial state is HELD
        ReservationRecord initialRes = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(initialRes.status()).isEqualTo("HELD");

        // 3. Wait up to 10 seconds for the background scheduled sweeper alone (no manual call) to expire it
        long deadline = System.currentTimeMillis() + 10_000L;
        boolean expired = false;
        while (System.currentTimeMillis() < deadline) {
            Optional<ReservationRecord> resOpt = reservationRepository.findById(reservationId);
            if (resOpt.isPresent() && "EXPIRED".equals(resOpt.get().status())) {
                expired = true;
                break;
            }
            Thread.sleep(200L);
        }

        assertThat(expired)
                .as("Reservation %s should have been transitioned to EXPIRED by scheduled sweeper within 10 seconds", reservationId)
                .isTrue();

        ReservationRecord finalRes = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(finalRes.status()).isEqualTo("EXPIRED");
        assertThat(finalRes.expiredAt()).isNotNull();

        SeatRecord finalSeat = seatRepository.findById(seatId).orElseThrow();
        assertThat(finalSeat.status()).isEqualTo("AVAILABLE");
        assertThat(finalSeat.version()).isEqualTo(2L);

        assertThat(stringRedisTemplate.hasKey(RedisKeys.seatKey(eventId, seatId))).isFalse();
    }
}


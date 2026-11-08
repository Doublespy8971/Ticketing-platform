package com.ticketing.order;

import com.ticketing.order.domain.OrderItemRecord;
import com.ticketing.order.domain.OrderRecord;
import com.ticketing.order.domain.OrderRepository;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
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

@SpringBootTest
@Testcontainers
class OrderDatabaseIntegrationTest {

    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"))
                    .withDatabaseName("order_test_db")
                    .withUsername("ticketing")
                    .withPassword("ticketing_secret");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("ticketing.outbox.enabled", () -> "false");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @BeforeEach
    void cleanTables() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        jdbcTemplate.execute("TRUNCATE TABLE order_items, orders CASCADE");
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
                "orders",
                "order_items",
                "outbox_events",
                "idempotency_keys",
                "processed_events"
        );
    }

    @Test
    void mandatoryPropagationRejectsWritesWithoutActiveTransaction() {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        OrderRecord order = new OrderRecord(
                orderId, orderId, UUID.randomUUID(), UUID.randomUUID(),
                "RESERVED", 10000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );

        assertThatThrownBy(() -> orderRepository.insert(order, List.of()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void insertAndFindOrderWithItemsAndRowLock() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID seat1Id = UUID.randomUUID();
        UUID seat2Id = UUID.randomUUID();
        UUID item1Id = UUID.randomUUID();
        UUID item2Id = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        OrderRecord order = new OrderRecord(
                orderId, orderId, userId, eventId,
                "RESERVED", 20000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );
        OrderItemRecord item1 = new OrderItemRecord(item1Id, orderId, seat1Id, "SEC-A", "1", "1", 10000L);
        OrderItemRecord item2 = new OrderItemRecord(item2Id, orderId, seat2Id, "SEC-A", "1", "2", 10000L);

        transactionTemplate.executeWithoutResult(status -> orderRepository.insert(order, List.of(item1, item2)));

        Optional<OrderRecord> found = orderRepository.findById(orderId);
        assertThat(found).isPresent();
        assertThat(found.get().orderId()).isEqualTo(orderId);
        assertThat(found.get().reservationId()).isEqualTo(orderId);
        assertThat(found.get().status()).isEqualTo("RESERVED");
        assertThat(found.get().totalAmountCents()).isEqualTo(20000L);
        assertThat(found.get().currency()).isEqualTo("USD");

        List<OrderItemRecord> items = orderRepository.findItemsByOrderId(orderId);
        assertThat(items).hasSize(2);
        assertThat(items).extracting(OrderItemRecord::orderItemId).containsExactlyInAnyOrder(item1Id, item2Id);

        // Test row lock inside transaction
        transactionTemplate.executeWithoutResult(status -> {
            Optional<OrderRecord> locked = orderRepository.findByIdForUpdate(orderId);
            assertThat(locked).isPresent();
            assertThat(locked.get().orderId()).isEqualTo(orderId);
        });
    }

    @Test
    void everyDocumentedStatusValueIsAccepted() {
        List<String> validStatuses = List.of(
                "RESERVED",
                "PAYMENT_PENDING",
                "CONFIRMED",
                "CANCELLED_PAYMENT_FAILED",
                "CANCELLED_EXPIRED",
                "REFUND_PENDING",
                "CANCELLED_REFUNDED"
        );

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        for (String status : validStatuses) {
            UUID id = UUID.randomUUID();
            OrderRecord order = new OrderRecord(
                    id, id, UUID.randomUUID(), UUID.randomUUID(),
                    status, 10000L, "USD", null, null, null, 0L,
                    now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
            );

            transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order, List.of()));

            Optional<OrderRecord> loaded = orderRepository.findById(id);
            assertThat(loaded).isPresent();
            assertThat(loaded.get().status()).isEqualTo(status);
        }
    }

    @Test
    void invalidStatusValuesAreRejectedByCheckConstraint() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        OrderRecord order = new OrderRecord(
                id, id, UUID.randomUUID(), UUID.randomUUID(),
                "INVALID_STATUS", 10000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order, List.of())))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_orders_status");
    }

    @Test
    void uniquenessRuleThatMapsOneOrderToOneReservationIsEnforced() {
        UUID order1Id = UUID.randomUUID();
        UUID order2Id = UUID.randomUUID();
        UUID reservationId = order1Id; // Same reservation
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        OrderRecord order1 = new OrderRecord(
                order1Id, reservationId, UUID.randomUUID(), UUID.randomUUID(),
                "RESERVED", 10000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );
        transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order1, List.of()));

        // Second order with the same reservationId (violates UNIQUE constraint and check order_id = reservation_id)
        // Even if order2Id != reservationId, chk_orders_id_equals_reservation would fail.
        // If we try inserting another row with reservation_id = reservationId:
        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO orders (order_id, reservation_id, user_id, event_id, status, total_amount_cents, reserved_at, expires_at) VALUES (?, ?, ?, ?, 'RESERVED', 10000, ?, ?)",
                new Object[]{order2Id, reservationId, UUID.randomUUID(), UUID.randomUUID(), toOdt(now), toOdt(now.plus(10, ChronoUnit.MINUTES))},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.OTHER, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE}))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void orderIdMustStrictlyEqualReservationId() {
        UUID orderId = UUID.randomUUID();
        UUID differentReservationId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        OrderRecord order = new OrderRecord(
                orderId, differentReservationId, UUID.randomUUID(), UUID.randomUUID(),
                "RESERVED", 10000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(s -> orderRepository.insert(order, List.of())))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_orders_id_equals_reservation");
    }

    @Test
    void foreignKeysAreEnforced() {
        UUID nonExistentOrderId = UUID.randomUUID();
        OrderItemRecord item = new OrderItemRecord(
                UUID.randomUUID(), nonExistentOrderId, UUID.randomUUID(), "SEC-A", "1", "1", 10000L
        );

        assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO order_items (order_item_id, order_id, seat_id, section_code, row_label, seat_number, price_cents) VALUES (?, ?, ?, ?, ?, ?, ?)",
                new Object[]{item.orderItemId(), item.orderId(), item.seatId(), item.sectionCode(), item.rowLabel(), item.seatNumber(), item.priceCents()},
                new int[]{Types.OTHER, Types.OTHER, Types.OTHER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT}))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void amountAndCurrencyConstraintsBehaveAsDocumented() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        // Order total_amount_cents must be > 0
        OrderRecord zeroAmountOrder = new OrderRecord(
                id, id, UUID.randomUUID(), UUID.randomUUID(),
                "RESERVED", 0L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(s -> orderRepository.insert(zeroAmountOrder, List.of())))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_orders_total_positive");

        // Order Item price_cents must be > 0
        OrderRecord validOrder = new OrderRecord(
                id, id, UUID.randomUUID(), UUID.randomUUID(),
                "RESERVED", 10000L, "USD", null, null, null, 0L,
                now, now.plus(10, ChronoUnit.MINUTES), null, null, null, null, null, now, now
        );
        OrderItemRecord zeroPriceItem = new OrderItemRecord(
                UUID.randomUUID(), id, UUID.randomUUID(), "SEC-A", "1", "1", 0L
        );
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(s -> orderRepository.insert(validOrder, List.of(zeroPriceItem))))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("chk_order_items_price_positive");

        // Unique order_id, seat_id in order_items
        UUID seatId = UUID.randomUUID();
        OrderItemRecord item1 = new OrderItemRecord(UUID.randomUUID(), id, seatId, "SEC-A", "1", "1", 5000L);
        OrderItemRecord item2 = new OrderItemRecord(UUID.randomUUID(), id, seatId, "SEC-A", "1", "1", 5000L);
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(s -> orderRepository.insert(validOrder, List.of(item1, item2))))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("uq_order_items_order_seat");
    }

    private static OffsetDateTime toOdt(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }
}


package com.ticketing.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.EventEnvelope;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Single-leader polling publisher for {@code outbox_events} (docs/arch/05-outbox-idempotency.md, section 5.2).
 *
 * <p>Leader election uses a dedicated unpooled JDBC connection holding a PostgreSQL session advisory lock
 * ({@code SELECT pg_try_advisory_lock(:fixedLockId)}). All outbox batch reads, pre-send liveness checks
 * ({@code SELECT 1}), and post-ack {@code published_at} updates run in autocommit mode on the dedicated
 * leader connection. If any statement on the leader connection fails, leadership is dropped immediately
 * and the batch stops before sending or marking any further rows.
 */
public class OutboxPoller implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    public static final String HEADER_EVENT_TYPE = "event-type";
    public static final String HEADER_EVENT_ID = "event-id";
    public static final String HEADER_SCHEMA_VERSION = "schema-version";

    private static final String SELECT_UNPUBLISHED_SQL = """
            SELECT event_id,
                   aggregate_type,
                   aggregate_id,
                   topic,
                   event_type,
                   correlation_id,
                   schema_version,
                   payload::text AS payload_json,
                   created_at
            FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at ASC, event_id ASC
            LIMIT ?
            """;

    private static final String MARK_PUBLISHED_SQL = """
            UPDATE outbox_events
            SET published_at = NOW()
            WHERE event_id = ?
              AND published_at IS NULL
            """;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final KafkaOperations<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxProperties properties;
    private final OutboxMetrics metrics;

    private final ReentrantLock pollLock = new ReentrantLock();
    private final Object connectionLock = new Object();

    private volatile Connection leaderConnection;
    private volatile boolean isLeader;
    private volatile boolean stopping;
    private volatile BiConsumer<Integer, OutboxRecord> beforeSendHook;

    public OutboxPoller(DataSource dataSource,
                        JdbcTemplate jdbcTemplate,
                        KafkaOperations<String, String> kafkaTemplate,
                        ObjectMapper objectMapper,
                        OutboxProperties properties,
                        OutboxMetrics metrics) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");

        this.properties.validate();
        OutboxProducerProperties.validateConfiguration(this.kafkaTemplate);
    }

    /**
     * Registers an optional hook invoked before each row's pre-send {@code SELECT 1} check and Kafka send.
     *
     * @param beforeSendHook callback receiving {@code (rowIndexInBatch, OutboxRecord)}
     */
    public void setBeforeSendHook(BiConsumer<Integer, OutboxRecord> beforeSendHook) {
        this.beforeSendHook = beforeSendHook;
    }

    /**
     * Executes a single poll cycle if this instance is (or becomes) the advisory-lock leader.
     *
     * @return the number of rows marked published during this cycle
     */
    @Scheduled(
            fixedDelayString = "${ticketing.outbox.poll-interval-ms:${OUTBOX_POLL_INTERVAL_MS:200}}",
            scheduler = "ticketingOutboxScheduler"
    )
    public int poll() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalTransactionStateException(
                    "OutboxPoller.poll() must not be called inside an active transaction");
        }
        if (stopping) {
            return 0;
        }
        if (!pollLock.tryLock()) {
            return 0;
        }
        try {
            if (stopping || !ensureLeadership()) {
                return 0;
            }

            List<OutboxRecord> batch;
            try {
                batch = selectUnpublishedBatch(leaderConnection);
            } catch (SQLException e) {
                log.warn("Failed to select unpublished outbox batch on leader connection; dropping leadership", e);
                releaseLeadership();
                return 0;
            }

            int publishedCount = 0;
            long timeoutMs = Math.max(1L, properties.getSendTimeout().toMillis());

            for (int i = 0; i < batch.size(); i++) {
                if (stopping) {
                    break;
                }
                OutboxRecord row = batch.get(i);
                BiConsumer<Integer, OutboxRecord> hook = this.beforeSendHook;
                if (hook != null) {
                    hook.accept(i, row);
                }
                if (stopping) {
                    break;
                }
                if (!verifyLeaderSelectOne(leaderConnection)) {
                    log.warn("Leader connection SELECT 1 check failed before sending outbox event {}; dropping leadership and stopping batch",
                            row.eventId());
                    releaseLeadership();
                    break;
                }

                try {
                    ProducerRecord<String, String> record = buildProducerRecord(row);
                    kafkaTemplate.send(record).get(timeoutMs, TimeUnit.MILLISECONDS);
                    markPublished(row.eventId());
                    metrics.recordPublished(row.topic(), row.createdAt(), Instant.now());
                    publishedCount++;
                } catch (Exception e) {
                    metrics.recordFailure();
                    log.error("Failed to publish outbox event {} (aggregateId={}, eventType={}); stopping batch to preserve ordering",
                            row.eventId(), row.aggregateId(), row.eventType(), e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    break;
                }
            }
            return publishedCount;
        } finally {
            pollLock.unlock();
        }
    }

    /** Returns {@code true} if this poller currently holds the dedicated leader connection and advisory lock. */
    public boolean isLeader() {
        synchronized (connectionLock) {
            return isLeader && verifyLeaderSelectOne(leaderConnection);
        }
    }

    /** Unlocks the session advisory lock and closes (or aborts on failure) the dedicated leader connection. */
    public void releaseLeadership() {
        Connection conn;
        synchronized (connectionLock) {
            isLeader = false;
            conn = this.leaderConnection;
            this.leaderConnection = null;
        }
        if (conn != null) {
            boolean unlocked = false;
            try {
                if (!conn.isClosed()) {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                        ps.setLong(1, properties.getAdvisoryLockId());
                        try (ResultSet rs = ps.executeQuery()) {
                            unlocked = rs.next() && rs.getBoolean(1);
                        }
                    }
                }
            } catch (SQLException e) {
                log.warn("Failed to unlock advisory lock {}; aborting leader connection",
                        properties.getAdvisoryLockId(), e);
                abortConnectionQuietly(conn);
                return;
            }
            if (!unlocked) {
                abortConnectionQuietly(conn);
                return;
            }
            try {
                conn.close();
            } catch (SQLException e) {
                abortConnectionQuietly(conn);
            }
        }
    }

    @Override
    public void destroy() {
        this.stopping = true;
        long waitMs = Math.max(1L, properties.getSendTimeout().toMillis());
        boolean acquired = false;
        try {
            acquired = pollLock.tryLock(waitMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                releaseLeadership();
            } finally {
                if (acquired) {
                    pollLock.unlock();
                }
            }
        }
    }

    void markPublished(UUID eventId) throws SQLException {
        Connection conn = this.leaderConnection;
        if (conn == null || conn.isClosed()) {
            releaseLeadership();
            throw new SQLException("Leader connection is closed before marking event published: " + eventId);
        }
        try {
            conn.setAutoCommit(true);
            try (PreparedStatement ps = conn.prepareStatement(MARK_PUBLISHED_SQL)) {
                ps.setObject(1, eventId, Types.OTHER);
                int updated = ps.executeUpdate();
                if (updated != 1) {
                    releaseLeadership();
                    throw new SQLException(
                            "Expected markPublished update count 1 for event " + eventId + " but got " + updated);
                }
            }
        } catch (SQLException e) {
            releaseLeadership();
            throw e;
        }
    }

    private List<OutboxRecord> selectUnpublishedBatch(Connection conn) throws SQLException {
        if (conn == null || conn.isClosed()) {
            throw new SQLException("Leader connection is not open");
        }
        conn.setAutoCommit(true);
        List<OutboxRecord> records = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_UNPUBLISHED_SQL)) {
            ps.setInt(1, properties.getBatchSize());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    OffsetDateTime createdAtOdt = rs.getObject("created_at", OffsetDateTime.class);
                    Instant createdAt = createdAtOdt.toInstant().truncatedTo(ChronoUnit.MICROS);
                    records.add(new OutboxRecord(
                            rs.getObject("event_id", UUID.class),
                            rs.getString("aggregate_type"),
                            rs.getObject("aggregate_id", UUID.class),
                            rs.getString("topic"),
                            rs.getString("event_type"),
                            rs.getObject("correlation_id", UUID.class),
                            rs.getInt("schema_version"),
                            rs.getString("payload_json"),
                            createdAt,
                            null
                    ));
                }
            }
        }
        return records;
    }

    private ProducerRecord<String, String> buildProducerRecord(OutboxRecord row) throws Exception {
        EventEnvelope<RawValue> envelope = new EventEnvelope<>(
                row.eventId(),
                row.eventType(),
                row.createdAt(),
                row.correlationId().toString(),
                row.schemaVersion(),
                new RawValue(row.payloadJson())
        );
        String body = objectMapper.writeValueAsString(envelope);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                row.topic(),
                null,
                row.aggregateId().toString(),
                body
        );
        record.headers().add(CorrelationConstants.KAFKA_HEADER,
                row.correlationId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HEADER_EVENT_TYPE,
                row.eventType().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HEADER_EVENT_ID,
                row.eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HEADER_SCHEMA_VERSION,
                Integer.toString(row.schemaVersion()).getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private boolean ensureLeadership() {
        synchronized (connectionLock) {
            if (leaderConnection != null) {
                if (isLeader && verifyLeaderSelectOne(leaderConnection)) {
                    return true;
                }
                releaseLeadership();
            }

            Connection candidate = null;
            try {
                candidate = openUnpooledConnection();
                candidate.setAutoCommit(true);
                try (PreparedStatement ps = candidate.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                    ps.setLong(1, properties.getAdvisoryLockId());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next() && rs.getBoolean(1)) {
                            leaderConnection = candidate;
                            isLeader = true;
                            log.info("Acquired outbox advisory lock {}", properties.getAdvisoryLockId());
                            return true;
                        }
                    }
                }
                candidate.close();
                return false;
            } catch (SQLException e) {
                log.warn("Could not acquire or verify outbox advisory lock {}", properties.getAdvisoryLockId(), e);
                if (candidate != null) {
                    abortConnectionQuietly(candidate);
                }
                return false;
            }
        }
    }

    private Connection openUnpooledConnection() throws SQLException {
        if (dataSource instanceof HikariDataSource hikari) {
            return DriverManager.getConnection(hikari.getJdbcUrl(), hikari.getUsername(), hikari.getPassword());
        }
        if (dataSource.isWrapperFor(HikariDataSource.class)) {
            HikariDataSource hikari = dataSource.unwrap(HikariDataSource.class);
            return DriverManager.getConnection(hikari.getJdbcUrl(), hikari.getUsername(), hikari.getPassword());
        }
        return dataSource.getConnection();
    }

    private boolean verifyLeaderSelectOne(Connection conn) {
        if (conn == null) {
            return false;
        }
        try {
            if (conn.isClosed()) {
                return false;
            }
            conn.setAutoCommit(true);
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1");
                 ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) == 1;
            }
        } catch (SQLException e) {
            return false;
        }
    }

    private static void abortConnectionQuietly(Connection conn) {
        try {
            conn.abort(Runnable::run);
        } catch (SQLException ignored) {
        }
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }
}

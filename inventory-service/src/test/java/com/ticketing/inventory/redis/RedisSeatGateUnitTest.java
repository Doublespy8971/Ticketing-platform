package com.ticketing.inventory.redis;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.net.ConnectException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisSeatGateUnitTest {

    private static class StubStringRedisTemplate extends StringRedisTemplate {
        private Object nextResult;
        private RuntimeException nextException;

        void setNextResult(Object result) {
            this.nextResult = result;
            this.nextException = null;
        }

        void setNextException(RuntimeException exception) {
            this.nextException = exception;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T execute(RedisScript<T> script, List<String> keys, Object... args) {
            if (nextException != null) {
                throw nextException;
            }
            return (T) nextResult;
        }
    }

    private StubStringRedisTemplate stringRedisTemplate;
    private MeterRegistry meterRegistry;
    private RedisSeatGate seatGate;

    private final UUID eventId = UUID.randomUUID();
    private final UUID seatId = UUID.randomUUID();
    private final UUID reservationId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        stringRedisTemplate = new StubStringRedisTemplate();
        meterRegistry = new SimpleMeterRegistry();

        DefaultRedisScript<String> holdScript = new DefaultRedisScript<>();
        DefaultRedisScript<Long> releaseScript = new DefaultRedisScript<>();
        DefaultRedisScript<String> confirmScript = new DefaultRedisScript<>();

        InventoryRedisProperties properties = new InventoryRedisProperties();
        Clock clock = Clock.fixed(Instant.parse("2026-10-29T12:00:00Z"), ZoneOffset.UTC);

        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("meterRegistry", meterRegistry);

        seatGate = new RedisSeatGate(
                stringRedisTemplate,
                holdScript,
                releaseScript,
                confirmScript,
                properties,
                clock,
                beanFactory.getBeanProvider(MeterRegistry.class)
        );
    }

    @Test
    @DisplayName("hold: null reply from Redis template throws RedisUnavailableException")
    void holdNullReplyThrowsRedisUnavailableException() {
        stringRedisTemplate.setNextResult(null);

        assertThrows(RedisUnavailableException.class, () ->
                seatGate.hold(eventId, List.of(seatId), reservationId)
        );
    }

    @Test
    @DisplayName("hold: unrecognized reply string from Redis template throws RedisUnavailableException")
    void holdUnrecognizedReplyThrowsRedisUnavailableException() {
        stringRedisTemplate.setNextResult("UNKNOWN_OUTCOME");

        assertThrows(RedisUnavailableException.class, () ->
                seatGate.hold(eventId, List.of(seatId), reservationId)
        );
    }

    @Test
    @DisplayName("release: null reply from Redis template throws RedisUnavailableException")
    void releaseNullReplyThrowsRedisUnavailableException() {
        stringRedisTemplate.setNextResult(null);

        assertThrows(RedisUnavailableException.class, () ->
                seatGate.release(eventId, List.of(seatId), reservationId)
        );
    }

    @Test
    @DisplayName("confirm: null reply from Redis template throws RedisUnavailableException")
    void confirmNullReplyThrowsRedisUnavailableException() {
        stringRedisTemplate.setNextResult(null);

        assertThrows(RedisUnavailableException.class, () ->
                seatGate.confirm(eventId, List.of(seatId), reservationId)
        );
    }

    @Test
    @DisplayName("confirm: unrecognized reply string from Redis template throws RedisUnavailableException")
    void confirmUnrecognizedReplyThrowsRedisUnavailableException() {
        stringRedisTemplate.setNextResult("UNKNOWN_OUTCOME");

        assertThrows(RedisUnavailableException.class, () ->
                seatGate.confirm(eventId, List.of(seatId), reservationId)
        );
    }

    @Test
    @DisplayName("resolveCause: correctly categorizes timeout, connection, and other")
    void resolveCauseCategorizesExceptionsCorrectly() {
        assertThat(RedisSeatGate.resolveCause(new RedisCommandTimeoutException("Timeout"))).isEqualTo("timeout");
        assertThat(RedisSeatGate.resolveCause(new QueryTimeoutException("Query timeout"))).isEqualTo("timeout");
        assertThat(RedisSeatGate.resolveCause(new RuntimeException("timeout error", new ConnectException("Connection refused")))).isEqualTo("timeout");
        assertThat(RedisSeatGate.resolveCause(new RedisConnectionException("Cannot connect"))).isEqualTo("connection");
        assertThat(RedisSeatGate.resolveCause(new RedisConnectionFailureException("Conn failure", new ConnectException("Connection refused")))).isEqualTo("connection");
        assertThat(RedisSeatGate.resolveCause(new RuntimeException("Syntax error"))).isEqualTo("other");
        assertThat(RedisSeatGate.resolveCause(null)).isEqualTo("other");
    }

    @Test
    @DisplayName("metric: increments inventory_redis_unavailable_total for hold, release, and confirm")
    void incrementsUnavailableMetricForHoldReleaseAndConfirm() {
        // Hold failure with timeout
        stringRedisTemplate.setNextException(new RedisCommandTimeoutException("Command timed out"));
        assertThrows(RedisUnavailableException.class, () ->
                seatGate.hold(eventId, List.of(seatId), reservationId)
        );
        double holdCount = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "hold")
                .tag("cause", "timeout")
                .counter()
                .count();
        assertThat(holdCount).isEqualTo(1.0);

        // Release failure with connection
        stringRedisTemplate.setNextException(new RedisConnectionException("Connection refused"));
        assertThrows(RedisUnavailableException.class, () ->
                seatGate.release(eventId, List.of(seatId), reservationId)
        );
        double releaseCount = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "release")
                .tag("cause", "connection")
                .counter()
                .count();
        assertThat(releaseCount).isEqualTo(1.0);

        // Confirm failure with other
        stringRedisTemplate.setNextException(new io.lettuce.core.RedisException("ERR memory full"));
        assertThrows(RedisUnavailableException.class, () ->
                seatGate.confirm(eventId, List.of(seatId), reservationId)
        );
        double confirmCount = meterRegistry.get("inventory_redis_unavailable_total")
                .tag("service", "inventory-service")
                .tag("operation", "confirm")
                .tag("cause", "other")
                .counter()
                .count();
        assertThat(confirmCount).isEqualTo(1.0);
    }
}

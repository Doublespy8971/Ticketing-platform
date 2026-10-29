package com.ticketing.inventory.redis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisSeatGateUnitTest {

    private static class StubStringRedisTemplate extends StringRedisTemplate {
        private Object nextResult;

        void setNextResult(Object result) {
            this.nextResult = result;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T execute(RedisScript<T> script, List<String> keys, Object... args) {
            return (T) nextResult;
        }
    }

    private StubStringRedisTemplate stringRedisTemplate;
    private RedisSeatGate seatGate;

    private final UUID eventId = UUID.randomUUID();
    private final UUID seatId = UUID.randomUUID();
    private final UUID reservationId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        stringRedisTemplate = new StubStringRedisTemplate();

        DefaultRedisScript<String> holdScript = new DefaultRedisScript<>();
        DefaultRedisScript<Long> releaseScript = new DefaultRedisScript<>();
        DefaultRedisScript<String> confirmScript = new DefaultRedisScript<>();

        InventoryRedisProperties properties = new InventoryRedisProperties();
        Clock clock = Clock.fixed(Instant.parse("2026-10-29T12:00:00Z"), ZoneOffset.UTC);

        seatGate = new RedisSeatGate(
                stringRedisTemplate,
                holdScript,
                releaseScript,
                confirmScript,
                properties,
                clock,
                null
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
}

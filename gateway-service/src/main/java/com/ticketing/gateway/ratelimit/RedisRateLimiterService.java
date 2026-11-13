package com.ticketing.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Token bucket rate limiter backed by Redis reactive template and atomic Lua script.
 * Section 9.2.4 of docs/arch/08-redis.md.
 *
 * Fails open with a WARN log and a counter metric if Redis is unavailable.
 */
@Service
public class RedisRateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiterService.class);

    private static final String LUA_SCRIPT = """
            local capacity = tonumber(ARGV[1])
            local refill_rate = tonumber(ARGV[2])
            local now_ms = tonumber(ARGV[3])

            local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill_ms')
            local tokens = tonumber(bucket[1])
            local last_refill_ms = tonumber(bucket[2])

            if tokens == nil then
                tokens = capacity
                last_refill_ms = now_ms
            else
                local elapsed_sec = math.max(0, (now_ms - last_refill_ms) / 1000.0)
                tokens = math.min(capacity, tokens + (elapsed_sec * refill_rate))
                last_refill_ms = now_ms
            end

            if tokens < 1.0 then
                redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', last_refill_ms)
                redis.call('EXPIRE', KEYS[1], 60)
                return 0
            end

            tokens = tokens - 1.0
            redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', last_refill_ms)
            redis.call('EXPIRE', KEYS[1], 60)
            return 1
            """;

    private final ReactiveStringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> script;
    private final Counter redisFailuresCounter;

    public RedisRateLimiterService(ReactiveStringRedisTemplate redisTemplate, MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);
        this.redisFailuresCounter = meterRegistry.counter("gateway_redis_failures_total");
    }

    /**
     * Rate limit check for an authenticated user:
     * Key: ratelimit:user:{userId}, capacity: 10, refill: 5 req/s.
     */
    public Mono<Boolean> isAllowedUser(String userId) {
        String key = "ratelimit:user:" + userId;
        return executeScript(key, 10, 5);
    }

    /**
     * Rate limit check for an unauthenticated IP address:
     * Key: ratelimit:ip:{ipAddress}, capacity: 20, refill: 10 req/s.
     */
    public Mono<Boolean> isAllowedIp(String ipAddress) {
        String key = "ratelimit:ip:" + ipAddress;
        return executeScript(key, 20, 10);
    }

    private Mono<Boolean> executeScript(String key, int capacity, int refillRate) {
        long nowMs = System.currentTimeMillis();
        return redisTemplate.execute(
                        script,
                        List.of(key),
                        List.of(String.valueOf(capacity), String.valueOf(refillRate), String.valueOf(nowMs))
                )
                .next()
                .map(res -> res != null && res == 1L)
                .onErrorResume(ex -> {
                    log.warn("Redis rate limiter unavailable for key '{}': {}. Failing open.", key, ex.getMessage());
                    redisFailuresCounter.increment();
                    return Mono.just(true); // Fail open
                });
    }
}


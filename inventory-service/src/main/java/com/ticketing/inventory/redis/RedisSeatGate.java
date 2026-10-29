package com.ticketing.inventory.redis;

import io.lettuce.core.RedisException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class RedisSeatGate {

    private static final String SERVICE_NAME = "inventory-service";
    private static final String METRIC_LUA_DURATION = "ticketing_redis_lua_duration_seconds";

    private final StringRedisTemplate stringRedisTemplate;
    private final DefaultRedisScript<String> holdInventoryScript;
    private final DefaultRedisScript<Long> releaseInventoryScript;
    private final DefaultRedisScript<String> confirmInventoryScript;
    private final InventoryRedisProperties properties;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public RedisSeatGate(
            StringRedisTemplate stringRedisTemplate,
            @Qualifier("holdInventoryScript") DefaultRedisScript<String> holdInventoryScript,
            @Qualifier("releaseInventoryScript") DefaultRedisScript<Long> releaseInventoryScript,
            @Qualifier("confirmInventoryScript") DefaultRedisScript<String> confirmInventoryScript,
            InventoryRedisProperties properties,
            Clock clock,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this.stringRedisTemplate = Objects.requireNonNull(stringRedisTemplate, "stringRedisTemplate must not be null");
        this.holdInventoryScript = Objects.requireNonNull(holdInventoryScript, "holdInventoryScript must not be null");
        this.releaseInventoryScript = Objects.requireNonNull(releaseInventoryScript, "releaseInventoryScript must not be null");
        this.confirmInventoryScript = Objects.requireNonNull(confirmInventoryScript, "confirmInventoryScript must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.meterRegistry = meterRegistryProvider != null ? meterRegistryProvider.getIfAvailable() : null;
    }

    public HoldOutcome hold(UUID eventId, Collection<UUID> seatIds, UUID reservationId) {
        return hold(eventId, seatIds, reservationId, clock.millis());
    }

    public HoldOutcome hold(UUID eventId, Collection<UUID> seatIds, UUID reservationId, long nowEpochMs) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        if (seatIds.isEmpty()) {
            throw new IllegalArgumentException("seatIds must not be empty");
        }

        List<String> sortedSeatKeys = toSortedSeatKeys(eventId, seatIds);
        List<String> keys = new ArrayList<>(1 + sortedSeatKeys.size());
        keys.add(RedisKeys.eventDropTsKey(eventId));
        keys.addAll(sortedSeatKeys);

        long startNanos = System.nanoTime();
        String resultStr = "ERROR";
        try {
            String raw = stringRedisTemplate.execute(
                    holdInventoryScript,
                    keys,
                    reservationId.toString(),
                    String.valueOf(nowEpochMs),
                    String.valueOf(properties.getSeatHoldTtlSeconds())
            );
            HoldOutcome outcome = HoldOutcome.fromRaw(raw);
            resultStr = outcome.name();
            return outcome;
        } catch (DataAccessException | RedisException ex) {
            throw new RedisUnavailableException("Failed to execute hold_inventory.lua in Redis", ex);
        } finally {
            recordLuaDuration("hold_inventory", resultStr, startNanos);
        }
    }

    public long release(UUID eventId, Collection<UUID> seatIds, UUID reservationId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        if (seatIds.isEmpty()) {
            return 0L;
        }

        List<String> keys = toSortedSeatKeys(eventId, seatIds);
        long startNanos = System.nanoTime();
        String resultStr = "ERROR";
        try {
            Long released = stringRedisTemplate.execute(
                    releaseInventoryScript,
                    keys,
                    reservationId.toString()
            );
            if (released == null) {
                throw new RedisUnavailableException("release_inventory.lua returned null reply");
            }
            long count = released;
            resultStr = count > 0 ? "RELEASED" : "NOOP";
            return count;
        } catch (DataAccessException | RedisException ex) {
            throw new RedisUnavailableException("Failed to execute release_inventory.lua in Redis", ex);
        } finally {
            recordLuaDuration("release_inventory", resultStr, startNanos);
        }
    }

    public ConfirmOutcome confirm(UUID eventId, Collection<UUID> seatIds, UUID reservationId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(seatIds, "seatIds must not be null");
        Objects.requireNonNull(reservationId, "reservationId must not be null");
        if (seatIds.isEmpty()) {
            return ConfirmOutcome.CONFIRMED;
        }

        List<String> keys = toSortedSeatKeys(eventId, seatIds);
        long startNanos = System.nanoTime();
        String resultStr = "ERROR";
        try {
            String raw = stringRedisTemplate.execute(
                    confirmInventoryScript,
                    keys,
                    reservationId.toString()
            );
            ConfirmOutcome outcome = ConfirmOutcome.fromRaw(raw);
            resultStr = outcome.name();
            return outcome;
        } catch (DataAccessException | RedisException ex) {
            throw new RedisUnavailableException("Failed to execute confirm_inventory.lua in Redis", ex);
        } finally {
            recordLuaDuration("confirm_inventory", resultStr, startNanos);
        }
    }

    private static List<String> toSortedSeatKeys(UUID eventId, Collection<UUID> seatIds) {
        return seatIds.stream()
                .map(id -> Objects.requireNonNull(id, "seatId must not be null"))
                .distinct()
                .sorted(Comparator.comparing(UUID::toString))
                .map(seatId -> RedisKeys.seatKey(eventId, seatId))
                .toList();
    }

    private void recordLuaDuration(String script, String result, long startNanos) {
        if (meterRegistry != null) {
            Timer.builder(METRIC_LUA_DURATION)
                    .tag("service", SERVICE_NAME)
                    .tag("script", script)
                    .tag("result", result)
                    .register(meterRegistry)
                    .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }
}


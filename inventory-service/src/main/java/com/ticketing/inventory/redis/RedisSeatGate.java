package com.ticketing.inventory.redis;

import com.ticketing.common.correlation.CorrelationConstants;
import io.lettuce.core.RedisException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class RedisSeatGate {

    private static final Logger log = LoggerFactory.getLogger(RedisSeatGate.class);

    private static final String SERVICE_NAME = "inventory-service";
    private static final String METRIC_LUA_DURATION = "ticketing_redis_lua_duration_seconds";
    private static final String METRIC_REDIS_UNAVAILABLE = "inventory_redis_unavailable_total";

    private final StringRedisTemplate stringRedisTemplate;
    private final DefaultRedisScript<String> holdInventoryScript;
    private final DefaultRedisScript<Long> releaseInventoryScript;
    private final DefaultRedisScript<String> confirmInventoryScript;
    private final InventoryRedisProperties properties;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final ExecutorService compensationExecutor;

    private volatile Runnable beforeHoldHook;
    private volatile Runnable afterHoldScriptHook;
    private volatile Runnable beforeReleaseHook;

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
        this.compensationExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "redis-compensation-thread");
            t.setDaemon(true);
            return t;
        });
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
            Runnable preHook = this.beforeHoldHook;
            if (preHook != null) {
                preHook.run();
            }
            String raw = stringRedisTemplate.execute(
                    holdInventoryScript,
                    keys,
                    reservationId.toString(),
                    String.valueOf(nowEpochMs),
                    String.valueOf(properties.getSeatHoldTtlSeconds())
            );
            Runnable hook = this.afterHoldScriptHook;
            if (hook != null) {
                hook.run();
            }
            HoldOutcome outcome = HoldOutcome.fromRaw(raw);
            resultStr = outcome.name();
            return outcome;
        } catch (DataAccessException | RedisException ex) {
            String cause = resolveCause(ex);
            recordUnavailable("hold", cause);
            bestEffortRelease(eventId, seatIds, reservationId);
            RedisUnavailableException rue = new RedisUnavailableException("Failed to execute hold_inventory.lua in Redis", ex);
            rue.setMetricRecorded(true);
            rue.setCompensationAttempted(true);
            throw rue;
        } catch (RedisUnavailableException ex) {
            String cause = resolveCause(ex);
            if (!ex.isMetricRecorded()) {
                recordUnavailable("hold", cause);
                ex.setMetricRecorded(true);
            }
            if (!ex.isCompensationAttempted()) {
                bestEffortRelease(eventId, seatIds, reservationId);
                ex.setCompensationAttempted(true);
            }
            throw ex;
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
            Runnable preHook = this.beforeReleaseHook;
            if (preHook != null) {
                preHook.run();
            }
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
            String cause = resolveCause(ex);
            recordUnavailable("release", cause);
            RedisUnavailableException rue = new RedisUnavailableException("Failed to execute release_inventory.lua in Redis", ex);
            rue.setMetricRecorded(true);
            throw rue;
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
                    keys
            );
            ConfirmOutcome outcome = ConfirmOutcome.fromRaw(raw);
            resultStr = outcome.name();
            return outcome;
        } catch (DataAccessException | RedisException ex) {
            String cause = resolveCause(ex);
            recordUnavailable("confirm", cause);
            RedisUnavailableException rue = new RedisUnavailableException("Failed to execute confirm_inventory.lua in Redis", ex);
            rue.setMetricRecorded(true);
            throw rue;
        } finally {
            recordLuaDuration("confirm_inventory", resultStr, startNanos);
        }
    }

    public void bestEffortRelease(UUID eventId, Collection<UUID> seatIds, UUID reservationId) {
        if (seatIds == null || seatIds.isEmpty()) {
            return;
        }
        String correlationId = MDC.get(CorrelationConstants.MDC_KEY);
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        try {
            CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
                if (mdcContext != null) {
                    MDC.setContextMap(mdcContext);
                }
                try {
                    return release(eventId, seatIds, reservationId);
                } finally {
                    MDC.clear();
                }
            }, compensationExecutor);
            Long releasedCount = future.get(300, TimeUnit.MILLISECONDS);
            log.info("Best-effort release compensation succeeded: releasedCount={}, eventId={}, reservationId={}, correlationId={}",
                    releasedCount, eventId, reservationId, correlationId);
        } catch (TimeoutException ex) {
            log.warn("Best-effort release compensation timed out (300 ms): eventId={}, reservationId={}, correlationId={}; relying on 660s safety-net TTL",
                    eventId, reservationId, correlationId);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Best-effort release compensation interrupted: eventId={}, reservationId={}, correlationId={}",
                    eventId, reservationId, correlationId);
        } catch (Throwable ex) {
            log.warn("Best-effort release compensation failed: eventId={}, reservationId={}, correlationId={}: {}; relying on 660s safety-net TTL",
                    eventId, reservationId, correlationId, ex.getMessage());
        }
    }

    public void recordUnavailable(String operation, String cause) {
        if (meterRegistry != null) {
            Counter.builder(METRIC_REDIS_UNAVAILABLE)
                    .tag("service", SERVICE_NAME)
                    .tag("operation", operation)
                    .tag("cause", cause)
                    .register(meterRegistry)
                    .increment();
        }
    }

    public static String resolveCause(Throwable ex) {
        if (ex == null) {
            return "other";
        }
        Throwable current = ex;
        while (current != null) {
            String className = current.getClass().getName();
            String simpleName = current.getClass().getSimpleName();
            String message = current.getMessage() != null ? current.getMessage().toLowerCase() : "";

            if (current instanceof io.lettuce.core.RedisCommandTimeoutException
                    || current instanceof org.springframework.dao.QueryTimeoutException
                    || current instanceof java.util.concurrent.TimeoutException
                    || simpleName.contains("Timeout")
                    || className.contains("Timeout")
                    || message.contains("timed out")
                    || message.contains("timeout")) {
                return "timeout";
            }

            if (current instanceof io.lettuce.core.RedisConnectionException
                    || current instanceof org.springframework.data.redis.RedisConnectionFailureException
                    || current instanceof java.net.ConnectException
                    || current instanceof java.net.SocketException
                    || simpleName.contains("Connection")
                    || className.contains("Connection")
                    || message.contains("connection")
                    || message.contains("connect")) {
                return "connection";
            }

            current = current.getCause();
        }
        return "other";
    }

    public void setBeforeHoldHook(Runnable beforeHoldHook) {
        this.beforeHoldHook = beforeHoldHook;
    }

    public void setAfterHoldScriptHook(Runnable afterHoldScriptHook) {
        this.afterHoldScriptHook = afterHoldScriptHook;
    }

    public void setBeforeReleaseHook(Runnable beforeReleaseHook) {
        this.beforeReleaseHook = beforeReleaseHook;
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

package com.ticketing.inventory.redis;

import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRepository;
import io.lettuce.core.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Service
public class RedisRewarmService {

    private static final Logger log = LoggerFactory.getLogger(RedisRewarmService.class);
    private static final byte[] SOLD_BYTES = RedisKeys.SOLD_MARKER.getBytes(StandardCharsets.UTF_8);
    private static final Set<String> REWARM_ELIGIBLE_STATUSES = Set.of("SCHEDULED", "ON_SALE", "SOLD_OUT");

    private final StringRedisTemplate stringRedisTemplate;
    private final EventRepository eventRepository;
    private final ReservationRepository reservationRepository;
    private final SeatRepository seatRepository;
    private final RedisSeatGate redisSeatGate;
    private final TransactionTemplate repeatableReadTxTemplate;
    private final InventoryRedisProperties properties;

    private final ConcurrentMap<UUID, CompletableFuture<Boolean>> inFlightRewarms = new ConcurrentHashMap<>();

    // Test hooks & observability
    private final AtomicInteger rewarmExecutionCount = new AtomicInteger(0);
    private volatile Runnable onPipelineBatchFlushed;
    private volatile Runnable afterHeldLocksReadHook;
    private volatile Runnable insideRewarmHook;

    public RedisRewarmService(
            StringRedisTemplate stringRedisTemplate,
            EventRepository eventRepository,
            ReservationRepository reservationRepository,
            SeatRepository seatRepository,
            RedisSeatGate redisSeatGate,
            PlatformTransactionManager transactionManager,
            InventoryRedisProperties properties) {
        this.stringRedisTemplate = Objects.requireNonNull(stringRedisTemplate, "stringRedisTemplate must not be null");
        this.eventRepository = Objects.requireNonNull(eventRepository, "eventRepository must not be null");
        this.reservationRepository = Objects.requireNonNull(reservationRepository, "reservationRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
        this.redisSeatGate = Objects.requireNonNull(redisSeatGate, "redisSeatGate must not be null");
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");

        this.repeatableReadTxTemplate = new TransactionTemplate(transactionManager);
        this.repeatableReadTxTemplate.setReadOnly(true);
        this.repeatableReadTxTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (properties.isRewarmOnStartup()) {
            log.info("Running startup Redis state rewarm from PostgreSQL");
            rewarmAll();
        }
    }

    public void rewarm() {
        rewarmAll();
    }

    public void rewarmAll() {
        List<EventRecord> events = eventRepository.findRewarmEligibleEvents();
        log.info("Rewarming {} eligible events", events.size());
        for (EventRecord event : events) {
            rewarmEvent(event.eventId());
        }
    }

    public boolean rewarmEvent(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");

        CompletableFuture<Boolean> newFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> existingFuture = inFlightRewarms.putIfAbsent(eventId, newFuture);

        if (existingFuture != null) {
            log.debug("Single-flight: joining existing rewarm for event {}", eventId);
            try {
                return existingFuture.join();
            } catch (CompletionException ce) {
                Throwable cause = ce.getCause();
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                if (cause instanceof Error err) {
                    throw err;
                }
                throw ce;
            }
        }

        try {
            boolean result = doRewarmEvent(eventId);
            newFuture.complete(result);
            return result;
        } catch (Throwable t) {
            newFuture.completeExceptionally(t);
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(t);
        } finally {
            inFlightRewarms.remove(eventId, newFuture);
        }
    }

    private boolean doRewarmEvent(UUID eventId) {
        if (insideRewarmHook != null) {
            insideRewarmHook.run();
        }
        rewarmExecutionCount.incrementAndGet();

        Optional<EventRecord> eventOpt = eventRepository.findById(eventId);
        if (eventOpt.isEmpty()) {
            return false;
        }
        EventRecord event = eventOpt.get();
        if (event.status() == null || !REWARM_ELIGIBLE_STATUSES.contains(event.status())) {
            log.info("Event {} has status {}; skipping rewarm", eventId, event.status());
            return false;
        }

        // 1. Read HELD locks of event in one read-only transaction with REPEATABLE_READ isolation
        List<ReservationRepository.HeldSeatLock> heldLocks = repeatableReadTxTemplate.execute(status ->
                reservationRepository.findActiveHeldSeatsByEventId(eventId)
        );
        if (heldLocks == null) {
            heldLocks = List.of();
        }

        if (afterHeldLocksReadHook != null) {
            afterHeldLocksReadHook.run();
        }

        // 2. Page through SOLD seats with keyset pagination on seat_id in pages of 1000,
        // flushing each page to Redis in a pipeline before reading the next page.
        int soldPageSize = properties.getSoldSeatsPageSize();
        UUID lastSeatId = null;
        while (true) {
            List<SeatRepository.SoldSeat> page = seatRepository.findSoldSeatsByEventIdKeyset(eventId, lastSeatId, soldPageSize);
            if (page.isEmpty()) {
                break;
            }

            List<Consumer<RedisConnection>> soldOps = new ArrayList<>(page.size());
            for (SeatRepository.SoldSeat sold : page) {
                byte[] key = utf8(RedisKeys.seatKey(eventId, sold.seatId()));
                soldOps.add(conn -> conn.stringCommands().set(key, SOLD_BYTES));
            }
            executeInBatchedPipelines(soldOps);

            lastSeatId = page.get(page.size() - 1).seatId();
            if (page.size() < soldPageSize) {
                break;
            }
        }

        // 3. Write HELD seat locks
        if (!heldLocks.isEmpty()) {
            long safetyMarginSeconds = properties.getHoldTtlSafetyMarginSeconds();
            List<Consumer<RedisConnection>> heldOps = new ArrayList<>(heldLocks.size());
            for (ReservationRepository.HeldSeatLock held : heldLocks) {
                byte[] key = utf8(RedisKeys.seatKey(eventId, held.seatId()));
                byte[] value = utf8(held.reservationId().toString());
                long ttlSeconds = Math.max(1L, Math.max(1L, held.remainingSeconds()) + safetyMarginSeconds);
                heldOps.add(conn -> conn.stringCommands().set(
                        key,
                        value,
                        Expiration.seconds(ttlSeconds),
                        RedisStringCommands.SetOption.SET_IF_ABSENT
                ));
            }
            executeInBatchedPipelines(heldOps);
        }

        // 4. Ordering barrier: write event:{eventId}:drop_ts LAST,
        // only after every seat command of that event has completed.
        long eventTtlSeconds = properties.getEventDropTsTtlSeconds();
        byte[] eventKey = utf8(RedisKeys.eventDropTsKey(eventId));
        byte[] eventVal = utf8(String.valueOf(event.dropTime().toEpochMilli()));
        executeInBatchedPipelines(List.of(conn -> conn.stringCommands().set(
                eventKey,
                eventVal,
                Expiration.seconds(eventTtlSeconds),
                RedisStringCommands.SetOption.UPSERT
        )));

        // 5. Ghost lock protection: re-read status of exactly those reservations from PostgreSQL
        // and for any that are no longer HELD call release_inventory.lua for their seats
        if (!heldLocks.isEmpty()) {
            Map<UUID, List<UUID>> seatsByRes = new LinkedHashMap<>();
            for (ReservationRepository.HeldSeatLock held : heldLocks) {
                seatsByRes.computeIfAbsent(held.reservationId(), k -> new ArrayList<>()).add(held.seatId());
            }
            for (Map.Entry<UUID, List<UUID>> entry : seatsByRes.entrySet()) {
                UUID resId = entry.getKey();
                List<UUID> seats = entry.getValue();
                Optional<String> statusOpt = reservationRepository.findStatusById(resId);
                if (statusOpt.isEmpty() || !"HELD".equals(statusOpt.get())) {
                    log.info("Ghost lock protection: reservation {} is no longer HELD ({}), releasing {} seats",
                            resId, statusOpt.orElse("NOT_FOUND"), seats.size());
                    redisSeatGate.release(eventId, seats, resId);
                }
            }
        }

        return true;
    }

    private void executeInBatchedPipelines(List<Consumer<RedisConnection>> operations) {
        if (operations.isEmpty()) {
            return;
        }
        int batchSize = Math.max(1, properties.getPipelineBatchSize());
        try {
            for (int start = 0; start < operations.size(); start += batchSize) {
                int end = Math.min(start + batchSize, operations.size());
                List<Consumer<RedisConnection>> batch = operations.subList(start, end);
                stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                    for (Consumer<RedisConnection> op : batch) {
                        op.accept(connection);
                    }
                    return null;
                });
                Runnable hook = this.onPipelineBatchFlushed;
                if (hook != null) {
                    hook.run();
                }
            }
        } catch (DataAccessException | RedisException ex) {
            throw new RedisUnavailableException("Failed to rewarm Redis state from PostgreSQL", ex);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // Testing hooks
    public void setOnPipelineBatchFlushed(Runnable onPipelineBatchFlushed) {
        this.onPipelineBatchFlushed = onPipelineBatchFlushed;
    }

    public void setAfterHeldLocksReadHook(Runnable afterHeldLocksReadHook) {
        this.afterHeldLocksReadHook = afterHeldLocksReadHook;
    }

    public void setInsideRewarmHook(Runnable insideRewarmHook) {
        this.insideRewarmHook = insideRewarmHook;
    }

    public int getRewarmExecutionCount() {
        return rewarmExecutionCount.get();
    }

    public void resetRewarmExecutionCount() {
        rewarmExecutionCount.set(0);
    }
}

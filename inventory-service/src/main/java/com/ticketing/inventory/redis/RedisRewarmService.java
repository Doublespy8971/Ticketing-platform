package com.ticketing.inventory.redis;

import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class RedisRewarmService {

    private static final Logger log = LoggerFactory.getLogger(RedisRewarmService.class);
    private static final byte[] SOLD_BYTES = RedisKeys.SOLD_MARKER.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate stringRedisTemplate;
    private final EventRepository eventRepository;
    private final ReservationRepository reservationRepository;
    private final SeatRepository seatRepository;
    private final InventoryRedisProperties properties;

    public RedisRewarmService(
            StringRedisTemplate stringRedisTemplate,
            EventRepository eventRepository,
            ReservationRepository reservationRepository,
            SeatRepository seatRepository,
            InventoryRedisProperties properties) {
        this.stringRedisTemplate = Objects.requireNonNull(stringRedisTemplate, "stringRedisTemplate must not be null");
        this.eventRepository = Objects.requireNonNull(eventRepository, "eventRepository must not be null");
        this.reservationRepository = Objects.requireNonNull(reservationRepository, "reservationRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
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
        List<EventRecord> events = eventRepository.findAll();
        List<ReservationRepository.HeldSeatLock> heldLocks = reservationRepository.findAllActiveHeldSeats();
        List<SeatRecord> soldSeats = seatRepository.findAllSoldSeats();

        List<Consumer<RedisConnection>> operations = new ArrayList<>(
                events.size() + heldLocks.size() + soldSeats.size()
        );

        long eventTtlSeconds = properties.getEventDropTsTtlSeconds();
        long safetyMarginSeconds = properties.getHoldTtlSafetyMarginSeconds();

        for (EventRecord event : events) {
            byte[] key = utf8(RedisKeys.eventDropTsKey(event.eventId()));
            byte[] value = utf8(String.valueOf(event.dropTime().toEpochMilli()));
            operations.add(conn -> conn.stringCommands().set(
                    key,
                    value,
                    Expiration.seconds(eventTtlSeconds),
                    RedisStringCommands.SetOption.UPSERT
            ));
        }

        for (ReservationRepository.HeldSeatLock held : heldLocks) {
            byte[] key = utf8(RedisKeys.seatKey(held.eventId(), held.seatId()));
            byte[] value = utf8(held.reservationId().toString());
            long ttlSeconds = Math.max(1L, Math.max(1L, held.remainingSeconds()) + safetyMarginSeconds);
            operations.add(conn -> conn.stringCommands().set(
                    key,
                    value,
                    Expiration.seconds(ttlSeconds),
                    RedisStringCommands.SetOption.SET_IF_ABSENT
            ));
        }

        for (SeatRecord sold : soldSeats) {
            byte[] key = utf8(RedisKeys.seatKey(sold.eventId(), sold.seatId()));
            operations.add(conn -> conn.stringCommands().set(key, SOLD_BYTES));
        }

        executeInBatchedPipelines(operations);
    }

    public boolean rewarmEvent(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Optional<EventRecord> eventOpt = eventRepository.findById(eventId);
        if (eventOpt.isEmpty()) {
            return false;
        }

        EventRecord event = eventOpt.get();
        List<ReservationRepository.HeldSeatLock> heldLocks = reservationRepository.findActiveHeldSeatsByEventId(eventId);
        List<UUID> soldSeatIds = seatRepository.findSoldSeatIdsByEventId(eventId);

        List<Consumer<RedisConnection>> operations = new ArrayList<>(
                1 + heldLocks.size() + soldSeatIds.size()
        );

        long eventTtlSeconds = properties.getEventDropTsTtlSeconds();
        long safetyMarginSeconds = properties.getHoldTtlSafetyMarginSeconds();

        byte[] eventKey = utf8(RedisKeys.eventDropTsKey(eventId));
        byte[] eventVal = utf8(String.valueOf(event.dropTime().toEpochMilli()));
        operations.add(conn -> conn.stringCommands().set(
                eventKey,
                eventVal,
                Expiration.seconds(eventTtlSeconds),
                RedisStringCommands.SetOption.UPSERT
        ));

        for (ReservationRepository.HeldSeatLock held : heldLocks) {
            byte[] key = utf8(RedisKeys.seatKey(eventId, held.seatId()));
            byte[] value = utf8(held.reservationId().toString());
            long ttlSeconds = Math.max(1L, Math.max(1L, held.remainingSeconds()) + safetyMarginSeconds);
            operations.add(conn -> conn.stringCommands().set(
                    key,
                    value,
                    Expiration.seconds(ttlSeconds),
                    RedisStringCommands.SetOption.SET_IF_ABSENT
            ));
        }

        for (UUID soldSeatId : soldSeatIds) {
            byte[] key = utf8(RedisKeys.seatKey(eventId, soldSeatId));
            operations.add(conn -> conn.stringCommands().set(key, SOLD_BYTES));
        }

        executeInBatchedPipelines(operations);
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
            }
        } catch (DataAccessException | RedisException ex) {
            throw new RedisUnavailableException("Failed to rewarm Redis state from PostgreSQL", ex);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}


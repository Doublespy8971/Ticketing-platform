package com.ticketing.inventory.service;

import com.ticketing.common.error.ConflictException;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ResourceNotFoundException;
import com.ticketing.inventory.api.dto.BulkCreateSeatsRequest;
import com.ticketing.inventory.api.dto.BulkCreateSeatsResponse;
import com.ticketing.inventory.api.dto.CreateEventRequest;
import com.ticketing.inventory.api.dto.CreateSeatItemRequest;
import com.ticketing.inventory.api.dto.EventResponse;
import com.ticketing.inventory.api.dto.SeatResponseItem;
import com.ticketing.inventory.api.dto.UpdateEventStatusRequest;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.RedisKeys;
import com.ticketing.inventory.redis.RedisRewarmService;
import com.ticketing.outbox.UuidV7;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Service
public class InventoryAdminService {

    private static final Logger log = LoggerFactory.getLogger(InventoryAdminService.class);

    private final EventRepository eventRepository;
    private final SeatRepository seatRepository;
    private final RedisRewarmService redisRewarmService;
    private final StringRedisTemplate stringRedisTemplate;
    private volatile Runnable redisSyncHook;

    public InventoryAdminService(
            EventRepository eventRepository,
            SeatRepository seatRepository,
            RedisRewarmService redisRewarmService,
            StringRedisTemplate stringRedisTemplate) {
        this.eventRepository = Objects.requireNonNull(eventRepository, "eventRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
        this.redisRewarmService = Objects.requireNonNull(redisRewarmService, "redisRewarmService must not be null");
        this.stringRedisTemplate = Objects.requireNonNull(stringRedisTemplate, "stringRedisTemplate must not be null");
    }

    public void setRedisSyncHook(Runnable hook) {
        this.redisSyncHook = hook;
    }

    @Transactional
    public EventResponse createEvent(CreateEventRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.dropTime() != null && request.eventTime() != null
                && !request.dropTime().isBefore(request.eventTime())) {
            throw new IllegalArgumentException("dropTime must be before eventTime");
        }

        UUID eventId = UuidV7.generate();
        Instant now = Instant.now();
        EventRecord event = new EventRecord(
                eventId,
                request.name(),
                request.venueName(),
                request.eventTime(),
                request.dropTime(),
                "SCHEDULED",
                now,
                now);

        eventRepository.insert(event);

        return new EventResponse(
                eventId,
                event.name(),
                event.venueName(),
                event.eventTime(),
                event.dropTime(),
                event.status(),
                now);
    }

    @Transactional
    public BulkCreateSeatsResponse bulkCreateSeats(UUID eventId, BulkCreateSeatsRequest request) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(request, "request must not be null");

        // Verify event exists
        eventRepository.findById(eventId)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found with ID: " + eventId));

        List<CreateSeatItemRequest> items = request.seats();
        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("seats must not be empty");
        }
        if (items.size() > 5000) {
            throw new IllegalArgumentException("seats count cannot exceed 5000");
        }

        // Reject duplicates within one request
        Set<String> seenLocations = new HashSet<>();
        for (CreateSeatItemRequest item : items) {
            String key = item.sectionCode().trim() + "#" + item.rowLabel().trim() + "#" + item.seatNumber().trim();
            if (!seenLocations.add(key)) {
                throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                        "Duplicate seat location within request: section " + item.sectionCode()
                                + ", row " + item.rowLabel() + ", seat " + item.seatNumber());
            }
        }

        Instant now = Instant.now();
        List<SeatRecord> seatRecords = new ArrayList<>(items.size());
        List<SeatResponseItem> responseItems = new ArrayList<>(items.size());

        for (CreateSeatItemRequest item : items) {
            UUID seatId = UuidV7.generate();
            seatRecords.add(new SeatRecord(
                    seatId,
                    eventId,
                    item.sectionCode(),
                    item.rowLabel(),
                    item.seatNumber(),
                    item.priceCents(),
                    "USD",
                    "AVAILABLE",
                    0L,
                    now,
                    now));
            responseItems.add(new SeatResponseItem(
                    seatId,
                    item.sectionCode(),
                    item.rowLabel(),
                    item.seatNumber(),
                    item.priceCents(),
                    "AVAILABLE"));
        }

        // Reject against existing seats with documented conflict error
        try {
            seatRepository.bulkInsert(seatRecords);
        } catch (DataIntegrityViolationException ex) {
            throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                    "One or more seats already exist for event " + eventId);
        }

        return new BulkCreateSeatsResponse(
                eventId,
                seatRecords.size(),
                0,
                responseItems);
    }

    @Transactional
    public EventResponse updateEventStatus(UUID eventId, UpdateEventStatusRequest request) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(request, "request must not be null");
        String newStatus = Objects.requireNonNull(request.status(), "status must not be null").trim();

        EventRecord event = eventRepository.findByIdForUpdate(eventId)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found with ID: " + eventId));

        String currentStatus = event.status();
        if (!isAllowedTransition(currentStatus, newStatus)) {
            throw new ConflictException(ErrorCode.INVALID_STATE_TRANSITION,
                    "Invalid event status transition from " + currentStatus + " to " + newStatus);
        }

        Instant updatedAt = Instant.now();
        eventRepository.updateStatus(eventId, newStatus, updatedAt);

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    postCommitRedisSync(eventId, newStatus);
                }
            });
        } else {
            postCommitRedisSync(eventId, newStatus);
        }

        return new EventResponse(
                eventId,
                event.name(),
                event.venueName(),
                event.eventTime(),
                event.dropTime(),
                newStatus,
                event.createdAt());
    }

    public static boolean isAllowedTransition(String currentStatus, String newStatus) {
        if (currentStatus == null || newStatus == null) {
            return false;
        }
        if (currentStatus.equals(newStatus)) {
            return false;
        }
        return switch (currentStatus) {
            case "SCHEDULED" -> "ON_SALE".equals(newStatus) || "CANCELLED".equals(newStatus);
            case "ON_SALE" ->
                "SOLD_OUT".equals(newStatus) || "CANCELLED".equals(newStatus) || "COMPLETED".equals(newStatus);
            case "SOLD_OUT" ->
                "ON_SALE".equals(newStatus) || "COMPLETED".equals(newStatus) || "CANCELLED".equals(newStatus);
            default -> false;
        };
    }

    private void postCommitRedisSync(UUID eventId, String newStatus) {
        try {
            if (redisSyncHook != null) {
                redisSyncHook.run();
            }
            if ("ON_SALE".equals(newStatus)) {
                redisRewarmService.rewarmEvent(eventId);
            } else if ("CANCELLED".equals(newStatus) || "COMPLETED".equals(newStatus)) {
                stringRedisTemplate.delete(RedisKeys.eventDropTsKey(eventId));
            }
        } catch (Throwable t) {
            log.error("Failed to synchronize Redis state for event {}: {}", eventId, t.getMessage(), t);
        }
    }
}

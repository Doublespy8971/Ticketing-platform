package com.ticketing.inventory.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.error.ConflictException;
import com.ticketing.common.error.ErrorCode;
import com.ticketing.common.error.ResourceNotFoundException;
import com.ticketing.common.events.SeatLine;
import com.ticketing.common.events.SeatReservedPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.inventory.api.IdempotencyPayloadMismatchException;
import com.ticketing.inventory.api.IdempotencyRequestInProgressException;
import com.ticketing.inventory.api.dto.CreateReservationRequest;
import com.ticketing.inventory.api.dto.ReservationResponse;
import com.ticketing.inventory.api.dto.ReservedSeatItemResponse;
import com.ticketing.inventory.domain.EventRecord;
import com.ticketing.inventory.domain.EventRepository;
import com.ticketing.inventory.domain.ReservationItemRecord;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRecord;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.idempotency.IdempotencyKeyRecord;
import com.ticketing.inventory.idempotency.IdempotencyKeyRepository;
import com.ticketing.inventory.redis.HoldOutcome;
import com.ticketing.inventory.redis.RedisRewarmService;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.inventory.redis.RedisUnavailableException;
import com.ticketing.outbox.OutboxWriter;
import com.ticketing.outbox.UuidV7;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    public static final String ENDPOINT = "POST /api/v1/reservations";
    public static final int MAX_SEATS_PER_RESERVATION = 6;

    private static final String SERVICE_NAME = "inventory-service";
    private static final String METRIC_RESERVATIONS_CREATED = "ticketing_reservations_created_total";
    private static final String METRIC_RESERVATIONS_REJECTED = "ticketing_reservations_rejected_total";
    private static final String METRIC_HOLD_REQUESTS = "inventory_hold_requests_total";
    private static final String METRIC_HOLD_DURATION = "inventory_hold_duration_seconds";
    private static final String METRIC_REDIS_COMPENSATIONS = "inventory_redis_compensations_total";

    private static final Set<String> OPEN_EVENT_STATUSES = Set.of("SCHEDULED", "ON_SALE");

    public record HoldExecutionResult(
            int httpStatus,
            ReservationResponse response,
            boolean idempotentReplay
    ) {
    }

    private final EventRepository eventRepository;
    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final RedisSeatGate redisSeatGate;
    private final RedisRewarmService redisRewarmService;
    private final OutboxWriter outboxWriter;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final long holdTtlSeconds;

    // Deterministic test hooks for failure injection
    private volatile Runnable inTransactionHook;
    private volatile Runnable beforeCommitHook;

    public ReservationService(
            EventRepository eventRepository,
            SeatRepository seatRepository,
            ReservationRepository reservationRepository,
            IdempotencyKeyRepository idempotencyKeyRepository,
            RedisSeatGate redisSeatGate,
            RedisRewarmService redisRewarmService,
            OutboxWriter outboxWriter,
            PlatformTransactionManager transactionManager,
            ObjectMapper objectMapper,
            Clock clock,
            ObjectProvider<MeterRegistry> meterRegistryProvider,
            @Value("${POSTGRES_HOLD_TTL_SECONDS:600}") long holdTtlSeconds) {
        this.eventRepository = Objects.requireNonNull(eventRepository, "eventRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
        this.reservationRepository = Objects.requireNonNull(reservationRepository, "reservationRepository must not be null");
        this.idempotencyKeyRepository = Objects.requireNonNull(idempotencyKeyRepository, "idempotencyKeyRepository must not be null");
        this.redisSeatGate = Objects.requireNonNull(redisSeatGate, "redisSeatGate must not be null");
        this.redisRewarmService = Objects.requireNonNull(redisRewarmService, "redisRewarmService must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.meterRegistry = meterRegistryProvider != null ? meterRegistryProvider.getIfAvailable() : null;
        this.holdTtlSeconds = holdTtlSeconds;
    }

    public HoldExecutionResult createReservation(
            UUID userId,
            UUID idempotencyKey,
            CreateReservationRequest request) {
        long startNanos = System.nanoTime();
        String outcomeTag = "ERROR";

        try {
            // 1. Validate request and seat count limit (1..6 distinct seat UUIDs)
            validateRequest(userId, idempotencyKey, request);

            UUID eventId = request.eventId();
            List<UUID> requestedSeatIds = request.seatIds();
            String correlationId = MDC.get(CorrelationConstants.MDC_KEY);

            // 2. HTTP Idempotency check & claim
            String requestHash = computeRequestHash(request);
            boolean claimed = claimOrCheckIdempotency(userId, idempotencyKey, requestHash);
            if (!claimed) {
                IdempotencyKeyRecord existing = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)
                        .orElseThrow(() -> new IdempotencyRequestInProgressException(
                                "Concurrent request with Idempotency-Key " + idempotencyKey + " is in progress."));
                ReservationResponse storedResponse = deserializeResponse(existing.responseBody());
                outcomeTag = "REPLAY";
                log.info("Idempotent replay for userId={}, idempotencyKey={}, reservationId={}, correlationId={}",
                        userId, idempotencyKey, storedResponse.reservationId(), correlationId);
                return new HoldExecutionResult(
                        existing.responseStatus() != null ? existing.responseStatus() : HttpStatus.CREATED.value(),
                        storedResponse,
                        true
                );
            }

            boolean idempotencyCommitted = false;
            try {
                // 3. Load event and seats from PostgreSQL and check the event is on sale and drop time has passed
                EventRecord event = eventRepository.findById(eventId)
                        .orElseThrow(() -> {
                            log.info("Event not found: eventId={}, userId={}, correlationId={}", eventId, userId, correlationId);
                            return new ResourceNotFoundException("Event not found with ID: " + eventId);
                        });

                checkEventOpenForSale(event, correlationId);

                List<SeatRecord> loadedSeats = seatRepository.findByEventIdAndSeatIds(eventId, requestedSeatIds);
                if (loadedSeats.size() != requestedSeatIds.size()) {
                    log.info("One or more requested seats not found: eventId={}, requested={}, found={}, correlationId={}",
                            eventId, requestedSeatIds.size(), loadedSeats.size(), correlationId);
                    throw new ResourceNotFoundException("One or more requested seats do not exist for event " + eventId);
                }

                Map<UUID, SeatRecord> seatsById = new HashMap<>(loadedSeats.size());
                for (SeatRecord seat : loadedSeats) {
                    seatsById.put(seat.seatId(), seat);
                }

                // Preserve caller seat ordering for the response and calculate total in minor units
                List<SeatRecord> seatsInRequestOrder = new ArrayList<>(requestedSeatIds.size());
                long totalAmountCents = 0L;
                String currency = "USD";
                for (UUID seatId : requestedSeatIds) {
                    SeatRecord seat = seatsById.get(seatId);
                    if (seat == null) {
                        throw new ResourceNotFoundException("Seat " + seatId + " does not exist for event " + eventId);
                    }
                    seatsInRequestOrder.add(seat);
                    totalAmountCents = Math.addExact(totalAmountCents, seat.priceCents());
                    if (seat.currency() != null && !seat.currency().isBlank()) {
                        currency = seat.currency().trim();
                    }
                }

                // 4. Generate reservationId as UUIDv7 (orderId == reservationId)
                UUID reservationId = UuidV7.generate();

                // 5. Call RedisSeatGate.hold; on EVENT_NOT_CACHED call rewarmEvent once and retry once
                HoldOutcome holdOutcome = acquireRedisSeatHold(eventId, requestedSeatIds, reservationId, correlationId);
                if (holdOutcome == HoldOutcome.EVENT_NOT_OPEN) {
                    outcomeTag = "EVENT_NOT_OPEN";
                    recordRejected("EVENT_NOT_OPEN");
                    log.info("Redis gate rejected hold (EVENT_NOT_OPEN): eventId={}, userId={}, correlationId={}",
                            eventId, userId, correlationId);
                    throw new ConflictException(ErrorCode.EVENT_NOT_OPEN,
                            "Event " + eventId + " has not reached its drop time yet.");
                }
                if (holdOutcome == HoldOutcome.SEAT_TAKEN) {
                    outcomeTag = "SEAT_ALREADY_TAKEN";
                    recordRejected("SEAT_ALREADY_TAKEN");
                    log.info("Redis gate rejected hold (SEAT_TAKEN): eventId={}, seatIds={}, userId={}, correlationId={}",
                            eventId, requestedSeatIds, userId, correlationId);
                    throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                            "One or more requested seats are currently HELD or SOLD.");
                }

                // 6. After a successful Redis hold (HELD), open ONE PostgreSQL transaction and compensate in finally on any failure
                boolean pgCommitted = false;
                final long finalTotalAmountCents = totalAmountCents;
                final String finalCurrency = currency;
                try {
                    ReservationResponse createdResponse = executeHoldTransaction(
                            userId,
                            idempotencyKey,
                            eventId,
                            reservationId,
                            seatsInRequestOrder,
                            loadedSeats,
                            finalTotalAmountCents,
                            finalCurrency
                    );
                    pgCommitted = true;
                    idempotencyCommitted = true;
                    outcomeTag = "CREATED";
                    recordCreated(eventId);
                    log.info("Seat hold created in Redis and PostgreSQL: reservationId={}, orderId={}, eventId={}, userId={}, totalAmountCents={}, expiresAt={}, correlationId={}",
                            reservationId, reservationId, eventId, userId, finalTotalAmountCents, createdResponse.expiresAt(), correlationId);
                    return new HoldExecutionResult(HttpStatus.CREATED.value(), createdResponse, false);
                } catch (ConflictException conflictEx) {
                    if (conflictEx.errorCode() == ErrorCode.SEAT_ALREADY_TAKEN) {
                        outcomeTag = "SEAT_ALREADY_TAKEN";
                        recordRejected("SEAT_ALREADY_TAKEN");
                    } else if (conflictEx.errorCode() == ErrorCode.EVENT_NOT_OPEN) {
                        outcomeTag = "EVENT_NOT_OPEN";
                        recordRejected("EVENT_NOT_OPEN");
                    }
                    throw conflictEx;
                } finally {
                    if (!pgCommitted) {
                        compensateRedisHold(eventId, requestedSeatIds, reservationId, correlationId);
                    }
                }
            } finally {
                if (!idempotencyCommitted) {
                    safeDeleteInProgress(userId, idempotencyKey);
                }
            }
        } catch (ResourceNotFoundException ex) {
            outcomeTag = "NOT_FOUND";
            throw ex;
        } finally {
            recordHoldMetrics(outcomeTag, startNanos);
        }
    }

    private void validateRequest(UUID userId, UUID idempotencyKey, CreateReservationRequest request) {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        if (request == null) {
            throw new IllegalArgumentException("Request body must not be null");
        }
        if (request.eventId() == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        List<UUID> seatIds = request.seatIds();
        if (seatIds == null || seatIds.isEmpty() || seatIds.size() > MAX_SEATS_PER_RESERVATION) {
            throw new IllegalArgumentException("seatIds must contain between 1 and 6 seat UUIDs");
        }
        Set<UUID> uniqueSeats = new HashSet<>(seatIds.size());
        for (UUID seatId : seatIds) {
            if (seatId == null) {
                throw new IllegalArgumentException("seatIds must not contain null elements");
            }
            if (!uniqueSeats.add(seatId)) {
                throw new IllegalArgumentException("seatIds must contain distinct UUIDs");
            }
        }
    }

    private void checkEventOpenForSale(EventRecord event, String correlationId) {
        String status = event.status();
        if (status == null || !OPEN_EVENT_STATUSES.contains(status)) {
            if ("SOLD_OUT".equals(status)) {
                recordRejected("SEAT_ALREADY_TAKEN");
                log.info("Event is SOLD_OUT: eventId={}, correlationId={}", event.eventId(), correlationId);
                throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                        "Event " + event.eventId() + " is sold out.");
            }
            recordRejected("EVENT_NOT_OPEN");
            log.info("Event status is not open for sale: eventId={}, status={}, correlationId={}",
                    event.eventId(), status, correlationId);
            throw new ConflictException(ErrorCode.EVENT_NOT_OPEN,
                    "Event " + event.eventId() + " is not open for reservations (status=" + status + ").");
        }

        Instant now = clock.instant();
        if (now.isBefore(event.dropTime())) {
            recordRejected("EVENT_NOT_OPEN");
            log.info("Event drop time has not passed: eventId={}, dropTime={}, now={}, correlationId={}",
                    event.eventId(), event.dropTime(), now, correlationId);
            throw new ConflictException(ErrorCode.EVENT_NOT_OPEN,
                    "Event " + event.eventId() + " drop time (" + event.dropTime() + ") has not been reached.");
        }
    }

    private HoldOutcome acquireRedisSeatHold(
            UUID eventId,
            List<UUID> seatIds,
            UUID reservationId,
            String correlationId) {
        HoldOutcome outcome = redisSeatGate.hold(eventId, seatIds, reservationId);
        if (outcome == HoldOutcome.EVENT_NOT_CACHED) {
            log.info("Redis returned EVENT_NOT_CACHED for eventId={}, reservationId={}, correlationId={}; rewarming once",
                    eventId, reservationId, correlationId);
            boolean rewarmed = redisRewarmService.rewarmEvent(eventId);
            if (!rewarmed) {
                throw new ResourceNotFoundException("Event not found with ID: " + eventId);
            }
            outcome = redisSeatGate.hold(eventId, seatIds, reservationId);
            if (outcome == HoldOutcome.EVENT_NOT_CACHED) {
                throw new RedisUnavailableException(
                        "Redis still returned EVENT_NOT_CACHED after rewarming event " + eventId);
            }
        }
        return outcome;
    }

    private ReservationResponse executeHoldTransaction(
            UUID userId,
            UUID idempotencyKey,
            UUID eventId,
            UUID reservationId,
            List<SeatRecord> seatsInRequestOrder,
            List<SeatRecord> seatsSortedById,
            long totalAmountCents,
            String currency) {
        return transactionTemplate.execute(status -> {
            Runnable commitHook = this.beforeCommitHook;
            if (commitHook != null && TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void beforeCommit(boolean readOnly) {
                        commitHook.run();
                    }
                });
            }

            // 1. Insert reservation with expires_at computed from DB clock as created_at + 600s
            ReservationRepository.ReservationTimestamps timestamps =
                    reservationRepository.insertHeldReservationWithDbClock(
                            reservationId,
                            eventId,
                            userId,
                            totalAmountCents,
                            currency,
                            holdTtlSeconds
                    );

            // 2. Insert reservation_items with seat details and prices (sorted by seat_id for deterministic lock order)
            List<SeatRecord> sortedForItems = new ArrayList<>(seatsSortedById);
            sortedForItems.sort(Comparator.comparing(SeatRecord::seatId));

            List<ReservationItemRecord> itemRecords = new ArrayList<>(sortedForItems.size());
            for (SeatRecord seat : sortedForItems) {
                itemRecords.add(new ReservationItemRecord(
                        UuidV7.generate(),
                        reservationId,
                        eventId,
                        seat.seatId(),
                        seat.sectionCode(),
                        seat.rowLabel(),
                        seat.seatNumber(),
                        seat.priceCents(),
                        "HELD",
                        timestamps.createdAt(),
                        timestamps.createdAt()
                ));
            }

            try {
                reservationRepository.insertItems(itemRecords, timestamps.createdAt());
            } catch (DataIntegrityViolationException ex) {
                throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                        "One or more requested seats are already held or sold.");
            }

            // 3. Guarded update setting requested seats to HELD and incrementing version only where status = 'AVAILABLE'
            List<UUID> sortedSeatIds = sortedForItems.stream().map(SeatRecord::seatId).toList();
            int updatedSeats = seatRepository.holdAvailableSeats(eventId, sortedSeatIds);
            if (updatedSeats != sortedSeatIds.size()) {
                throw new ConflictException(ErrorCode.SEAT_ALREADY_TAKEN,
                        "One or more requested seats are no longer AVAILABLE.");
            }

            Runnable txHook = this.inTransactionHook;
            if (txHook != null) {
                txHook.run();
            }

            // 4. Write inventory.seat.reserved event through OutboxWriter
            List<SeatLine> seatLines = new ArrayList<>(seatsInRequestOrder.size());
            List<ReservedSeatItemResponse> responseSeats = new ArrayList<>(seatsInRequestOrder.size());
            for (SeatRecord seat : seatsInRequestOrder) {
                seatLines.add(new SeatLine(
                        seat.seatId(),
                        seat.sectionCode(),
                        seat.rowLabel(),
                        seat.seatNumber(),
                        seat.priceCents()
                ));
                responseSeats.add(new ReservedSeatItemResponse(
                        seat.seatId(),
                        seat.sectionCode(),
                        seat.rowLabel(),
                        seat.seatNumber(),
                        seat.priceCents()
                ));
            }

            SeatReservedPayload payload = new SeatReservedPayload(
                    reservationId,
                    reservationId,
                    userId,
                    eventId,
                    seatLines,
                    totalAmountCents,
                    currency,
                    timestamps.createdAt(),
                    timestamps.expiresAt()
            );
            outboxWriter.append(reservationId, EventTypes.INVENTORY_SEAT_RESERVED, payload);

            // 5. Build response and store idempotency result atomically
            ReservationResponse response = new ReservationResponse(
                    reservationId,
                    reservationId,
                    eventId,
                    userId,
                    "HELD",
                    responseSeats,
                    totalAmountCents,
                    currency,
                    timestamps.createdAt(),
                    timestamps.expiresAt()
            );
            String responseJson = serializeResponse(response);
            idempotencyKeyRepository.markCompleted(userId, idempotencyKey, HttpStatus.CREATED.value(), responseJson);

            return response;
        });
    }

    private boolean claimOrCheckIdempotency(UUID userId, UUID idempotencyKey, String requestHash) {
        if (idempotencyKeyRepository.tryClaim(userId, idempotencyKey, ENDPOINT, requestHash)) {
            return true;
        }

        IdempotencyKeyRecord existing = idempotencyKeyRepository.findByUserAndKey(userId, idempotencyKey)
                .orElseThrow(() -> new IdempotencyRequestInProgressException(
                        "Concurrent request with Idempotency-Key " + idempotencyKey + " is currently in progress."));

        if (!ENDPOINT.equals(existing.endpoint()) || !requestHash.equals(existing.requestHash())) {
            throw new IdempotencyPayloadMismatchException(
                    "Idempotency-Key " + idempotencyKey + " was already used with a different request payload.");
        }

        if (IdempotencyKeyRepository.STATUS_IN_PROGRESS.equals(existing.status())) {
            if (idempotencyKeyRepository.reclaimStaleInProgress(userId, idempotencyKey)) {
                return true;
            }
            throw new IdempotencyRequestInProgressException(
                    "Request with Idempotency-Key " + idempotencyKey + " is currently in progress. Retry after 1 second.");
        }

        return false;
    }

    private void compensateRedisHold(UUID eventId, List<UUID> seatIds, UUID reservationId, String correlationId) {
        recordCompensation();
        try {
            long releasedCount = redisSeatGate.release(eventId, seatIds, reservationId);
            log.info("Compensated Redis seat hold: reservationId={}, eventId={}, releasedCount={}, correlationId={}",
                    reservationId, eventId, releasedCount, correlationId);
        } catch (Throwable releaseEx) {
            log.error("Failed to release Redis seat hold during compensation for reservationId={}, eventId={}, correlationId={}; relying on 660s Redis safety-net TTL",
                    reservationId, eventId, correlationId, releaseEx);
        }
    }

    private void safeDeleteInProgress(UUID userId, UUID idempotencyKey) {
        try {
            idempotencyKeyRepository.deleteInProgress(userId, idempotencyKey);
        } catch (Throwable ex) {
            log.warn("Failed to clean up IN_PROGRESS idempotency key for userId={}, idempotencyKey={}",
                    userId, idempotencyKey, ex);
        }
    }

    private String computeRequestHash(CreateReservationRequest request) {
        try {
            String canonicalJson = objectMapper.writeValueAsString(request);
            String input = "POST:/api/v1/reservations:" + canonicalJson;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes);
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to compute request SHA-256 hash", e);
        }
    }

    private String serializeResponse(ReservationResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ReservationResponse", e);
        }
    }

    private ReservationResponse deserializeResponse(String json) {
        try {
            return objectMapper.readValue(json, ReservationResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize stored ReservationResponse", e);
        }
    }

    private void recordCreated(UUID eventId) {
        if (meterRegistry != null) {
            Counter.builder(METRIC_RESERVATIONS_CREATED)
                    .tag("service", SERVICE_NAME)
                    .tag("event_id", eventId.toString())
                    .register(meterRegistry)
                    .increment();
        }
    }

    private void recordRejected(String reason) {
        if (meterRegistry != null) {
            Counter.builder(METRIC_RESERVATIONS_REJECTED)
                    .tag("service", SERVICE_NAME)
                    .tag("reason", reason)
                    .register(meterRegistry)
                    .increment();
        }
    }

    private void recordCompensation() {
        if (meterRegistry != null) {
            Counter.builder(METRIC_REDIS_COMPENSATIONS)
                    .tag("service", SERVICE_NAME)
                    .register(meterRegistry)
                    .increment();
        }
    }

    private void recordHoldMetrics(String outcome, long startNanos) {
        if (meterRegistry != null) {
            long durationNanos = System.nanoTime() - startNanos;
            Counter.builder(METRIC_HOLD_REQUESTS)
                    .tag("service", SERVICE_NAME)
                    .tag("outcome", outcome)
                    .register(meterRegistry)
                    .increment();
            Timer.builder(METRIC_HOLD_DURATION)
                    .tag("service", SERVICE_NAME)
                    .tag("outcome", outcome)
                    .register(meterRegistry)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    // Test hooks for deterministic failure injection
    public void setInTransactionHook(Runnable inTransactionHook) {
        this.inTransactionHook = inTransactionHook;
    }

    public void setBeforeCommitHook(Runnable beforeCommitHook) {
        this.beforeCommitHook = beforeCommitHook;
    }
}

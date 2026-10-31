package com.ticketing.inventory.service;

import com.ticketing.common.correlation.CorrelationConstants;
import com.ticketing.common.events.ReservationExpiredPayload;
import com.ticketing.common.messaging.EventTypes;
import com.ticketing.inventory.config.ExpirySweeperConfig;
import com.ticketing.inventory.config.ExpirySweeperProperties;
import com.ticketing.inventory.domain.ReservationRepository;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.redis.RedisSeatGate;
import com.ticketing.outbox.OutboxWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL-authoritative reservation hold expiry sweeper (docs/arch/08-redis.md, Section 9.3.2;
 * docs/arch/02-saga.md, Section 2.3 & 2.4.2).
 *
 * <p>Runs every 1000 ms (configurable) on its own dedicated single-thread scheduler
 * ({@link ExpirySweeperConfig#SCHEDULER_BEAN_NAME}), separate from the outbox scheduler.
 * Each sweep selects up to 500 {@code HELD} reservations with {@code expires_at <= clock_timestamp()} using
 * {@code FOR UPDATE SKIP LOCKED} and expires each reservation in its own short PostgreSQL
 * transaction before releasing the Redis seat locks post-commit.
 */
@Service
public class ExpirySweeper implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ExpirySweeper.class);

    public static final String SERVICE_NAME = "inventory-service";
    public static final String TRIGGER_POSTGRES_SWEEPER = "postgres_sweeper";

    public static final String METRIC_TICKETING_RESERVATIONS_EXPIRED = "ticketing_reservations_expired_total";
    public static final String METRIC_SWEEP_DURATION = "inventory_expiry_sweep_duration_seconds";
    public static final String METRIC_SWEEP_FAILURES = "inventory_expiry_sweep_failures_total";
    public static final String METRIC_OLDEST_OVERDUE_HOLD = "inventory_oldest_overdue_hold_seconds";

    private record ExpiredReservationOutcome(
            UUID reservationId,
            UUID eventId,
            UUID userId,
            List<UUID> seatIds,
            Instant expiredAt
    ) {
    }

    private final ReservationRepository reservationRepository;
    private final SeatRepository seatRepository;
    private final RedisSeatGate redisSeatGate;
    private final OutboxWriter outboxWriter;
    private final TransactionTemplate batchSelectTxTemplate;
    private final TransactionTemplate perReservationTxTemplate;
    private final ExpirySweeperProperties properties;
    private final MeterRegistry meterRegistry;

    private final AtomicLong oldestOverdueSecondsDoubleBits = new AtomicLong(Double.doubleToLongBits(0.0));
    private final AtomicInteger lastCandidateCount = new AtomicInteger(0);
    private volatile boolean stopped = false;

    // Cached Micrometer meters created once in constructor
    private final Counter expiredReservationsCounter;
    private final Counter sweepFailuresCounter;
    private final Timer sweepDurationTimer;

    // Optional deterministic hooks for concurrency and failure testing
    private volatile Runnable afterReservationLockHook;
    private volatile Runnable afterExpireCommitBeforeRedisReleaseHook;

    public ExpirySweeper(
            ReservationRepository reservationRepository,
            SeatRepository seatRepository,
            RedisSeatGate redisSeatGate,
            OutboxWriter outboxWriter,
            PlatformTransactionManager transactionManager,
            ExpirySweeperProperties properties,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this.reservationRepository = Objects.requireNonNull(reservationRepository, "reservationRepository must not be null");
        this.seatRepository = Objects.requireNonNull(seatRepository, "seatRepository must not be null");
        this.redisSeatGate = Objects.requireNonNull(redisSeatGate, "redisSeatGate must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.batchSelectTxTemplate = new TransactionTemplate(transactionManager);
        this.batchSelectTxTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.perReservationTxTemplate = new TransactionTemplate(transactionManager);
        this.perReservationTxTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.meterRegistry = meterRegistryProvider != null ? meterRegistryProvider.getIfAvailable() : null;

        if (this.meterRegistry != null) {
            this.expiredReservationsCounter = Counter.builder(METRIC_TICKETING_RESERVATIONS_EXPIRED)
                    .tag("service", SERVICE_NAME)
                    .tag("trigger", TRIGGER_POSTGRES_SWEEPER)
                    .register(this.meterRegistry);
            this.sweepFailuresCounter = Counter.builder(METRIC_SWEEP_FAILURES)
                    .tag("service", SERVICE_NAME)
                    .register(this.meterRegistry);
            this.sweepDurationTimer = Timer.builder(METRIC_SWEEP_DURATION)
                    .tag("service", SERVICE_NAME)
                    .register(this.meterRegistry);

            registerMetrics(this.meterRegistry);
        } else {
            this.expiredReservationsCounter = null;
            this.sweepFailuresCounter = null;
            this.sweepDurationTimer = null;
        }

        log.info("Reservation ExpirySweeper started: enabled={}, intervalMs={}",
                this.properties.isEnabled(), this.properties.getIntervalMs());
    }

    @Override
    public void destroy() {
        this.stopped = true;
    }

    @Scheduled(
            fixedDelayString = "${ticketing.sweeper.interval-ms:${HOLD_EXPIRY_SWEEP_INTERVAL_MS:1000}}",
            scheduler = ExpirySweeperConfig.SCHEDULER_BEAN_NAME
    )
    public int scheduledSweep() {
        if (stopped || !properties.isEnabled()) {
            return 0;
        }
        int totalExpired = 0;
        int maxPages = properties.getMaxPagesPerTick();
        int batchSize = properties.getBatchSize();
        for (int page = 0; page < maxPages && !stopped; page++) {
            int expired = sweep();
            totalExpired += expired;
            if (lastCandidateCount.get() < batchSize) {
                break;
            }
        }
        return totalExpired;
    }

    /**
     * Executes a single expiry sweep batch: selects up to {@code batchSize} (default 500)
     * {@code HELD} reservation IDs with {@code expires_at <= clock_timestamp()} ordered by {@code expires_at ASC}
     * using {@code FOR UPDATE SKIP LOCKED}, and expires each reservation in its own short transaction.
     *
     * @return the number of reservations transitioned to {@code EXPIRED} during this sweep
     */
    public int sweep() {
        if (stopped) {
            return 0;
        }
        long startNanos = System.nanoTime();
        int expiredCount = 0;
        try {
            int batchSize = properties.getBatchSize();
            List<UUID> candidates;
            try {
                candidates = batchSelectTxTemplate.execute(
                        status -> reservationRepository.findExpiredHeldReservationIds(batchSize));
            } catch (Throwable ex) {
                recordSweepFailure();
                log.error("Failed to select expired HELD reservations during expiry sweep", ex);
                lastCandidateCount.set(0);
                return 0;
            }
            int candidateCount = candidates != null ? candidates.size() : 0;
            lastCandidateCount.set(candidateCount);

            if (candidates == null || candidates.isEmpty()) {
                return 0;
            }

            Set<UUID> processedInSweep = new LinkedHashSet<>(candidates.size());
            for (UUID reservationId : candidates) {
                if (stopped) {
                    break;
                }
                if (reservationId == null || !processedInSweep.add(reservationId)) {
                    continue;
                }
                try {
                    if (expireReservation(reservationId)) {
                        expiredCount++;
                    }
                } catch (Throwable ex) {
                    recordSweepFailure();
                    log.error("Failed to expire reservation {} during sweep; continuing with rest of batch",
                            reservationId, ex);
                }
            }
            return expiredCount;
        } finally {
            updateOldestOverdueGauge();
            recordSweepDuration(startNanos);
        }
    }

    /**
     * Expires a single reservation in its own short PostgreSQL transaction and, only after
     * a successful commit, releases its seat locks in Redis.
     *
     * <p>Concurrency guarantee: This method acquires
     * {@code SELECT ... FROM reservations WHERE reservation_id = ? FOR UPDATE SKIP LOCKED}.
     * If the row lock cannot be acquired because another transaction holds it, the method skips
     * the reservation without blocking, leaves it {@code HELD}, and logs at {@code DEBUG} level.
     *
     * @param reservationId the reservation to expire
     * @return {@code true} if the reservation was transitioned from {@code HELD} to {@code EXPIRED};
     *         {@code false} if it was skipped (locked), did not exist, was no longer {@code HELD}, or had not yet expired
     */
    public boolean expireReservation(UUID reservationId) {
        Objects.requireNonNull(reservationId, "reservationId must not be null");

        String existingCorrelationId = MDC.get(CorrelationConstants.MDC_KEY);
        boolean generatedCorrelationId = false;
        String correlationId = existingCorrelationId;
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
            MDC.put(CorrelationConstants.MDC_KEY, correlationId);
            generatedCorrelationId = true;
        }

        try {
            ExpiredReservationOutcome outcome = perReservationTxTemplate.execute(
                    status -> executeExpireTransaction(reservationId));
            if (outcome == null) {
                log.debug("Skipped expiring reservation {}", reservationId);
                return false;
            }

            recordReservationExpired();

            Runnable postCommitHook = this.afterExpireCommitBeforeRedisReleaseHook;
            if (postCommitHook != null) {
                postCommitHook.run();
            }

            // PostgreSQL committed first; now release seat locks in Redis second (token-checked by reservationId)
            releaseSeatsInRedisSafely(outcome, correlationId);
            log.info("Expired reservation in PostgreSQL and processed Redis release: reservationId={}, orderId={}, eventId={}, userId={}, seatCount={}, expiredAt={}, correlationId={}",
                     reservationId, reservationId, outcome.eventId(), outcome.userId(),
                     outcome.seatIds().size(), outcome.expiredAt(), correlationId);
            return true;
        } finally {
            if (generatedCorrelationId) {
                MDC.remove(CorrelationConstants.MDC_KEY);
            }
        }
    }

    private ExpiredReservationOutcome executeExpireTransaction(UUID reservationId) {
        // 1. Non-blocking lock: acquire row lock with SKIP LOCKED and re-check that status is still HELD and expires_at has passed (DB clock)
        Optional<ReservationRepository.LockedReservation> lockedOpt =
                reservationRepository.lockForUpdateSkipLocked(reservationId);
        if (lockedOpt.isEmpty()) {
            log.debug("Skipped expiring reservation {}: row is locked by another transaction or does not exist", reservationId);
            return null;
        }

        ReservationRepository.LockedReservation locked = lockedOpt.get();
        Runnable lockHook = this.afterReservationLockHook;
        if (lockHook != null) {
            lockHook.run();
        }

        if (!"HELD".equals(locked.status()) || !locked.expired()) {
            log.debug("Skipped expiring reservation {}: status={} (expected HELD), isExpired={}",
                    reservationId, locked.status(), locked.expired());
            return null;
        }

        // 2. Set the reservation to EXPIRED with expired_at from the database clock
        Instant expiredAt = reservationRepository.markExpiredWithDbClock(reservationId)
                .orElseThrow(() -> new IllegalStateException(
                        "Failed to mark HELD reservation EXPIRED: " + reservationId));

        // 3. Set its items from HELD to EXPIRED
        List<UUID> seatIds = reservationRepository.expireHeldItems(reservationId);
        if (seatIds.isEmpty()) {
            throw new IllegalStateException(
                    "Expired reservation " + reservationId + " had no HELD reservation_items");
        }

        // 4. Set those seats (where status is HELD and event matches) back to AVAILABLE with version incremented,
        // and verify the updated count equals the item count or fail the transaction
        int updatedSeats = seatRepository.releaseHeldSeats(locked.eventId(), seatIds);
        if (updatedSeats != seatIds.size()) {
            throw new IllegalStateException(
                    "Seat update count mismatch when expiring reservation " + reservationId
                            + ": expected " + seatIds.size() + " HELD seats for event " + locked.eventId()
                            + ", but updated " + updatedSeats);
        }

        // 5. Write inventory.reservation.expired event through OutboxWriter with the documented payload
        ReservationExpiredPayload payload = new ReservationExpiredPayload(
                reservationId,
                reservationId,
                locked.userId(),
                locked.eventId(),
                seatIds,
                expiredAt
        );
        outboxWriter.append(reservationId, EventTypes.INVENTORY_RESERVATION_EXPIRED, payload);

        return new ExpiredReservationOutcome(
                reservationId,
                locked.eventId(),
                locked.userId(),
                seatIds,
                expiredAt
        );
    }

    private void releaseSeatsInRedisSafely(ExpiredReservationOutcome outcome, String correlationId) {
        boolean interrupted = Thread.interrupted();
        try {
            long releasedCount = redisSeatGate.release(outcome.eventId(), outcome.seatIds(), outcome.reservationId());
            log.debug("Released expired reservation seats in Redis: reservationId={}, eventId={}, releasedCount={}, correlationId={}",
                    outcome.reservationId(), outcome.eventId(), releasedCount, correlationId);
        } catch (Throwable ex) {
            log.warn("Failed to release Redis seat hold after expiring reservationId={}, eventId={}, correlationId={}; relying on 660s safety-net TTL",
                    outcome.reservationId(), outcome.eventId(), correlationId, ex);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void updateOldestOverdueGauge() {
        try {
            double oldest = reservationRepository.queryOldestOverdueHoldSeconds();
            oldestOverdueSecondsDoubleBits.set(Double.doubleToLongBits(Math.max(0.0, oldest)));
        } catch (Throwable ex) {
            log.warn("Failed to update oldest overdue hold gauge after sweep", ex);
        }
    }

    public synchronized void registerMetrics(MeterRegistry registry) {
        if (registry == null) {
            return;
        }
        if (registry.find(METRIC_OLDEST_OVERDUE_HOLD).gauge() == null) {
            Gauge.builder(METRIC_OLDEST_OVERDUE_HOLD, this.oldestOverdueSecondsDoubleBits, bits -> Double.longBitsToDouble(bits.get()))
                    .strongReference(true)
                    .tag("service", SERVICE_NAME)
                    .description("Age in seconds of the oldest overdue HELD reservation")
                    .register(registry);
        }
    }

    public double getOldestOverdueHoldSeconds() {
        return Double.longBitsToDouble(oldestOverdueSecondsDoubleBits.get());
    }

    public void invalidateOverdueHoldGaugeCache() {
        updateOldestOverdueGauge();
    }

    public int getLastCandidateCount() {
        return lastCandidateCount.get();
    }

    private void recordReservationExpired() {
        if (expiredReservationsCounter != null) {
            expiredReservationsCounter.increment();
        }
    }

    private void recordSweepFailure() {
        if (sweepFailuresCounter != null) {
            sweepFailuresCounter.increment();
        }
    }

    private void recordSweepDuration(long startNanos) {
        if (sweepDurationTimer != null) {
            sweepDurationTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void setAfterReservationLockHook(Runnable afterReservationLockHook) {
        this.afterReservationLockHook = afterReservationLockHook;
    }

    public void setAfterExpireCommitBeforeRedisReleaseHook(Runnable afterExpireCommitBeforeRedisReleaseHook) {
        this.afterExpireCommitBeforeRedisReleaseHook = afterExpireCommitBeforeRedisReleaseHook;
    }
}

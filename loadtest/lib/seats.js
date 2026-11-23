import { SharedArray } from 'k6/data';

/**
 * Loads results/<ts>/seats.json (written externally by scripts/loadtest-seed.py)
 * via SharedArray in the k6 init stage so VUs share ONE single read-only copy in memory.
 *
 * Returns:
 *   - seatIds: SharedArray of all seat UUID strings (flat, indexed 0..N-1)
 *   - eventIds: SharedArray of event UUID strings (one per 5000-seat event)
 *   - meta: plain small object { totalSeats, seatsPerEvent, warmupSeats, measuredSeats, seatSupply, plan }
 */
export function loadSeatManifest(manifestPath) {
  const path = manifestPath || __ENV.SEATS_MANIFEST || '/results/seats.json';

  const seatIds = new SharedArray('manifest_seat_ids', function () {
    const raw = JSON.parse(open(path));
    const flat = [];
    for (const ev of raw.events || []) {
      for (const sid of ev.seat_ids || []) {
        flat.push(sid);
      }
    }
    return flat;
  });

  const eventIds = new SharedArray('manifest_event_ids', function () {
    const raw = JSON.parse(open(path));
    return (raw.events || []).map((ev) => ev.event_id);
  });

  const metaArr = new SharedArray('manifest_meta', function () {
    const raw = JSON.parse(open(path));
    const plan = raw.plan || {};
    return [{
      totalSeats: raw.total_seats || 0,
      seatsPerEvent: raw.seats_per_event || 5000,
      eventCount: raw.event_count || 0,
      warmupSeats: plan.warmup_seats || 0,
      measuredSeats: plan.measured_seats || raw.total_seats || 0,
      seatSupply: plan.seat_supply || 'ample',
      plan: plan,
    }];
  });

  return {
    seatIds: seatIds,
    eventIds: eventIds,
    meta: metaArr[0],
  };
}

/**
 * Resolves a flat seat index into { eventId, seatId } without duplicating eventId strings.
 */
export function seatAt(manifest, index) {
  const spe = manifest.meta.seatsPerEvent;
  const evIdx = Math.floor(index / spe);
  return {
    eventId: manifest.eventIds[evIdx],
    seatId: manifest.seatIds[index],
  };
}

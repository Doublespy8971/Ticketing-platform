import http from 'k6/http';
import { sleep } from 'k6';

const BULK_CHUNK_SIZE = 2500;

/**
 * Builds common JSON headers with optional Bearer token and Idempotency-Key.
 */
function buildHeaders(token, idempotencyKey, includeContentType = true) {
  const headers = {};
  if (includeContentType) {
    headers['Content-Type'] = 'application/json';
  }
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  return headers;
}

/**
 * Creates an event via POST /api/v1/admin/events.
 */
export function createEvent(baseUrl, adminToken, name, venueName, dropTimeIso, eventTimeIso) {
  const payload = JSON.stringify({
    name: name,
    venueName: venueName,
    dropTime: dropTimeIso,
    eventTime: eventTimeIso,
  });

  const res = http.post(`${baseUrl}/api/v1/admin/events`, payload, {
    headers: buildHeaders(adminToken, null, true),
    tags: { name: 'setup_create_event' },
    timeout: '15s',
  });

  if (res.status !== 201) {
    throw new Error(`Failed to create event ${name}: HTTP ${res.status} body=${res.body}`);
  }
  const body = res.json();
  if (!body || !body.eventId) {
    throw new Error(`Create event response missing eventId: ${res.body}`);
  }
  return body;
}

/**
 * Bulk-creates totalSeats unique seats for eventId via POST /api/v1/admin/events/{eventId}/seats/bulk.
 * Ensures unique (sectionCode, rowLabel, seatNumber) tuples across all seats.
 */
export function bulkCreateSeats(baseUrl, adminToken, eventId, totalSeats = 5000) {
  const seatIds = [];
  const rowsPerSection = 10;
  const seatsPerRow = 25;
  const seatsPerSection = rowsPerSection * seatsPerRow; // 250

  for (let offset = 0; offset < totalSeats; offset += BULK_CHUNK_SIZE) {
    const count = Math.min(BULK_CHUNK_SIZE, totalSeats - offset);
    const seats = new Array(count);
    for (let j = 0; j < count; j++) {
      const idx = offset + j;
      const sectionIdx = Math.floor(idx / seatsPerSection);
      const inSection = idx % seatsPerSection;
      const rowIdx = Math.floor(inSection / seatsPerRow) + 1;
      const seatNum = (inSection % seatsPerRow) + 1;

      seats[j] = {
        sectionCode: `S${String(sectionIdx).padStart(2, '0')}`,
        rowLabel: `R${String(rowIdx).padStart(2, '0')}`,
        seatNumber: String(seatNum).padStart(2, '0'),
        priceCents: 10000 + (idx % 5) * 1000,
      };
    }

    const res = http.post(
      `${baseUrl}/api/v1/admin/events/${eventId}/seats/bulk`,
      JSON.stringify({ seats: seats }),
      {
        headers: buildHeaders(adminToken, null, true),
        tags: { name: 'setup_bulk_seats' },
        timeout: '30s',
      }
    );

    if (res.status !== 201) {
      throw new Error(`Failed to bulk-create seats for event ${eventId} (offset=${offset}): HTTP ${res.status} body=${res.body}`);
    }
    const body = res.json();
    if (!body || !Array.isArray(body.seats) || body.seats.length !== count) {
      throw new Error(`Bulk create returned unexpected seat count: expected ${count}, body=${res.body}`);
    }
    for (const s of body.seats) {
      seatIds.push(s.seatId);
    }

    if (offset + BULK_CHUNK_SIZE < totalSeats) {
      sleep(0.25);
    }
  }

  return seatIds;
}

/**
 * Transitions an event to ON_SALE via PATCH /api/v1/admin/events/{eventId}/status.
 */
export function setEventOnSale(baseUrl, adminToken, eventId) {
  const res = http.patch(
    `${baseUrl}/api/v1/admin/events/${eventId}/status`,
    JSON.stringify({ status: 'ON_SALE' }),
    {
      headers: buildHeaders(adminToken, null, true),
      tags: { name: 'setup_event_on_sale' },
      timeout: '15s',
    }
  );
  if (res.status !== 200) {
    throw new Error(`Failed to set event ${eventId} ON_SALE: HTTP ${res.status} body=${res.body}`);
  }
  return res.json();
}

/**
 * Waits until an event and its seats are visible via public GET /api/v1/events/{eventId}/seats.
 */
export function waitEventVisible(baseUrl, eventId, maxAttempts = 20) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = http.get(`${baseUrl}/api/v1/events/${eventId}/seats?limit=1`, {
      tags: { name: 'setup_wait_visible' },
      timeout: '10s',
    });
    if (res.status === 200) {
      const body = res.json();
      const items = (body && (body.items || body.seats)) || [];
      if (items.length >= 1) {
        return true;
      }
    }
    sleep(0.25);
  }
  throw new Error(`Event ${eventId} did not become visible after ${maxAttempts} attempts`);
}

/**
 * Fetches all seat IDs for an existing event via keyset pagination.
 */
export function fetchAllSeatIds(baseUrl, eventId) {
  const seatIds = [];
  let cursor = null;
  let pages = 0;

  while (pages < 50) {
    pages++;
    const url = cursor
      ? `${baseUrl}/api/v1/events/${eventId}/seats?limit=1000&cursor=${cursor}`
      : `${baseUrl}/api/v1/events/${eventId}/seats?limit=1000`;

    const res = http.get(url, {
      tags: { name: 'setup_fetch_seats' },
      timeout: '15s',
    });
    if (res.status !== 200) {
      throw new Error(`Failed to fetch seats for event ${eventId}: HTTP ${res.status} body=${res.body}`);
    }
    const body = res.json();
    const items = (body && (body.items || body.seats)) || [];
    for (const s of items) {
      seatIds.push(s.seatId || s.id);
    }
    cursor = body ? body.nextCursor : null;
    if (!cursor) {
      break;
    }
    sleep(0.15);
  }

  return seatIds;
}

/**
 * Setup helper: creates two events with seatsPerEvent (default 5000) unique seats each,
 * past dropTime, puts them ON_SALE, waits until visible, and returns both event objects.
 */
export function setupTwoEvents(baseUrl, adminToken, runId, seatsPerEvent = 5000) {
  const nowMs = Date.now();
  const dropTimeIso = new Date(nowMs - 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  const eventTimeIso = new Date(nowMs + 24 * 3600 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

  const events = [];
  const labels = ['flash', 'journey'];

  for (let i = 0; i < 2; i++) {
    const evName = `loadtest-${runId}-${labels[i]}`;
    const created = createEvent(baseUrl, adminToken, evName, 'LoadTest Arena', dropTimeIso, eventTimeIso);
    const seatIds = bulkCreateSeats(baseUrl, adminToken, created.eventId, seatsPerEvent);
    setEventOnSale(baseUrl, adminToken, created.eventId);
    waitEventVisible(baseUrl, created.eventId);

    events.push({
      eventId: created.eventId,
      name: evName,
      seatIds: seatIds,
    });
    sleep(0.25);
  }

  return {
    flashEvent: events[0],
    journeyEvent: events[1],
  };
}

/**
 * Setup helper: creates `eventCount` events of `seatsPerEvent` (default 5000) unique seats each,
 * bulk-creating in chunks of 2500 (within the documented 5000-seat bulk limit), puts each ON_SALE,
 * waits until visible, and fails clearly if fewer than expected seats are created.
 */
export function setupMultipleEvents(baseUrl, adminToken, runId, labelPrefix, eventCount, seatsPerEvent = 5000) {
  if (eventCount < 1 || seatsPerEvent < 1) {
    throw new Error(`Invalid setupMultipleEvents arguments: eventCount=${eventCount}, seatsPerEvent=${seatsPerEvent}`);
  }
  const nowMs = Date.now();
  const dropTimeIso = new Date(nowMs - 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  const eventTimeIso = new Date(nowMs + 24 * 3600 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

  const events = [];
  let totalSeatsCreated = 0;

  for (let i = 0; i < eventCount; i++) {
    const suffix = String(i + 1).padStart(2, '0');
    const evName = `loadtest-${runId}-${labelPrefix}-${suffix}`;
    const created = createEvent(baseUrl, adminToken, evName, 'LoadTest Arena', dropTimeIso, eventTimeIso);
    const seatIds = bulkCreateSeats(baseUrl, adminToken, created.eventId, seatsPerEvent);
    if (seatIds.length !== seatsPerEvent) {
      throw new Error(
        `Unable to create required seats for ${evName}: expected ${seatsPerEvent}, got ${seatIds.length}`
      );
    }
    setEventOnSale(baseUrl, adminToken, created.eventId);
    waitEventVisible(baseUrl, created.eventId);
    events.push({
      eventId: created.eventId,
      name: evName,
      seatIds: seatIds,
    });
    totalSeatsCreated += seatIds.length;
    if (i + 1 < eventCount) {
      sleep(0.2);
    }
  }

  console.log(
    `[setupMultipleEvents] Created ${events.length} events (${totalSeatsCreated} total seats, ${seatsPerEvent}/event) for run ${runId}`
  );
  return events;
}

/**
 * POST /api/v1/reservations (Create Seat Hold)
 */
export function postHold(baseUrl, userToken, eventId, seatId, idempotencyKey, expectedStatusesCallback, extraTags = null) {
  const tags = extraTags ? Object.assign({ name: 'hold' }, extraTags) : { name: 'hold' };
  const params = {
    headers: buildHeaders(userToken, idempotencyKey, true),
    tags: tags,
    timeout: '15s',
  };
  if (expectedStatusesCallback) {
    params.responseCallback = expectedStatusesCallback;
  }
  return http.post(
    `${baseUrl}/api/v1/reservations`,
    JSON.stringify({ eventId: eventId, seatIds: [seatId] }),
    params
  );
}

/**
 * POST /api/v1/orders/{orderId}/checkout (Checkout Order)
 */
export function postCheckout(baseUrl, userToken, orderId, paymentMethodToken, idempotencyKey, expectedStatusesCallback, extraTags = null) {
  const tags = extraTags ? Object.assign({ name: 'checkout' }, extraTags) : { name: 'checkout' };
  const params = {
    headers: buildHeaders(userToken, idempotencyKey, true),
    tags: tags,
    timeout: '15s',
  };
  if (expectedStatusesCallback) {
    params.responseCallback = expectedStatusesCallback;
  }
  return http.post(
    `${baseUrl}/api/v1/orders/${orderId}/checkout`,
    JSON.stringify({ paymentMethodToken: paymentMethodToken }),
    params
  );
}

/**
 * GET /api/v1/orders/{orderId} (Poll Order Status — strictly no body per gateway rules)
 */
export function getOrder(baseUrl, userToken, orderId, expectedStatusesCallback, extraTags = null) {
  const tags = extraTags ? Object.assign({ name: 'get_order' }, extraTags) : { name: 'get_order' };
  const params = {
    headers: buildHeaders(userToken, null, false),
    tags: tags,
    timeout: '15s',
  };
  if (expectedStatusesCallback) {
    params.responseCallback = expectedStatusesCallback;
  }
  return http.get(`${baseUrl}/api/v1/orders/${orderId}`, params);
}

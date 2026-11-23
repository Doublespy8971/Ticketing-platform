#!/usr/bin/env python3
"""
scripts/loadtest-seed.py — External Seat & Event Seeder for k6 Scenarios

Python 3 standard library only.
Mints a local HS256 ADMIN JWT using JWT_SECRET from infra/.env (never printed),
creates ON_SALE events via the gateway admin API, bulk-creates seats in chunks
within the documented 5,000-seat bulk limit (chunk=2,500), and writes a compact
manifest results/<ts>/seats.json for k6 SharedArray init loading.

Enforces the hard cap of 150,000 seats total. If a planned breakpoint staircase
plus warm-up plus 10% margin exceeds 150,000 seats, automatically lowers the
staircase MAX to the highest step that fits within 150,000 seats, prints the
exact lowering, and records it in seats.json.
"""

import argparse
import base64
import datetime
import hashlib
import hmac
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
import uuid


DOCUMENTED_MAX_BULK = 5000
BULK_CHUNK_SIZE = 2500
DEFAULT_SEATS_PER_EVENT = 5000
HARD_MAX_SEATS = 150000


def load_jwt_secret(repo_root):
    secret = os.environ.get("JWT_SECRET", "").strip()
    if secret:
        return secret
    env_file = os.path.join(repo_root, "infra", ".env")
    if not os.path.isfile(env_file):
        env_file = os.path.join(repo_root, "infra", ".env.example")
    if os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("JWT_SECRET="):
                    val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if val:
                        return val
    raise RuntimeError("JWT_SECRET not found in environment or infra/.env")


def b64url(data_bytes):
    return base64.urlsafe_b64encode(data_bytes).rstrip(b"=").decode("ascii")


def mint_admin_jwt(secret, ttl_seconds=7200):
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": str(uuid.uuid4()),
        "roles": ["ADMIN", "USER"],
        "iat": now,
        "exp": now + int(ttl_seconds),
    }
    h_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{h_b64}.{p_b64}.{b64url(sig)}"


def http_json(method, url, token=None, body=None, timeout=30):
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {url}: {err_body[:400]}") from exc


def compute_staircase_plan(start, step, max_rate, step_seconds, warmup_rate, warmup_seconds,
                           seat_supply="ample", hard_cap=HARD_MAX_SEATS, seats_per_event=DEFAULT_SEATS_PER_EVENT):
    """
    Computes required seats for warm-up + staircase + 10% margin.
    In ample mode, if needed seats exceed hard_cap (150,000), lowers max_rate
    to the highest step that fits within hard_cap.
    In scarce mode, reserves warm-up seats + 1 event (seats_per_event) for measured contention.
    """
    warmup_planned = int(math.ceil(warmup_rate * warmup_seconds))
    warmup_seats = int(math.ceil(warmup_planned * 1.10)) if warmup_planned > 0 else 0

    if seat_supply == "scarce":
        total_seats = min(hard_cap, warmup_seats + seats_per_event)
        return {
            "seat_supply": "scarce",
            "requested_max": max_rate,
            "effective_max": max_rate,
            "cap_applied": False,
            "cap_reason": None,
            "warmup_seats": warmup_seats,
            "measured_seats": total_seats - warmup_seats,
            "total_seats": total_seats,
            "planned_requests": warmup_planned + seats_per_event,
        }

    def staircase_reqs(eff_max):
        total = 0
        steps = 0
        r = start
        while r <= eff_max and steps < 40:
            total += int(r * step_seconds)
            steps += 1
            r += step
        return total, steps

    orig_reqs, _ = staircase_reqs(max_rate)
    orig_needed = int(math.ceil((warmup_planned + orig_reqs) * 1.10))

    if orig_needed <= hard_cap:
        return {
            "seat_supply": "ample",
            "requested_max": max_rate,
            "effective_max": max_rate,
            "cap_applied": False,
            "cap_reason": None,
            "warmup_seats": warmup_seats,
            "measured_seats": orig_needed - warmup_seats,
            "total_seats": orig_needed,
            "planned_requests": warmup_planned + orig_reqs,
        }

    # Lower effective_max to highest step fitting within hard_cap
    eff_max = start
    best_reqs = 0
    best_needed = 0
    r = start
    steps = 0
    while r <= max_rate and steps < 40:
        cand_reqs, _ = staircase_reqs(r)
        cand_needed = int(math.ceil((warmup_planned + cand_reqs) * 1.10))
        if cand_needed <= hard_cap:
            eff_max = r
            best_reqs = cand_reqs
            best_needed = cand_needed
        else:
            break
        steps += 1
        r += step

    if best_needed == 0 or best_needed > hard_cap:
        raise RuntimeError(
            f"Even minimum step START={start} ({step_seconds}s) + warmup ({warmup_planned}) exceeds hard cap {hard_cap}"
        )

    reason = (
        f"Planned ample seats ({orig_needed:,} = ({warmup_planned:,} warmup + {orig_reqs:,} staircase) * 1.10) "
        f"exceeded hard cap ({hard_cap:,}); lowered staircase MAX from {max_rate} to {eff_max} "
        f"({best_needed:,} seats across {math.ceil(best_needed / seats_per_event)} events)."
    )
    return {
        "seat_supply": "ample",
        "requested_max": max_rate,
        "effective_max": eff_max,
        "cap_applied": True,
        "cap_reason": reason,
        "warmup_seats": warmup_seats,
        "measured_seats": best_needed - warmup_seats,
        "total_seats": best_needed,
        "planned_requests": warmup_planned + best_reqs,
    }


def compute_steady_plan(rate, duration_seconds, ramp_seconds, warmup_rate, warmup_seconds,
                        seat_supply="ample", hard_cap=HARD_MAX_SEATS, seats_per_event=DEFAULT_SEATS_PER_EVENT):
    warmup_planned = int(math.ceil(warmup_rate * warmup_seconds))
    warmup_seats = int(math.ceil(warmup_planned * 1.10)) if warmup_planned > 0 else 0
    if seat_supply == "scarce":
        total = min(hard_cap, warmup_seats + seats_per_event)
        return {
            "seat_supply": "scarce",
            "requested_max": rate,
            "effective_max": rate,
            "cap_applied": False,
            "cap_reason": None,
            "warmup_seats": warmup_seats,
            "measured_seats": total - warmup_seats,
            "total_seats": total,
            "planned_requests": warmup_planned + seats_per_event,
        }
    measured_planned = int(math.ceil(rate * (duration_seconds + ramp_seconds)))
    total_needed = int(math.ceil((warmup_planned + measured_planned) * 1.10))
    if total_needed > hard_cap:
        raise RuntimeError(
            f"Steady/sustained/soak plan requires {total_needed:,} seats (warmup={warmup_planned:,}, "
            f"measured={measured_planned:,}, +10% margin), exceeding hard cap {hard_cap:,}."
        )
    return {
        "seat_supply": "ample",
        "requested_max": rate,
        "effective_max": rate,
        "cap_applied": False,
        "cap_reason": None,
        "warmup_seats": warmup_seats,
        "measured_seats": total_needed - warmup_seats,
        "total_seats": total_needed,
        "planned_requests": warmup_planned + measured_planned,
    }


def seed_events_and_seats(base_url, admin_token, run_id, label, total_seats, seats_per_event=DEFAULT_SEATS_PER_EVENT):
    now = datetime.datetime.now(datetime.timezone.utc)
    drop_iso = (now - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    event_iso = (now + datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    events = []
    remaining = int(total_seats)
    ev_idx = 0
    rows_per_section = 10
    seats_per_row = 25
    seats_per_section = rows_per_section * seats_per_row

    while remaining > 0:
        ev_idx += 1
        this_event_seats = min(seats_per_event, remaining)
        ev_name = f"loadtest-{run_id}-{label}-{ev_idx:02d}"
        _, ev_body = http_json(
            "POST",
            f"{base_url}/api/v1/admin/events",
            token=admin_token,
            body={
                "name": ev_name,
                "venueName": "LoadTest Arena",
                "dropTime": drop_iso,
                "eventTime": event_iso,
            },
        )
        event_id = ev_body["eventId"]

        seat_ids = []
        for offset in range(0, this_event_seats, BULK_CHUNK_SIZE):
            count = min(BULK_CHUNK_SIZE, this_event_seats - offset)
            seats_payload = []
            for j in range(count):
                idx = offset + j
                sec = idx // seats_per_section
                in_sec = idx % seats_per_section
                row = (in_sec // seats_per_row) + 1
                num = (in_sec % seats_per_row) + 1
                seats_payload.append({
                    "sectionCode": f"S{sec:02d}",
                    "rowLabel": f"R{row:02d}",
                    "seatNumber": f"{num:02d}",
                    "priceCents": 10000 + (idx % 5) * 1000,
                })
            _, bulk_body = http_json(
                "POST",
                f"{base_url}/api/v1/admin/events/{event_id}/seats/bulk",
                token=admin_token,
                body={"seats": seats_payload},
                timeout=45,
            )
            created_list = bulk_body.get("seats", [])
            if len(created_list) != count:
                raise RuntimeError(f"Bulk create returned {len(created_list)} seats, expected {count}")
            seat_ids.extend(s["seatId"] for s in created_list)
            if offset + BULK_CHUNK_SIZE < this_event_seats:
                time.sleep(0.1)

        http_json(
            "PATCH",
            f"{base_url}/api/v1/admin/events/{event_id}/status",
            token=admin_token,
            body={"status": "ON_SALE"},
        )
        # Verify visible
        for _ in range(20):
            status, vis_body = http_json("GET", f"{base_url}/api/v1/events/{event_id}/seats?limit=1")
            if status == 200 and (vis_body.get("items") or vis_body.get("seats")):
                break
            time.sleep(0.2)

        events.append({"event_id": event_id, "name": ev_name, "seat_ids": seat_ids})
        remaining -= this_event_seats

    return events


def main():
    parser = argparse.ArgumentParser(description="Seed events and seats externally into results/<ts>/seats.json")
    parser.add_argument("--mode", required=True, choices=["staircase", "steady"])
    parser.add_argument("--base-url", default=os.environ.get("SEED_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--label", default="seed")
    parser.add_argument("--output", required=True, help="Output manifest path (e.g. results/<ts>/seats.json)")
    parser.add_argument("--seat-supply", default="ample", choices=["ample", "scarce"])
    parser.add_argument("--seats-per-event", type=int, default=DEFAULT_SEATS_PER_EVENT)
    parser.add_argument("--hard-cap", type=int, default=HARD_MAX_SEATS)
    # Staircase params
    parser.add_argument("--start", type=int, default=25)
    parser.add_argument("--step", type=int, default=25)
    parser.add_argument("--max-rate", type=int, default=600)
    parser.add_argument("--step-seconds", type=int, default=30)
    # Steady params
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--ramp-seconds", type=int, default=30)
    # Warmup params
    parser.add_argument("--warmup-rate", type=int, default=50)
    parser.add_argument("--warmup-seconds", type=int, default=90)
    args = parser.parse_args()

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    secret = load_jwt_secret(repo_root)
    admin_token = mint_admin_jwt(secret)

    if args.mode == "staircase":
        plan = compute_staircase_plan(
            start=args.start,
            step=args.step,
            max_rate=args.max_rate,
            step_seconds=args.step_seconds,
            warmup_rate=args.warmup_rate,
            warmup_seconds=args.warmup_seconds,
            seat_supply=args.seat_supply,
            hard_cap=args.hard_cap,
            seats_per_event=args.seats_per_event,
        )
    else:
        plan = compute_steady_plan(
            rate=args.rate,
            duration_seconds=args.duration_seconds,
            ramp_seconds=args.ramp_seconds,
            warmup_rate=args.warmup_rate,
            warmup_seconds=args.warmup_seconds,
            seat_supply=args.seat_supply,
            hard_cap=args.hard_cap,
            seats_per_event=args.seats_per_event,
        )

    if plan["cap_applied"]:
        print(f"[seed] CAP APPLIED: {plan['cap_reason']}")

    t0 = time.monotonic()
    events = seed_events_and_seats(
        base_url=args.base_url.rstrip("/"),
        admin_token=admin_token,
        run_id=args.run_id,
        label=args.label,
        total_seats=plan["total_seats"],
        seats_per_event=args.seats_per_event,
    )
    elapsed = time.monotonic() - t0
    actual_total = sum(len(ev["seat_ids"]) for ev in events)

    manifest = {
        "run_id": args.run_id,
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seats_per_event": args.seats_per_event,
        "total_seats": actual_total,
        "event_count": len(events),
        "plan": plan,
        "events": events,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(manifest, f)

    # Also write lightweight metadata copy without seat_ids for report/scripts
    meta_path = os.path.join(os.path.dirname(os.path.abspath(args.output)), "seed-meta.json")
    with open(meta_path, "w") as f:
        json.dump({k: v for k, v in manifest.items() if k != "events"}, f, indent=2)
        f.write("\n")

    print(
        f"[seed] Seeded {actual_total:,} seats across {len(events)} ON_SALE events in {elapsed:.1f}s "
        f"(supply={plan['seat_supply']}, warmup_seats={plan['warmup_seats']:,}, measured_seats={plan['measured_seats']:,}, "
        f"effective_max={plan['effective_max']}) -> {args.output}"
    )


if __name__ == "__main__":
    main()

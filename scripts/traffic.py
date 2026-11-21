#!/usr/bin/env python3
"""
Traffic Generator for Antigravity Ticketing Platform.
Generates realistic multi-user load through gateway-service (:8080).
Python 3 standard library only, concurrency through threads.
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import queue
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

# Load JWT_SECRET from infra/.env or environment if present
def load_env_defaults():
    env_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "infra", ".env"))
    defaults = {
        "GATEWAY_URL": "http://localhost:8080",
        "JWT_SECRET": "dev-only-hs256-secret-key-minimum-32-bytes-long!",
        "POSTGRES_USER": "ticketing"
    }
    if os.path.isfile(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                defaults[k.strip()] = v.strip().strip('"').strip("'")
    return defaults

ENV_DEFAULTS = load_env_defaults()

def mint_jwt(sub, roles, secret, ttl_seconds=3600):
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    payload = {"sub": sub, "roles": roles, "iat": now, "exp": now + ttl_seconds}
    b64 = lambda d: base64.urlsafe_b64encode(json.dumps(d, separators=(",", ":")).encode()).decode().rstrip("=")
    signing = f"{b64(header)}.{b64(payload)}".encode()
    sig = base64.urlsafe_b64encode(hmac.new(secret.encode(), signing, hashlib.sha256).digest()).decode().rstrip("=")
    return f"{signing.decode()}.{sig}"

class TrafficStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total_requests = 0
        self.status_counts = {}
        self.status_class_counts = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0}
        self.latencies_ms = []
        self.recent_latencies_ms = []
        self.errors = []
        self.inflight = 0

    def record(self, status, latency_ms, error=None):
        with self.lock:
            self.total_requests += 1
            self.status_counts[status] = self.status_counts.get(status, 0) + 1
            if 200 <= status < 300:
                self.status_class_counts["2xx"] += 1
            elif 300 <= status < 400:
                self.status_class_counts["3xx"] += 1
            elif 400 <= status < 500:
                self.status_class_counts["4xx"] += 1
            elif 500 <= status < 600:
                self.status_class_counts["5xx"] += 1
            else:
                self.status_class_counts["other"] += 1

            self.latencies_ms.append(latency_ms)
            self.recent_latencies_ms.append(latency_ms)
            if error:
                self.errors.append(error)

    def inc_inflight(self):
        with self.lock:
            self.inflight += 1

    def dec_inflight(self):
        with self.lock:
            self.inflight = max(0, self.inflight - 1)

    def get_percentiles(self, recent=False):
        with self.lock:
            arr = list(self.recent_latencies_ms if recent else self.latencies_ms)
            if recent:
                self.recent_latencies_ms.clear()
        if not arr:
            return 0.0, 0.0, 0.0
        arr.sort()
        n = len(arr)
        p50 = arr[int(n * 0.50)]
        p95 = arr[min(n - 1, int(n * 0.95))]
        p99 = arr[min(n - 1, int(n * 0.99))]
        return p50, p95, p99

    def get_summary_snapshot(self):
        with self.lock:
            total = self.total_requests
            s5xx = self.status_class_counts["5xx"]
            ratio_5xx = (s5xx / total) if total > 0 else 0.0
            return {
                "total": total,
                "classes": dict(self.status_class_counts),
                "counts": dict(self.status_counts),
                "ratio_5xx": ratio_5xx,
                "inflight": self.inflight,
                "error_count": len(self.errors)
            }

class TrafficGenerator:
    def __init__(self, gateway_url, jwt_secret, postgres_user, duration, rate, concurrency, force_expiry):
        self.gateway_url = gateway_url.rstrip("/")
        self.jwt_secret = jwt_secret
        self.postgres_user = postgres_user
        self.duration = duration
        self.target_rate = rate  # requests per second
        self.concurrency = concurrency
        self.force_expiry = force_expiry
        self.stats = TrafficStats()
        self.stop_event = threading.Event()
        self.seat_pool = queue.Queue()
        self.active_events = []
        self.recent_held_seats = queue.Queue(maxsize=100)
        self.admin_token = None
        self.event_lock = threading.Lock()

    def get_token(self, user_id, roles):
        # First attempt via dev token endpoint
        url = f"{self.gateway_url}/api/v1/auth/token"
        req_body = json.dumps({"userId": user_id, "roles": roles, "ttlSeconds": 3600}).encode()
        r = urllib.request.Request(url, data=req_body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                token = data.get("data", {}).get("accessToken")
                if token:
                    return token
        except Exception:
            pass
        # Fallback to local HS256 minting
        return mint_jwt(user_id, roles, self.jwt_secret)

    def http_request(self, method, path, token=None, body=None, idem_key=None, expected_statuses=None):
        url = f"{self.gateway_url}{path}"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if idem_key:
            headers["Idempotency-Key"] = idem_key

        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        self.stats.inc_inflight()
        t0 = time.time()
        status = 0
        resp_data = None
        err = None
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                body_bytes = resp.read()
                if body_bytes:
                    try:
                        resp_data = json.loads(body_bytes.decode())
                    except Exception:
                        resp_data = body_bytes.decode()
        except urllib.error.HTTPError as e:
            status = e.code
            try:
                resp_data = json.loads(e.read().decode())
            except Exception:
                resp_data = {}
        except Exception as e:
            status = 0
            err = str(e)
        finally:
            latency_ms = (time.time() - t0) * 1000.0
            self.stats.dec_inflight()
            self.stats.record(status, latency_ms, err)

        return status, resp_data

    def create_event_batch(self, count=200):
        with self.event_lock:
            admin_id = str(uuid.uuid4())
            admin_token = self.get_token(admin_id, ["ADMIN", "USER"])
            now = time.time()
            event_name = f"traffic-event-{int(now)}-{random.randint(100, 999)}"
            drop_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 60))
            event_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 86400))

            st, ev = self.http_request("POST", "/api/v1/admin/events", admin_token, {
                "name": event_name,
                "venueName": "Platform Arena",
                "dropTime": drop_time,
                "eventTime": event_time
            })
            if st not in (200, 201) or not ev or "eventId" not in ev:
                return

            event_id = ev["eventId"]
            self.active_events.append(event_id)

            # Generate unique seats
            seats = []
            for i in range(count):
                sec = f"S{i // 100}"
                row = str((i % 100) // 10 + 1)
                num = str((i % 10) + 1)
                seats.append({
                    "sectionCode": sec,
                    "rowLabel": row,
                    "seatNumber": num,
                    "priceCents": 10000 + (i % 5) * 1000
                })

            st, bulk_resp = self.http_request("POST", f"/api/v1/admin/events/{event_id}/seats/bulk", admin_token, {"seats": seats})
            if st not in (200, 201):
                return

            if isinstance(bulk_resp, dict) and "seats" in bulk_resp:
                for s in bulk_resp["seats"]:
                    self.seat_pool.put((event_id, s["seatId"]))

            st, _ = self.http_request("PATCH", f"/api/v1/admin/events/{event_id}/status", admin_token, {"status": "ON_SALE"})
            if st != 200:
                return

            # Also check public seats endpoint if pool is empty
            if self.seat_pool.empty():
                st, seats_resp = self.http_request("GET", f"/api/v1/events/{event_id}/seats", None)
                if st == 200 and isinstance(seats_resp, dict):
                    items = seats_resp.get("items") or seats_resp.get("seats") or []
                    for s in items:
                        self.seat_pool.put((event_id, s["seatId"]))

    def run_user_journey(self):
        # 1. Periodically simulate unauthorized request (~5% probability)
        if random.random() < 0.05:
            # Unauthorized request: no token or bad token
            if random.random() < 0.5:
                self.http_request("POST", "/api/v1/reservations", None, {"eventId": str(uuid.uuid4())})  # 401 Unauthorized
            else:
                user_id = str(uuid.uuid4())
                user_token = self.get_token(user_id, ["USER"])
                self.http_request("POST", "/api/v1/admin/events", user_token, {"name": "forbidden"})  # 403 Forbidden
            return

        # 2. Periodically simulate conflict (~7% probability if recent held seats exist)
        if random.random() < 0.07 and not self.recent_held_seats.empty():
            try:
                ev_id, st_id = self.recent_held_seats.get_nowait()
                user_id = str(uuid.uuid4())
                user_token = self.get_token(user_id, ["USER"])
                # Trying to hold already held seat -> 409 SEAT_ALREADY_TAKEN
                self.http_request("POST", "/api/v1/reservations", user_token, {
                    "eventId": ev_id,
                    "seatIds": [st_id]
                }, idem_key=str(uuid.uuid4()))
            except queue.Empty:
                pass
            return

        # 3. Normal seat reservation flow
        if self.seat_pool.qsize() < 20:
            self.create_event_batch(100)

        try:
            event_id, seat_id = self.seat_pool.get(timeout=2)
        except queue.Empty:
            return

        user_id = str(uuid.uuid4())
        user_token = self.get_token(user_id, ["USER"])
        hold_idem = str(uuid.uuid4())

        st, res = self.http_request("POST", "/api/v1/reservations", user_token, {
            "eventId": event_id,
            "seatIds": [seat_id]
        }, idem_key=hold_idem)

        if st != 201 or not res or "reservationId" not in res:
            return

        res_id = res["reservationId"]

        # Keep for occasional conflict checks
        try:
            self.recent_held_seats.put_nowait((event_id, seat_id))
        except queue.Full:
            pass

        # Poll order until RESERVED / AWAITING_PAYMENT (up to 3s)
        order_ready = False
        for _ in range(15):
            st_ord, ord_body = self.http_request("GET", f"/api/v1/orders/{res_id}", user_token)
            if st_ord == 200 and ord_body and ord_body.get("status") in ("RESERVED", "AWAITING_PAYMENT"):
                order_ready = True
                break
            time.sleep(0.15)

        if not order_ready:
            return

        # Outcome mix: 80% tok_success, 10% tok_decline, 10% abandon
        rand_val = random.random()
        if rand_val < 0.80:
            # 80% tok_success
            checkout_idem = str(uuid.uuid4())
            st_chk, _ = self.http_request("POST", f"/api/v1/orders/{res_id}/checkout", user_token, {
                "paymentMethodToken": "tok_success"
            }, idem_key=checkout_idem)

            if st_chk == 202:
                # Poll order until CONFIRMED (up to 5s)
                for _ in range(20):
                    st_ord, ord_body = self.http_request("GET", f"/api/v1/orders/{res_id}", user_token)
                    if st_ord == 200 and ord_body and ord_body.get("status") == "CONFIRMED":
                        break
                    time.sleep(0.25)

        elif rand_val < 0.90:
            # 10% tok_decline
            checkout_idem = str(uuid.uuid4())
            st_chk, _ = self.http_request("POST", f"/api/v1/orders/{res_id}/checkout", user_token, {
                "paymentMethodToken": "tok_decline"
            }, idem_key=checkout_idem)

            if st_chk == 202:
                for _ in range(20):
                    st_ord, ord_body = self.http_request("GET", f"/api/v1/orders/{res_id}", user_token)
                    if st_ord == 200 and ord_body and ord_body.get("status") == "CANCELLED_PAYMENT_FAILED":
                        break
                    time.sleep(0.25)
        else:
            # 10% abandon
            if self.force_expiry:
                # Force expiry in database as in smoke-inventory.sh
                sql = f"UPDATE reservations SET created_at = NOW() - INTERVAL '1 hour', expires_at = NOW() - INTERVAL '1 minute' WHERE reservation_id = '{res_id}'::uuid;"
                cmd = ["docker", "exec", "postgres-inventory", "sh", "-c", f"psql -U {self.postgres_user} -d inventory_db -c \"{sql}\""]
                try:
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                except Exception:
                    pass

    def worker_loop(self):
        while not self.stop_event.is_set():
            try:
                self.run_user_journey()
            except Exception:
                pass
            # Slight jitter between user journeys
            time.sleep(random.uniform(0.05, 0.2))

    def reporter_loop(self):
        start_time = time.time()
        while not self.stop_event.wait(10.0):
            elapsed = int(time.time() - start_time)
            m, s = divmod(elapsed, 60)
            snap = self.stats.get_summary_snapshot()
            p50, p95, p99 = self.stats.get_percentiles(recent=True)
            ratio_pct = snap["ratio_5xx"] * 100.0
            print(f"[traffic] {m:02d}:{s:02d} | Reqs: {snap['total']} | 2xx: {snap['classes']['2xx']} 4xx: {snap['classes']['4xx']} 5xx: {snap['classes']['5xx']} ({ratio_pct:.2f}%) | Latency (ms): p50={p50:.1f} p95={p95:.1f} p99={p99:.1f} | Inflight: {snap['inflight']}")

    def run(self):
        print(f"=== Starting Traffic Generator ===")
        print(f"Gateway URL: {self.gateway_url}")
        print(f"Duration:    {self.duration}s")
        print(f"Target Rate: ~{self.target_rate} req/s (Concurrency: {self.concurrency})")
        print(f"Force Expiry: {self.force_expiry}")

        # Seed initial events and seats
        print("[traffic] Creating initial event and seats via admin API...")
        self.create_event_batch(300)
        print(f"[traffic] Seeded {self.seat_pool.qsize()} seats across {len(self.active_events)} event(s).")

        reporter = threading.Thread(target=self.reporter_loop, daemon=True)
        reporter.start()

        workers = []
        for _ in range(self.concurrency):
            w = threading.Thread(target=self.worker_loop, daemon=True)
            w.start()
            workers.append(w)

        # Run for specified duration
        start_time = time.time()
        try:
            while time.time() - start_time < self.duration:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("\n[traffic] Interrupted by user.")
        finally:
            self.stop_event.set()

        # Wait briefly for workers to wind down
        for w in workers:
            w.join(timeout=2.0)

        # Final Summary
        total_time = int(time.time() - start_time)
        snap = self.stats.get_summary_snapshot()
        p50, p95, p99 = self.stats.get_percentiles(recent=False)
        ratio_pct = snap["ratio_5xx"] * 100.0

        print("\n" + "=" * 70)
        print("TRAFFIC GENERATOR FINAL SUMMARY")
        print("=" * 70)
        print(f"Duration:       {total_time}s")
        print(f"Total Requests: {snap['total']}")
        print(f"Status Classes: 2xx={snap['classes']['2xx']} 3xx={snap['classes']['3xx']} 4xx={snap['classes']['4xx']} 5xx={snap['classes']['5xx']} other={snap['classes']['other']}")
        print("Status Codes:")
        for code, count in sorted(snap["counts"].items()):
            print(f"  HTTP {code}: {count}")
        print(f"5xx Error Ratio: {ratio_pct:.2f}%")
        print(f"Latency Percentiles (ms): p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms")
        print(f"Errors Encountered: {snap['error_count']}")
        print("=" * 70)

        if snap["ratio_5xx"] > 0.01:
            print(f"\n[FAIL] 5xx error ratio ({ratio_pct:.2f}%) exceeded maximum allowed threshold (1.00%)!")
            sys.exit(1)
        else:
            print("\n[SUCCESS] Traffic generator finished within error budget limits.")

def main():
    parser = argparse.ArgumentParser(description="Ticketing Platform Traffic Generator")
    parser.add_argument("--gateway-url", default=ENV_DEFAULTS.get("GATEWAY_URL", "http://localhost:8080"), help="Gateway base URL")
    parser.add_argument("--jwt-secret", default=ENV_DEFAULTS.get("JWT_SECRET", "dev-only-hs256-secret-key-minimum-32-bytes-long!"), help="JWT secret key")
    parser.add_argument("--postgres-user", default=ENV_DEFAULTS.get("POSTGRES_USER", "ticketing"), help="PostgreSQL user")
    parser.add_argument("--duration", type=int, default=180, help="Traffic duration in seconds (default 180s = 3m)")
    parser.add_argument("--rate", type=float, default=5.0, help="Target requests per second (default 5)")
    parser.add_argument("--concurrency", type=int, default=6, help="Worker thread count (default 6)")
    parser.add_argument("--force-expiry", action="store_true", help="Force SQL hold expiry for abandoned holds")

    args = parser.parse_args()

    gen = TrafficGenerator(
        gateway_url=args.gateway_url,
        jwt_secret=args.jwt_secret,
        postgres_user=args.postgres_user,
        duration=args.duration,
        rate=args.rate,
        concurrency=args.concurrency,
        force_expiry=args.force_expiry
    )
    gen.run()

if __name__ == "__main__":
    main()

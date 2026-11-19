#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${PROM_URL:-http://127.0.0.1:9090}"

python3 - "$PROM_URL" << 'PYEOF'
import json, sys, time, urllib.request

prom_url = sys.argv[1].rstrip("/")
url = f"{prom_url}/api/v1/targets?state=active"
deadline = time.time() + 30

while True:
    try:
        data = json.loads(urllib.request.urlopen(url, timeout=5).read().decode("utf-8"))
        targets = data.get("data", {}).get("activeTargets", [])
        unknown = [t for t in targets if t.get("health") == "unknown"]
        down = [t for t in targets if t.get("health") == "down"]
        if targets and not unknown and not down:
            print(f"[OK] All {len(targets)} Prometheus targets are UP.")
            for t in targets:
                job = t.get("labels", {}).get("job")
                print(f"  - {job}: {t.get('scrapeUrl')} ({t.get('health')})")
            sys.exit(0)
        if time.time() >= deadline:
            print("[FAIL] Prometheus target check failed:")
            for t in targets:
                if t.get("health") != "up":
                    job = t.get("labels", {}).get("job")
                    print(f"  DOWN/UNKNOWN: job={job} url={t.get('scrapeUrl')} health={t.get('health')} error={t.get('lastError')}")
            sys.exit(1)
    except Exception as e:
        if time.time() >= deadline:
            print(f"[FAIL] Could not query Prometheus targets: {e}")
            sys.exit(1)
    time.sleep(2)
PYEOF

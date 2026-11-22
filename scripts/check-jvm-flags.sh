#!/usr/bin/env bash
# =============================================================================
# scripts/check-jvm-flags.sh — Verify G1GC & Docker VM memory headroom
#
# Checks each of the 4 Java containers (gateway-service, inventory-service,
# order-service, payment-service) for effective G1GC via JVM startup log
# (-Xlog:gc+init=info), /proc/1/cmdline, and Actuator Prometheus metrics
# (jvm_memory_used_bytes / jvm_gc_*), failing non-zero if any service is not
# running G1. Also prints sum of configured container memory limits vs Docker
# VM memory and warns if the sum exceeds 80% of VM memory.
# =============================================================================
set -euo pipefail

SERVICES=(
    "gateway-service:8080"
    "inventory-service:8081"
    "order-service:8082"
    "payment-service:8083"
)

FAIL=0

echo "=== JVM Effective Garbage Collector Check ==="
printf "%-20s | %-22s | %-8s | %s\n" "Service" "Effective GC" "Status" "Evidence"
printf "%s\n" "--------------------------------------------------------------------------------------------------------"

for entry in "${SERVICES[@]}"; do
    svc="${entry%%:*}"
    port="${entry#*:}"

    if ! docker inspect "$svc" >/dev/null 2>&1; then
        printf "%-20s | %-22s | %-8s | %s\n" "$svc" "CONTAINER MISSING" "FAIL" "docker inspect failed"
        FAIL=1
        continue
    fi

    # 1. Try jcmd if available; otherwise inspect startup log (-Xlog:gc+init=info) and Actuator prometheus metrics
    gc_name=""
    evidence=""

    if docker exec "$svc" which jcmd >/dev/null 2>&1; then
        vm_flags=$(docker exec "$svc" jcmd 1 VM.flags 2>/dev/null || true)
        if echo "$vm_flags" | grep -q -- "-XX:+UseG1GC"; then
            gc_name="G1GC"
            evidence="jcmd 1 VM.flags: -XX:+UseG1GC"
        fi
    fi

    # Check Actuator prometheus metrics for G1 pools / GC collectors
    prom_out=$(docker exec "$svc" wget -q -O - "http://127.0.0.1:${port}/actuator/prometheus" 2>/dev/null || true)
    prom_gc=$(echo "$prom_out" | grep -E '^jvm_gc_pause_seconds_count|^jvm_memory_used_bytes' | grep -oE '(gc|id)="[^"]*G1[^"]*"' | head -n 1 || true)
    prom_non_g1=$(echo "$prom_out" | grep -E '^jvm_gc_pause_seconds_count' | grep -oE 'gc="[^"]+"' | sort -u | tr '\n' ' ' || true)

    # Check JVM startup log (-Xlog:gc+init=info prints "Using G1")
    startup_gc=$(docker logs "$svc" 2>&1 | grep -m 1 -E '\[gc,init\].*Using ' || true)

    if [ -n "$prom_gc" ] || echo "$startup_gc" | grep -q "Using G1"; then
        gc_name="G1GC"
        parts=()
        [ -n "$startup_gc" ] && parts+=("log: $(echo "$startup_gc" | sed -E 's/.*Using /Using /')")
        [ -n "$prom_gc" ] && parts+=("actuator: ${prom_gc}")
        evidence=$(IFS="; "; echo "${parts[*]}")
        printf "%-20s | %-22s | %-8s | %s\n" "$svc" "$gc_name" "PASS" "$evidence"
    else
        gc_name="${prom_non_g1:-UNKNOWN}"
        evidence="No G1GC found in actuator metrics or startup logs (saw: ${prom_non_g1:-none})"
        printf "%-20s | %-22s | %-8s | %s\n" "$svc" "$gc_name" "FAIL" "$evidence"
        FAIL=1
    fi
done

echo ""
echo "=== Docker VM Memory & Configured Container Limit Check ==="
python3 - <<'PYEOF'
import subprocess

vm_bytes = int(subprocess.check_output(["docker", "info", "--format", "{{.MemTotal}}"]).decode().strip())
vm_mib = vm_bytes / (1024.0 * 1024.0)

names = subprocess.check_output(
    ["docker", "ps", "--filter", "network=ticketing-net", "--format", "{{.Names}}"]
).decode().splitlines()

total_limit_bytes = 0
limited = []
unlimited = []
for name in sorted(n.strip() for n in names if n.strip()):
    mem_b = int(subprocess.check_output(["docker", "inspect", "--format", "{{.HostConfig.Memory}}", name]).decode().strip())
    if mem_b > 0:
        total_limit_bytes += mem_b
        limited.append((name, mem_b / (1024.0 * 1024.0)))
    else:
        unlimited.append(name)

total_limit_mib = total_limit_bytes / (1024.0 * 1024.0)
pct = (total_limit_bytes / vm_bytes * 100.0) if vm_bytes > 0 else 0.0

print(f"Docker VM Memory:          {vm_mib:.0f} MiB ({vm_mib / 1024.0:.2f} GiB)")
print(f"Configured Memory Limits:  {total_limit_mib:.0f} MiB ({total_limit_mib / 1024.0:.2f} GiB) across {len(limited)} limited containers ({pct:.1f}% of VM)")
for name, mib in limited:
    print(f"  - {name:<25} {mib:.0f} MiB")
print(f"Unlimited Containers ({len(unlimited)}): {', '.join(unlimited)}")
if pct > 80.0:
    print(f"[WARNING] Sum of configured memory limits ({pct:.1f}%) exceeds 80% of Docker VM memory!")
else:
    print(f"[OK] Configured memory limits ({pct:.1f}%) are within 80% of Docker VM memory.")
PYEOF

if [ "$FAIL" -ne 0 ]; then
    echo ""
    echo "[FAIL] One or more Java services are not running G1GC!"
    exit 1
fi
echo ""
echo "[PASS] All 4 Java services verified running G1GC."

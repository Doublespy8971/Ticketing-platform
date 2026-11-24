#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-cluster.sh — Idempotent k3d cluster lifecycle & preflight checks
# Subcommands: create | destroy | preflight
# =============================================================================
set -euo pipefail

CMD="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.35.5-k3s1}"
INGRESS_HOST_PORT="${K8S_INGRESS_PORT:-8088}"

HOST_PORTS=(8080 8081 8082 8083 8084 5433 5434 5435 6379 29092 9090 3000)

require_tools() {
    local missing=0
    if ! command -v k3d >/dev/null 2>&1; then
        echo "[ERROR] Required tool 'k3d' is not installed." >&2
        echo "  Install on macOS: brew install k3d" >&2
        missing=1
    fi
    if ! command -v kubectl >/dev/null 2>&1; then
        echo "[ERROR] Required tool 'kubectl' is not installed." >&2
        echo "  Install on macOS: brew install kubectl" >&2
        missing=1
    fi
    if ! command -v docker >/dev/null 2>&1; then
        echo "[ERROR] Required tool 'docker' is not installed or not in PATH." >&2
        missing=1
    fi
    if [ "$missing" -ne 0 ]; then
        exit 1
    fi
}

check_compose_ports() {
    local conflicts=()
    # Check running compose containers by known names
    local compose_running
    compose_running=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^(gateway-service|inventory-service|order-service|payment-service|notification-service|kafka|redis|postgres-inventory|postgres-order|postgres-payment|prometheus|grafana)$' || true)
    if [ -n "$compose_running" ]; then
        echo "[ERROR] Docker Compose stack containers are currently running:" >&2
        printf '  - %s\n' $compose_running >&2
        echo "[ERROR] Stop the Docker Compose stack before creating the k3d cluster:" >&2
        echo "        make stack-down && make obs-down" >&2
        exit 1
    fi

    for p in "${HOST_PORTS[@]}"; do
        if command -v lsof >/dev/null 2>&1; then
            local owner
            owner=$(lsof -nP -iTCP:"$p" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $1 "(pid=" $2 ")"}' | paste -sd ',' - || true)
            if [ -n "$owner" ]; then
                conflicts+=("port ${p}: ${owner}")
            fi
        fi
    done

    if [ "${#conflicts[@]}" -gt 0 ]; then
        echo "[ERROR] Host ports required for Kubernetes port-forwarding are already in use:" >&2
        printf '  - %s\n' "${conflicts[@]}" >&2
        echo "[ERROR] If the Docker Compose stack is running, stop it first with:" >&2
        echo "        make stack-down && make obs-down" >&2
        exit 1
    fi
}

check_vm_memory() {
    local vm_bytes
    vm_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
    if ! [[ "$vm_bytes" =~ ^[0-9]+$ ]] || [ "$vm_bytes" -le 0 ]; then
        echo "[WARN] Unable to determine Docker VM memory via 'docker info'."
        return 0
    fi

    python3 - "$vm_bytes" "$REPO_ROOT" <<'PYEOF'
import re, subprocess, sys

vm_bytes = int(sys.argv[1])
repo_root = sys.argv[2]
vm_mib = vm_bytes / (1024.0 * 1024.0)
vm_gib = vm_bytes / (1024.0 ** 3)

rendered = subprocess.check_output(
    ["kubectl", "kustomize", "--load-restrictor=LoadRestrictionsNone", f"{repo_root}/deploy/k8s/overlays/local"],
    stderr=subprocess.PIPE,
).decode("utf-8")

def parse_mem_mib(val):
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$", val.strip().strip('"').strip("'"))
    if not m:
        return 0.0
    n, u = float(m.group(1)), m.group(2)
    factors = {"": 1.0 / (1024*1024), "Ki": 1.0/1024, "Mi": 1.0, "Gi": 1024.0, "K": 1000.0/(1024*1024), "M": 1e6/(1024*1024), "G": 1e9/(1024*1024)}
    return n * factors.get(u, 0.0)

total_limit_mib = 0.0
breakdown = []
for doc in rendered.split("\n---"):
    kind_m = re.search(r"^kind:\s*(\S+)", doc, re.M)
    name_m = re.search(r"^metadata:\s*\n(?:[ \t]+.*\n)*?[ \t]+name:\s*(\S+)", doc, re.M)
    if not kind_m or kind_m.group(1) not in ("Deployment", "StatefulSet"):
        continue
    name = name_m.group(1) if name_m else "unknown"
    rep_m = re.search(r"^\s*replicas:\s*(\d+)", doc, re.M)
    replicas = int(rep_m.group(1)) if rep_m else 1
    # Sum main container memory limits only (Kustomize sorts keys alphabetically so initContainers follows containers)
    containers_part = doc.split("\n      containers:", 1)
    if len(containers_part) < 2:
        continue
    main_containers = containers_part[1].split("\n      initContainers:", 1)[0]
    pod_limits = 0.0
    for lim_m in re.finditer(r"limits:\s*\n(?:[ \t]+cpu:.*\n)?[ \t]+memory:\s*(\S+)", main_containers):
        pod_limits += parse_mem_mib(lim_m.group(1))
    svc_total = pod_limits * replicas
    total_limit_mib += svc_total
    breakdown.append((name, replicas, pod_limits, svc_total))

pct = (total_limit_mib / vm_mib * 100.0) if vm_mib > 0 else 0.0
print(f"[k8s-cluster] Docker VM Memory:          {vm_gib:.2f} GiB ({vm_mib:.0f} MiB)")
print(f"[k8s-cluster] Local Overlay Mem Limits:  {total_limit_mib:.0f} MiB ({pct:.1f}% of Docker VM)")
for name, rep, per_pod, tot in breakdown:
    print(f"    - {name}: {rep} x {per_pod:.0f} MiB = {tot:.0f} MiB")
if pct > 75.0:
    print(f"[WARN] Sum of local overlay memory limits ({total_limit_mib:.0f} MiB = {pct:.1f}%) exceeds 75% of Docker VM memory ({vm_mib:.0f} MiB).")
    print("       Steady-state actual usage is lower, but increase Docker VM RAM (e.g. >= 8-10 GiB) if pods experience OOM or pressure.")
PYEOF
}

cluster_exists() {
    k3d cluster list --no-headers 2>/dev/null | awk '{print $1}' | grep -Fxq "$CLUSTER_NAME"
}

cmd_preflight() {
    require_tools
    check_compose_ports
    check_vm_memory
    echo "[k8s-cluster] Preflight checks passed."
}

cmd_create() {
    require_tools
    check_vm_memory

    if cluster_exists; then
        echo "[k8s-cluster] Cluster '${CLUSTER_NAME}' already exists; ensuring it is started and kubeconfig is set."
        k3d cluster start "$CLUSTER_NAME" >/dev/null 2>&1 || true
        k3d kubeconfig merge "$CLUSTER_NAME" --kubeconfig-switch-context >/dev/null
        kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null
        kubectl get nodes -o wide
        return 0
    fi

    check_compose_ports

    echo "[k8s-cluster] Creating k3d cluster '${CLUSTER_NAME}' (image=${K3S_IMAGE}, 1 server + 2 agents, host :${INGRESS_HOST_PORT} -> lb :80)..."
    k3d cluster create "$CLUSTER_NAME" \
        --image "$K3S_IMAGE" \
        --servers 1 \
        --agents 2 \
        --port "${INGRESS_HOST_PORT}:80@loadbalancer" \
        --k3s-arg "--disable-network-policy=false@server:0" \
        --wait

    kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null
    echo "[k8s-cluster] Cluster '${CLUSTER_NAME}' is ready:"
    kubectl get nodes -o wide
}

cmd_destroy() {
    require_tools
    if [ -x "${SCRIPT_DIR}/k8s-forward.sh" ]; then
        bash "${SCRIPT_DIR}/k8s-forward.sh" stop >/dev/null 2>&1 || true
    fi
    if cluster_exists; then
        echo "[k8s-cluster] Deleting k3d cluster '${CLUSTER_NAME}'..."
        k3d cluster delete "$CLUSTER_NAME"
    else
        echo "[k8s-cluster] Cluster '${CLUSTER_NAME}' does not exist (nothing to delete)."
    fi
}

case "$CMD" in
    preflight) cmd_preflight ;;
    create)    cmd_create ;;
    destroy)   cmd_destroy ;;
    *)
        echo "Usage: $0 {preflight|create|destroy}" >&2
        exit 1
        ;;
esac

#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-up.sh — Bring up the full local Kubernetes deployment on k3d
# Steps: cluster create -> build & import images -> secrets -> apply local
# overlay -> wait up to 6 minutes for all StatefulSets, kafka-init Job, and
# Deployments to become ready.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"
WAIT_LIMIT_SEC="${K8S_UP_TIMEOUT_SEC:-360}"
SKIP_BUILD="${K8S_SKIP_BUILD:-0}"

T_START=$(date +%s)

echo "=== [k8s-up] Step 1/5: Ensure k3d cluster '${CLUSTER_NAME}' exists ==="
bash "${SCRIPT_DIR}/k8s-cluster.sh" create

echo "=== [k8s-up] Step 2/5: Build, tag & import service images ==="
if [ "$SKIP_BUILD" = "1" ]; then
    echo "[k8s-up] K8S_SKIP_BUILD=1 set; skipping image build/import."
else
    bash "${SCRIPT_DIR}/k8s-images.sh"
fi

echo "=== [k8s-up] Step 3/5: Create runtime Secret from infra/.env ==="
bash "${SCRIPT_DIR}/k8s-secrets.sh"

echo "=== [k8s-up] Step 4/5: Apply Kustomize local overlay ==="
kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null
# Delete any pre-existing kafka-init Job so re-running k8s-up never fails on immutable Job spec
kubectl -n "$NAMESPACE" delete job kafka-init --ignore-not-found >/dev/null 2>&1 || true

kubectl kustomize --load-restrictor=LoadRestrictionsNone "${REPO_ROOT}/deploy/k8s/overlays/local" | kubectl apply -f -

echo "=== [k8s-up] Step 5/5: Waiting up to ${WAIT_LIMIT_SEC}s for all workloads & kafka-init ==="

dump_diagnostics() {
    echo "" >&2
    echo "=== [k8s-up] FAILURE DIAGNOSTICS ===" >&2
    echo "--- Pods not Ready ---" >&2
    kubectl -n "$NAMESPACE" get pods -o wide >&2 || true
    echo "--- Recent namespace events (last 30) ---" >&2
    kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp 2>/dev/null | tail -n 30 >&2 || true
    for pod in $(kubectl -n "$NAMESPACE" get pods --no-headers 2>/dev/null | awk '$2 !~ /^([0-9]+)\/\1$/ && $3 != "Completed" {print $1}'); do
        echo "--- Describe non-ready pod: ${pod} ---" >&2
        kubectl -n "$NAMESPACE" describe pod "$pod" 2>/dev/null | tail -n 30 >&2 || true
    done
}

DEADLINE=$(( $(date +%s) + WAIT_LIMIT_SEC ))
STATEFULSETS=(kafka postgres-inventory postgres-order postgres-payment redis)
DEPLOYMENTS=(inventory-service order-service payment-service notification-service gateway-service)

while true; do
    now=$(date +%s)
    elapsed=$((now - T_START))
    if [ "$now" -ge "$DEADLINE" ]; then
        echo "[ERROR] Timed out after ${WAIT_LIMIT_SEC}s waiting for workloads in namespace '${NAMESPACE}'." >&2
        dump_diagnostics
        exit 1
    fi

    pending=()

    for sts in "${STATEFULSETS[@]}"; do
        spec_rep=$(kubectl -n "$NAMESPACE" get statefulset "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
        ready_rep=$(kubectl -n "$NAMESPACE" get statefulset "$sts" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        ready_rep="${ready_rep:-0}"
        if [ "$spec_rep" -le 0 ] || [ "$ready_rep" -lt "$spec_rep" ]; then
            pending+=("sts/${sts}(${ready_rep}/${spec_rep})")
        fi
    done

    job_complete=$(kubectl -n "$NAMESPACE" get job kafka-init -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
    job_failed=$(kubectl -n "$NAMESPACE" get job kafka-init -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
    if [ "$job_failed" = "True" ]; then
        echo "[ERROR] Job kafka-init FAILED!" >&2
        dump_diagnostics
        exit 1
    fi
    if [ "$job_complete" != "True" ]; then
        pending+=("job/kafka-init")
    fi

    for dep in "${DEPLOYMENTS[@]}"; do
        spec_rep=$(kubectl -n "$NAMESPACE" get deployment "$dep" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
        avail_rep=$(kubectl -n "$NAMESPACE" get deployment "$dep" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo "0")
        avail_cond=$(kubectl -n "$NAMESPACE" get deployment "$dep" -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null || echo "")
        avail_rep="${avail_rep:-0}"
        if [ "$spec_rep" -le 0 ] || [ "$avail_rep" -lt "$spec_rep" ] || [ "$avail_cond" != "True" ]; then
            pending+=("deploy/${dep}(${avail_rep}/${spec_rep})")
        fi
    done

    if [ "${#pending[@]}" -eq 0 ]; then
        T_END=$(date +%s)
        TOTAL=$((T_END - T_START))
        echo ""
        echo "[OK] All StatefulSets, kafka-init Job, and Deployments are READY in ${TOTAL}s!"
        echo ""
        kubectl -n "$NAMESPACE" get pods -o wide
        exit 0
    fi

    echo "  [k8s-up +${elapsed}s] Waiting on (${#pending[@]}): ${pending[*]}"
    sleep 5
done

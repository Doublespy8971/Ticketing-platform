#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-down.sh — Delete namespace resources (and PVCs) while keeping cluster
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"

# Stop port-forwards first so no dangling sockets remain
bash "${SCRIPT_DIR}/k8s-forward.sh" stop >/dev/null 2>&1 || true

kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null 2>&1 || true

if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[k8s-down] Deleting namespace '${NAMESPACE}' (all workloads, jobs, secrets, and PVCs) while keeping k3d cluster '${CLUSTER_NAME}'..."
    kubectl delete namespace "$NAMESPACE" --timeout=120s
    echo "[k8s-down] Namespace '${NAMESPACE}' deleted cleanly."
else
    echo "[k8s-down] Namespace '${NAMESPACE}' does not exist (nothing to delete)."
fi

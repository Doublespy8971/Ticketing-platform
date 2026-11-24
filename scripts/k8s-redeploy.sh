#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-redeploy.sh — Rebuild, import, rollout restart & wait for one service
# Usage: scripts/k8s-redeploy.sh <SERVICE>
# =============================================================================
set -euo pipefail

SERVICE="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"

if [ -z "$SERVICE" ]; then
    echo "Usage: $0 <gateway-service|inventory-service|order-service|payment-service|notification-service>" >&2
    exit 1
fi

bash "${SCRIPT_DIR}/k8s-images.sh" "$SERVICE"

kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null
echo "[k8s-redeploy] Restarting deployment/${SERVICE} in namespace '${NAMESPACE}'..."
kubectl -n "$NAMESPACE" rollout restart "deployment/${SERVICE}"
kubectl -n "$NAMESPACE" rollout status "deployment/${SERVICE}" --timeout=180s
echo "[k8s-redeploy] deployment/${SERVICE} rolled out cleanly."

#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-secrets.sh — Generate ticketing-secrets from infra/.env at deploy time
# Never commits secrets, never writes secret values to disk, never echoes values.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/infra/.env"
NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"

if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] ${ENV_FILE} is missing. Create it (e.g. 'cp infra/.env.example infra/.env') and retry." >&2
    exit 1
fi

read_env_var() {
    local key="$1"
    local val
    val=$(grep -E "^${key}=" "$ENV_FILE" | head -n 1 | cut -d= -f2- | tr -d '\r' | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
    if [ -z "$val" ]; then
        echo "[ERROR] Required key '${key}' is missing or empty in ${ENV_FILE}." >&2
        exit 1
    fi
    printf '%s' "$val"
}

POSTGRES_USER_VAL="$(read_env_var POSTGRES_USER)"
POSTGRES_PASSWORD_VAL="$(read_env_var POSTGRES_PASSWORD)"
JWT_SECRET_VAL="$(read_env_var JWT_SECRET)"

kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null 2>&1 || true

# Ensure namespace exists with restricted PodSecurity labels before applying Secret
kubectl apply -f "${REPO_ROOT}/deploy/k8s/base/namespace.yaml" >/dev/null

# Stream directly to kubectl apply without writing any file or printing values
kubectl create secret generic ticketing-secrets \
    --namespace "$NAMESPACE" \
    --from-literal="POSTGRES_USER=${POSTGRES_USER_VAL}" \
    --from-literal="POSTGRES_PASSWORD=${POSTGRES_PASSWORD_VAL}" \
    --from-literal="JWT_SECRET=${JWT_SECRET_VAL}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "[k8s-secrets] Applied Secret 'ticketing-secrets' in namespace '${NAMESPACE}' from infra/.env (keys: POSTGRES_USER, POSTGRES_PASSWORD, JWT_SECRET; values suppressed)."

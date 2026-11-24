#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-images.sh — Build, tag (:<short-sha> and :dev) & import app images
# Usage: scripts/k8s-images.sh [SERVICE]
# =============================================================================
set -euo pipefail

SERVICE="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"

ALL_SERVICES=(gateway-service inventory-service order-service payment-service notification-service)

if [ -n "$SERVICE" ]; then
    valid=0
    for s in "${ALL_SERVICES[@]}"; do
        if [ "$s" = "$SERVICE" ]; then valid=1; break; fi
    done
    if [ "$valid" -ne 1 ]; then
        echo "[ERROR] Unknown service '${SERVICE}'. Valid services: ${ALL_SERVICES[*]}" >&2
        exit 1
    fi
    TARGETS=("$SERVICE")
else
    TARGETS=("${ALL_SERVICES[@]}")
fi

if ! command -v k3d >/dev/null 2>&1; then
    echo "[ERROR] Required tool 'k3d' is not installed. Install on macOS: brew install k3d" >&2
    exit 1
fi

SHORT_SHA=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "nogit")
echo "[k8s-images] Building image(s): ${TARGETS[*]} (tags: :${SHORT_SHA}, :dev)"

# Reuse the existing stack-build mechanism (docker-compose.full.yml)
if [ ! -f "${REPO_ROOT}/infra/.env" ]; then
    cp "${REPO_ROOT}/infra/.env.example" "${REPO_ROOT}/infra/.env"
fi

if [ -n "$SERVICE" ]; then
    DOCKER_BUILDKIT=1 docker compose -f "${REPO_ROOT}/infra/docker-compose.yml" -f "${REPO_ROOT}/infra/docker-compose.full.yml" build "$SERVICE"
else
    make -C "$REPO_ROOT" stack-build
fi

IMPORT_IMAGES=()
for svc in "${TARGETS[@]}"; do
    src="ticketing/${svc}:latest"
    tag_sha="ticketing/${svc}:${SHORT_SHA}"
    tag_dev="ticketing/${svc}:dev"
    docker tag "$src" "$tag_sha"
    docker tag "$src" "$tag_dev"
    echo "[k8s-images] Tagged ${src} -> ${tag_sha} and ${tag_dev}"
    IMPORT_IMAGES+=("$tag_sha" "$tag_dev")
done

if k3d cluster list --no-headers 2>/dev/null | awk '{print $1}' | grep -Fxq "$CLUSTER_NAME"; then
    echo "[k8s-images] Importing ${#IMPORT_IMAGES[@]} image tag(s) into k3d cluster '${CLUSTER_NAME}'..."
    k3d image import -c "$CLUSTER_NAME" "${IMPORT_IMAGES[@]}"
    # Also import locally cached infra/init images if present to avoid redundant pulls inside k3d
    for extra in busybox:1.36.1 redis:7.4.1-alpine postgres:16.4-alpine apache/kafka:3.8.0; do
        if docker image inspect "$extra" >/dev/null 2>&1; then
            k3d image import -c "$CLUSTER_NAME" "$extra" >/dev/null 2>&1 || true
        fi
    done
    echo "[k8s-images] Import complete."
else
    echo "[k8s-images] Cluster '${CLUSTER_NAME}' not created yet; built and tagged images locally (run k8s-cluster create then k8s-images to import)."
fi

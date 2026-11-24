#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-lint.sh — Render base & local overlay with kubectl kustomize and
# validate with pinned kubeconform (strict) and kube-linter Docker images.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

KUBECONFORM_IMAGE="${KUBECONFORM_IMAGE:-ghcr.io/yannh/kubeconform:v0.6.7-alpine}"
KUBELINTER_IMAGE="${KUBELINTER_IMAGE:-stackrox/kube-linter:v0.7.1}"
# Match the k3s cluster minor version (v1.35.5-k3s1 -> 1.35.0; fallback overrideable via K8S_SCHEMA_VERSION)
K8S_SCHEMA_VERSION="${K8S_SCHEMA_VERSION:-1.35.0}"

if ! command -v kubectl >/dev/null 2>&1; then
    echo "[ERROR] Required tool 'kubectl' is not installed. Install on macOS: brew install kubectl" >&2
    exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "[ERROR] Required tool 'docker' is not installed." >&2
    exit 1
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ticketing-k8s-lint.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

render_and_validate() {
    local label="$1"
    local target_dir="$2"
    local out_file="${TMP_DIR}/${label}.yaml"

    echo "=== [k8s-lint] Rendering ${label} (${target_dir}) ==="
    kubectl kustomize --load-restrictor=LoadRestrictionsNone "$target_dir" > "$out_file"
    local res_count
    res_count=$(grep -c '^kind:' "$out_file" || true)
    echo "[k8s-lint] Rendered ${res_count} Kubernetes resources to ${out_file}"

    echo "--- [k8s-lint] kubeconform (strict, k8s ${K8S_SCHEMA_VERSION}, image=${KUBECONFORM_IMAGE}) ---"
    docker run --rm -v "${TMP_DIR}:/manifests:ro" "$KUBECONFORM_IMAGE" \
        -strict \
        -summary \
        -ignore-missing-schemas \
        -kubernetes-version "$K8S_SCHEMA_VERSION" \
        "/manifests/${label}.yaml"

    echo "--- [k8s-lint] kube-linter (config=deploy/k8s/.kube-linter.yaml, image=${KUBELINTER_IMAGE}) ---"
    docker run --rm \
        -v "${TMP_DIR}:/manifests:ro" \
        -v "${REPO_ROOT}/deploy/k8s/.kube-linter.yaml:/config/.kube-linter.yaml:ro" \
        "$KUBELINTER_IMAGE" \
        lint --config /config/.kube-linter.yaml "/manifests/${label}.yaml"
}

render_and_validate "base" "${REPO_ROOT}/deploy/k8s/base"
render_and_validate "local" "${REPO_ROOT}/deploy/k8s/overlays/local"

echo "=== [k8s-lint] ALL CHECKS PASSED (base + overlays/local) ==="

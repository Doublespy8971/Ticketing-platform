#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-status.sh — Display cluster workloads, services, ingress & metrics
# =============================================================================
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"

kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null 2>&1 || true

echo "=== Nodes ==="
kubectl get nodes -o wide

echo ""
echo "=== Pods (namespace: ${NAMESPACE}) ==="
kubectl -n "$NAMESPACE" get pods -o wide

echo ""
echo "=== StatefulSets, Deployments & Jobs ==="
kubectl -n "$NAMESPACE" get statefulsets,deployments,jobs

echo ""
echo "=== Services & Ingress ==="
kubectl -n "$NAMESPACE" get svc,ingress

echo ""
echo "=== HorizontalPodAutoscalers ==="
echo "(None configured: HPA is intentionally omitted in this local/base manifest set; scaling is fixed via Kustomize replicas.)"

echo ""
echo "=== Resource Usage (kubectl top) ==="
if kubectl top nodes >/dev/null 2>&1; then
    kubectl top nodes
    echo ""
    kubectl -n "$NAMESPACE" top pods --containers=false || true
else
    echo "(metrics-server metrics not yet available; retry in ~30s)"
fi

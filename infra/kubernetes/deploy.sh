#!/usr/bin/env bash
#
# Build both images into minikube's Docker daemon and apply every manifest.
#
#   ./infra/kubernetes/deploy.sh [tag]
#
# Building directly into minikube's daemon avoids needing a registry for local work; the manifests
# use imagePullPolicy: IfNotPresent so the kubelet does not try to pull a tag that only exists there.
# Phase 9 pushes to GHCR for the real pipeline.
set -euo pipefail

TAG="${1:-dev}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE=hookrelay

cd "$REPO_ROOT"

echo "==> pointing docker at minikube's daemon"
eval "$(minikube docker-env)"

# One Dockerfile, two targets, one shared builder stage: dependencies resolve once instead of once
# per image. DOCKER_BUILDKIT is required for the ~/.m2 cache mount that makes rebuilds fast.
export DOCKER_BUILDKIT=1

echo "==> building hookrelay-api:${TAG}"
docker build -q -f infra/docker/Dockerfile --target api -t "hookrelay-api:${TAG}" .

echo "==> building hookrelay-worker:${TAG}"
docker build -q -f infra/docker/Dockerfile --target worker -t "hookrelay-worker:${TAG}" .

echo "==> applying manifests"
kubectl apply -f infra/kubernetes/00-namespace.yaml
kubectl apply -f infra/kubernetes/10-config.yaml

kubectl apply -f infra/kubernetes/20-postgres.yaml
kubectl apply -f infra/kubernetes/21-rabbitmq.yaml

echo "==> waiting for datastores"
kubectl -n "$NAMESPACE" rollout status deployment/postgres --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/rabbitmq --timeout=300s

# The receiver script comes from tools/webhook_receiver.py rather than being duplicated into YAML,
# so there is one copy in the repository and it cannot drift from the one the tests use. This must
# come AFTER the manifest is applied, or applying the manifest overwrites it.
kubectl apply -f infra/kubernetes/40-receiver.yaml
kubectl -n "$NAMESPACE" create configmap receiver-script \
  --from-file=webhook_receiver.py=tools/webhook_receiver.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" rollout restart deployment/receiver >/dev/null 2>&1 || true
kubectl apply -f infra/kubernetes/30-api.yaml
kubectl apply -f infra/kubernetes/31-worker.yaml

# Force the new image even when the tag is unchanged: same tag, new content means the Deployment spec
# is identical and Kubernetes would otherwise do nothing.
kubectl -n "$NAMESPACE" set image deployment/api "api=hookrelay-api:${TAG}" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/worker "worker=hookrelay-worker:${TAG}" >/dev/null
kubectl -n "$NAMESPACE" rollout restart deployment/api deployment/worker >/dev/null

echo "==> waiting for application rollout"
kubectl -n "$NAMESPACE" rollout status deployment/receiver --timeout=120s
kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=300s

echo
kubectl -n "$NAMESPACE" get pods -o wide
echo
echo "API:      kubectl -n $NAMESPACE port-forward svc/api 8080:8080"
echo "RabbitMQ: kubectl -n $NAMESPACE port-forward svc/rabbitmq 15672:15672"

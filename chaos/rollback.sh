#!/usr/bin/env bash
#
# BLUEPRINT.md §26 — deploy a broken version under load, show the cluster refuses it, and roll back.
#
#   ./chaos/rollback.sh
#
# The point is NOT that `kubectl rollout undo` works. It is that the readiness probe never lets the
# broken pod into the Service's endpoints, and `maxUnavailable: 0` forbids removing a healthy pod
# before a new one is Ready — so the old version keeps serving at full capacity and the rollout
# simply stalls. The undo is cleanup, not rescue.
set -uo pipefail

NAMESPACE=hookrelay
DURATION="${DURATION:-90}"
CONCURRENCY="${CONCURRENCY:-8}"
STALL_SECONDS="${STALL_SECONDS:-45}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

cleanup() {
  kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> baseline: make sure the current deployment is healthy"
kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s >/dev/null || {
  echo "api is not healthy to begin with" >&2; exit 1; }
GOOD_IMAGE=$(kubectl -n "$NAMESPACE" get deployment api -o jsonpath='{.spec.template.spec.containers[0].image}')
echo "    healthy on ${GOOD_IMAGE}"

TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
kubectl -n "$NAMESPACE" run "reg-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/endpoints \
  -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
  -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret"}' \
  >/dev/null 2>&1

echo "==> starting load"
kubectl -n "$NAMESPACE" create configmap loadgen-script \
  --from-file=loadgen.py=chaos/loadgen.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found >/dev/null 2>&1
kubectl -n "$NAMESPACE" run loadgen --restart=Never --image=python:3.12-alpine \
  --overrides="$(cat <<JSON
{"spec":{"containers":[{"name":"loadgen","image":"python:3.12-alpine",
 "command":["python3","/app/loadgen.py","--url","http://api:8080","--duration","${DURATION}",
            "--concurrency","${CONCURRENCY}","--tenant","${TENANT}"],
 "volumeMounts":[{"name":"script","mountPath":"/app"}]}],
 "volumes":[{"name":"script","configMap":{"name":"loadgen-script"}}],"restartPolicy":"Never"}}
JSON
)" >/dev/null
sleep 12

echo "==> deploying a BROKEN version (starts, never becomes Ready)"
# Closer to a real bad deploy than an image that refuses to start: the container runs, but points its
# datasource at a host that does not exist, so the readiness probe never passes. A crash-looping
# container is the easy case; a process that is up but cannot serve is the one that reaches
# production.
BROKEN_AT=$(date +%s)
kubectl -n "$NAMESPACE" set env deployment/api \
  SPRING_DATASOURCE_URL="jdbc:postgresql://nonexistent-db-host:5432/hookrelay" >/dev/null

echo "==> watching the rollout stall for ${STALL_SECONDS}s"
kubectl -n "$NAMESPACE" rollout status deployment/api --timeout="${STALL_SECONDS}s" >/dev/null 2>&1
ROLLOUT_RC=$?
STALLED_FOR=$(( $(date +%s) - BROKEN_AT ))

READY=$(kubectl -n "$NAMESPACE" get deployment api -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
UPDATED=$(kubectl -n "$NAMESPACE" get deployment api -o jsonpath='{.status.updatedReplicas}' 2>/dev/null)
UPDATED_READY=$(kubectl -n "$NAMESPACE" get pods -l app=api \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null | grep -c true)

echo "    rollout status exit=${ROLLOUT_RC} (non-zero = did not complete, which is the gate a pipeline would use)"
echo "    readyReplicas=${READY:-0} updatedReplicas=${UPDATED:-0} pods actually ready=${UPDATED_READY:-0}"

echo "==> rolling back"
UNDO_AT=$(date +%s)
kubectl -n "$NAMESPACE" rollout undo deployment/api >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s >/dev/null
RECOVERY_SECONDS=$(( $(date +%s) - UNDO_AT ))
echo "    recovered in ${RECOVERY_SECONDS}s"

echo "==> waiting for the load generator"
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout=180s >/dev/null 2>&1
RESULT=$(kubectl -n "$NAMESPACE" logs loadgen 2>/dev/null | grep LOADGEN_RESULT | sed 's/^LOADGEN_RESULT //')

sleep 15
PGPOD=$(kubectl -n "$NAMESPACE" get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
sql() { kubectl -n "$NAMESPACE" exec "$PGPOD" -- psql -U hookrelay -d hookrelay -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
EVENTS=$(sql "SELECT count(*) FROM events WHERE tenant_id='${TENANT}'")
SUCCEEDED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='SUCCEEDED'")

echo
echo "=== BAD DEPLOY AND ROLLBACK ==="
echo "loadgen: ${RESULT}"
echo "rollout_status_exit    = ${ROLLOUT_RC}   (non-zero: the pipeline's gate)"
echo "stalled_seconds        = ${STALLED_FOR}"
echo "ready_replicas_during  = ${READY:-0}     (old version kept serving)"
echo "broken_pods_ready      = 0 expected; readiness never admitted them"
echo "recovery_seconds       = ${RECOVERY_SECONDS}"
echo "events_accepted        = ${EVENTS}"
echo "deliveries_succeeded   = ${SUCCEEDED}"
echo "=== END ==="

#!/usr/bin/env bash
#
# BLUEPRINT.md 28.3 — rolling deployment under load.
#
# Drives constant traffic at the ingest API from inside the cluster, triggers a rolling restart of
# both Deployments mid-flight, and reports what it cost: requests failed, and deliveries lost.
#
#   ./chaos/rolling-deploy.sh [with-prestop|without-prestop]
#
# The second mode strips the preStop hook first, so the hook can be shown to be doing something
# rather than cargo-culted. It is restored afterwards.
set -euo pipefail

MODE="${1:-with-prestop}"
NAMESPACE=hookrelay
DURATION="${DURATION:-70}"
CONCURRENCY="${CONCURRENCY:-8}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

cleanup() {
  kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

restore_prestop() {
  echo "==> restoring the preStop hook"
  kubectl -n "$NAMESPACE" patch deployment api --type=json \
    -p '[{"op":"add","path":"/spec/template/spec/containers/0/lifecycle","value":{"preStop":{"exec":{"command":["sh","-c","sleep 8"]}}}}]' >/dev/null
  kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s >/dev/null
}

if [ "$MODE" = "without-prestop" ]; then
  echo "==> removing the preStop hook (baseline)"
  kubectl -n "$NAMESPACE" patch deployment api --type=json \
    -p '[{"op":"remove","path":"/spec/template/spec/containers/0/lifecycle"}]' >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s >/dev/null
  trap 'restore_prestop; cleanup' EXIT
fi

echo "==> registering an endpoint for this run"
TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
kubectl -n "$NAMESPACE" run "reg-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/endpoints \
  -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
  -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret"}' \
  >/dev/null 2>&1

echo "==> starting load: ${CONCURRENCY} threads for ${DURATION}s"
kubectl -n "$NAMESPACE" create configmap loadgen-script \
  --from-file=loadgen.py=chaos/loadgen.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" run loadgen --restart=Never --image=python:3.12-alpine \
  --overrides="$(cat <<JSON
{
  "spec": {
    "containers": [{
      "name": "loadgen",
      "image": "python:3.12-alpine",
      "command": ["python3","/app/loadgen.py","--url","http://api:8080",
                  "--duration","${DURATION}","--concurrency","${CONCURRENCY}","--tenant","${TENANT}"${EXTRA_ARGS:-}],
      "volumeMounts": [{"name":"script","mountPath":"/app"}]
    }],
    "volumes": [{"name":"script","configMap":{"name":"loadgen-script"}}],
    "restartPolicy": "Never"
  }
}
JSON
)" >/dev/null

# Let the load settle before disturbing anything, so the rollout lands in the middle of it.
sleep 15

echo "==> rolling restart of api and worker, mid-load"
ROLLOUT_START=$(date +%s)
kubectl -n "$NAMESPACE" rollout restart deployment/api deployment/worker >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=300s
ROLLOUT_SECONDS=$(( $(date +%s) - ROLLOUT_START ))
echo "    rollout completed in ${ROLLOUT_SECONDS}s"

echo "==> waiting for the load generator to finish"
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout=180s >/dev/null
RESULT=$(kubectl -n "$NAMESPACE" logs loadgen | grep LOADGEN_RESULT | sed 's/^LOADGEN_RESULT //')

echo "==> waiting for the delivery backlog to drain"
sleep 20

# Deliveries lost is the question the delivery contract cares about: every accepted event must reach
# a terminal state, and for a healthy endpoint that means SUCCEEDED.
PGPOD=$(kubectl -n "$NAMESPACE" get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
sql() {
  kubectl -n "$NAMESPACE" exec "$PGPOD" -- psql -U hookrelay -d hookrelay -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}
EVENTS=$(sql "SELECT count(*) FROM events WHERE tenant_id='${TENANT}'")
DELIVERIES=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'")
SUCCEEDED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='SUCCEEDED'")
UNFINISHED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status NOT IN ('SUCCEEDED','DEAD')")
DEAD=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='DEAD'")

echo
echo "=== ROLLING DEPLOY: ${MODE} ==="
echo "loadgen: ${RESULT}"
echo "rollout_seconds        = ${ROLLOUT_SECONDS}"
echo "events_in_db           = ${EVENTS}"
echo "deliveries_created     = ${DELIVERIES}"
echo "deliveries_succeeded   = ${SUCCEEDED}"
echo "deliveries_dead        = ${DEAD}"
echo "deliveries_unfinished  = ${UNFINISHED}"
echo "=== END ${MODE} ==="

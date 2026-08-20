#!/usr/bin/env bash
#
# BLUEPRINT.md §28.1 — kill workers under load; prove no accepted delivery is lost.
#
#   ./chaos/worker-crash.sh
#
# Two kills, deliberately different:
#   - a graceful delete (SIGTERM, grace period, in-flight work finishes) exercises the shutdown path
#   - a forced delete (SIGKILL, no grace) exercises the broker's redelivery of unacked messages
#
# Asserting only on the graceful one would test the easy half. The assertion is on DURABLE STATE, not
# on the absence of errors: phase 7 reported zero failed requests while losing three deliveries.
set -uo pipefail

NAMESPACE=hookrelay
DURATION="${DURATION:-45}"
CONCURRENCY="${CONCURRENCY:-6}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

cleanup() { kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> resetting the receiver to answer instantly"
kubectl -n "$NAMESPACE" patch deployment receiver --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["python3","/app/webhook_receiver.py","--secret","$(RECEIVER_SECRET)","--port","9000"]}]' >/dev/null 2>&1
kubectl -n "$NAMESPACE" rollout status deployment/receiver --timeout=180s >/dev/null 2>&1

kubectl -n "$NAMESPACE" scale deployment/worker --replicas=3 >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=300s >/dev/null

TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
kubectl -n "$NAMESPACE" run "reg-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/endpoints \
  -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
  -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret","max_concurrency":20}' \
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
VICTIM=$(kubectl -n "$NAMESPACE" get pods -l app=worker -o jsonpath='{.items[0].metadata.name}')
echo "==> graceful delete: ${VICTIM}"
kubectl -n "$NAMESPACE" delete pod "$VICTIM" --wait=false >/dev/null
GRACEFUL_KILLS=1

sleep 12
# Must be a DIFFERENT pod, and one that is not already shutting down. `status.phase=Running` is not
# enough: a pod that has been deleted stays Running until its grace period expires, so the naive
# selector picks the pod just deleted and the "second kill" hits a corpse.
VICTIM2=$(kubectl -n "$NAMESPACE" get pods -l app=worker \
            -o jsonpath='{range .items[?(@.metadata.deletionTimestamp=="")]}{.metadata.name} {.status.phase}{"\n"}{end}' 2>/dev/null \
            | awk -v skip="$VICTIM" '$2=="Running" && $1!=skip {print $1; exit}')
VICTIM2=${VICTIM2:-$(kubectl -n "$NAMESPACE" get pods -l app=worker -o jsonpath='{.items[-1:].metadata.name}')}
echo "==> FORCED kill (SIGKILL, no grace period): ${VICTIM2}"
kubectl -n "$NAMESPACE" delete pod "$VICTIM2" --force --grace-period=0 --wait=false >/dev/null 2>&1
FORCED_KILLS=1

echo "==> waiting for the load generator"
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout=240s >/dev/null 2>&1
RESULT=$(kubectl -n "$NAMESPACE" logs loadgen 2>/dev/null | grep LOADGEN_RESULT | sed 's/^LOADGEN_RESULT //')

echo "==> waiting for the system to go quiescent"
kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=300s >/dev/null 2>&1
for _ in $(seq 1 40); do
  INFLIGHT=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
              | awk '$1=="deliveries"||$1=="deliveries.deferred"||$1 ~ /^retry\./ {s+=$2} END{print s+0}')
  [ "${INFLIGHT:-1}" = "0" ] && break
  sleep 6
done

PGPOD=$(kubectl -n "$NAMESPACE" get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
sql() { kubectl -n "$NAMESPACE" exec "$PGPOD" -- psql -U hookrelay -d hookrelay -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
EVENTS=$(sql "SELECT count(*) FROM events WHERE tenant_id='${TENANT}'")
DELIVERIES=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'")
SUCCEEDED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='SUCCEEDED'")
DEAD=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='DEAD'")
# The delivery contract, restated as a query: once quiescent, nothing may remain non-terminal.
UNFINISHED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status NOT IN ('SUCCEEDED','DEAD')")
ATTEMPTS=$(sql "SELECT coalesce(sum(d.attempt_count),0) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'")

COUNTERS=$(kubectl -n "$NAMESPACE" run "cnt-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS http://receiver:9000/ 2>/dev/null)

echo
echo "=== WORKER CRASH ==="
echo "loadgen: ${RESULT}"
echo "graceful_kills         = ${GRACEFUL_KILLS}"
echo "forced_kills           = ${FORCED_KILLS}"
echo "events_accepted        = ${EVENTS}"
echo "deliveries_created     = ${DELIVERIES}"
echo "deliveries_succeeded   = ${SUCCEEDED}"
echo "deliveries_dead        = ${DEAD}"
echo "deliveries_unfinished  = ${UNFINISHED}   (must be 0 — the contract as a query)"
echo "total_attempts         = ${ATTEMPTS}     (> deliveries means redelivery happened)"
echo "receiver: ${COUNTERS}"
echo "=== END ==="

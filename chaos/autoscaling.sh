#!/usr/bin/env bash
#
# BLUEPRINT.md §22 — CPU-based HPA versus KEDA queue-depth scaling, under an identical workload.
#
#   ./chaos/autoscaling.sh cpu
#   ./chaos/autoscaling.sh keda
#
# Builds a backlog with a deliberately SLOW customer endpoint rather than with enormous ingest
# throughput: a backlog forms when deliveries arrive faster than they complete, and completion rate
# is governed by how fast endpoints respond. That is the real-world cause of backlogs, and it puts
# workers in exactly the I/O-bound state where CPU stops carrying information.
set -euo pipefail

ARM="${1:-keda}"
NAMESPACE=hookrelay
# Sized so the backlog is large enough that CPU-based scaling visibly fails to react, but small
# enough that a scaled-up worker pool can actually drain it inside the watch window. An earlier run
# used 16 threads for 45s at a 1s delay and produced a 73,000-message backlog that would have taken
# half an hour to clear — good for showing the failure, useless for comparing drain times.
CONCURRENCY="${CONCURRENCY:-8}"
LOAD_SECONDS="${LOAD_SECONDS:-5}"
ENDPOINT_DELAY="${ENDPOINT_DELAY:-0.3}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"
MAX_WATCH_SECONDS="${MAX_WATCH_SECONDS:-360}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

cleanup() {
  kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found --wait=false >/dev/null 2>&1 || true
  # Put the receiver back to answering instantly, so the slow endpoint does not leak into the next
  # experiment or the smoke test.
  kubectl -n "$NAMESPACE" patch deployment receiver --type=json \
    -p '[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["python3","/app/webhook_receiver.py","--secret","$(RECEIVER_SECRET)","--port","9000"]}]' \
    >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> resetting: remove both autoscalers, scale worker to 2, drain queues"
kubectl -n "$NAMESPACE" delete hpa worker-cpu --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" delete scaledobject worker-queue-depth --ignore-not-found >/dev/null 2>&1 || true
sleep 3
kubectl -n "$NAMESPACE" scale deployment/worker --replicas=2 >/dev/null

# Purge EVERY queue, not just `deliveries`. Messages sitting in the retry tiers expire back into
# `deliveries` via their dead-letter exchange, so purging only the main queue lets a previous arm's
# backlog reappear seconds later — which is exactly how one run inherited 73,000 messages from
# another. The explicit timeout matters too: purging a large queue silently exceeds the default.
for Q in deliveries deliveries.deferred deliveries.dlq \
         retry.5s retry.30s retry.2m retry.10m retry.30m retry.1h retry.3h; do
  kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c "rabbitmqctl purge_queue $Q --timeout 120" \
    >/dev/null 2>&1 || true
done
# Only now wait for the scale-down. Waiting first meant workers still chewing a previous arm's
# backlog kept the rollout incomplete until it timed out — which under `set -e` killed the run.
kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=180s >/dev/null 2>&1 || true

echo "==> applying the ${ARM} autoscaler"
case "$ARM" in
  cpu)  kubectl apply -f infra/kubernetes/50-hpa-cpu.yaml >/dev/null ;;
  keda) kubectl apply -f infra/kubernetes/51-keda.yaml >/dev/null ;;
  *) echo "usage: $0 [cpu|keda]" >&2; exit 2 ;;
esac
sleep 10

echo "==> pointing the receiver at a ${ENDPOINT_DELAY}s delay"
kubectl -n "$NAMESPACE" set env deployment/receiver RECEIVER_DELAY="$ENDPOINT_DELAY" >/dev/null
kubectl -n "$NAMESPACE" patch deployment receiver --type=json -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/command\",\"value\":[\"python3\",\"/app/webhook_receiver.py\",\"--secret\",\"\$(RECEIVER_SECRET)\",\"--port\",\"9000\",\"--delay\",\"${ENDPOINT_DELAY}\"]}]" >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/receiver --timeout=180s >/dev/null

# THREE endpoints, so every event fans out to three deliveries. That makes the events/sec versus
# deliveries/sec distinction from BLUEPRINT.md 25 concrete rather than a footnote, and builds the
# backlog three times faster.
TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
for _ in 1 2 3; do
  kubectl -n "$NAMESPACE" run "reg-$RANDOM" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/endpoints \
    -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
    -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret","max_concurrency":50}' \
    >/dev/null 2>&1
done

echo "==> driving load: ${CONCURRENCY} threads for ${LOAD_SECONDS}s"
kubectl -n "$NAMESPACE" create configmap loadgen-script \
  --from-file=loadgen.py=chaos/loadgen.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" delete pod loadgen --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" run loadgen --restart=Never --image=python:3.12-alpine \
  --overrides="$(cat <<JSON
{"spec":{"containers":[{"name":"loadgen","image":"python:3.12-alpine",
 "command":["python3","/app/loadgen.py","--url","http://api:8080","--duration","${LOAD_SECONDS}",
            "--concurrency","${CONCURRENCY}","--tenant","${TENANT}"],
 "volumeMounts":[{"name":"script","mountPath":"/app"}]}],
 "volumes":[{"name":"script","configMap":{"name":"loadgen-script"}}],"restartPolicy":"Never"}}
JSON
)" >/dev/null

echo
echo "elapsed_s,queue_depth,worker_replicas,worker_cpu_millicores"
STARTED=$(date +%s)
PEAK_DEPTH=0
PEAK_REPLICAS=0
MIN_CPU=999999
MAX_CPU=0
DRAIN_AT=""
SAW_BACKLOG=0
LAST_DEPTH=0
LAST_REPLICAS=2

while :; do
  ELAPSED=$(( $(date +%s) - STARTED ))
  [ "$ELAPSED" -gt "$MAX_WATCH_SECONDS" ] && break

  # Every kubectl call here is guarded. A transient API-server hiccup inside an unguarded command
  # substitution kills the whole run under `set -e`, silently and after several minutes of sampling.
  DEPTH=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
            | awk '$1=="deliveries"{print $2}' | head -1 || true)
  DEPTH=${DEPTH:-$LAST_DEPTH}
  REPLICAS=$(kubectl -n "$NAMESPACE" get deployment worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  REPLICAS=${REPLICAS:-$LAST_REPLICAS}
  LAST_DEPTH=$DEPTH
  LAST_REPLICAS=$REPLICAS
  CPU=$(kubectl -n "$NAMESPACE" top pods -l app=worker --no-headers 2>/dev/null \
          | awk '{gsub(/m/,"",$2); sum+=$2; n++} END{if(n>0) print int(sum/n); else print 0}' || true)
  CPU=${CPU:-0}

  echo "${ELAPSED},${DEPTH},${REPLICAS},${CPU}"

  [ "$DEPTH" -gt "$PEAK_DEPTH" ] && PEAK_DEPTH=$DEPTH
  [ "$REPLICAS" -gt "$PEAK_REPLICAS" ] && PEAK_REPLICAS=$REPLICAS
  [ "$CPU" -gt "$MAX_CPU" ] && MAX_CPU=$CPU
  [ "$CPU" -lt "$MIN_CPU" ] && MIN_CPU=$CPU
  [ "$DEPTH" -gt 200 ] && SAW_BACKLOG=1

  # "Drained" must mean the work is DONE, not merely absent from this one queue. Watching only
  # `deliveries` counted messages parked in `deliveries.deferred` as drained and reported success at
  # 41s when 611 of 1635 deliveries had actually completed.
  INFLIGHT=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
              | awk '$1=="deliveries"||$1=="deliveries.deferred"||$1 ~ /^retry\./ {s+=$2} END{print s+0}' || true)
  INFLIGHT=${INFLIGHT:-$DEPTH}

  if [ "$SAW_BACKLOG" = "1" ] && [ "$INFLIGHT" -le 5 ] && [ -z "$DRAIN_AT" ]; then
    DRAIN_AT=$ELAPSED
    break
  fi
  sleep "$SAMPLE_SECONDS"
done

set +e   # from here on, a failed query must degrade the report, not discard it
RESULT=$(kubectl -n "$NAMESPACE" logs loadgen 2>/dev/null | grep LOADGEN_RESULT | sed 's/^LOADGEN_RESULT //')

PGPOD=$(kubectl -n "$NAMESPACE" get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
sql() { kubectl -n "$NAMESPACE" exec "$PGPOD" -- psql -U hookrelay -d hookrelay -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
EVENTS_DB=$(sql "SELECT count(*) FROM events WHERE tenant_id='${TENANT}'")
DELIVERIES_DB=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'")
SUCCEEDED=$(sql "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}' AND d.status='SUCCEEDED'")

echo
echo "=== AUTOSCALING: ${ARM} ==="
echo "loadgen: ${RESULT}"
echo "peak_queue_depth       = ${PEAK_DEPTH}"
echo "peak_worker_replicas   = ${PEAK_REPLICAS}"
echo "worker_cpu_millicores  = ${MIN_CPU}..${MAX_CPU}  (request 100m)"
echo "backlog_drain_seconds  = ${DRAIN_AT:-not drained within ${MAX_WATCH_SECONDS}s}"
echo "events_in_db           = ${EVENTS_DB}"
echo "deliveries_created     = ${DELIVERIES_DB}"
echo "deliveries_succeeded   = ${SUCCEEDED}"
if [ "${EVENTS_DB:-0}" -gt 0 ]; then
  echo "average_fanout         = $(awk -v d="$DELIVERIES_DB" -v e="$EVENTS_DB" 'BEGIN{printf "%.2f", d/e}')"
fi
echo "=== END ${ARM} ==="

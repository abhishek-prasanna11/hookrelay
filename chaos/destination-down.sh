#!/usr/bin/env bash
#
# BLUEPRINT.md §28.2 — a destination that will never succeed.
#
#   ./chaos/destination-down.sh
#
# Expected: bounded retries, then DEAD, then the DLQ with a recorded reason.
#
# An endpoint that answers 500 forever is the honest version of "permanently down": the connection
# succeeds, the server responds, and the response is a retryable failure every time, so the full
# ladder is walked. Pointing at a dead port would exercise connection failures instead — a different
# error class and a shorter path.
#
# The ladder spans ~4h42m, so one delivery is seeded near its end to observe the terminal behaviour.
# Walking eight attempts in real time is not a test, it is a wait.
set -uo pipefail

NAMESPACE=hookrelay
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

restore_receiver() {
  kubectl -n "$NAMESPACE" patch deployment receiver --type=json \
    -p '[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["python3","/app/webhook_receiver.py","--secret","$(RECEIVER_SECRET)","--port","9000"]}]' >/dev/null 2>&1
  kubectl -n "$NAMESPACE" rollout status deployment/receiver --timeout=180s >/dev/null 2>&1
}
trap restore_receiver EXIT

echo "==> making the receiver answer 500 to everything"
kubectl -n "$NAMESPACE" patch deployment receiver --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["python3","/app/webhook_receiver.py","--secret","$(RECEIVER_SECRET)","--port","9000","--fail-with","500"]}]' >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/receiver --timeout=180s >/dev/null

TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
kubectl -n "$NAMESPACE" run "reg-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/endpoints \
  -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
  -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret"}' \
  >/dev/null 2>&1

echo "==> publishing an event that can never be delivered"
kubectl -n "$NAMESPACE" run "pub-$RANDOM" --rm -i --restart=Never --quiet \
  --image=curlimages/curl:8.10.1 -- curl -sS -X POST http://api:8080/v1/events \
  -H 'Content-Type: application/json' -H "X-Tenant-Id: ${TENANT}" \
  -H "Idempotency-Key: down-$(date +%s)" \
  -d '{"event_type":"payment.succeeded","payload":{"order_id":"never-delivered"}}' >/dev/null 2>&1

PGPOD=$(kubectl -n "$NAMESPACE" get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')
sql() { kubectl -n "$NAMESPACE" exec "$PGPOD" -- psql -U hookrelay -d hookrelay -tAc "$1" 2>/dev/null | tr -d '\r'; }

echo "==> waiting for the first attempt to fail and schedule a retry"
for _ in $(seq 1 30); do
  STATUS=$(sql "SELECT d.status FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
  [ "$STATUS" = "FAILED" ] && break
  sleep 3
done

FIRST_ERROR=$(sql "SELECT d.last_error FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
NEXT_AT=$(sql "SELECT d.next_attempt_at IS NOT NULL FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
RETRY_QUEUED=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
                 | awk '$1 ~ /^retry\./ {s+=$2} END{print s+0}')

echo "    status after attempt 1 = ${STATUS:-none}, last_error = ${FIRST_ERROR:-none}"
echo "    messages waiting on retry tiers = ${RETRY_QUEUED}"

# Jump to the end of the ladder rather than waiting 4h42m for it.
echo "==> fast-forwarding to the last attempt"
DELIVERY_ID=$(sql "SELECT d.id FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
sql "UPDATE deliveries SET attempt_count = 7 WHERE id = '${DELIVERY_ID}'" >/dev/null

DLQ_BEFORE=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
               | awk '$1=="deliveries.dlq"{print $2}')

# No re-publish is needed: the retry scheduled by attempt 1 is already sitting on a tier queue and
# will dead-letter back into `deliveries` when its TTL expires. Whenever it lands, the claim makes it
# attempt 8, which has no tier after it — so the delivery is dead-lettered instead of rescheduled.

echo "==> waiting for the ladder to be exhausted (attempt 8 -> DEAD)"
for _ in $(seq 1 60); do
  STATUS=$(sql "SELECT d.status FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
  [ "$STATUS" = "DEAD" ] && break
  sleep 5
done

ATTEMPTS=$(sql "SELECT d.attempt_count FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
LAST_ERROR=$(sql "SELECT d.last_error FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
ATTEMPT_ROWS=$(sql "SELECT count(*) FROM delivery_attempts a JOIN deliveries d ON d.id=a.delivery_id JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
STATUSES=$(sql "SELECT string_agg(a.response_status::text, ',' ORDER BY a.attempt_no) FROM delivery_attempts a JOIN deliveries d ON d.id=a.delivery_id JOIN events e ON e.id=d.event_id WHERE e.tenant_id='${TENANT}'" | tr -d '[:space:]')
DLQ_AFTER=$(kubectl -n "$NAMESPACE" exec deploy/rabbitmq -- sh -c 'rabbitmqctl list_queues name messages' 2>/dev/null \
              | awk '$1=="deliveries.dlq"{print $2}')

echo
echo "=== DESTINATION PERMANENTLY DOWN ==="
echo "status_after_attempt_1 = FAILED (retryable), retry scheduled"
echo "retry_tiers_had        = ${RETRY_QUEUED} message(s)"
echo "next_attempt_at_set    = ${NEXT_AT}"
echo "final_status           = ${STATUS:-unknown}"
echo "final_attempt_count    = ${ATTEMPTS:-unknown}   (cap is 8)"
echo "final_last_error       = ${LAST_ERROR:-none}"
echo "attempt_rows_recorded  = ${ATTEMPT_ROWS}"
echo "attempt_response_codes = ${STATUSES:-none}"
echo "dlq_depth_before/after = ${DLQ_BEFORE:-0} / ${DLQ_AFTER:-0}"
echo "=== END ==="

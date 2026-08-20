#!/usr/bin/env bash
#
# End-to-end check against the cluster: register an endpoint pointing at the in-cluster receiver,
# publish an event, and assert it is actually delivered and signature-verified.
#
#   ./infra/kubernetes/smoke.sh
set -euo pipefail

NAMESPACE=hookrelay
TENANT="$(uuidgen | tr '[:upper:]' '[:lower:]')"
KEY="smoke-$(date +%s)"

fail() { echo "FAIL: $*" >&2; exit 1; }

# Runs curl from inside the cluster so the Services are reachable without a port-forward — which
# would also route every request to a single pod and hide anything load-balancing related.
incluster() {
  kubectl -n "$NAMESPACE" run "smoke-$RANDOM" --rm -i --restart=Never \
    --image=curlimages/curl:8.10.1 --quiet -- "$@" 2>/dev/null
}

echo "==> registering an endpoint pointing at the in-cluster receiver"
ENDPOINT_JSON=$(incluster curl -sS -X POST http://api:8080/v1/endpoints \
  -H 'Content-Type: application/json' \
  -H "X-Tenant-Id: ${TENANT}" \
  -d '{"url":"http://receiver:9000/hook","event_types":["payment.succeeded"],"secret":"demo-endpoint-secret"}')

echo "$ENDPOINT_JSON" | grep -q '"id"' || fail "endpoint registration failed: $ENDPOINT_JSON"
echo "    ok"

echo "==> publishing an event"
EVENT_JSON=$(incluster curl -sS -X POST http://api:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H "X-Tenant-Id: ${TENANT}" \
  -H "Idempotency-Key: ${KEY}" \
  -d '{"event_type":"payment.succeeded","payload":{"order_id":"smoke-1"}}')

echo "$EVENT_JSON" | grep -q '"deliveries_created":1' || fail "expected 1 delivery: $EVENT_JSON"
EVENT_ID=$(echo "$EVENT_JSON" | sed -n 's/.*"event_id":"\([^"]*\)".*/\1/p')
echo "    event ${EVENT_ID}"

echo "==> waiting for delivery"
for _ in $(seq 1 30); do
  STATUS=$(incluster curl -sS "http://api:8080/v1/events/${EVENT_ID}" -H "X-Tenant-Id: ${TENANT}" \
    | sed -n 's/.*"status":"\([A-Z_]*\)".*/\1/p' | head -1)
  [ "$STATUS" = "SUCCEEDED" ] && break
  sleep 2
done
[ "${STATUS:-}" = "SUCCEEDED" ] || fail "delivery did not succeed (status=${STATUS:-none})"
echo "    delivered"

echo "==> checking the receiver verified the signature"
COUNTERS=$(incluster curl -sS http://receiver:9000/)
echo "    ${COUNTERS}"
echo "$COUNTERS" | grep -q '"rejected": 0' || fail "receiver rejected a signature: $COUNTERS"

echo
echo "SMOKE PASSED"

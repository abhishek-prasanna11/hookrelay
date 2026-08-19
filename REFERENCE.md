# HookRelay — Reference

Current as of **phase 4**. Sections for the Kubernetes topology arrive with the phases that build
them.

---

## Running it locally

```bash
docker compose up -d
```

Starts PostgreSQL and RabbitMQ. The RabbitMQ management UI is at http://localhost:15672
(guest/guest) — useful for watching `deliveries` fill up while nothing consumes it yet.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api spring-boot:run -pl api
```

Maven's own JVM on this machine is JDK 26; the project targets 21, so `JAVA_HOME` is pinned
explicitly on every build and run command below.

### Build and test

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -DskipTests package
```

Integration tests start their own PostgreSQL 16 and RabbitMQ 3.13 containers via Testcontainers —
Docker must be running, but no local database or broker is required.

Run one test class or one method:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api -Dtest=IngestIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## API

All endpoints require `X-Tenant-Id`. There is no authentication in phase 1 — see
docs/phase01-ingest.md §4.5.

The wire format is `snake_case`; nulls are omitted.

### `POST /v1/events`

Accept an event. Requires `Idempotency-Key`.

```bash
curl -i -X POST localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H "X-Tenant-Id: 7c9e6679-7425-40de-944b-e07fc1f90ae7" \
  -H 'Idempotency-Key: order-A1-created' \
  -d '{"event_type": "payment.succeeded", "payload": {"order_id": "A-1", "amount": 4200}}'
```

```json
{ "event_id": "0198f2c1-...-7a3b", "deliveries_created": 3 }
```

| Status | Meaning |
|---|---|
| `202 Accepted` | This call created the event. Event, and one delivery per matching endpoint, are committed. |
| `200 OK` | Same key, same request — the original `event_id` is returned. Nothing new was written. |
| `409 Conflict` | Same key, **different** request body or event type. |
| `400 Bad Request` | Missing `Idempotency-Key` or `X-Tenant-Id`, blank `event_type`, malformed JSON, non-object payload. |
| `422 Unprocessable Entity` | Payload exceeds `hookrelay.ingest.max-payload-bytes`. |

`deliveries_created: 0` is a success — an event nobody subscribes to is still a valid event.

### `GET /v1/events/{id}`

The event plus a summary of every delivery created for it.

### `POST /v1/endpoints`

```bash
curl -i -X POST localhost:8080/v1/endpoints \
  -H 'Content-Type: application/json' \
  -H "X-Tenant-Id: 7c9e6679-7425-40de-944b-e07fc1f90ae7" \
  -d '{"url": "https://example.com/hook",
       "event_types": ["payment.succeeded", "payment.refunded"],
       "max_concurrency": 5}'
```

| Field | Required | Notes |
|---|---|---|
| `url` | yes | Absolute, `http` or `https`. Syntactic validation only in phase 1; SSRF checks arrive in phase 5. |
| `event_types` | yes | Non-empty. Trimmed, deduplicated and sorted on write. Exact match — no wildcards. |
| `max_concurrency` | no | Defaults to 5. Per-endpoint in-flight cap, enforced by the worker from phase 5. |
| `secret` | no | Generated (256-bit, hex) if omitted. |

`201 Created`. **The response contains `secret`, and it is the only response that ever will** —
later reads omit it.

### `GET /v1/endpoints` · `GET /v1/endpoints/{id}` · `DELETE /v1/endpoints/{id}`

`DELETE` is a soft delete: `active` becomes false, so the endpoint stops matching future events.
Deliveries already created for it remain valid obligations (blueprint §8).

### `GET /v1/deliveries/{id}`

Delivery status and its full attempt history. Tenant-scoped through the parent event; another
tenant's delivery returns `404`, not `403`, so the endpoint cannot be used to probe which ids exist.

### `GET /actuator/health`

Liveness and readiness probes are enabled at `/actuator/health/liveness` and
`/actuator/health/readiness`.

---

## Configuration

| Property | Default | Purpose |
|---|---|---|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/hookrelay?reWriteBatchedInserts=true` | `reWriteBatchedInserts` collapses batched delivery inserts into one multi-row statement. |
| `spring.jpa.hibernate.ddl-auto` | `validate` | Flyway owns the schema; Hibernate only checks the mapping agrees and refuses to start otherwise. |
| `spring.jpa.properties.hibernate.jdbc.batch_size` | `50` | Fan-out writes one row per matching endpoint. |
| `hookrelay.ingest.max-payload-bytes` | `262144` | Above this, `422`. |
| `spring.rabbitmq.publisher-confirm-type` | `simple` | Lets the publisher block on `waitForConfirms` for a whole batch on one channel. Without confirms, publishing is fire-and-forget and "succeeds" against a dead broker. |
| `spring.rabbitmq.publisher-returns` | `true` | Required for `mandatory` to produce a return instead of a silent discard when a message is unroutable. |
| `hookrelay.outbox.publisher.enabled` | `true` | Gate, not a hard-coded decision. Running the same image with this off in the API and on elsewhere is all it takes to give the publisher its own Deployment. |
| `hookrelay.outbox.publisher.poll-interval-ms` | `200` | |
| `hookrelay.outbox.publisher.batch-size` | `100` | Bounds how many rows are row-locked while waiting on the broker. |
| `hookrelay.outbox.publisher.confirm-timeout-ms` | `5000` | |
| `hookrelay.outbox.purge.enabled` | `true` | Gates the schedule only; the bean and `purgeNow` always exist. |
| `hookrelay.outbox.purge.retention-hours` | `24` | How long published rows are kept for diagnosis. |
| `server.port` | `8080` | |

---

## Broker topology

```text
   publish (routing key "delivery", mandatory, persistent)
             │
             ▼
   ┌──────────────────────┐
   │ exchange: hookrelay  │   direct, durable
   └──────────┬───────────┘
              │ binding: "delivery"
              ▼
   ┌──────────────────────┐
   │ queue: deliveries    │   durable
   └──────────────────────┘
```

Declared by both the API and the worker so either can start first.

### Retry tiers and the dead-letter queue

```text
   a retryable failure                        attempts exhausted, or
             │                                a permanent failure
             ▼                                          │
   ┌──────────────────────┐                             ▼
   │ exchange:            │                ┌──────────────────────┐
   │   hookrelay.retry    │                │ exchange:            │
   └──────────┬───────────┘                │   hookrelay.dlq      │
              │ routing key = tier name    └──────────┬───────────┘
              ▼                                       ▼
   retry.5s  retry.30s  retry.2m  retry.10m  ┌──────────────────────┐
   retry.30m retry.1h   retry.3h             │ queue:               │
              │                              │   deliveries.dlq     │
              │ no consumers; on TTL expiry  │ terminal: no TTL,    │
              │ the queue's dead-letter      │ no DLX, no consumer  │
              └──► hookrelay ──► deliveries  └──────────────────────┘
```

Each tier queue has no consumer, `x-dead-letter-exchange = hookrelay`,
`x-dead-letter-routing-key = delivery`, and `x-message-ttl` at the top of its jitter range as a
backstop. The per-message `expiration` carries the jittered delay and normally decides, since
RabbitMQ honours whichever is lower.

**One queue per tier, not one shared delay queue.** RabbitMQ only inspects the message at the head
of a queue for expiry, so a shared queue lets a 3-hour retry block every 5-second retry behind it —
measured at a 15.1× overshoot in [RESULTS.md](RESULTS.md#42-how-bad-is-head-of-line-blocking-really).

### The retry schedule

| Completed attempt | Next | Nominal delay | Tier |
|---:|---:|---|---|
| 1 | 2 | 5s | `retry.5s` |
| 2 | 3 | 30s | `retry.30s` |
| 3 | 4 | 2m | `retry.2m` |
| 4 | 5 | 10m | `retry.10m` |
| 5 | 6 | 30m | `retry.30m` |
| 6 | 7 | 1h | `retry.1h` |
| 7 | 8 | 3h | `retry.3h` |
| 8 | — | — | dead-lettered |

Every delay is multiplied by a uniform random factor in `[0.8, 1.2]`. Total window ≈ 4h 42m.

Jitter exists because an endpoint going down fails every delivery to it within the same second —
without it, all of them would retry in the *same* second, aimed at a service that was just
recovering.

### Reading the dead-letter queue

The message body is the usual claim check; the reason rides along as headers, so the queue explains
itself in the management UI without a database lookup.

| Header | Values |
|---|---|
| `x-hookrelay-reason` | `attempts_exhausted` · `permanent_failure` |
| `x-hookrelay-attempts` | attempts made |
| `x-hookrelay-last-error` | `HTTP 500` · `TIMEOUT` · `DNS` … |
| `x-hookrelay-endpoint-id` | the endpoint's UUID |
| `x-hookrelay-failed-at` | ISO-8601 |

Redrive is deliberately out of scope (BLUEPRINT.md §32).

Message body is a claim check — the delivery id only, not the payload:

```json
{ "delivery_id": "0198f2c1-...-7a3b" }
```

The AMQP `message-id` property carries the **outbox row id**, which is what correlates a returned
(unroutable) message back to the row that must not be marked published.

### The publish cycle

```text
   every 200ms:
     BEGIN
       SELECT unpublished ... LIMIT 100 FOR UPDATE SKIP LOCKED
       publish each, mandatory + persistent, on one channel
       waitForConfirms(5s)
         ├─ confirmed, none returned  →  UPDATE published_at = now()
         └─ timeout / nack / returned →  UPDATE attempt_count += 1, last_error
                                          (published_at stays NULL → retried)
     COMMIT
```

Publish happens **before** the row is marked, never after. The failure mode of that ordering is a
duplicate message, which this system tolerates by design; the other ordering's failure mode is a
lost message, which it does not.

---

## The webhook a receiver gets

```http
POST /your/endpoint HTTP/1.1
Content-Type: application/json
User-Agent: HookRelay/0.1
X-HookRelay-Delivery-Id: 0198f2c1-...-7a3b
X-HookRelay-Event-Id: 0198f2c0-...-1c4d
X-HookRelay-Event-Type: payment.succeeded
X-HookRelay-Attempt: 1
X-HookRelay-Signature: t=1755624000,v1=9f8c...e21a

{"id":"0198f2c1-...-7a3b","event_id":"0198f2c0-...-1c4d",
 "event_type":"payment.succeeded","created_at":"2026-08-19T15:02:47Z",
 "data":{"order_id":"A-1","amount":4200}}
```

The producer's payload is nested under `data` so a payload containing its own `id` or `event_type`
cannot collide with the envelope. `id` is the **delivery** id, and it is stable across every retry —
it is what a receiver deduplicates on.

### Verifying the signature

```text
signed string = "<t>" + "." + <raw request body bytes>
v1            = lowercase hex HMAC-SHA256(endpoint secret, signed string)
```

A receiver must: parse `t` and `v1`, reject if `|now - t|` exceeds its tolerance (300s is the
recommended default), recompute over the **raw bytes** it received, and compare in constant time.

Reference implementations, both pinned to the same golden vector:
`common/.../webhook/WebhookSignature.java` and [`tools/webhook_receiver.py`](tools/webhook_receiver.py).

```bash
python3 tools/webhook_receiver.py --secret <endpoint-secret> --port 9000
```

```bash
python3 tools/webhook_receiver.py --selftest
```

`--fail-with 500` to exercise retries, `--delay 30` to exercise timeouts, `GET /` for counters.

### Failure classification

| Class | Triggers | Resulting delivery status |
|---|---|---|
| `SUCCESS` | 2xx | `SUCCEEDED` |
| `RETRYABLE` | timeout, connect refused, DNS, TLS, 5xx, **429**, 408 | `FAILED` |
| `PERMANENT` | other 4xx, and **3xx** (redirects are never followed) | `DEAD` |

429 is retryable because it explicitly means "try later". 3xx is permanent because following a
redirect would let a customer register a public URL and redirect to an internal address, stepping
around the SSRF checks that arrive in phase 5.

`FAILED` means "attempted, failed, retry scheduled" — the delivery is on a tier queue and
`next_attempt_at` says roughly when it will fire. `SUCCEEDED` and `DEAD` are terminal.

---

## Worker configuration

| Property | Default | Purpose |
|---|---|---|
| `spring.rabbitmq.listener.simple.acknowledge-mode` | `manual` | The entire crash-safety mechanism. Automatic acknowledgement treats a message as delivered the moment it hits the socket, so a worker holding a prefetch window that dies loses all of it. |
| `spring.rabbitmq.listener.simple.prefetch` | `8` | Deliberately small. A large prefetch lets one worker hoard a backlog while other replicas idle, which would undermine queue-depth autoscaling in phase 8. |
| `spring.rabbitmq.listener.simple.concurrency` | `4` | |
| `hookrelay.delivery.connect-timeout-ms` | `5000` | |
| `hookrelay.delivery.request-timeout-ms` | `15000` | Total budget for the exchange. Java's `HttpClient` has no separate socket-read timeout, so this bounds an endpoint that trickles bytes. |
| `hookrelay.delivery.max-response-bytes` | `512` | A hostile endpoint returning gigabytes must not OOM the worker. Matches the `CHECK` on `delivery_attempts.response_body`. |
| `spring.flyway.enabled` | `false` | The API owns migrations; the worker only validates against them. |

---

## Schema

Migrations live in `common/src/main/resources/db/migration` — shared, because the worker's tests
need the schema too. Only the **API** applies them; the worker sets `spring.flyway.enabled: false`
and merely validates its entity mappings against whatever the API migrated. Two services racing to
migrate one database is a deadlock waiting for a deploy. Schema changes only ever happen through a
new numbered migration.

| Table | Holds |
|---|---|
| `endpoints` | Registered destinations. `event_types text[]` with a GIN index. |
| `events` | Immutable facts. `UNIQUE (tenant_id, idempotency_key)`. |
| `deliveries` | One obligation per event × matching endpoint. `id` is the public delivery id. |
| `delivery_attempts` | Append-only audit trail, one row per HTTP attempt. |
| `outbox_events` | Committed intent to publish. Partial index covers unpublished rows only. |

`deliveries.next_attempt_at` is **observability, not a scheduler**. The retry schedule lives in
RabbitMQ's delay queues; nothing polls this column, and nothing should, or the database and the
broker would race to retry the same delivery.

Constraints that encode a rule rather than trusting code:

| Constraint | Prevents |
|---|---|
| `events_idempotency_uq` | Duplicate events from concurrent submissions of the same key. |
| `deliveries_event_endpoint_uq` | Double delivery if phase 2's at-least-once outbox publisher ever re-runs fan-out. |
| `delivery_attempts_body_len` | An unbounded response body if the worker's 512-byte truncation is wrong. |
| `deliveries_status_valid` | A status outside the known set. |
| `outbox_delivery_uq` | A second queue message for a delivery that is already queued. |

---

## Metrics

Exposed on `/actuator/prometheus` from phase 6; registered now.

| Metric | Meaning |
|---|---|
| `hookrelay_outbox_lag_seconds` | Age of the oldest unpublished outbox row. The number that reveals a wedged publisher — counters of events accepted and messages published both look healthy while a backlog grows. |
| `hookrelay_outbox_published_total` | Outbox rows confirmed by the broker. |

---

## Repository layout

```text
hookrelay/
├── common/     domain, repositories, broker topology, webhook signing, SQL migrations
├── api/        ingest API, runs Flyway, integration tests
├── worker/     delivery worker: consumes the queue, signs and POSTs
├── tools/      webhook_receiver.py — reference receiver and signature verifier
├── infra/      docker/ kubernetes/ rabbitmq/ prometheus/   (phases 6-8)
├── load-tests/ (phase 8)
├── chaos/      (phase 10)
└── docs/       phaseNN-*.md — one per phase, written before its code
```

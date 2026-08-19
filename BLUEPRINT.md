# HookRelay — Blueprint

## 0. Project Goal

HookRelay is a production-oriented webhook delivery platform.

A producer submits an event once. HookRelay durably records the event, determines its matching
endpoints, and asynchronously delivers the event to those endpoints while surviving:

- worker crashes
- temporary destination failures
- slow destinations
- duplicate submissions
- retries
- rolling deployments
- traffic spikes

The project is intentionally scoped around four strong engineering stories:

1. Reliable asynchronous delivery
2. Failure handling and endpoint isolation
3. Kubernetes and intelligent autoscaling
4. Performance testing and chaos engineering

The goal is not to build every feature a commercial webhook platform might have.

---

## 1. Core Delivery Contract

### At-least-once delivery

Every accepted event is durably recorded. Every matching delivery will either end:

```text
SUCCEEDED
```

or:

```text
DEAD
```

with a recorded reason.

There must be no silent loss of accepted work.

### Not exactly-once

Exactly-once delivery is not guaranteed. A receiver can process a request successfully and the
network can fail before HookRelay receives the response.

Therefore HookRelay provides:

```text
at-least-once delivery
+
stable delivery ID
```

Every delivery has:

```text
X-HookRelay-Delivery-Id
```

The same ID is reused across retries so receivers can deduplicate.

### Not ordered

Delivery ordering is not guaranteed. A failed delivery may retry several times while newer
deliveries continue. Ordering is therefore an explicit non-goal.

### Durable before acknowledgement

`POST /v1/events` returns:

```text
202 Accepted
```

only after the event, delivery records, and outbox records have been committed to PostgreSQL.

If the API returns 202, the system has durable knowledge of the work.

### Bounded retry

The default retry policy is:

```text
Attempt 1 → immediate
Attempt 2 → 5s
Attempt 3 → 30s
Attempt 4 → 2m
Attempt 5 → 10m
Attempt 6 → 30m
Attempt 7 → 1h
Attempt 8 → 3h
```

Each retry receives ±20% jitter.

After the eighth attempt:

```text
DEAD
```

and the delivery enters the DLQ.

### Endpoint isolation

A slow or dead endpoint should not significantly degrade delivery to healthy endpoints.
This is one of the project's primary engineering goals.

---

## 2. Non-Goals

The following are deliberately outside the core scope:

- exactly-once delivery
- ordered delivery
- multi-region deployment
- customer-facing UI
- arbitrary historical event replay
- payload transformation
- OAuth endpoint authentication
- service mesh
- Terraform
- ArgoCD
- Istio
- distributed tracing as a required feature
- complex global rate limiting
- unnecessary infrastructure added only for resume keywords

Optional improvements may be added only after the core project is complete.

---

## 3. Architecture

```text
                         ┌──────────────────────┐
                         │      Spring API      │
                         │                      │
POST /v1/events ────────►│ validation           │
                         │ idempotency          │
                         │ event persistence    │
                         │ fan-out              │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     PostgreSQL       │
                         │                      │
                         │ events               │
                         │ endpoints            │
                         │ deliveries           │
                         │ delivery_attempts    │
                         │ outbox               │
                         └──────────┬───────────┘
                                    │
                           transactional outbox
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Outbox Publisher   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       RabbitMQ       │
                         │                      │
                         │ deliveries           │
                         │ retry queues         │
                         │ DLQ                  │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
                Worker 1        Worker 2        Worker N
                    │               │               │
                    └───────────────┼───────────────┘
                                    │
                         ┌──────────┴──────────┐
                         │                     │
                    Circuit breaker      Endpoint semaphore
                         │                     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         Customer HTTP endpoints


                  Kubernetes
                      │
              ┌───────┴────────┐
              │                │
           KEDA            Prometheus
              │                │
              ▼                ▼
          autoscaling       Grafana

              GitHub Actions
                    │
                    ▼
                   GHCR
                    │
                    ▼
                Kubernetes
```

---

## 4. Service Boundaries

### API

The API is:

- synchronous
- latency-sensitive
- CPU-light
- responsible for accepting events
- responsible for validation
- responsible for idempotency
- responsible for durable persistence
- responsible for fan-out
- responsible for creating outbox records

### Worker

The worker is:

- asynchronous
- I/O-bound
- responsible for HTTP delivery
- responsible for retries
- responsible for circuit breaking
- responsible for endpoint concurrency
- responsible for recording delivery attempts

API and worker are separate Kubernetes Deployments. They scale independently because they have
different workload characteristics.

---

## 5. Data Model

### endpoints

```text
endpoints
    id              uuid primary key
    tenant_id       uuid
    url             text
    secret          text
    event_types     text[]
    active          boolean
    max_concurrency int
    created_at      timestamptz
    updated_at      timestamptz
```

The endpoint URL is validated during registration and again immediately before delivery.
The secret is protected at rest and stored through Kubernetes Secrets in deployment.

### events

```text
events
    id              uuid primary key
    tenant_id       uuid
    event_type      text
    payload         jsonb
    idempotency_key text
    request_hash    text
    created_at      timestamptz

    unique (tenant_id, idempotency_key)
```

`request_hash` prevents the same idempotency key from being reused with a different request body.

### deliveries

One row exists for every:

```text
event × matching endpoint
```

```text
deliveries
    id              uuid primary key
    event_id        uuid foreign key
    endpoint_id     uuid foreign key
    status          enum(
                        PENDING,
                        IN_FLIGHT,
                        SUCCEEDED,
                        FAILED,
                        DEAD
                    )
    attempt_count   int
    next_attempt_at timestamptz
    last_error      text
    created_at      timestamptz
    updated_at      timestamptz
```

`deliveries.id` is the stable:

```text
X-HookRelay-Delivery-Id
```

### delivery_attempts

Append-only audit trail:

```text
delivery_attempts
    id              bigserial primary key
    delivery_id     uuid
    attempt_no      int
    started_at      timestamptz
    duration_ms     int
    response_status int nullable
    error_class     text nullable
    response_body   text
```

The retained response body is limited to 512 bytes.

### outbox

```text
outbox_events
    id              uuid primary key
    delivery_id     uuid
    created_at      timestamptz
    published_at    timestamptz nullable
    attempt_count   int
    last_error      text nullable
```

---

## 6. Transactional Outbox

The database and RabbitMQ must not be treated as two unrelated writes.

Incorrect:

```text
COMMIT PostgreSQL
      ↓
publish RabbitMQ
```

A crash between those operations can leave durable database state without a broker message.

Instead:

```text
BEGIN TRANSACTION

insert event
insert delivery rows
insert outbox rows

COMMIT
```

Then:

```text
Outbox Publisher
      ↓
RabbitMQ
```

If the publisher crashes, the outbox record remains and can be retried.

The outbox publisher is therefore itself at-least-once. Duplicate broker publication is acceptable
because delivery processing is designed to tolerate duplicates.

---

## 7. Ingest Idempotency

The client provides:

```http
Idempotency-Key: abc123
```

The database enforces:

```text
unique(tenant_id, idempotency_key)
```

This prevents two API instances racing to create duplicate events.

Behavior:

```text
same key + same request hash
    → return original event

same key + different request hash
    → 409 Conflict
```

The database constraint, rather than:

```text
if exists then insert
```

is responsible for concurrency correctness.

---

## 8. Subscription Semantics

Subscriptions are evaluated when an event is accepted.

Example:

```text
Event E
  │
  ├── Endpoint A → delivery
  ├── Endpoint B → delivery
  └── Endpoint C → delivery
```

Later changes to subscription state do not modify those existing delivery records.

Therefore:

- disabling an endpoint does not automatically cancel existing deliveries
- enabling an endpoint does not replay old events
- event replay is outside the core project

---

## 9. RabbitMQ

RabbitMQ provides asynchronous delivery work.

Main queue:

```text
deliveries
```

Retry queues:

```text
retry.5s
retry.30s
retry.2m
retry.10m
retry.30m
retry.1h
retry.3h
```

Dead-letter queue:

```text
deliveries.dlq
```

Workers use manual acknowledgements. Worker prefetch is bounded to prevent one worker from
reserving an excessive amount of work.

---

## 10. Retry Design

The retry schedule is:

```text
Attempt 1 → immediate
Attempt 2 → 5s
Attempt 3 → 30s
Attempt 4 → 2m
Attempt 5 → 10m
Attempt 6 → 30m
Attempt 7 → 1h
Attempt 8 → 3h
```

Each delay receives ±20% jitter.

The initial implementation deliberately uses a naive single delay queue with per-message TTL.
Then demonstrate the head-of-line problem.

Example:

```text
Queue:

[6 hour message]
[5 second message]
```

The second message can be delayed behind the first.

After measuring the problem, replace the design with tiered queues:

```text
retry.5s
retry.30s
retry.2m
retry.10m
retry.30m
retry.1h
retry.3h
```

The important outcome is the measured improvement.

---

## 11. Delivery Worker

The worker:

1. consumes a delivery message
2. loads the delivery record
3. checks whether it is already `SUCCEEDED`
4. acquires endpoint concurrency capacity
5. checks the circuit breaker
6. validates the destination
7. signs the request
8. performs the HTTP request
9. records the attempt
10. updates delivery state
11. ACKs RabbitMQ

If the worker crashes before ACK:

```text
RabbitMQ
   ↓
message becomes unacknowledged
   ↓
message is redelivered
```

If the delivery was already successfully persisted:

```text
SUCCEEDED
```

the worker skips the HTTP request and acknowledges the redelivered message.

---

## 12. HTTP Delivery

HTTP requests have bounded:

- connection timeout
- read timeout
- overall request timeout

Initial failure classification can remain simple:

```text
SUCCESS
RETRYABLE_FAILURE
PERMANENT_FAILURE
```

Typical retryable failures:

```text
timeouts
connection failures
DNS failures
5xx responses
```

Typical permanent failures can include certain 4xx responses.

The exact classification is documented during implementation and tested.

---

## 13. HMAC Signing

Each request is signed using HMAC-SHA256.

Header:

```text
X-HookRelay-Signature:
    t=<unix_seconds>,v1=<hex_signature>
```

The signed message is:

```text
timestamp + "." + raw_body
```

The timestamp prevents indefinite replay of captured requests.

Signature comparison uses constant-time comparison.

The receiver can validate:

```text
timestamp freshness
+
HMAC signature
```

---

## 14. SSRF Protection

Endpoint URLs are attacker-controlled. The worker therefore validates destinations before delivery.

Protection includes:

- loopback addresses
- private IP ranges
- link-local addresses
- metadata service addresses
- internal Kubernetes addresses
- other explicitly forbidden internal ranges

Validation happens:

```text
registration
+
delivery time
+
after DNS resolution
```

This protects against DNS rebinding.

Example:

```text
registration:
example.com → public IP

later:
example.com → private IP
```

The private destination must be rejected.

---

## 15. Redirect Protection

Automatic redirects are disabled by default.

If redirects are supported, every redirect destination must independently pass SSRF validation.

A redirect cannot bypass the SSRF protection.

---

## 16. Response Body Protection

Customer endpoints can return arbitrarily large responses. The worker must never buffer an
unbounded response body.

Only:

```text
512 bytes
```

are retained for diagnostics.

---

## 17. Endpoint Isolation

The core isolation mechanisms are intentionally simple.

### Per-endpoint concurrency

Each endpoint has:

```text
max_concurrency
```

Workers use a bounded semaphore for each endpoint. This prevents one endpoint from consuming all
worker concurrency.

For example:

```text
Endpoint A:
max_concurrency = 5

Endpoint B:
max_concurrency = 5
```

A slow A cannot consume B's available slots.

The initial implementation uses worker-local concurrency control. Global distributed coordination
is explicitly considered a future improvement rather than a core requirement.

### Circuit breaker

Each endpoint has a worker-local circuit breaker:

```text
CLOSED
OPEN
HALF_OPEN
```

Repeated failures move the endpoint to `OPEN`. After a cooldown, a probe is allowed in `HALF_OPEN`.
A successful probe returns the endpoint to `CLOSED`.

The circuit breaker prevents continuously hammering a failing endpoint.

---

## 18. Isolation Experiment

Create two endpoints:

```text
Endpoint A → intentionally slow / failing
Endpoint B → healthy
```

Run the same workload before and after isolation mechanisms.

Measure:

```text
Endpoint B p95 latency
Endpoint B p99 latency
successful deliveries
worker utilization
queue depth
```

Expected result:

```text
without isolation:
slow endpoint affects healthy endpoint

with isolation:
healthy endpoint remains responsive
```

This becomes one of the project's primary measurable results.

---

## 19. Kubernetes

Deploy:

```text
api Deployment
worker Deployment
```

Include:

- Docker images
- Kubernetes Deployments
- Services where necessary
- readiness probes
- liveness probes
- resource requests
- resource limits
- graceful shutdown
- rolling updates
- Kubernetes Secrets
- ConfigMaps for non-secret configuration

Do not put secrets in:

```text
source code
Dockerfile
Git
ConfigMap
container image
```

---

## 20. Graceful Shutdown

Worker shutdown:

```text
SIGTERM
   ↓
stop accepting new work
   ↓
finish or safely release current work
   ↓
persist state
   ↓
ACK completed messages
   ↓
terminate
```

If the process terminates before ACK:

```text
RabbitMQ redelivers the message
```

This behavior is tested explicitly.

---

## 21. Autoscaling

Workers are I/O-bound. CPU is therefore not necessarily a good representation of pending work.

Example:

```text
traffic spike
    ↓
queue depth: 200 → 50,000
    ↓
worker CPU: 8% → 11%
    ↓
CPU HPA sees little change
    ↓
backlog remains high
```

KEDA is used to scale workers based on RabbitMQ queue depth.

---

## 22. Autoscaling Experiment

Run the same workload using:

```text
Configuration A:
CPU-based HPA
```

and:

```text
Configuration B:
KEDA + RabbitMQ queue depth
```

Measure:

- queue depth
- worker replica count
- CPU utilization
- throughput
- p95 latency
- p99 latency
- backlog drain time

The goal is to demonstrate why queue depth is a better scaling signal for this workload.

---

## 23. Observability

Use:

```text
Micrometer
Prometheus
Grafana
structured JSON logs
```

Core metrics:

```text
hookrelay_deliveries_total{result}

hookrelay_delivery_duration_seconds

hookrelay_end_to_end_latency_seconds

hookrelay_queue_depth

hookrelay_attempts_total{attempt_no}

hookrelay_dlq_total

hookrelay_circuit_breaker_state

hookrelay_worker_inflight

hookrelay_ingest_latency_seconds

hookrelay_outbox_lag_seconds
```

Avoid high-cardinality Prometheus labels such as raw:

```text
endpoint_id
delivery_id
event_id
```

Keep those identifiers in structured logs.

---

## 24. End-to-End Latency

Track:

```text
event accepted
      ↓
database commit
      ↓
delivery attempt
      ↓
successful customer response
```

This is separate from individual HTTP attempt duration.

For example:

```text
attempt duration = 40ms
```

does not mean:

```text
end-to-end delivery latency = 40ms
```

if the delivery only succeeds on attempt 8.

Both metrics should be measured separately.

---

## 25. Load Testing

The load test must distinguish:

```text
events/sec
```

from:

```text
deliveries/sec
```

because one event can fan out to multiple endpoints.

Example:

```text
1,000 events/sec
×
10 average endpoints
=
10,000 deliveries/sec
```

Measure:

- events/sec
- deliveries/sec
- average fan-out
- p50 ingest latency
- p95 ingest latency
- p99 ingest latency
- p50 delivery latency
- p95 delivery latency
- p99 delivery latency
- queue depth
- worker replicas
- CPU
- memory
- success rate
- retry rate
- DLQ rate
- backlog drain time

---

## 26. CI/CD

GitHub Actions pipeline:

```text
git push
   ↓
run tests
   ↓
build application
   ↓
build Docker image
   ↓
push to GHCR
   ↓
deploy to Kubernetes
   ↓
smoke test
```

Demonstrate rollback:

```text
Version N
   ↓
Version N+1
   ↓
failure
   ↓
rollback
   ↓
Version N
```

---

## 27. Phase Plan

### Phase 0 — Blueprint

Build:

- architecture
- delivery contract
- data model
- failure model
- project scope

Deliverable:

```text
BLUEPRINT.md
```

### Phase 1 — Ingest API + PostgreSQL

Build:

- Spring Boot API
- event validation
- endpoint registration
- PostgreSQL
- event persistence
- fan-out
- idempotency
- request hash validation
- integration tests with Testcontainers

Demonstrate:

```text
POST /v1/events
    ↓
202
    ↓
durable event
    ↓
delivery records
```

### Phase 2 — Outbox + RabbitMQ

Build:

- transactional outbox
- outbox publisher
- RabbitMQ exchange
- delivery queue
- manual ACK

Demonstrate recovery from:

```text
database committed
but
RabbitMQ publication not completed
```

### Phase 3 — Worker + Delivery

Build:

- worker service
- HTTP client
- HMAC signing
- delivery state machine
- attempt recording
- stable delivery ID
- timeout handling
- successful delivery

Demonstrate real delivery to a local receiver.

### Phase 4 — Retry + DLQ

Build:

- retry classification
- exponential backoff
- jitter
- delay queues
- DLQ

First:

```text
naive delay queue
```

Measure head-of-line blocking.

Then:

```text
tiered delay queues
```

Measure improvement.

### Phase 5 — Isolation + Security

Build:

- per-endpoint concurrency
- circuit breaker
- timeout controls
- SSRF protection
- DNS rebinding protection
- redirect protection
- response body limits

Demonstrate:

```text
slow endpoint
    ↓
healthy endpoint remains responsive
```

Measure before/after p99 latency.

### Phase 6 — Observability

Build:

- Micrometer
- Prometheus
- Grafana
- structured JSON logging
- queue metrics
- latency metrics
- retry metrics
- DLQ metrics
- worker saturation metrics

Deliverable:

```text
operational dashboard
```

### Phase 7 — Docker + Kubernetes

Build:

- Docker images
- API Deployment
- Worker Deployment
- probes
- resource limits
- graceful shutdown
- rolling updates
- Secrets
- ConfigMaps

Demonstrate rolling deployment under load.

### Phase 8 — KEDA + Load Testing

Build:

- CPU HPA baseline
- KEDA
- RabbitMQ queue-depth scaling
- load generator

Run:

```text
1,000 events/sec
```

Compare:

```text
CPU HPA
vs
KEDA
```

### Phase 9 — CI/CD

Build:

- GitHub Actions
- automated tests
- Docker build
- GHCR push
- Kubernetes deployment
- smoke tests
- rollback

Demonstrate the complete pipeline.

### Phase 10 — Chaos + Results

Run:

1. worker crash
2. permanently failing destination
3. rolling deployment
4. traffic spike

Record all results in:

```text
RESULTS.md
```

---

## 28. Required Failure Demonstrations

### 28.1 Worker crash

Under load:

```text
kubectl delete pod
```

Expected:

```text
zero accepted events lost
messages redelivered
successful deliveries remain successful
stable delivery IDs preserved
```

### 28.2 Destination permanently down

Expected:

```text
bounded retries
    ↓
final DEAD state
    ↓
DLQ
```

Record:

- attempt timestamps
- attempt number
- error
- final DLQ reason

### 28.3 Rolling deployment

Deploy a new version while processing traffic.

Expected:

```text
zero silently lost accepted events
zero silently dropped deliveries
```

### 28.4 Traffic spike

Generate approximately:

```text
1,000 events/sec
```

Observe:

```text
queue depth
worker replicas
latency
throughput
backlog drain
```

Verify KEDA responds to backlog growth.

---

## 29. RESULTS.md

The final project should emphasize measurements rather than technology lists.

Example:

```text
## CPU HPA vs KEDA

| Metric | CPU HPA | KEDA |
|---|---:|---:|
| Peak queue depth | ... | ... |
| Peak replicas | ... | ... |
| p99 latency | ... | ... |
| Backlog drain time | ... | ... |
```

Isolation:

```text
## Endpoint Isolation

Healthy endpoint p99 before isolation: ...
Healthy endpoint p99 after isolation: ...
```

Crash recovery:

```text
## Worker Crash

Events accepted: ...
Deliveries created: ...
Successful: ...
Lost: ...
Redelivered: ...
DLQ: ...
```

Every result should include:

- workload
- environment
- command
- measurement
- interpretation

---

## 30. Documentation

### BLUEPRINT.md

Contains:

- architecture
- contracts
- design decisions
- scope
- phase plan

### docs/phaseNN-*.md

Written before each phase's implementation.

Each document explains:

1. concepts
2. problem
3. design options
4. chosen design
5. implementation
6. experiment
7. failures
8. lessons learned

### REFERENCE.md

Contains:

- API reference
- configuration
- topology
- operational commands

### RESULTS.md

Contains reproducible measurements.

### README.md

Contains:

- problem
- architecture
- key design decisions
- technology stack
- setup
- experiments
- measured results

---

## 31. Repository Structure

```text
hookrelay/
│
├── api/
│   └── ...
│
├── worker/
│   └── ...
│
├── infra/
│   ├── docker/
│   ├── kubernetes/
│   ├── rabbitmq/
│   └── prometheus/
│
├── load-tests/
│   └── ...
│
├── chaos/
│   └── ...
│
├── docs/
│   ├── phase01-ingest.md
│   ├── phase02-outbox-rabbitmq.md
│   ├── phase03-worker.md
│   ├── phase04-retries.md
│   ├── phase05-isolation-security.md
│   ├── phase06-observability.md
│   ├── phase07-kubernetes.md
│   ├── phase08-autoscaling.md
│   ├── phase09-cicd.md
│   └── phase10-chaos.md
│
├── BLUEPRINT.md
├── RESULTS.md
├── REFERENCE.md
└── README.md
```

---

## 32. Optional Future Improvements

Only consider these after the core project is complete:

- Redis-based distributed endpoint concurrency
- Redis-based distributed circuit breaker
- OpenTelemetry tracing
- event replay
- customer-facing dashboard
- configurable retry policies
- tenant quotas
- advanced authentication
- multi-region deployment

These are not required for the project's four main resume stories.

---

## 33. Four Resume-Level Outcomes

The project should ultimately produce four strong engineering stories.

### 1. Reliable Distributed Delivery

Demonstrate:

```text
Spring Boot
+
PostgreSQL
+
transactional outbox
+
RabbitMQ
+
worker pool
+
idempotency
+
at-least-once delivery
```

### 2. Failure Handling and Isolation

Demonstrate:

```text
retries
+
exponential backoff
+
jitter
+
DLQ
+
endpoint concurrency
+
circuit breaker
+
SSRF protection
```

with measured noisy-neighbor behavior.

### 3. Kubernetes and Autoscaling

Demonstrate:

```text
Docker
+
Kubernetes
+
separate API/worker deployments
+
KEDA
+
Prometheus/Grafana
```

with a CPU-HPA vs queue-depth experiment.

### 4. Performance and Chaos Engineering

Demonstrate:

```text
1,000 events/sec
+
p95/p99 measurements
+
worker crash
+
rolling deployment
+
destination failure
+
traffic spike
```

with reproducible results.

---

## 34. Definition of Done

The project is complete when:

- [ ] Events are durably persisted before 202 is returned.
- [ ] Duplicate submissions are prevented with database-enforced idempotency.
- [ ] Idempotency-key payload mismatches are rejected.
- [ ] Event fan-out is durable.
- [ ] Transactional outbox prevents DB/message-broker dual-write loss.
- [ ] RabbitMQ provides asynchronous delivery.
- [ ] Workers perform signed HTTP delivery.
- [ ] Delivery IDs remain stable across retries.
- [ ] Worker crashes result in safe message redelivery.
- [ ] Retries use bounded exponential backoff and jitter.
- [ ] Head-of-line blocking is measured and fixed.
- [ ] Failed deliveries enter the DLQ.
- [ ] Endpoint concurrency is isolated.
- [ ] Circuit breakers protect failing endpoints.
- [ ] SSRF protection works at registration and delivery time.
- [ ] DNS rebinding is considered.
- [ ] Redirects cannot bypass SSRF protection.
- [ ] Response bodies are bounded.
- [ ] HTTP timeouts are bounded.
- [ ] Prometheus metrics exist.
- [ ] Grafana dashboard exists.
- [ ] Structured logs correlate deliveries.
- [ ] API and worker run as separate Kubernetes Deployments.
- [ ] Probes and resource limits are configured.
- [ ] Graceful shutdown works.
- [ ] Rolling deployment is demonstrated.
- [ ] CPU HPA is compared against KEDA.
- [ ] Queue-depth autoscaling is demonstrated.
- [ ] Load testing reaches approximately 1,000 events/sec.
- [ ] Events/sec and deliveries/sec are reported separately.
- [ ] p50/p95/p99 latency is measured.
- [ ] GitHub Actions performs CI/CD.
- [ ] Images are published to GHCR.
- [ ] Rollback is demonstrated.
- [ ] Worker crash experiment passes.
- [ ] Permanent destination failure experiment passes.
- [ ] Rolling deployment experiment passes.
- [ ] Traffic spike experiment passes.
- [ ] RESULTS.md contains reproducible measurements.
- [ ] Each phase has a learning document.

---

## 35. Final Scope Rule

Do not add technology simply because it appears in a production architecture.

Every component must answer a concrete question:

```text
What problem does this solve?
How will I demonstrate that it solves it?
What will I measure?
```

If the answer is unclear, it does not belong in the core project.

The project should remain a focused webhook delivery system with four excellent engineering
stories, rather than becoming an attempt to recreate a commercial webhook platform.

# HookRelay — Measured Results

Every entry records the workload, the environment, the exact command, the measurement, and what it
means. Numbers that have not been measured yet are absent rather than estimated.

**Environment (all results below unless stated otherwise)**

| | |
|---|---|
| Machine | Apple Silicon Mac, macOS (Darwin 25.3.0) |
| JDK | OpenJDK 21.0.9 (Homebrew) |
| Maven | 3.9.11 |
| PostgreSQL | 16-alpine, via Testcontainers |
| RabbitMQ | 3.13-management-alpine, via Testcontainers |
| Docker | 29.1.3 |
| Spring Boot | 3.5.9 |

> Maven's own JVM on this machine is JDK 26, so every build below pins the toolchain explicitly
> with `JAVA_HOME=$(/usr/libexec/java_home -v 21)`.

---

## Phase 1 — Ingest API

### 1.1 Test suite

**Workload:** full suite — 12 unit tests in `common`, 25 integration tests in `api` against a real
PostgreSQL 16 container.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 12 | 0 | 0 |
| `hookrelay-api` | 25 | 0 | 0 |
| **Total** | **37** | **0** | **0** |

---

### 1.2 Does the unique constraint actually matter?

**Question.** Ingest idempotency is enforced by a database constraint rather than an
application-level existence check. Does that choice change the observable outcome, or is it
stylistic?

**Workload.** 20 threads submit the byte-identical request with the same `Idempotency-Key`,
released simultaneously from a `CountDownLatch`, against a real PostgreSQL. Two endpoints are
registered, so a correct fan-out produces exactly 2 delivery rows.

**Configurations.**

- **A — check-then-act.** `events_idempotency_uq` removed from `V1__init.sql` (replaced with a
  non-unique index), and `IngestService.ingest` given a pre-check:
  `if (findByTenantIdAndIdempotencyKey(...).isPresent()) return duplicate;`
- **B — as shipped.** Unique constraint present, no pre-check, violation caught and resolved.

**Result.**

| Measurement | A: check-then-act | B: unique constraint | Correct |
|---|---:|---:|---:|
| Event rows created | **16** | **1** | 1 |
| Delivery rows created | **32** | **2** | 2 |
| HTTP 202 (created) | 16 | 1 | 1 |
| HTTP 200 (duplicate observed) | 0 | 19 | 19 |
| HTTP 5xx | **4** | **0** | 0 |
| Distinct event ids returned to callers | 16 | 1 | 1 |

**Interpretation.**

Sixteen of twenty threads passed the existence check before any of them had committed. The
consequence is not a cosmetic duplicate row: fan-out ran 16 times, so a customer endpoint would
receive the same webhook 16 times, and 16 different `event_id`s were returned to callers who each
believe theirs is authoritative.

The 4 HTTP 5xx responses were not failed writes — they were failed *reads*:

```
NonUniqueResultException: Query did not return a unique result: 9 results were returned
```

`EventRepository.findByTenantIdAndIdempotencyKey` returns `Optional<Event>`, whose contract is "at
most one" — a contract that was only ever true because of the constraint. Once duplicates exist,
the lookup that is supposed to *recover* from a duplicate submission throws instead. The corruption
disables its own remedy.

**Reproduce.** Revert the two files as described above, then:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api -Dtest=IngestIntegrationTest#concurrentIdenticalSubmissionsCreateExactlyOneEvent -Dsurefire.failIfNoSpecifiedTests=false test
```

In the shipped configuration this scenario is a permanent regression test:
`IngestIntegrationTest#concurrentIdenticalSubmissionsCreateExactlyOneEvent`.

---

## Phase 2 — Transactional outbox + RabbitMQ

### 2.1 Test suite

**Workload:** full suite — 12 unit tests in `common`, 38 integration tests in `api` against a real
PostgreSQL 16 and a real RabbitMQ 3.13.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 12 | 0 | 0 |
| `hookrelay-api` | 38 | 0 | 0 |
| **Total** | **50** | **0** | **0** |

---

### 2.2 Does the outbox actually save anything?

**Question.** BLUEPRINT.md §27 requires a demonstration of recovery from "database committed but
RabbitMQ publication not completed". Does the outbox change the observable outcome versus committing
and then publishing directly?

#### First attempt — and why it proved nothing

**Workload.** 10 events, one matching endpoint, broker made unavailable by pausing its container
(`docker pause`, which preserves the port mapping and the established TCP connection).

| Measurement | Dual write, broker paused |
|---|---:|
| Events accepted | 10 |
| Deliveries created | 10 |
| Messages on the queue after recovery | **10** |
| Deliveries never queued | **0** |

**Interpretation.** The dual-write implementation lost nothing, so the experiment was wrong.

A paused broker does not refuse writes: `basic.publish` writes into the TCP socket, the bytes wait
in the kernel buffer, and `waitForConfirms` merely times out because no ack comes back. On unpause
the broker reads the buffer and queues every message. The application logged **ten "publish failed,
message lost" errors for ten messages that were delivered.**

A confirm timeout does not mean the message was not delivered — it means you do not know. That is
the same ambiguity that makes exactly-once impossible on the receiving side, one hop earlier.
Pausing reproduces "the broker is slow", which buffering and confirms already handle. It is not the
failure the outbox exists for.

#### Second attempt — the crash window

**Workload.** Identical, but the injection is now the gap the outbox actually closes: publishing is
impossible for the duration of the outage (the process is dead), and both configurations then get a
recovery attempt standing in for a restarted process or a surviving replica. Publisher timer off in
both runs, so recovery is explicit and the result does not depend on timing.

| Measurement | A: dual write | B: outbox | Correct |
|---|---:|---:|---:|
| Events accepted (202) | 10 | 10 | 10 |
| Deliveries created | 10 | 10 | 10 |
| Durable record of an unperformed publish | **none** | **10 rows** | — |
| Messages on the queue after recovery | **0** | **10** | 10 |
| Deliveries silently never queued | **10** | **0** | 0 |

**Interpretation.** All ten deliveries in configuration A are permanently invisible: the event row
exists, the delivery row sits in `PENDING`, and nothing will ever act on it or report it. That is
exactly the silent loss of accepted work the delivery contract forbids. Configuration B recovers all
ten on the first poll after restart, because the intent to publish was committed alongside the data.

**Reproduce.** The permanent regression tests are `OutboxRecoveryTest#survivesBrokerOutage` (broker
outage against the real timer-driven publisher) and
`OutboxIntegrationTest#concurrentPollersDoNotDuplicate` (the `SKIP LOCKED` property):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api -Dtest=OutboxRecoveryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

### 2.3 Concurrent pollers

**Workload.** 20 unpublished outbox rows, 4 threads calling `publishBatch()` simultaneously.

| Measurement | Value | Correct |
|---|---:|---:|
| Rows published, summed across pollers | 20 | 20 |
| Messages on the queue | 20 | 20 |
| Duplicate delivery ids on the queue | 0 | 0 |
| Rows left unpublished | 0 | 0 |

**Interpretation.** `FOR UPDATE SKIP LOCKED` lets several pollers drain one table in parallel
without publishing anything twice and without serialising behind each other's locks. This is what
makes running the publisher inside a multi-replica API safe.

---

## Phase 3 — Delivery worker

### 3.1 Test suite

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 22 | 0 | 0 |
| `hookrelay-api` | 38 | 0 | 0 |
| `hookrelay-worker` | 22 | 0 | 0 |
| **Total** | **82** | **0** | **0** |

Plus an independent Python implementation of signature verification, pinned to the same golden
vector as the Java tests:

```bash
python3 tools/webhook_receiver.py --selftest
```

---

### 3.2 What does a worker crash actually cost?

**Question.** The redelivery window between the customer's HTTP 200 and the broker acknowledgement
cannot be closed by reordering. Two mechanisms handle it — a worker-side `SUCCEEDED` check and the
receiver-side delivery-id contract. Do they cover the same crashes or different ones?

**Workload.** 10 deliveries against a real HTTP receiver. Every delivery's first processing crashes;
the broker redelivers; the run continues until all 10 are `SUCCEEDED` and the queue is empty. The
configurations differ only in where the crash lands relative to the database commit.

| Measurement | A: crash **after** commit, before ack | B: crash **before** commit |
|---|---:|---:|
| Deliveries dispatched | 10 | 10 |
| Simulated crashes | 10 | 10 |
| HTTP requests the customer received | **10** | **20** |
| Distinct delivery ids seen | 10 | 10 |
| **Duplicate HTTP calls** | **0** | **10** |
| Attempt rows recorded | 10 | **10** |
| Deliveries lost | **0** | **0** |
| Messages left on the queue | 0 | 0 |

**Interpretation.**

Nothing is lost in either configuration — that guarantee comes from manual acknowledgement, and it
holds regardless of where the crash lands.

The two mechanisms cover **disjoint** cases. In A the redelivered message finds the delivery already
`SUCCEEDED`, the claim is refused, and the customer is never called twice. In B the database has no
record that the call happened, so the redelivery legitimately re-attempts and the customer receives
the identical webhook again — 10 duplicates out of 10. Nothing server-side prevents that; the only
thing that saves the receiver is that both requests carry the same `X-HookRelay-Delivery-Id`. This
is the third appearance of the same ambiguity, after phase 1's lost HTTP response and phase 2's
confirm timeout.

**Unplanned finding: `delivery_attempts` undercounts real HTTP calls.** Configuration B made 20
requests and recorded 10 attempt rows, because an attempt that crashes before its row is written
leaves no trace. The audit trail is "attempts we know completed", not "requests the customer
received" — a lower bound, which matters before phase 10 uses it to reason about a chaos run.

**Reproduce.** The permanent regression tests are
`DeliveryListenerTest#redeliveryDoesNotCallTwice` (configuration A's property, through the real
broker) and `DeliveryProcessorTest#alreadySucceededIsSkipped`:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=DeliveryListenerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## Phase 4 — Retry, backoff and the DLQ

### 4.1 Test suite

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 31 | 0 | 0 |
| `hookrelay-api` | 38 | 0 | 0 |
| `hookrelay-worker` | 36 | 0 | 0 |
| **Total** | **105** | **0** | **0** |

---

### 4.2 How bad is head-of-line blocking, really?

**Question.** Exponential backoff needs delays spanning seconds to hours. Does putting them all in
one RabbitMQ delay queue with per-message TTLs actually break, and by how much?

**Workload.** Publish a long-delay message, then immediately a short-delay one, and measure when the
short one is released. Two topologies, identical otherwise: one shared delay queue versus one queue
per delay bucket. Both dead-letter into the same landing queue, whose arrivals are timestamped.
Nominal delays: long 6000 ms, short 400 ms.

| Measurement | Naive: one shared queue | Tiered: one queue per bucket |
|---|---:|---:|
| Short retry's nominal delay | 400 ms | 400 ms |
| **Short retry's actual delay** | **6024 ms** | **405 ms** |
| Blocking attributable to the long message | **5624 ms** | **5 ms** |
| Overshoot factor | **15.1×** | **1.01×** |
| Long retry's actual delay | 6021 ms | 6007 ms |

**Interpretation.** RabbitMQ only inspects the message at the **head** of a queue for expiry, because
checking every message would mean scanning the queue continuously. The short retry's TTL elapsed
after 400 ms, but it could not expire until it reached the head, and it could not reach the head
until the message ahead of it expired. Its own TTL was irrelevant.

Scaled to the real schedule, whose longest tier is three hours rather than six seconds, one customer
stuck on the 3h tier would hold every five-second retry behind it for three hours — converting a
graceful backoff curve into a system where one slow endpoint stalls everyone's retries.

### 4.3 Bounded blocking within a tier

**Question.** Jitter needs per-message TTLs, which reintroduces head-of-line blocking inside a tier.
How much?

**Workload.** Two messages in the same queue with TTLs at the top and bottom of a 1000 ms tier's
±20% jitter range, the slower one published first (the worst case).

| Measurement | Value |
|---|---:|
| Faster message's nominal delay | 800 ms |
| Faster message's actual delay | 1207 ms |
| **Blocking** | **407 ms** |
| Predicted bound (jitter spread `1200 − 800`) | 400 ms |

**Interpretation.** 407 ms against a predicted 400 ms; the extra 7 ms is broker overhead. Head-of-line
blocking is **not eliminated** — it is reduced from the full range of the schedule to the width of one
tier's jitter. On the 5s tier that is about two seconds of skew, against hours for the naive design.

**Reproduce.** `DelayQueueHeadOfLineTest` declares both topologies itself:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=DelayQueueHeadOfLineTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## Phase 5 — Endpoint isolation and destination security

### 5.1 Test suite

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 70 | 0 | 0 |
| `hookrelay-api` | 43 | 0 | 0 |
| `hookrelay-worker` | 57 | 0 | 0 |
| **Total** | **170** | **0** | **0** |

---

### 5.2 Does a slow endpoint starve a healthy one?

**Question.** One customer's endpoint goes slow. How much does that cost every other customer, and
how much of it does a per-endpoint concurrency limit recover?

**Workload.** Two endpoints share the worker pool (4 listener threads, prefetch 8). The slow one takes
2 s per request and **succeeds**, so the circuit breaker stays closed and concurrency is the only
variable; the healthy one answers instantly. A backlog of 24 slow deliveries is published first, then
10 healthy ones. Latency is measured from publishing the healthy batch to the request arriving.

The baseline is not a feature flag — an endpoint with `max_concurrency = 10 000` has a semaphore that
never blocks, which *is* the unisolated behaviour, so both arms run the shipped code path.

**Result** (representative run; range across three runs in brackets):

| Measurement | Without isolation | With isolation | Improvement |
|---|---:|---:|---:|
| Healthy endpoint **p50** | **12 644 ms** [12.3–16.3 s] | **72 ms** [63–192 ms] | **176×** |
| Healthy endpoint p95 | 12 661 ms | 2 067 ms | 6.1× |
| Healthy endpoint **p99** | **12 661 ms** | **2 067 ms** [2.07–2.11 s] | **6.1×** |

**Interpretation.**

The unisolated number is exactly what the mechanism predicts: 24 slow requests × 2 s ÷ 4 worker
threads = 12 s. Every healthy delivery waited behind the slow endpoint's backlog in the consumers'
prefetch buffers. Nothing failed and nothing was logged — every metric except latency looked normal,
which is what makes this failure mode worth engineering against.

The residual 2 s at p99 with isolation is **not noise and not fixable by this mechanism**. A semaphore
stops a healthy delivery *queueing* behind the slow endpoint, but cannot preempt a slow request
already in flight on the thread that then picks the healthy one up. One in-flight 2-second request is
the worst case, and 2 067 ms is that worst case; p50 of 72 ms shows the common case is unaffected.

**Reproduce — standalone, not inside the full suite:**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=NoisyNeighbourTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Inside the full suite the unisolated arm reports p50 ≈ 3 200 ms instead of ≈ 12 600 ms. That is
contamination, not a better result: Spring's test-context cache keeps every worker test class's
application alive for the whole JVM, each with its own listener container consuming the same
`deliveries` queue, so the effective pool is several times four threads.

**Two earlier versions of this experiment were wrong**, both documented in
docs/phase05-isolation-security.md §7: publishing the two endpoints' deliveries interleaved let
RabbitMQ's round-robin partition the work across consumers so the baseline never starved (and
isolation looked *worse* at the tail); and the test HTTP server's default executor is single-threaded,
which inflated the baseline to ~44 s for reasons unrelated to worker starvation.

---

## Phase 6 — Observability

### 6.1 Test suite

| Module | Tests | Failures | Errors |
|---|---:|---:|---:|
| `hookrelay-common` | 70 | 0 | 0 |
| `hookrelay-api` | 43 | 0 | 0 |
| `hookrelay-worker` | 68 | 0 | 0 |
| **Total** | **181** | **0** | **0** |

---

### 6.2 What does one high-cardinality label cost?

**Question.** BLUEPRINT.md §23 forbids `endpoint_id` as a metric label. What is the actual cost of
breaking that rule?

**Workload.** Two real `PrometheusMeterRegistry` instances, the same counter, the same traffic:
50 endpoints × 3 results. One tagged `{result}`, the other `{result, endpoint_id}`. Both rendered
exactly as Prometheus would scrape them.

| Measurement | `{result}` | `{result, endpoint_id}` | Factor |
|---|---:|---:|---:|
| Time series | **3** | **150** | **50×** |
| Scrape payload | 232 B | 15 278 B | **66×** |
| Bytes per series | — | 101 B | |

**Extrapolated to 10 000 endpoints** at the measured 101 bytes per series:

| | Series | Payload per scrape |
|---|---:|---:|
| `{result}` | **3** | 232 B |
| `{result, endpoint_id}` | **30 000** | **2.9 MB** |

**Interpretation.** At a 5-second scrape interval that is ~35 MB/min of scrape traffic from **one
counter**, growing every time a customer registers an endpoint — and multiplying again if
`event_type` is added. The bounded version costs 3 series and 232 bytes regardless of customer
count: the metric's size is a function of the design, not of business success.

This is why identifiers live in structured logs instead. Metrics answer *how much and how bad*; logs
answer *which one*. `MetricsIntegrationTest#noHighCardinalityLabels` scans every registered
`hookrelay*` meter and fails if a label value looks like a UUID, so the rule is enforced rather than
merely documented.

**Reproduce:**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=CardinalityCostTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## Phase 7 — Containers and Kubernetes

Run on minikube v1.38.1 (single node, 10 CPU / 12 GB allocatable), Kubernetes client v1.36.0.

### 7.1 Images and build

| Measurement | Value |
|---|---:|
| `hookrelay-api:dev` | **270 MB** |
| `hookrelay-worker:dev` | **268 MB** |
| Build + deploy, first run — separate Dockerfiles, no cache | **~33 min** |
| Build + deploy, cached — shared builder + BuildKit `~/.m2` cache mount | **60 s** |

The two original Dockerfiles each ran `dependency:go-offline` with a different `-pl` list, so Docker
saw different commands and the entire dependency tree downloaded twice.

### 7.2 Smoke test

```bash
./infra/kubernetes/smoke.sh
```

Registers an endpoint pointing at the in-cluster receiver, publishes an event, waits for delivery,
and checks the receiver's counters. The receiver verifies HMAC signatures independently, so this also
proves the signing contract end to end inside the cluster:

```
{"received": 1, "verified": 1, "rejected": 0, "duplicates": 0}
SMOKE PASSED
```

---

### 7.3 Rolling deployment under load — BLUEPRINT.md §28.3

**Question.** What does a rolling deployment cost in dropped requests and lost deliveries, and does
the `preStop` hook measurably help?

**Workload.** A load generator pod inside the cluster drives the `api` Service for 70 s; 15 s in,
both Deployments are rolling-restarted underneath it. Every request carries a unique
`Idempotency-Key`, so a failure is real rather than a deduplicated retry. Afterwards the database is
queried for deliveries that never reached a terminal state.

| | with `preStop` | without `preStop` |
|---|---:|---:|
| **Keep-alive** — sent / failed | 2 367 / **0** | 3 620 / **0** |
| **Connection per request** — sent / failed | 3 713 / **0** | 3 537 / **0** |
| Rollout duration | 78–103 s | 84–99 s |

Across all four arms: **13 237 requests, 0 failed. 13 237 deliveries created, 13 237 succeeded, 0
lost, 0 dead.**

**Interpretation — this is a null result for `preStop`, not a validation.** The hook covers the
endpoint-propagation race (docs/phase07-kubernetes.md §1.6), and the experiment **did not reproduce
that race at all**: the baseline without the hook was equally clean. The first run used keep-alive,
which was a fair objection since the race affects *new* connections; both arms were re-run with a
fresh connection per request, and still measured zero.

The window stays closed here because the cluster is **single-node** (one kube-proxy, one iptables
table, sub-second propagation), `maxUnavailable: 0` keeps a Ready pod available throughout,
`server.shutdown: graceful` finishes in-flight requests, and ~50 rps produces few new connections
inside a sub-second window. The hook is retained as a judgement about multi-node production
clusters, where the race widens — **not** because this data shows it helping. It costs ~8 s per pod.

---

### 7.4 A silent-loss bug found by the experiment

The **first** run reported 0 failed requests but **3 of 1578 deliveries stranded** in `PENDING`
permanently — `attempt_count = 0`, outbox row *published*, every queue empty including
unacknowledged. A message had been published, confirmed, consumed, acknowledged, and the delivery had
evaporated.

**Cause.** `RetryPublisher`'s defer, retry and dead-letter publishes used `convertAndSend` —
fire-and-forget, no confirm — and `DeliveryListener` acknowledges the original message the instant
that returns. A defer publish lost during a worker shutdown left the delivery with no queue message,
no error, and no way back. The worker's configuration had not even enabled `publisher-confirm-type`.

This violates the contract's first rule: no silent loss of accepted work. The outbox publisher had
waited for broker confirms since phase 2 for exactly this reason; the discipline was never carried
across to the component that needed it equally.

**Fix.** All three publishes now wait for a confirm and **throw** on failure, so the listener
negatively acknowledges and the message is redelivered — a possible duplicate instead of a
guaranteed loss.

| | before fix | after fix |
|---|---:|---:|
| Deliveries stranded in `PENDING` | **3 of 1 578** | **0 of 5 987** |

Pinned by `PublishFailureTest` (4 tests), including one that asserts an unconfirmable publish throws
rather than returning quietly.

---

## Phase 8 — Autoscaling and load testing

minikube v1.38.1 (single node, 10 CPU / 12 GB), KEDA v2.15.1, metrics-server enabled.

### 8.1 Can CPU see a backlog?

**Question.** Does CPU-based autoscaling react to a delivery backlog, and does queue depth?

**Workload.** A slow endpoint (300 ms per request), three registered endpoints so fan-out is 3, load
for 5 s. Identical bounds (`min 2`, `max 6`) and identical scaling policies in both arms — KEDA
creates an ordinary HPA underneath, so only the **signal** differs.

| | CPU HPA | KEDA queue depth |
|---|---:|---:|
| Peak queue depth | 1 047 | 2 587 |
| **Peak worker replicas** | **2 — never scaled** | **6** |
| Worker CPU (100m request) | **28m, flat** | 71–412m |
| Events accepted | 355 | 872 |
| Deliveries created | 1 065 | 2 616 |
| Average fan-out | 3.00 | 3.00 |

CPU arm time series:

```text
   elapsed  depth   replicas  cpu(m)     target = 50% of a 100m request
     0          0      2       28
    10      1,047      2       28
    15      1,026      2       28
    21          4      2       28
```

**A thousand messages queued and the signal did not move** — 28% of the request, under the 50%
target, so the autoscaler had nothing to react to.

### 8.2 The stronger case: a more I/O-bound worker

An earlier run with a **1 second** endpoint delay and heavier load:

```text
   elapsed  depth     replicas  cpu(m)
     0           0       2       13
    24      16,204       2       13
    95      73,228       2       43
   181      73,618       3       71
```

**73,000 messages of backlog, CPU at 43 millicores, two replicas for three minutes.** The more time a
worker spends waiting on a socket, the less CPU says about how much work is waiting.

**Why this is not a tuning problem.** The target was already 50% of a 100m request — 50 millicores,
stricter than any production setting. Lowering it further scales on noise. The relationship between
CPU and pending work is weak for I/O-bound services, and a weak relationship cannot be strengthened
by moving a threshold.

**The feedback loop it creates:** few workers → each modestly loaded → CPU low → no scale-up → few
workers. CPU utilisation is a *consequence* of how many workers you have. Queue depth measures the
*cause* — work waiting — and is immune.

### 8.3 Throughput — BLUEPRINT.md §25

Measured separately with a fast endpoint, since throughput and backlog behaviour are different
questions:

| Measurement | Value |
|---|---:|
| Ingest | **268 events/sec** |
| Average fan-out | **3.00** |
| Deliveries created | **~805 deliveries/sec** |
| Ingest p50 / p95 / p99 | **9.7 / 94.9 / 192.3 ms** |
| Requests failed | 0 of 5 364 |

Reporting a single "throughput" number would hide which side of the system a bottleneck is on, and
make capacity planning wrong by the fan-out factor.

### 8.4 What this does not show

**Backlog drain time is deliberately not reported.** Two disqualifying reasons:

1. **The arms did not receive identical workloads.** Same load-generator settings, but achieved
   ingest differed (71 vs 174 events/sec), so KEDA faced 2 587 messages against CPU's 1 047. A
   drain-time difference across different workloads measures nothing.
2. **The receiver saturated.** With 6 workers the in-cluster Python receiver became the bottleneck,
   so drain time reflects the test endpoint's capacity, not the worker pool's.

Doing it properly needs a destination that scales past the worker pool and a fixed event count rather
than a fixed duration. That is not done here.

**Reproduce:**

```bash
./chaos/autoscaling.sh cpu
```

```bash
./chaos/autoscaling.sh keda
```

---

## Phase 9 — CI/CD

### 9.1 Pipeline

`.github/workflows/ci.yml` runs on every push and pull request:

```text
   job: test      JDK 21, full Testcontainers suite, plus the Python receiver's golden-vector selftest
   job: images    needs: test — builds both targets, pushes to GHCR tagged sha-<commit>
```

Images are tagged by commit SHA; `latest` is a convenience pointer and is never what a deployment
references, because it means something different tomorrow and so cannot express "the artifact we
tested".

**The pipeline stops at the registry, deliberately.** GitHub-hosted runners cannot reach a minikube
cluster on this laptop, so deployment is a local script. Closing that gap needs a self-hosted runner,
a reachable API server with a kubeconfig secret, or a pull-based deployer — and Argo CD is an explicit
non-goal (BLUEPRINT.md §2). A `deploy` job that could never run would be worse than the admission.

---

### 9.2 A broken deployment under load — BLUEPRINT.md §26

**Question.** What does deploying a broken version cost, and what actually protects against it?

**Workload.** Continuous load; mid-flight, deploy a version that starts normally but points its
datasource at a nonexistent host, so the process runs and readiness never passes. Chosen because a
crash-looping container is the easy case — a process that is up and useless is the one that reaches
production.

| Measurement | Value |
|---|---:|
| Requests sent during the bad deployment | **8 183** |
| **Requests failed** | **0** |
| Ingest p50 / p95 / p99 | 51.4 / 299.6 / 682.5 ms |
| `kubectl rollout status` exit code | **1** — did not complete |
| Rollout stalled before intervention | **46 s** |
| Ready replicas throughout | **2** — old version kept serving |
| Broken pods admitted to the Service | **0** |
| Recovery after `rollout undo` | **2 s** |
| Events accepted / deliveries succeeded | 8 183 / **8 183** |

**Interpretation.** The rollback is the least important part. Safety came from the **readiness
probe** never admitting the broken pods to the Service, and `maxUnavailable: 0` forbidding removal of
a healthy pod before a new one was Ready — so capacity never dropped and the rollout simply refused
to finish. A deployment that cannot become Ready is not an outage; it is a deployment that did not
happen. `rollout undo` took 2 seconds and was cleanup.

The non-zero exit from `rollout status` is the interface a CD job gates on, which is why the number
worth quoting is the 46 seconds of *safe* stall rather than any recovery time.

**Reproduce:**

```bash
./chaos/rollback.sh
```

---

## Phase 10 — Chaos: the four required demonstrations

BLUEPRINT.md §28 requires four failure scenarios. All four are measured; two were run in this phase
and two in earlier ones.

### 10.1 Worker crash — §28.1

**Workload.** Three workers under load. One deleted gracefully (SIGTERM, grace period), then a
*different* one force-killed (`--force --grace-period=0`, SIGKILL, no shutdown hook).

| Measurement | Value |
|---|---:|
| Requests sent / failed | 2 866 / **0** |
| Deliveries created | 2 866 |
| **Deliveries succeeded** | **2 866** |
| **Deliveries unfinished** | **0** |
| Total attempts | **2 867** — one more than deliveries |
| Receiver verified / rejected | 7 145 / **0** |

**Interpretation.** Nothing lost across a graceful kill and a SIGKILL. The single extra attempt *is*
the redelivery: one delivery was in flight when its worker died, the broker never got an
acknowledgement, another worker took it. Manual acknowledgement, visible as a difference of one.

```bash
./chaos/worker-crash.sh
```

### 10.2 Destination permanently down — §28.2

**Workload.** Receiver answering 500 to everything; the delivery fast-forwarded to the end of the
ladder rather than waiting 4h42m.

| Measurement | Value |
|---|---|
| After attempt 1 | `FAILED`, `last_error = HTTP 500`, retry queued, `next_attempt_at` set |
| **Final status** | **`DEAD`** |
| **Final attempt count** | **8** — the cap |
| Attempt response codes | `500,500` |
| **DLQ depth before / after** | **0 → 1** |

Bounded retries, a terminal state, a dead-lettered message carrying its reason.

```bash
./chaos/destination-down.sh
```

### 10.3 Rolling deployment — §28.3

Phase 7: **13 237 requests across four rolling restarts, 0 dropped, 0 deliveries lost** — see §7.3.

### 10.4 Traffic spike — §28.4

Phase 8: a **73 228-message backlog** absorbed with no loss; queue-depth scaling **2 → 6** where CPU
never moved off 2 — see §8.1–8.2.

---

### 10.5 A probe that manufactured the outage it was watching for

The first clean run of §10.1 reported **824 deliveries stranded in `PENDING`**, `attempt_count = 0`,
outbox rows published, all queues empty — the exact signature of the phase 7 silent-loss bug, which
had already been fixed and regression-tested.

It was not the application. **RabbitMQ restarted three times during the run.** Its liveness probe ran
`rabbitmq-diagnostics ping` with a 15s timeout every 30s; under a large backlog on a contended node
that command takes longer, so the kubelet killed a broker that was *busy, not broken*, and queued
messages went with it.

| | before | after relaxing the probe |
|---|---:|---:|
| RabbitMQ restarts during the run | **3** | **0** |
| Deliveries stranded | **824** | **0** |
| Deliveries succeeded | 625 of 1 451 | **2 866 of 2 866** |

Probe relaxed to a 30s timeout, 60s period, `failureThreshold: 5`.

**The load generator reported 0 failed requests in the run that lost 824 deliveries.** Any experiment
asserting "no errors" would have passed and published a false result — which is why every scenario
asserts on durable state instead:

```sql
SELECT count(*) FROM deliveries d JOIN events e ON e.id = d.event_id
 WHERE e.tenant_id = ? AND d.status NOT IN ('SUCCEEDED', 'DEAD');   -- must be 0
```

---

## Not yet measured

Listed so their absence is explicit rather than an oversight.

| Result | Phase |
|---|---|
| Ingest throughput at ~1 000 events/sec on a multi-node cluster | — |
| Backlog drain time, CPU HPA vs KEDA, with a destination that outscales the workers | — |

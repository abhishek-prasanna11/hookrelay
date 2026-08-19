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

## Not yet measured

Listed so their absence is explicit rather than an oversight.

| Result | Phase |
|---|---|
| Rolling deployment under load: dropped requests, lost deliveries | 7 |
| CPU-based HPA vs KEDA queue-depth scaling | 8 |
| Ingest throughput and p50/p95/p99 at ~1,000 events/sec | 8 |
| Events/sec vs deliveries/sec at measured fan-out | 8 |
| CI/CD pipeline duration; image size; rollback time | 9 |
| Worker crash: events accepted / lost / redelivered / DLQ | 10 |

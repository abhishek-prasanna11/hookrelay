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

## Not yet measured

Listed so their absence is explicit rather than an oversight.

| Result | Phase |
|---|---|
| Retry delay-queue head-of-line blocking, naive vs tiered | 4 |
| Endpoint isolation: healthy-endpoint p95/p99 with and without the semaphore + breaker | 5 |
| Rolling deployment under load: dropped requests, lost deliveries | 7 |
| CPU-based HPA vs KEDA queue-depth scaling | 8 |
| Ingest throughput and p50/p95/p99 at ~1,000 events/sec | 8 |
| Events/sec vs deliveries/sec at measured fan-out | 8 |
| CI/CD pipeline duration; image size; rollback time | 9 |
| Worker crash: events accepted / lost / redelivered / DLQ | 10 |

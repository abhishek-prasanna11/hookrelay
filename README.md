# HookRelay

A webhook delivery platform. You hand it an event once; it guarantees the event reaches every
subscribed HTTP endpoint — surviving worker crashes, dead destinations, slow destinations,
duplicate submissions, its own redeployments, and traffic spikes.

**Status: phases 1–4 of 10 complete.** Ingest API, durable event store, transactional outbox,
RabbitMQ publishing, a worker performing real signed HTTP delivery, and bounded exponential retries
with a dead-letter queue are built and tested. Endpoint isolation starts in phase 5.

---

## The problem

The naive way to send a webhook is a `POST` inline in the request handler:

```java
void handleEvent(Event e) {
    db.save(e);
    httpClient.post(customerUrl, e.payload());   // everything wrong lives here
}
```

Seven independent failures, each of which this project exists to fix:

| What goes wrong | Consequence |
|---|---|
| Customer's server is down | Event lost forever |
| Customer's server takes 30s | *Your* request handler blocks 30s |
| Your process crashes mid-POST | Event lost, nobody knows |
| Customer returns 500 | No retry, event lost |
| One customer is dead, others fine | The dead one starves the healthy ones |
| Network fails *after* they processed it | You retry, they double-process |
| You deploy a new version | In-flight deliveries dropped |

Every feature in this repository traces back to exactly one row of that table. If a feature does
not, it does not belong here — see [BLUEPRINT.md](BLUEPRINT.md) §35.

---

## Architecture

```text
   POST /v1/events  ──►  API  ──►  PostgreSQL  ──►  Outbox publisher  ──►  RabbitMQ
                                   (event +                                   │
                                    deliveries +                    ┌─────────┼─────────┐
                                    outbox, one                     ▼         ▼         ▼
                                    transaction)                 Worker    Worker    Worker
                                                                    │         │         │
                                                       circuit breaker + per-endpoint semaphore
                                                                    │         │         │
                                                                    ▼         ▼         ▼
                                                              Customer HTTP endpoints
```

`api` and `worker` are separate deployments because they have opposite scaling signals: the API is
synchronous, latency-sensitive and CPU-light; the worker is asynchronous and I/O-bound, and scales
on queue depth.

---

## The delivery contract

- **At-least-once.** Every accepted event reaches `SUCCEEDED` or `DEAD` with a recorded reason.
  Never silently dropped.
- **Not exactly-once.** The acknowledgement can always be lost after the receiver committed, so
  instead of pretending otherwise, every delivery carries a stable `X-HookRelay-Delivery-Id` that is
  reused across retries. Receivers deduplicate on it.
- **Not ordered.** A delivery retrying for 30 minutes would have to block everything behind it.
  Ordering and retries are mutually exclusive; ordering is the explicit non-goal.
- **Durable before acknowledged.** `202 Accepted` is returned only after the transaction commits.
  If the API says 202, a power cut cannot erase the event.
- **Bounded retry.** 8 attempts with exponential backoff and ±20% jitter over ~4h 42m, then the
  dead-letter queue with the reason attached. Retrying forever is a capacity leak, not persistence.

---

## Key design decisions

**The database arbitrates idempotency, not the application.** Ingest does not check whether a key
exists before inserting — it inserts and lets `UNIQUE (tenant_id, idempotency_key)` refuse. With a
`if (!exists) insert` pre-check instead, 20 concurrent identical submissions produce **16 events and
32 deliveries** where there should be 1 and 2. Measured, in [RESULTS.md](RESULTS.md#12-does-the-unique-constraint-actually-matter).

**`request_hash` alongside the idempotency key.** A key alone would let a producer bug reuse a key
for genuinely different content, and we would silently discard the second request while reporting
success. Same key + different body is a `409`, not a quiet no-op.

**Fan-out happens in the API, inside the event's transaction.** An event that is visible but is
missing one of its delivery obligations would be under-delivered forever with nothing recording
that an endpoint was owed anything.

**A transactional outbox, not a direct publish.** Committing to PostgreSQL and then publishing to
RabbitMQ is two writes with a crash window between them. The outbox row commits with the event; a
separate publisher drains it. Under a simulated crash in that window, direct publishing lost
**10 of 10** deliveries permanently and the outbox lost **none**.

**Publish first, then mark published — never the reverse.** Marking first and crashing loses the
message forever; publishing first and crashing sends it twice. This system has stable delivery ids
and receiver-side deduplication precisely so the second failure is survivable.

**`SELECT ... FOR UPDATE SKIP LOCKED` for the outbox poll.** Turns the table into a work queue
several API replicas can drain in parallel — no leader election, no distributed lock. 4 concurrent
pollers over 20 rows produce 20 messages and 0 duplicates.

**One delay queue per backoff tier, not one shared queue.** RabbitMQ only inspects the message at
the *head* of a queue for expiry, so in a shared delay queue a 3-hour retry blocks every 5-second
retry behind it. Measured: a 400 ms retry stuck behind a 6000 ms one came out at **6024 ms**
(15.1× overshoot); with per-tier queues, **405 ms**. Jitter reintroduces blocking *within* a tier,
bounded by the jitter spread — measured at 407 ms against a predicted 400 ms.

**UUIDv7 primary keys.** `deliveries.id` is public, so it cannot be a sequential integer — but
random UUIDv4 scatters inserts across the B-tree and splits pages constantly. v7 embeds a
millisecond timestamp, so inserts append to the index's right edge while staying unguessable.

---

## Measured results

| Result | Value |
|---|---|
| Test suite | 105 tests, 0 failures |
| Duplicate events under 20 concurrent identical submissions — app-level check | **16** |
| Duplicate events under 20 concurrent identical submissions — DB constraint | **1** |
| Deliveries lost to a crash between commit and publish — direct publish | **10 of 10** |
| Deliveries lost to a crash between commit and publish — outbox | **0 of 10** |
| Duplicate messages from 4 concurrent outbox pollers | **0** |
| Duplicate HTTP calls from a crash *after* the commit, before the ack | **0 of 10** |
| Duplicate HTTP calls from a crash *before* the commit | **10 of 10** — why the delivery-id contract exists |
| Deliveries lost to a worker crash, either window | **0** |
| Short retry stuck behind a long one — one shared delay queue | **6024 ms** (nominal 400 ms) |
| Short retry — one queue per backoff tier | **405 ms** |
| Head-of-line blocking within a tier vs predicted jitter bound | **407 ms** vs 400 ms |

Full detail, with commands to reproduce: [RESULTS.md](RESULTS.md).

---

## Stack

Java 21 · Spring Boot 3.5 · PostgreSQL 16 · Flyway · RabbitMQ · Testcontainers · Docker ·
Kubernetes · KEDA · Prometheus · Grafana · GitHub Actions

---

## Setup

```bash
docker compose up -d
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn test
```

Integration tests start their own PostgreSQL and RabbitMQ containers, so Docker must be running but
no local database or broker is needed. Full command reference: [REFERENCE.md](REFERENCE.md).

---

## Phases

| # | Phase | Status |
|---|---|---|
| 0 | Blueprint | done |
| 1 | Ingest API + PostgreSQL | done |
| 2 | Outbox + RabbitMQ | done |
| 3 | Worker + delivery | done |
| 4 | Retry + DLQ | done |
| 5 | Isolation + security | |
| 6 | Observability | |
| 7 | Docker + Kubernetes | |
| 8 | KEDA + load testing | |
| 9 | CI/CD | |
| 10 | Chaos + results | |

Each phase has a learning document in [docs/](docs/), written **before** that phase's code:
concepts → problem → design options → chosen design → implementation → experiment → failures →
lessons learned.

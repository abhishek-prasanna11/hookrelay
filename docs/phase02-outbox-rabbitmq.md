# Phase 2 — Transactional Outbox + RabbitMQ

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** a delivery obligation committed to PostgreSQL always reaches RabbitMQ,
even if the broker is unreachable at the moment the event is accepted, and even if the API process
dies between the commit and the publish. Nothing consumes the queue yet — that is phase 3.

**Definition-of-Done items this phase closes:**
- Transactional outbox prevents DB/message-broker dual-write loss.
- RabbitMQ provides asynchronous delivery.

---

## 1. Concepts

### 1.1 The dual-write problem

Phase 1 ends with delivery rows sitting in PostgreSQL that nothing will ever pick up. The obvious
next step is to publish to RabbitMQ once the transaction commits:

```java
@Transactional
void ingest(...) {
    persist(event);
    persist(deliveries);
}                            // ← commit happens here

rabbit.send(deliveryIds);    // ← and then we publish
```

This is a **dual write**: two independent systems that must both end up agreeing, with no shared
transaction between them. There is no ordering of those two lines that is correct.

```text
   Order A: commit, then publish              Order B: publish, then commit

     COMMIT        ✓ durable                    publish       ✓ queued
        ↓                                          ↓
     ✗ CRASH                                    ✗ CRASH
        ↓                                          ↓
     publish       never happens                COMMIT        never happens
        ↓                                          ↓
   Delivery row exists, nothing              Worker gets a delivery id
   will ever process it. Silent              that does not exist in the
   under-delivery, forever.                  database. Dangling message.
```

Order A loses work silently — the worst possible failure for a system whose entire contract is "no
silent loss of accepted work." Order B is arguably worse in a different way: the message can be
consumed before the transaction commits even *without* a crash, so a worker looks up a delivery id
the database has not made visible yet.

The window is small. It is not zero, and "small" multiplied by a million events a day is a support
ticket every day.

### 1.2 Why not a distributed transaction

The textbook answer is **two-phase commit (2PC)**: a coordinator asks every participant to prepare,
and only if all say yes does it tell them to commit.

```text
   Coordinator ──prepare──► PostgreSQL   ✓ ready
               ──prepare──► RabbitMQ     ✓ ready
               ──commit───► both
```

It is rejected here, and it is worth knowing why rather than just following fashion:

- **It moves the window, it does not close it.** If the coordinator dies after PostgreSQL commits
  and before RabbitMQ does, both participants sit in an in-doubt state holding locks until a human
  or a recovery process resolves them.
- **In-doubt transactions hold locks.** A blocked prepared transaction in PostgreSQL keeps its row
  locks and blocks vacuum. One coordinator crash can degrade the whole database.
- **RabbitMQ does not support it.** It has no XA. This alone settles it.

The outbox pattern gets the same guarantee by refusing to have two systems in the transaction at
all.

### 1.3 The outbox pattern

The insight is small and complete: **if you cannot atomically write to two systems, only write to
one.**

The intent to publish is recorded *in the database*, in the same transaction as the data it
describes. A separate process reads those rows and publishes them.

```text
   ┌─ ONE PostgreSQL transaction ──────────────┐
   │   INSERT events                            │
   │   INSERT deliveries      (N rows)          │
   │   INSERT outbox_events   (N rows)          │
   └─ COMMIT ──────────────────────────────────┘
                     │
                     │   ... any amount of time, any number of crashes ...
                     ▼
   ┌─ Outbox publisher (separate, repeating) ──┐
   │   SELECT unpublished rows                  │
   │   publish to RabbitMQ                      │
   │   wait for broker confirmation             │
   │   UPDATE published_at = now()              │
   └───────────────────────────────────────────┘
```

Atomicity is now trivially guaranteed, because all three inserts are in one transaction against one
system. The publish step is no longer required to be atomic with anything — it only has to be
*eventually* performed, and it can be retried forever because the row survives until it succeeds.

The cost is honest and worth stating: **the outbox publisher is itself at-least-once**, so a
message can be published twice (§1.7). That is acceptable precisely because this project already
committed to at-least-once delivery with receiver-side deduplication in BLUEPRINT.md §1.

### 1.4 Getting the rows out: polling vs change data capture

| Approach | How | Trade-off |
|---|---|---|
| **Polling** | `SELECT ... WHERE published_at IS NULL` on a timer | Simple, no extra infrastructure, easy to reason about and test. Costs a query per interval even when idle, and adds up to one poll interval of latency. |
| Change data capture (Debezium) | Tail PostgreSQL's write-ahead log, publish every insert | Near-zero latency, no polling load. Needs Kafka Connect or equivalent, logical replication slots, and a whole new operational surface. |
| `LISTEN`/`NOTIFY` | Trigger sends a notification on insert | Low latency, no new infrastructure. But `NOTIFY` is *not durable*: a notification delivered while no session is listening is simply gone, so polling is still needed as a backstop. It is an optimization, not a mechanism. |

**Chosen: polling**, 200 ms interval. At that interval the added latency is invisible next to the
network round trip to a customer's endpoint, and the whole publisher is one class that can be
tested without new infrastructure. `NOTIFY` as a latency nudge is recorded as optional future work.

### 1.5 Concurrent pollers: `FOR UPDATE SKIP LOCKED`

If the publisher runs inside the API and the API has three replicas, three pollers select from the
same table at the same time. Without coordination, all three read the same unpublished rows and
publish each one three times.

A plain `SELECT ... FOR UPDATE` takes a row lock, which fixes correctness but destroys throughput:

```text
   Pod A: SELECT ... FOR UPDATE  → locks rows 1-100, starts publishing
   Pod B: SELECT ... FOR UPDATE  → BLOCKS, waiting for A's lock
   Pod C: SELECT ... FOR UPDATE  → BLOCKS, waiting for A's lock
```

B and C sit idle for as long as A holds its transaction open. Three replicas do the work of one.

`SKIP LOCKED` changes the semantics from "wait for these rows" to "give me rows nobody else has
locked":

```text
   Pod A: SELECT ... FOR UPDATE SKIP LOCKED  → rows   1-100
   Pod B: SELECT ... FOR UPDATE SKIP LOCKED  → rows 101-200   (skips A's)
   Pod C: SELECT ... FOR UPDATE SKIP LOCKED  → rows 201-300   (skips both)
```

This turns a table into a work queue that multiple consumers can drain in parallel with no
coordination, no external lock service, and no leader election. It is the single most useful
PostgreSQL feature for this kind of job, and it is one of the things H2 cannot emulate — which is
the second concrete justification for phase 1's Testcontainers decision.

### 1.6 Publisher confirms: "sent" is not "received"

By default, AMQP publishing is fire-and-forget. `basic.publish` writes bytes to a socket and
returns. It does not wait for the broker, so it succeeds even when the broker has already died and
the TCP buffer simply accepted the write.

If the publisher marks a row published on that basis, the outbox has been defeated — the row is
gone and the message never existed.

**Publisher confirms** make the broker send back an `ack` once it has taken responsibility for the
message (for a durable queue, once it is on disk). The rule this phase enforces:

```text
   publish  →  wait for broker ack  →  ONLY THEN mark published_at
```

There is a second, subtler way to lose a message even with confirms. If a message is published to
an exchange that routes it nowhere — a typo in a routing key, a queue that was never declared —
RabbitMQ **acks it and discards it**. From the publisher's perspective that is indistinguishable
from success. The fix is to publish with the `mandatory` flag, which makes the broker *return* an
unroutable message instead of dropping it, plus a returns callback that treats a return as a
failure.

### 1.7 Ordering the publish and the mark

Two operations, and the same question as §1.1 in miniature:

```text
   Mark published, then publish          Publish, then mark published

     UPDATE published_at ✓                 publish + confirm ✓
        ↓                                      ↓
     ✗ CRASH                               ✗ CRASH
        ↓                                      ↓
     publish never happens                 UPDATE never happens
        ↓                                      ↓
   MESSAGE LOST — the row now             Row is still unpublished, so
   looks done and will never be           the next poll publishes it
   retried.                               again. DUPLICATE.
```

**Publish first, then mark.** The failure mode becomes a duplicate instead of a loss, and this
system is built to tolerate duplicates (stable delivery ids, receiver-side dedup) while it is built
to never tolerate loss. Trading an unacceptable failure for an acceptable one is the whole art here.

### 1.8 Claim check: what goes in the message

Two options for the message body:

- **The whole payload** — worker needs no database read.
- **Just the delivery id** ("claim check") — worker looks the rest up.

**Chosen: the delivery id alone.** Three reasons. The database is already the source of truth for
delivery status, and a worker must read the row anyway to check whether the delivery already
succeeded (BLUEPRINT.md §11 step 3), so the read is not avoidable. A copy of the payload in the
broker can go stale relative to the database. And a small message keeps queue memory low when a
backlog builds, which is precisely the situation phase 8 is engineering for.

### 1.9 Durability of the queue itself

Three separate settings, all required, each of which silently discards messages if missed:

| Setting | Without it |
|---|---|
| Durable **exchange** | Exchange vanishes on broker restart; publishes become unroutable. |
| Durable **queue** | Queue vanishes on broker restart, taking every message with it. |
| Persistent **messages** | Messages live only in memory; a broker restart drops them even from a durable queue. |

A durable queue holding transient messages is a very common and very quiet bug.

---

## 2. The problem this phase solves

1. Write outbox rows in the same transaction as the event and its deliveries.
2. Drain them to RabbitMQ reliably: exactly the rows that exist, at least once each, never marking
   a row published unless the broker confirmed it.
3. Survive the broker being unreachable — accepting events must keep working, and the backlog must
   drain when the broker returns.
4. Let multiple API replicas poll the same table without duplicating work.
5. Keep the outbox table from growing without bound.

Not in this phase: consuming the queue, HTTP delivery, retries, the DLQ.

---

## 3. Design options

### 3.1 Where does the publisher run?

BLUEPRINT.md §3 draws "Outbox Publisher" as its own box, but §4 defines only `api` and `worker`.
This has to be settled here.

| Option | Trade-off |
|---|---|
| **A. Inside the API process** | No third service to build, deploy or scale. The outbox is part of the write path's contract, so keeping it with the writer is conceptually tidy. Cost: background polling shares the API's connection pool, and the API is the latency-sensitive service. |
| B. Its own Deployment | Fully isolated resources; matches the blueprint diagram literally. Cost: a third image, a third Deployment, a third thing in CI — for a component that is one class and a timer. |
| C. Inside the worker | Conceptually backwards: the worker is the consumer of what the publisher produces. Rejected. |

**Chosen: A, but behind a toggle.** The publisher is a component gated on
`hookrelay.outbox.publisher.enabled` (default `true`). Splitting it into its own Deployment later is
then a configuration change and a second copy of the same image with the flag flipped — no code
change. This gets option A's simplicity now without making option B expensive later.

The polling cost is genuinely small: one indexed query every 200 ms against a partial index that
only covers unpublished rows, which in the steady state is nearly empty.

### 3.2 Mark published, or delete the row?

| Option | Trade-off |
|---|---|
| Delete on publish | Table stays tiny; no purge needed. Loses all history, and makes "how far behind is the outbox?" unanswerable. |
| **Mark `published_at`, purge later** | Keeps a short audit window and makes outbox lag measurable. Needs a purge job or the table grows forever. |

**Chosen: mark, then purge** rows published more than 24 hours ago, hourly. The unbounded-growth
trap is real — an outbox table nobody purges is a classic production incident — so the purge ships
in the same phase as the thing that fills it, not later.

### 3.3 How long is the row locked?

The publisher holds `FOR UPDATE` locks across a network call to RabbitMQ. That is a lock held
during I/O, which normally deserves suspicion.

The alternative is a three-step dance: lock and mark `IN_PROGRESS`, commit, publish, mark done. It
releases the lock sooner but triples the round trips and introduces a fourth state that itself needs
crash recovery.

**Chosen: hold the lock**, bounded by a **5 second confirm timeout** and a **batch size of 100**.
The worst case is a 5-second lock on at most 100 rows that no other poller wants anyway, because
`SKIP LOCKED` means they simply move on.

---

## 4. Chosen design

### 4.1 Schema — `V2__outbox.sql`

```sql
CREATE TABLE outbox_events (
    id            uuid        PRIMARY KEY,
    delivery_id   uuid        NOT NULL REFERENCES deliveries (id),
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz,
    attempt_count int         NOT NULL DEFAULT 0,
    last_error    text,

    CONSTRAINT outbox_delivery_uq UNIQUE (delivery_id)
);

-- Partial index: only unpublished rows are indexed, so the poll query stays fast no matter how
-- large the table grows between purges. A full index would keep every published row in it for
-- nothing — the poll never looks at those.
CREATE INDEX outbox_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
```

`outbox_delivery_uq` makes the outbox one-row-per-delivery. Combined with phase 1's
`deliveries_event_endpoint_uq`, a re-run of fan-out cannot produce a second queue message.

### 4.2 The poll query

```sql
SELECT * FROM outbox_events
 WHERE published_at IS NULL
 ORDER BY created_at
 LIMIT :batchSize
   FOR UPDATE SKIP LOCKED
```

### 4.3 RabbitMQ topology

```text
    publish (routing key "delivery", mandatory, persistent)
              │
              ▼
    ┌──────────────────────┐
    │ exchange: hookrelay  │   direct, durable
    └──────────┬───────────┘
               │  binding: "delivery"
               ▼
    ┌──────────────────────┐
    │ queue: deliveries    │   durable
    └──────────────────────┘
```

Retry queues and the DLQ are declared in phase 4. Message body is the claim check:

```json
{ "delivery_id": "0198f2c1-...-7a3b" }
```

### 4.4 The publish cycle

```text
   every 200ms:
     BEGIN
       SELECT unpublished rows ... LIMIT 100 FOR UPDATE SKIP LOCKED
       for each row: publish(deliveryId), mandatory, persistent
       waitForConfirms(5s)
         │
         ├─ all confirmed, none returned  →  UPDATE published_at = now()
         │
         └─ timeout / nack / returned     →  UPDATE attempt_count += 1,
                                             last_error = ...
                                             (published_at stays NULL → retried)
     COMMIT
```

Publishing happens on a single channel per batch so one `waitForConfirms` covers the whole batch.

Note what is deliberately absent: there is no backoff on outbox publish failure. If the broker is
down, the publisher retries every 200 ms forever. That is correct here — the broker being down is
an operational emergency, not a per-message condition, and the retry cost is one cheap query. The
backoff machinery in phase 4 is for *customer endpoints*, which are expected to fail routinely.

### 4.5 Metric

One gauge now, the rest in phase 6:

```text
hookrelay_outbox_lag_seconds   age of the oldest unpublished outbox row
```

This is the number that tells you the publisher is wedged. Deliveries-created and
deliveries-published counters would both look healthy while the lag silently grows.

### 4.6 Tests

| Test | Asserts |
|---|---|
| outbox row per delivery | 3 matching endpoints → 3 deliveries → 3 outbox rows, same transaction |
| no endpoints | 0 deliveries, 0 outbox rows |
| duplicate submission | no second set of outbox rows |
| publisher drains | rows get `published_at`, messages land on the queue |
| message content | body carries the delivery id, and it matches the row |
| message is persistent | delivery mode 2 |
| **broker unavailable** | events still accepted; rows stay unpublished; `attempt_count` climbs; nothing lost |
| **broker returns** | backlog drains, every delivery ends up on the queue exactly once |
| concurrent pollers | two pollers racing publish each row exactly once |
| purge | rows published >24h ago are removed; unpublished rows never are |
| lag gauge | reflects the age of the oldest unpublished row |

---

## 5. Implementation plan

1. `V2__outbox.sql`.
2. `OutboxEvent` entity, `OutboxEventRepository` with the `SKIP LOCKED` native query.
3. Extend `IngestTransactions.createEventAndFanOut` to insert outbox rows in the same transaction.
4. `RabbitTopology` — exchange, queue, binding beans; publisher confirms and returns configured.
5. `OutboxPublisher` — scheduled poll, batch publish, confirm, mark.
6. `OutboxPurge` — hourly delete of old published rows.
7. Outbox lag gauge.
8. RabbitMQ Testcontainer in the test base; the test table above.
9. The §6 experiment; record in `RESULTS.md`.
10. Update `REFERENCE.md` and `README.md`.

---

## 6. Experiment — does the outbox actually save anything?

### 6a. First attempt, and why it failed to prove anything

The plan was to make the broker unavailable by **pausing its container** (`docker pause` preserves
the port mapping and the established TCP connection, unlike stopping it, which would come back on a
new port), accept ten events during the outage, restore the broker, and count how many deliveries
never reached the queue.

| Measurement | Dual write, broker paused |
|---|---:|
| Events accepted | 10 |
| Deliveries created | 10 |
| Messages on the queue after recovery | **10** |
| Deliveries never queued | **0** |

**The dual-write implementation lost nothing.** The experiment was wrong, not the conclusion —
and the reason is worth more than the original result would have been.

A paused broker does not refuse writes. `basic.publish` writes into the TCP socket, the bytes sit in
the kernel buffer, and `waitForConfirms` simply times out because no ack comes back. When the
container is unpaused, the broker reads the buffered bytes and queues every one of those messages.

So the dual-write application logged **ten "publish failed, message lost" errors for ten messages
that were successfully delivered.** A confirm timeout does not mean the message was not delivered.
It means *you do not know* — which is the same ambiguity that makes exactly-once delivery
impossible on the receiving side (BLUEPRINT.md §1), appearing again one hop earlier.

Pausing reproduces "the broker is slow", and the outbox is not what protects you from that;
buffering and confirms are. The hole the outbox exists to close is a different one.

### 6b. The right injection: the crash window

The dual-write hole is specifically the gap **between the commit and the publish** — a window in
which the process can die with the data durable and the publish never attempted. So the injection
became exactly that: publishing is impossible for the duration of the outage (the process is dead),
and afterwards both configurations get a recovery attempt, standing in for a restarted process or a
surviving replica.

Both runs: 10 events, one matching endpoint, publisher timer off so recovery is explicit and the
result does not depend on timing.

| Measurement | A: dual write | B: outbox | Correct |
|---|---:|---:|---:|
| Events accepted (202) | 10 | 10 | 10 |
| Deliveries created | 10 | 10 | 10 |
| Durable record of an unperformed publish | **none** | **10 rows** | — |
| Messages on the queue after recovery | **0** | **10** | 10 |
| Deliveries silently never queued | **10** | **0** | 0 |

Every one of the ten deliveries in configuration A is permanently invisible. The event row exists,
the delivery row exists in `PENDING`, and nothing in the system will ever act on it or report it —
which is precisely the "silent loss of accepted work" that BLUEPRINT.md §1 forbids. Configuration B
recovers all ten on the first poll after restart, because the intent to publish was committed
alongside the data.

**Reproduce.** The permanent regression tests for this behaviour are
`OutboxRecoveryTest#survivesBrokerOutage` (broker outage, the timer-driven publisher) and
`OutboxIntegrationTest#concurrentPollersDoNotDuplicate` (the `SKIP LOCKED` property):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api -Dtest=OutboxRecoveryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## 7. Failures

**Four things went wrong. Three were mine, one was a genuine finding.**

**1. The experiment did not reproduce the failure it was designed to reproduce.** Covered in §6a.
Pausing the broker tests broker slowness, not the crash window, and the dual-write implementation
passed. Recording this rather than quietly swapping the method: an experiment that confirms what
you expected is worth less than one that tells you your model of the failure was wrong.

**2. `ReturnedMessage` is in `org.springframework.amqp.core`, not `org.springframework.amqp.rabbit.support`.** A
compile error in Spring AMQP 3.2. Trivial, but a reminder that the class moved between versions and
most search results still show the old package.

**3. `@ConditionalOnProperty` on `OutboxPurge` removed the bean, not just the schedule.** Tests
disable the purge timer so it cannot race their assertions, but they still need to call the purge
logic — and the annotation meant there was no bean to autowire. The fix was a design improvement
rather than a test workaround: the flag is now checked *inside* the scheduled method, so disabling
the timer never removes the capability. **When a component is "a piece of logic plus a schedule",
gate the schedule, not the component.**

**4. A test-isolation bug that was really a modelling insight.** Two `queueDepth()` assertions failed
by exactly one message. Every test isolates itself with a fresh random tenant id, which works for
every tenant-scoped table — but `publishBatch()` deliberately claims *every* unpublished row
regardless of tenant, so rows left behind by earlier tests were published into a later test's
queue-depth count.

The instinct is "flaky test". It is not: the outbox and the queue are **infrastructure, not tenant
data**, and they are the exact point where a tenant-scoped isolation strategy stops holding. The
fix clears both explicitly in `@BeforeEach`, with a comment saying why.

---

## 8. Lessons learned

**"The broker is down" and "the process died" are different failures with different remedies, and
conflating them hides which mechanism is doing the work.** Buffering, confirms and reconnection
handle an unavailable broker — the outbox contributes nothing there, as §6a proved by accident. The
outbox handles the crash window between two writes. Reaching for a pattern without being able to
state which failure it addresses means being unable to tell whether it is working.

**A failed publish is not a non-delivery.** The confirm timeout in §6a was ambiguous in exactly the
same way as a lost HTTP response: the operation may have fully succeeded. This is why
publish-then-mark (§1.7) is the only defensible ordering — on ambiguity the system must retry, and
therefore must tolerate duplicates. The whole architecture is downstream of accepting that
ambiguity is unavoidable rather than trying to engineer it away.

**`SELECT ... FOR UPDATE SKIP LOCKED` turns an ordinary table into a work queue that several
processes can drain in parallel.** No leader election, no distributed lock, no external
coordinator — one clause. It is what makes running the publisher inside a multi-replica API safe,
and it is the second thing in this project (after phase 1's unique violation) that only works
against a real PostgreSQL.

**Gate the schedule, not the bean.** Failure 3 above. `@ConditionalOnProperty` on a class deletes the
capability along with its trigger, which is almost never what "disabled" should mean for something
tests need to invoke directly.

**Infrastructure tables have no tenant, and that is where tenant-scoped test isolation quietly
stops working.** Failure 4. Worth knowing now, because phases 4 and 10 add more shared
infrastructure — retry queues, the DLQ — with the same property.

**The outbox publisher deliberately has no backoff, and that is not an oversight.** A dead broker is
an operational emergency; retrying every 200 ms costs one indexed query against a partial index
that is nearly empty in the steady state. Exponential backoff exists in this system for *customer
endpoints*, which are expected to fail routinely. Applying the same policy to both would mean a
brief broker blip taking minutes to drain.

# Phase 4 — Retry, Backoff and the Dead-Letter Queue

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** a retryable failure is retried on a bounded exponential schedule instead of
being acknowledged and abandoned, and a delivery that exhausts that schedule lands in a dead-letter
queue with a recorded reason. `FAILED` stops being the end of the line.

**Definition-of-Done items this phase closes:**
- Retries use bounded exponential backoff and jitter.
- Head-of-line blocking is measured and fixed.
- Failed deliveries enter the DLQ.

---

## 1. Concepts

### 1.1 Why a broker has no "deliver this later"

AMQP has no scheduled delivery. A message is either in a queue and available now, or it is not in
the queue. There is no timestamp field the broker honours.

The obvious workaround is to hold the message in the worker — sleep, then retry. It is wrong for
reasons that compound:

```text
   worker sleeps 3 hours holding the message
        │
        ├─ the thread is gone for 3 hours (or 8 threads are, or all of them)
        ├─ a deploy kills the worker and the pending retry vanishes with it
        ├─ the message stays unacknowledged, so the broker's consumer timeout may fire
        └─ nothing anywhere records that a retry is pending
```

The retry has to live **in the broker**, durably, not in a worker's memory.

### 1.2 Delay queues: TTL plus dead-lettering

RabbitMQ gives two primitives that combine into a timer:

- **Message TTL** — a message that has lived longer than its TTL *expires*.
- **Dead-letter exchange (DLX)** — when a message is expired, rejected, or dropped for length, the
  broker republishes it to a nominated exchange instead of discarding it.

Put them together on a queue that has **no consumer**, and you have a delay:

```text
   publish with a 5s TTL
        │
        ▼
   ┌──────────────────────────────┐
   │ queue: retry.5s              │   x-message-ttl = 5s
   │ (nobody consumes this queue) │   x-dead-letter-exchange = hookrelay
   └──────────────┬───────────────┘   x-dead-letter-routing-key = delivery
                  │  ... 5 seconds pass, the message expires ...
                  ▼
   ┌──────────────────────────────┐
   │ exchange: hookrelay          │
   └──────────────┬───────────────┘
                  ▼
   ┌──────────────────────────────┐
   │ queue: deliveries            │   a worker picks it up again
   └──────────────────────────────┘
```

The message is durable in the broker the whole time. A worker restart, a deploy, a broker restart —
none of them lose the pending retry.

### 1.3 The head-of-line trap

The tempting design is **one** delay queue with a **per-message** TTL, since each retry needs a
different delay. It does not work, and the reason is a detail of how RabbitMQ implements expiry.

A queue is a FIFO. RabbitMQ only inspects the message at the **head** of a queue for expiry —
checking every message would mean scanning the whole queue continuously. So a message expires when
it reaches the head *and* its TTL has elapsed.

```text
   one queue, per-message TTL

   head ──►  [ TTL 3h, enqueued now ]   ← expires in 3 hours
             [ TTL 5s, enqueued now ]   ← ready in 5 seconds, but it is not at the head
             [ TTL 5s, enqueued now ]
             [ TTL 5s, enqueued now ]

   Result: three retries that should fire in 5 seconds fire in 3 hours.
```

Every short retry queued behind one long retry inherits the long one's delay. Under a partial
outage — some endpoints failing for hours, most recovering in seconds — this converts a system
with a graceful backoff curve into one where a single slow customer stalls everybody's retries.

**The fix is one queue per delay bucket.** Every message in `retry.5s` has approximately the same
TTL, so head-of-line ordering and expiry order agree, and no message can be stuck behind one with a
radically longer delay.

### 1.4 Exponential backoff

Retrying at a fixed interval is either too aggressive for a long outage or too slow for a blip.
Exponential backoff starts fast and stretches out:

```text
   attempt 1  immediate
   attempt 2  +5s        cumulative 5s
   attempt 3  +30s                  35s
   attempt 4  +2m                    2m 35s
   attempt 5  +10m                  12m 35s
   attempt 6  +30m                  42m 35s
   attempt 7  +1h                    1h 42m
   attempt 8  +3h                    4h 42m
```

A momentary blip is recovered within seconds; a genuine multi-hour outage is survived without eight
hammering retries in the first minute. And it is **bounded** — after the eighth attempt the delivery
is dead. Retrying forever is not persistence, it is a slow leak of capacity into an endpoint that is
never coming back.

### 1.5 Jitter, and the thundering herd

If a customer's endpoint goes down, ten thousand deliveries fail within the same second — and
without jitter, all ten thousand retry in the *same* second, five seconds later. The retry is a
synchronised spike that can knock over an endpoint that was just recovering.

**Jitter** spreads them: each delay is multiplied by a random factor in `[0.8, 1.2]`, so the same
ten thousand retries land smeared across 4–6 seconds instead of stacked on one instant.

### 1.6 Jitter and tiered queues are in tension — and how far

Here is the part the two previous sections do not tell you.

- Jitter needs a **per-message** TTL, since each message's delay differs.
- §1.3 fixed head-of-line blocking by giving each **queue** a fixed TTL.

Doing both means per-message TTLs inside a tier, which reintroduces head-of-line blocking — but the
magnitude is completely different, and that is the whole point:

```text
   naive: one queue, TTLs from 5s to 3h
      worst-case block = 3h - 5s          ≈ 3 hours

   tiered: retry.5s only ever holds 4s-6s TTLs
      worst-case block = 6s - 4s          = 2 seconds
```

Head-of-line blocking is **bounded by the jitter spread**, not eliminated. Two seconds of skew on a
five-second retry is invisible; three hours is an outage. Stating this precisely matters more than
claiming the problem is solved — and each tier queue also carries a queue-level
`x-message-ttl` at the top of its jitter range as a backstop, so a message published without an
expiration cannot sit there forever.

### 1.7 The dead-letter queue

A delivery that exhausts its attempts, or fails permanently, must not simply vanish. The contract
(BLUEPRINT.md §1) is that every accepted delivery ends `SUCCEEDED` or `DEAD` **with a recorded
reason**.

`DEAD` in the database is the record. The DLQ is the *operational* half: a real queue holding the
messages, so an operator can see how many there are, inspect them, and — outside this project's
scope — redrive them.

The reason travels with the message as headers rather than only in the database, so the queue is
self-describing when someone is looking at it in the management UI at 3am.

### 1.8 Ordering, one more time

The worker must publish the retry (or DLQ) message **before** acknowledging the original. The
familiar shape:

```text
   ack first, then publish retry        publish retry first, then ack

     ack ✓                                publish ✓
       ↓                                    ↓
     ✗ CRASH                             ✗ CRASH
       ↓                                    ↓
     retry never published                ack never sent
       ↓                                    ↓
   DELIVERY SILENTLY ABANDONED          original redelivered → a second
   after one failed attempt.            retry is published. DUPLICATE.
```

The duplicate is survivable: `attempt_count` is the authority on how many attempts have happened,
it is incremented atomically by the claim, and it is capped at 8 — so a duplicated retry costs at
most one extra HTTP call, not an unbounded loop. Abandoning a delivery is not survivable. Same
trade as phase 2's outbox, for the same reason.

---

## 2. The problem this phase solves

1. On a retryable failure with attempts remaining, schedule the next attempt in the broker.
2. Use exponential backoff with jitter, on a bounded schedule.
3. Do not let one long-delayed retry stall short ones.
4. On a permanent failure, or when attempts are exhausted, mark the delivery `DEAD` and put it on
   the DLQ with the reason attached.
5. Never abandon a delivery silently.

Not in this phase: per-endpoint concurrency, circuit breakers, SSRF validation (all phase 5), DLQ
redrive (out of scope — BLUEPRINT.md §32).

---

## 3. Design options

### 3.1 How to demonstrate the head-of-line problem

BLUEPRINT.md §10 asks for the naive single-queue design to be built first, measured, then replaced.

Building it into the worker and reverting it would follow that literally, but it leaves nothing
behind: the measurement becomes a number in a document that nobody can re-run, and the naive code
is gone.

**Chosen: measure it directly against the broker, as a permanent test.** Head-of-line blocking is a
property of *RabbitMQ*, not of HookRelay — it depends only on how a queue expires messages. A test
that declares both topologies itself and times when messages arrive measures exactly the thing that
motivates the design, is re-runnable forever, and never requires shipping a design known to be
wrong. It is a deviation from the blueprint's stated method, in the blueprint's own spirit: the
important outcome is the measured improvement.

### 3.2 Where the routing decision lives

| Option | Trade-off |
|---|---|
| In the listener, from the returned outcome | Keeps publishing out of the processor. But the listener would need the attempt number and the delivery's state, re-deriving what the processor already has. |
| **In the processor, after recording the attempt** | It already knows the outcome, the attempt number and the delivery. The listener stays a thin ack/nack wrapper. |

**Chosen: the processor**, with a `RetryPublisher` collaborator, so the decision and the publish are
one step and the listener keeps its single responsibility.

### 3.3 What `next_attempt_at` is for

The column exists from phase 1 and it is **still not a scheduler**. The schedule lives in the
broker. It is written now because it makes "when will this be tried again?" answerable from the API
and from a SQL query, which is worth one column update.

Recorded again here because the temptation to add a poller over it — and thereby have two systems
racing to retry the same delivery — is exactly the trap flagged in phase 1.

---

## 4. Chosen design

### 4.1 Topology

```text
                                   ┌──────────────────┐
   worker publishes a retry ──────►│ hookrelay.retry  │  direct, durable
                                   └────────┬─────────┘
                    routing key = tier name │
        ┌───────────┬───────────┬───────────┼───────────┬───────────┬───────────┐
        ▼           ▼           ▼           ▼           ▼           ▼           ▼
   retry.5s    retry.30s    retry.2m   retry.10m   retry.30m    retry.1h    retry.3h
        │           │           │           │           │           │           │
        └───────────┴───────────┴─────┬─────┴───────────┴───────────┴───────────┘
                     on expiry, x-dead-letter-exchange
                                      ▼
                            ┌──────────────────┐
                            │    hookrelay     │ ──► queue: deliveries
                            └──────────────────┘

                                   ┌──────────────────┐
   worker publishes a dead ───────►│  hookrelay.dlq   │ ──► queue: deliveries.dlq
   delivery                        └──────────────────┘      (no consumer, no TTL, no DLX)
```

Each `retry.*` queue: no consumer, `x-dead-letter-exchange = hookrelay`,
`x-dead-letter-routing-key = delivery`, and `x-message-ttl` set to the top of its jitter range as a
backstop. Per-message `expiration` carries the jittered delay.

### 4.2 The schedule

| Completed attempt | Next attempt | Nominal delay | Tier |
|---:|---:|---|---|
| 1 | 2 | 5s | `retry.5s` |
| 2 | 3 | 30s | `retry.30s` |
| 3 | 4 | 2m | `retry.2m` |
| 4 | 5 | 10m | `retry.10m` |
| 5 | 6 | 30m | `retry.30m` |
| 6 | 7 | 1h | `retry.1h` |
| 7 | 8 | 3h | `retry.3h` |
| 8 | — | — | DLQ |

Every delay is multiplied by a uniform random factor in `[0.8, 1.2]`. Total window ≈ 4h 42m.

### 4.3 Routing an outcome

```text
   outcome
     │
     ├─ SUCCESS ─────────────────────► SUCCEEDED, ack
     │
     ├─ PERMANENT ───────────────────► DEAD, publish to DLQ, ack
     │
     └─ RETRYABLE
          ├─ attempt_count < 8 ──────► FAILED, set next_attempt_at,
          │                            publish to tier queue, ack
          └─ attempt_count == 8 ─────► DEAD, publish to DLQ, ack
```

Publish always happens before the ack (§1.8).

### 4.4 DLQ message headers

The body is the same claim check. The reason rides along:

| Header | Example |
|---|---|
| `x-hookrelay-reason` | `attempts_exhausted` · `permanent_failure` |
| `x-hookrelay-attempts` | `8` |
| `x-hookrelay-last-error` | `HTTP 500` · `TIMEOUT` |
| `x-hookrelay-endpoint-id` | the endpoint's UUID |
| `x-hookrelay-failed-at` | ISO-8601 |

### 4.5 Tests

| Test | Asserts |
|---|---|
| retryable failure schedules a retry | delivery `FAILED`, message on the expected tier queue |
| tier selection | attempt 1 → `retry.5s`, attempt 3 → `retry.2m`, attempt 7 → `retry.3h` |
| jitter | 200 samples all within ±20%, and not all identical |
| expiration header | the jittered delay is on the message |
| attempts exhausted | attempt 8 fails → `DEAD`, message on the DLQ, not on a retry queue |
| permanent failure | 400 → `DEAD` + DLQ immediately, no retry queued |
| DLQ headers | reason, attempts and last error present |
| success after failure | a retried delivery that succeeds ends `SUCCEEDED`, nothing further queued |
| **delay queue actually delays** | a message on `retry.5s` reappears on `deliveries` after ≈ its TTL |
| **head-of-line, naive vs tiered** | the §6 experiment |
| end to end | a 500-then-200 endpoint is retried and succeeds, through the real broker |

---

## 5. Implementation plan

1. `RetryTier` enum and `RetryPolicy` (tier selection, jitter, max attempts) in `common`.
2. Extend `RabbitTopology`: retry exchange, seven tier queues with DLX, DLQ exchange and queue.
3. `RetryPublisher` in the worker — publish to a tier, publish to the DLQ.
4. Route outcomes in `DeliveryProcessor`; write `next_attempt_at`.
5. `DeliveryStore.recordAttempt` gains the next-attempt timestamp.
6. Tests, including the head-of-line experiment.
7. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — how bad is head-of-line blocking, really?

**Method.** Publish a long-delay message, then immediately a short-delay one, and measure when the
short one is actually released. Two topologies, identical in every other respect: one shared delay
queue with per-message TTLs, versus one queue per delay bucket. Both dead-letter into the same
landing queue, whose arrivals are timestamped.

Nominal delays: **long 6000 ms, short 400 ms**.

| Measurement | Naive: one shared queue | Tiered: one queue per bucket |
|---|---:|---:|
| Short retry's nominal delay | 400 ms | 400 ms |
| **Short retry's actual delay** | **6024 ms** | **405 ms** |
| Blocking attributable to the long message | **5624 ms** | **5 ms** |
| Overshoot factor | **15.1×** | **1.01×** |
| Long retry's actual delay | 6021 ms | 6007 ms |

The short retry's TTL elapsed after 400 ms and it was released after **six seconds** — it could not
expire until it reached the head of the queue, and it could not reach the head until the message
ahead of it expired. Its own TTL was irrelevant. Separate queues remove the interference entirely:
405 ms against a 400 ms nominal.

Scaled to the real schedule, where the longest tier is three hours rather than six seconds, a single
customer stuck on the 3h tier would hold every five-second retry behind it for **three hours**.

### The bounded-blocking claim

§1.6 argued that jitter reintroduces blocking *within* a tier, but bounded by the jitter spread
rather than by the schedule's full range. Measured, with two messages in the same queue whose TTLs
differ only by jitter (1200 ms and 800 ms — the top and bottom of a 1000 ms tier):

| Measurement | Value |
|---|---:|
| Faster message's nominal delay | 800 ms |
| Faster message's actual delay | 1207 ms |
| **Blocking** | **407 ms** |
| Jitter spread (`1200 − 800`) | 400 ms |

Blocking is 407 ms against a predicted bound of 400 ms — the extra 7 ms is broker overhead. The
claim holds precisely: head-of-line blocking is not eliminated, it is reduced from *the full range
of the schedule* to *the width of one tier's jitter*. On the 5s tier that is two seconds of skew;
naively it was hours.

**Reproduce.** `DelayQueueHeadOfLineTest`, which declares both topologies itself:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=DelayQueueHeadOfLineTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## 7. Failures

**1. A queue-depth assertion that was a race, and passed anyway most of the time.**
`RetryIntegrationTest#tierFollowsAttemptNumber` failed with `expected: 1 but was: 0` after 0.04
seconds, while the structurally identical assertion in the test above it passed. Both were reading
`rabbitAdmin.getQueueInfo(...).getMessageCount()` immediately after a publish.

That count is a snapshot taken by the queue process, and it lags a message that was just published.
Asserting on it right after a publish is a race that resolves on timing luck — the dangerous kind of
flake, because the version that passes is not more correct than the version that fails.

The fix was not a sleep or a retry loop. Every such assertion became a **blocking receive**, which
waits for the message to genuinely exist and, by decoding it, also asserts *which* delivery was
scheduled rather than merely that something was. The test got both more reliable and stronger.
`assertNothingOn` similarly waits 200 ms rather than reading a count that might not have caught up.

**2. Deviation from the blueprint's stated method, taken deliberately.** BLUEPRINT.md §10 asks for
the naive single-queue design to be built into the worker, measured, then replaced. It was measured
against the broker directly instead, as a permanent test. The behaviour is RabbitMQ's, not
HookRelay's — it depends only on how a queue expires messages — so shipping and reverting a design
already known to be wrong would have bought nothing and left no re-runnable artifact. Flagged rather
than done silently.

---

## 8. Lessons learned

**A FIFO queue and a set of timers are different data structures, and RabbitMQ only gives you the
first one.** Delay queues look like scheduling, which invites the assumption that the broker will
release each message when *its* timer fires. It will not: it checks the head, because checking every
message would mean scanning the queue continuously. Every "TTL per message in one delay queue"
design has this bug, and it stays invisible until delays differ by an order of magnitude — which is
exactly what exponential backoff guarantees.

**Two correct-sounding requirements can be in direct tension, and the useful answer is a bound, not
a claim.** Jitter needs per-message TTLs; avoiding head-of-line blocking wants per-queue TTLs.
Tiering does not resolve that — it makes the residual blocking *small and measurable*: 407 ms
against a 400 ms spread. "Head-of-line blocking is bounded by the jitter width" is a more useful
statement than "we fixed head-of-line blocking", and it is checkable.

**Retrying forever is a leak, not persistence.** Eight attempts over roughly 4h 42m, then the
delivery is dead and says why in a header. An unbounded retry against an endpoint that is never
coming back consumes worker capacity indefinitely on work that cannot succeed.

**Publish before you acknowledge — the same trade, a third time.** Phase 2 chose duplicate messages
over lost ones for the outbox; phase 3 chose duplicate HTTP calls over lost deliveries for the ack;
phase 4 chooses a duplicate retry over an abandoned delivery. In each case the ordering that risks
doing something twice beats the ordering that risks not doing it at all, because `attempt_count` is
atomic and capped, so duplication costs one extra call while abandonment loses the delivery.

**Make the test assert the thing you actually care about.** "Is there a message on the 2m queue?" and
"was *this* delivery scheduled on the 2m queue?" cost the same to write. The second one catches a
routing bug that puts the wrong delivery on the right queue, and — as it turned out — is also the
one that is not a race.

# Phase 10 — Chaos and Results

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after the runs.

**Goal of this phase:** run the four failure demonstrations BLUEPRINT.md §28 requires against the
real cluster, and consolidate every measured result.

**Definition-of-Done items this phase closes:**
- Worker crash experiment passes.
- Permanent destination failure experiment passes.
- Rolling deployment experiment passes.
- Traffic spike experiment passes.
- `RESULTS.md` contains reproducible measurements.

---

## 1. Concepts

### 1.1 What a chaos experiment is for

Not "breaking things to see what happens". A chaos experiment states a **belief about the system**,
then tries to falsify it. The belief has to be specific enough to be wrong:

```text
   vague    "the system is resilient to worker failures"
   testable "killing a worker mid-delivery loses zero accepted deliveries,
             and produces at most one duplicate HTTP call per in-flight delivery"
```

The second can fail. That is the whole value. This project has already had three experiments falsify
the belief they were built to confirm — phase 2's outbox test proved nothing on its first design,
phase 5's isolation test pointed the wrong way, and phase 7's rolling deploy found a **silent-loss
bug** while measuring something else entirely.

### 1.2 Why the assertion must not be "no errors"

The phase 7 run reported zero failed HTTP requests and had lost three deliveries permanently. An
experiment that watches only the surface it expects to break confirms the surface it watched.

Every scenario here therefore asserts on **durable state** — what the database says happened — rather
than on the absence of errors:

```text
   weak    "0 requests failed"
   strong  "every accepted event reached SUCCEEDED or DEAD, and the counts reconcile"
```

That assertion is the delivery contract restated as a query, and it is the only one that catches
work disappearing quietly.

### 1.3 The four scenarios and what each one is really testing

| Scenario | The belief under test | The mechanism responsible |
|---|---|---|
| Worker crash | no accepted delivery is lost | manual acknowledgement — unacked messages are redelivered |
| Destination permanently down | retries are bounded and end in the DLQ with a reason | the retry ladder and `attempt_count` cap |
| Rolling deployment | no request dropped, no delivery lost | readiness probes, `maxUnavailable: 0`, graceful shutdown |
| Traffic spike | the backlog is absorbed and drains | queue durability and queue-depth autoscaling |

Two of these are already measured — the rolling deployment in phase 7 (13 237 requests, 0 dropped)
and the traffic spike in phase 8 (a 73 228-message backlog absorbed without loss). This phase runs
the two that are not, and consolidates all four.

### 1.4 Duplicates are expected, and that is the design

Killing a worker mid-delivery will produce duplicate HTTP calls whenever the crash lands before the
outcome is committed — phase 3 measured exactly that: **10 duplicates from 10 crashes** in that
window, and 0 in the other. So "zero duplicates" is the wrong assertion; it would fail a correct
system.

The right assertions are that **nothing is lost**, and that every duplicate carries the same
`X-HookRelay-Delivery-Id` so a receiver can drop it. The in-cluster receiver deduplicates on that
header and reports a `duplicates` counter, which turns the contract into something observable rather
than promised.

---

## 2. The problem this phase solves

1. Kill workers under load and prove no accepted delivery is lost.
2. Point a delivery at an endpoint that will never succeed and prove the retry ladder is bounded and
   terminates in the DLQ with a recorded reason.
3. Consolidate all four demonstrations, plus every earlier measurement, into `RESULTS.md`.

---

## 3. Design options

### 3.1 How to kill a worker

| Option | Trade-off |
|---|---|
| `kubectl delete pod` | Graceful: SIGTERM, `terminationGracePeriodSeconds`, in-flight work finishes. Tests the shutdown path, which is the *tidy* case. |
| `kubectl delete pod --force --grace-period=0` | Immediate SIGKILL. No shutdown hook, no ack, no chance to finish. This is the case redelivery exists for. |

**Chosen: both, in one run.** The graceful case exercises the shutdown path; the forced case
exercises the broker's redelivery. Asserting only on the graceful one would test the easy half.

### 3.2 Making "permanently down" genuinely permanent

An endpoint that returns 500 forever is the honest version — the connection succeeds, the server
answers, and the answer is a retryable failure every time. That walks the full ladder. Pointing at a
dead port instead would exercise connection failures, which is a different error class and a shorter
path.

Because the full ladder spans about 4h 42m, the run seeds a delivery near the end of the ladder and
asserts the terminal behaviour. Walking all eight attempts in real time is not a test, it is a wait.

---

## 4. Chosen design

```text
   chaos/worker-crash.sh       load → delete a worker gracefully → SIGKILL another → reconcile
   chaos/destination-down.sh   endpoint that always 500s → walk the ladder → DEAD + DLQ + reason
```

Both finish with the same reconciliation query, which is the delivery contract as SQL:

```sql
SELECT count(*) FROM deliveries d JOIN events e ON e.id = d.event_id
 WHERE e.tenant_id = ? AND d.status NOT IN ('SUCCEEDED', 'DEAD');
-- must be 0 once the system is quiescent
```

---

## 5. Implementation plan

1. `chaos/worker-crash.sh`, `chaos/destination-down.sh`.
2. Run both; run the already-built rolling-deploy and autoscaling scenarios for a consolidated set.
3. Rewrite `RESULTS.md`'s summary so the four required demonstrations are findable together.
4. Update `REFERENCE.md` and `README.md`.

---

## 6. Experiment — the four required demonstrations

### 6.1 Worker crash — BLUEPRINT.md §28.1

**Method.** Three workers under load. One deleted gracefully (SIGTERM, grace period, in-flight work
finishes), then a *different* one force-killed (`--force --grace-period=0`, SIGKILL, no shutdown
hook). The assertion is on durable state once quiescent, not on the absence of errors.

| Measurement | Value |
|---|---:|
| Requests sent / failed | 2 866 / **0** |
| Events accepted | 2 866 |
| Deliveries created | 2 866 |
| **Deliveries succeeded** | **2 866** |
| Deliveries dead | 0 |
| **Deliveries unfinished** | **0** — the contract as a query |
| Total attempts | **2 867** — one more than deliveries |
| Receiver: verified / rejected | 7 145 / **0** |
| RabbitMQ restarts during the run | 0 |

**Nothing was lost across a graceful kill and a SIGKILL.** The single extra attempt is the
redelivery: one delivery was in flight when its worker was killed, the broker never received an
acknowledgement, and another worker picked it up. That is manual acknowledgement doing exactly the
job it exists for, visible as a difference of one.

### 6.2 Destination permanently down — BLUEPRINT.md §28.2

**Method.** The receiver set to answer 500 to everything. One event published, then the delivery
fast-forwarded to the end of the retry ladder rather than waiting 4h42m for it.

| Measurement | Value |
|---|---|
| Status after attempt 1 | `FAILED`, `last_error = HTTP 500` |
| Retry scheduled | 1 message on a retry tier |
| `next_attempt_at` recorded | yes |
| **Final status** | **`DEAD`** |
| **Final attempt count** | **8** — the cap |
| Final error | `HTTP 500` |
| Attempt response codes | `500,500` |
| **DLQ depth before / after** | **0 → 1** |

Bounded retries, a terminal state, and a dead-lettered message carrying its reason. The delivery did
not disappear and did not retry forever.

### 6.3 Rolling deployment — BLUEPRINT.md §28.3

Measured in phase 7: **13 237 requests across four rolling restarts, 0 dropped, 0 deliveries lost**,
with both keep-alive and connection-per-request traffic. See RESULTS.md §7.3.

### 6.4 Traffic spike — BLUEPRINT.md §28.4

Measured in phase 8: a **73 228-message backlog** absorbed with no loss; queue-depth autoscaling took
workers **2 → 6** where CPU-based scaling never moved off 2. See RESULTS.md §8.1–8.2.

---

## 7. Failures

**1. A liveness probe restarted the broker under load, and 824 deliveries disappeared.**

The first clean run of the worker-crash scenario reported **824 deliveries stranded in `PENDING` with
`attempt_count = 0`**, their outbox rows published, and every queue empty. The exact signature of the
phase 7 silent-loss bug — which had been fixed and regression-tested.

It was not the application. RabbitMQ had **restarted three times during the run**. Its liveness probe
ran `rabbitmq-diagnostics ping` with a 15-second timeout every 30 seconds; under a large backlog on a
contended node that command takes longer than that, so the kubelet killed a broker that was **busy,
not broken**, and queued messages went with it.

Relaxed to a 30-second timeout, a 60-second period and `failureThreshold: 5`. The re-run: **0 broker
restarts, 0 deliveries lost, 2 866 of 2 866 succeeded.**

This is the third probe misconfiguration in this project — after the API's 90-second startup budget
turning slow JVM boot into a crash loop, and before it the same lesson not carrying between files.
**A liveness probe that fires under load manufactures the outage it is meant to detect**, and it
presents as data loss in the application.

**2. The forced kill hit a pod that was already dying.** The second victim was selected with
`--field-selector=status.phase=Running`, which still matches a pod that has been deleted but is
inside its grace period — so the "SIGKILL a healthy worker" step killed the corpse of the pod deleted
twelve seconds earlier, and the scenario silently tested one kill instead of two. Fixed by filtering
on an empty `deletionTimestamp` and excluding the first victim by name.

**3. `chaos/worker-crash.sh` was not executable.** `chmod +x` had been applied to its sibling only,
so the first attempt failed with `Permission denied` and the run silently continued to the next
scenario. A one-character mistake, listed because it cost a full cycle.

---

## 8. Lessons learned

**The assertion that caught the real problem was the boring one.** Every scenario ends with

```sql
SELECT count(*) FROM deliveries d JOIN events e ON e.id = d.event_id
 WHERE e.tenant_id = ? AND d.status NOT IN ('SUCCEEDED', 'DEAD');
```

— the delivery contract restated as a query. The load generator reported **0 failed requests** in the
run that lost 824 deliveries. Any experiment asserting "no errors" would have passed and published a
false result. **Assert on durable state, because the surface you are watching is not where work goes
missing.**

**Data loss is not evidence of an application bug.** The 824 stranded deliveries had precisely the
signature of a bug already found and fixed, and the instinct was to conclude the fix had regressed.
The cause was a probe timeout on the broker. Before re-opening a fixed bug, check whether the
*infrastructure under it* stayed up — a restart count is one command away and would have saved the
detour.

**Probes are a source of outages, not only a detector of them.** Three times now a probe
configuration has caused the failure it was meant to observe: the API's startup budget, and the
broker's liveness timeout twice over. The pattern is the same each time — a threshold chosen for an
idle machine, applied to a loaded one. Probes deserve the same "what does this look like under load?"
scrutiny as any other timeout in the system.

**"Zero duplicates" would have been the wrong assertion.** Killing a worker mid-delivery produces
duplicates by design when the crash lands before the outcome is committed — phase 3 measured 10 from
10 crashes in that window. The correct assertions are that nothing is *lost* and that every duplicate
carries the same delivery id, which the receiver's counters make observable. A stricter-sounding
assertion would have failed a correct system.

# Phase 8 — Autoscaling and Load Testing

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** show, with numbers, that CPU is the wrong autoscaling signal for this
workload, and that queue depth is the right one.

**Definition-of-Done items this phase closes:**
- CPU HPA is compared against KEDA.
- Queue-depth autoscaling is demonstrated.
- Load testing reaches a measured throughput.
- Events/sec and deliveries/sec are reported separately.
- p50/p95/p99 latency is measured.

---

## 1. Concepts

### 1.1 What an HPA actually does

A HorizontalPodAutoscaler is a control loop. Every 15 seconds it reads a metric for the pods in a
Deployment and computes:

```text
   desiredReplicas = ceil( currentReplicas × currentMetricValue / targetMetricValue )
```

With 2 replicas averaging 80% CPU against a 50% target: `ceil(2 × 80/50) = 4`. The controller then
clamps that to `minReplicas`/`maxReplicas` and applies stabilization — scale-down waits five minutes
by default, because flapping is worse than being briefly over-provisioned.

Two things follow immediately, and both matter here:

- **The metric is an average across pods**, so one saturated pod among ten barely moves it.
- **"Utilization" is a percentage of the CPU *request***, not of the node. A pod requesting 100m and
  using 50m is at 50%, whether the node is idle or on fire. Setting a request too low makes a pod
  look permanently overloaded; too high, permanently idle.

### 1.2 Why CPU is the wrong signal for a delivery worker

The worker spends nearly all of its time in `HttpClient.send`, waiting for a customer's server. A
thread waiting on a socket consumes no CPU. So under a backlog:

```text
   queue depth      200  ────────────────────►  50 000     the work is piling up
   worker CPU        8%  ────────────────────►     11%     the signal barely moves
   HPA decision       —  ────────────────────►  no change
   backlog                                       drains hours later
```

This is not a tuning problem to be fixed with a lower CPU target. The relationship between "how much
work is waiting" and "how much CPU the workers are using" is simply **weak** for I/O-bound work.
Lowering the target to 20% would make the HPA scale on noise instead; the metric does not carry the
information.

CPU works well for CPU-bound services — that is why it is the default, and why the default is
misleading. **The right signal is the thing you actually care about: the backlog.**

### 1.3 Queue depth, and roughly how many workers a backlog needs

Queue depth is a direct measure of unfinished work, and it responds instantly: one message enqueued,
depth plus one.

Little's Law gives the intuition for a target. For a queue in steady state:

```text
   L = λ × W        items in system = arrival rate × time in system
```

If each delivery takes ~200 ms of wall-clock (mostly waiting), one worker with 4 concurrent listener
threads handles ~20 deliveries/second. To drain a backlog of 1000 in 10 seconds needs ~100/second,
so ~5 workers.

Expressed as an HPA target that becomes **messages per replica**: `queueLength: 100` means "add a
replica for every 100 messages waiting". It is a crude model — it ignores per-endpoint concurrency
limits and how slow each endpoint is — but it is directionally right, which CPU is not.

### 1.4 What KEDA adds

Kubernetes' HPA can only read metrics it knows about: CPU and memory natively, plus custom and
external metrics if something serves the metrics API. Out of the box nothing publishes "how many
messages are in a RabbitMQ queue".

**KEDA** is that something. A `ScaledObject` names a trigger — here, a RabbitMQ queue — and KEDA:

1. registers itself as an **external metrics API server**, so the HPA can ask it for values;
2. **creates and manages an HPA** on the target Deployment;
3. polls RabbitMQ's management API on an interval and serves the depth as the metric.

```text
   KEDA operator ──polls──► RabbitMQ management API
        │
        │ serves external metric
        ▼
   HPA (created by KEDA) ──scales──► Deployment/worker
```

So this is not an alternative to the HPA; it is an adapter that lets the ordinary HPA see a number it
otherwise could not. That framing matters when reading the results: the *mechanism* is identical in
both arms, and only the **signal** differs — which is exactly what the experiment isolates.

KEDA also offers scale-to-zero, which the HPA cannot do. Not used here: a worker at zero replicas
adds cold-start latency to the first delivery after a quiet period, for a saving that does not matter
on a two-node demonstration cluster.

### 1.5 Events per second is not deliveries per second

BLUEPRINT.md §25 insists these are reported separately, and the reason is fan-out:

```text
   1 000 events/sec  ×  average fan-out 10 endpoints  =  10 000 deliveries/sec
```

The API's work scales with **events**; the worker's with **deliveries**. Quoting one number for
"throughput" hides which side of the system a bottleneck is on, and makes capacity planning wrong by
the fan-out factor. Every result here reports both, plus the measured fan-out that connects them.

### 1.6 Building a backlog honestly

The obvious way to create a backlog is enormous ingest throughput. On a single-node demonstration
cluster that mostly measures the load generator and the API, not the thing under test.

The realistic and cheaper route is a **slow endpoint**. A backlog forms when deliveries arrive faster
than they complete, and completion rate is governed by how quickly customer endpoints respond — which
is exactly the real-world cause of backlogs and precisely the situation in which worker CPU stays
near idle. A slow receiver produces a large queue from modest ingest, and puts the workers in the
I/O-bound state the whole phase is about.

Ingest throughput is measured separately and reported for what it is, rather than being inflated to
manufacture a backlog.

---

## 2. The problem this phase solves

1. Autoscale workers on RabbitMQ queue depth via KEDA.
2. Establish a CPU-HPA baseline under an identical workload.
3. Measure queue depth, replica count, CPU utilisation and backlog drain time for both.
4. Measure ingest throughput and latency percentiles, reporting events/sec and deliveries/sec
   separately with the fan-out that relates them.

Not in this phase: scale-to-zero, scaling the API, multi-node capacity work, or KEDA's other
triggers.

---

## 3. Design options

### 3.1 How to build the backlog

| Option | Trade-off |
|---|---|
| Very high ingest rate | Faithful to "traffic spike", but on one node it measures the load generator and the API rather than the worker. |
| **Slow customer endpoint** | Produces a large backlog from modest ingest, and puts workers in exactly the I/O-bound state the phase is about. This is also the real-world cause of backlogs. |

**Chosen: a slow endpoint**, with ingest throughput measured and reported separately rather than
conflated with it.

### 3.2 Making the comparison fair

Both arms must differ only in the scaling signal:

- identical workload (same event count, same endpoint delay, same fan-out)
- identical `minReplicas` / `maxReplicas`
- the CPU arm uses a plain HPA; the KEDA arm uses a `ScaledObject`, which creates an HPA underneath
- worker replicas reset to `minReplicas` between arms

The CPU target is set to **50%** of the worker's 100m request — deliberately *aggressive*. A stricter
target than anyone would choose in production makes the result stronger: if even a 50m-of-CPU trigger
does not fire, the problem is the signal, not the threshold.

---

## 4. Chosen design

### 4.1 Manifests

```text
   infra/kubernetes/50-hpa-cpu.yaml     HorizontalPodAutoscaler on worker CPU  (baseline arm)
   infra/kubernetes/51-keda.yaml        ScaledObject on RabbitMQ queue depth   (KEDA arm)
```

Only one is applied at a time; two controllers scaling one Deployment would fight.

### 4.2 The experiment

```text
   1. reset worker to minReplicas, drain queues
   2. apply the arm's autoscaler
   3. register a SLOW endpoint (delay per request)
   4. publish N events as fast as the API accepts them
   5. sample every 5s: queue depth, worker replicas, worker CPU
   6. stop when the backlog is drained, or a deadline passes
   7. report peak depth, peak replicas, CPU range, drain time
```

### 4.3 What gets reported

Per BLUEPRINT.md §22 and §25: peak queue depth · peak replicas · worker CPU range · backlog drain
time · events/sec · deliveries/sec · average fan-out · ingest p50/p95/p99.

---

## 5. Implementation plan

1. Install KEDA; confirm the external metrics API answers.
2. `50-hpa-cpu.yaml` and `51-keda.yaml`.
3. `chaos/autoscaling.sh` — runs one arm and emits a time series.
4. Run both arms; record.
5. A separate ingest-throughput run for the §25 numbers.
6. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — can CPU see a backlog?

**Method.** A slow endpoint (300 ms per request) and three registered endpoints, so every event fans
out to three deliveries. Load runs for 5 seconds; queue depth, ready replicas and average worker CPU
are sampled every 5 seconds until the backlog drains or a deadline passes. Identical bounds in both
arms (`min 2`, `max 6`) and identical scaling policies — KEDA creates an ordinary HPA underneath, so
only the **signal** differs.

### Result

| | CPU HPA | KEDA queue depth |
|---|---:|---:|
| Peak queue depth | 1 047 | 2 587 |
| **Peak worker replicas** | **2 — never scaled** | **6** |
| Worker CPU (100m request) | **28m, flat** | 71–412m |
| Events accepted | 355 | 872 |
| Deliveries created | 1 065 | 2 616 |
| Average fan-out | 3.00 | 3.00 |

The CPU arm's full time series:

```text
   elapsed  depth   replicas  cpu(m)     target = 50% of a 100m request
     0          0      2       28
    10      1,047      2       28
    15      1,026      2       28
    21          4      2       28
```

**A thousand messages queued and the signal did not move.** 28 millicores is 28% of the request,
comfortably under the 50% target, so the autoscaler had nothing to react to. KEDA, watching the same
workload through queue depth, went to 6 replicas.

### The stronger version of the same result

An earlier run used a **1 second** endpoint delay and heavier load — a more thoroughly I/O-bound
worker:

```text
   elapsed  depth     replicas  cpu(m)
     0           0       2       13
    24      16,204       2       13
    95      73,228       2       43
   181      73,618       3       71
```

**73,000 messages of backlog, worker CPU at 43 millicores, two replicas for three minutes.** The
more time a worker spends waiting on a socket, the less CPU carries any information about how much
work is waiting.

### The trap this exposes

In the CPU arm workers sat at 28m; in the KEDA arm they reached 412m. That is not a contradiction —
it is the mechanism:

```text
   few workers → each modestly loaded → CPU low → no scale-up → few workers
```

**CPU utilisation is a consequence of how much work you are doing, which is a consequence of how
many workers you have.** Two workers, throttled by per-endpoint concurrency and endpoint latency,
never became busy enough to trigger scaling, so they stayed at two. Queue depth breaks the loop
immediately because it measures the work that is *waiting*, not the work being *done*.

### Throughput (BLUEPRINT.md §25)

Measured separately, with a fast endpoint, since throughput and backlog behaviour are different
questions:

| Measurement | Value |
|---|---:|
| Ingest | **268 events/sec** |
| Average fan-out | **3.00** |
| Deliveries created | **~805 deliveries/sec** |
| Ingest p50 / p95 / p99 | **9.7 / 94.9 / 192.3 ms** |
| Requests failed | 0 of 5 364 |

Reporting one "throughput" number would hide which side of the system a bottleneck is on, and make
capacity planning wrong by the fan-out factor.

### What this experiment does **not** show

**Backlog drain time is not comparable between the arms, and the table above deliberately omits it.**
Two reasons, both disqualifying:

1. **The arms did not receive identical workloads.** Same load generator settings, but achieved
   ingest differed (71 vs 174 events/sec), so KEDA faced a 2 587-message backlog against CPU's
   1 047. A drain-time difference across different workloads measures nothing.
2. **The receiver saturated.** With 6 workers the in-cluster Python receiver became the bottleneck,
   so the KEDA arm's drain time reflects the test endpoint's capacity rather than the worker pool's.

Establishing drain time properly needs a destination that scales past the worker pool and a fixed
event count rather than a fixed duration. That is worth doing and is not done here.

**Reproduce:**

```bash
./chaos/autoscaling.sh cpu
```

```bash
./chaos/autoscaling.sh keda
```

---

## 7. Failures

This phase took five attempts to produce a usable measurement. Every failure was in the harness or
the environment; none were in the application.

**1. `set -e` plus an unguarded command substitution killed a four-minute run silently.** A transient
`kubectl` timeout inside `REPLICAS=$(kubectl get ...)` terminated the script after the sampling loop
but before it printed anything, discarding both arms. Every call in the loop is now guarded and falls
back to its last known value, and the reporting section runs under `set +e` so a failed query
degrades the report instead of destroying it.

**2. The reset purged one queue instead of ten.** Messages in the retry tiers expire back into
`deliveries` through their dead-letter exchange, so purging only the main queue let a previous arm's
backlog reappear seconds later — one run inherited **73,000 messages** from another. Compounded by
`rabbitmqctl purge_queue` silently exceeding its default timeout on a large queue, hidden by a
`|| true`.

**3. "Drained" was measured wrongly and reported a false success.** Watching only the `deliveries`
queue counted messages parked in `deliveries.deferred` as drained, and the script reported
`backlog_drain_seconds = 41` when 611 of 1 635 deliveries had actually completed. It now sums
`deliveries`, `deliveries.deferred` and all seven retry tiers.

**4. The reference receiver was single-threaded.** `tools/webhook_receiver.py` used `HTTPServer`,
which handles one request at a time, so at a 300 ms delay it capped the entire system at roughly
**3 deliveries/second regardless of worker count** — 693 of 16,092 deliveries completed in 420
seconds. The destination was the bottleneck, so the autoscaling comparison was measuring the test
receiver. Now `ThreadingHTTPServer`.

**This was the same bug, in a different file, that phase 5 had already found and fixed** in the
test receiver. See §8.

**5. The demonstration did not fit the machine.** `maxReplicas: 10` at 512Mi per worker asks for 5GB
of workers alone, on top of the API, PostgreSQL and RabbitMQ, on one node. KEDA behaved correctly —
its HPA asked for 10 replicas, the right answer to the backlog — and the node could not host them, so
pods went unready and **RabbitMQ was OOM-killed 13 times**, after which KEDA lost the connection it
was polling. Cause and effect looked exactly backwards: it presented as "KEDA cannot reach RabbitMQ".
Worker requests dropped to 256Mi, `maxReplicas` to 6, RabbitMQ's limit raised to 1280Mi.

---

## 8. Lessons learned

**A fix applied in one place does not reach its twin, and this project has now proved it three
times.** Publisher confirms went into the outbox in phase 2 and not into the retry publisher until a
chaos test found data loss in phase 7. A threading fix went into the test receiver in phase 5 and not
into the reference receiver until it silently capped this experiment. Drain accounting counted one
queue when the system has ten. Each looked like an isolated slip; together they are a pattern, and
the countermeasure is a question rather than more care: **when fixing something, ask what else in
this repository has the same shape.** Grepping for the sibling costs a minute.

**A signal can be uninformative in a way that no amount of tuning repairs.** The instinct on seeing
"CPU did not trigger scaling" is to lower the threshold. The target here was already 50% of a 100m
request — 50 millicores, stricter than anyone would set in production — and CPU still sat at 28m
under a thousand-message backlog. Lowering it further would scale on noise. The relationship between
CPU and pending work is weak for I/O-bound services, and a weak relationship cannot be strengthened
by moving a number.

**Autoscaling on a consequence creates a feedback loop that cannot start.** Few workers means each is
modestly loaded, which means low CPU, which means no scale-up, which means few workers. Scaling on
queue depth measures the *cause* — work waiting — and is immune to that loop. Any metric derived from
the current capacity has this defect.

**A load test is only as good as the thing on the other end.** For several runs the measurement was
of a single-threaded Python server, not of Kubernetes autoscaling. A destination that saturates
before the system under test does makes every number downstream meaningless, and it is invisible
unless you check that the *endpoint* can absorb what you are sending.

**Report what the experiment cannot support.** The drain-time comparison is omitted rather than
published with caveats, because the arms received different workloads and the receiver saturated. A
number that looks like a result and is not one is worse than an acknowledged gap.

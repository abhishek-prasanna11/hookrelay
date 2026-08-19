# Phase 6 — Observability

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** answer "is delivery healthy, and if not, which part is broken?" from a
dashboard rather than by reading the database.

**Definition-of-Done items this phase closes:**
- Prometheus metrics exist.
- Grafana dashboard exists.
- Structured logs correlate deliveries.

---

## 1. Concepts

### 1.1 Three instrument types, and why the difference matters

| Instrument | Question it answers | Example here |
|---|---|---|
| **Counter** | how many, since start — only goes up | `deliveries_total` |
| **Gauge** | what is it *right now* — goes up and down | `queue_depth`, `worker_inflight` |
| **Histogram** | how is a distribution shaped | `delivery_duration_seconds` |

A counter is not "how many per second". It is a monotonically increasing total, and the rate is
computed at query time (`rate(deliveries_total[5m])`). This is deliberate: a process that restarts
resets its counter to zero, and `rate()` recognises the reset. A gauge storing "deliveries per
second" would be a number nobody could aggregate across replicas or reconstruct after a restart.

### 1.2 Why an average latency is worthless, and how histograms fix it

The natural instinct is to record a mean. Consider 100 deliveries: 99 at 10 ms, one at 30 s.

```text
   mean = (99 × 10ms + 30 000ms) / 100 = 310ms
```

The mean describes nothing that happened: no delivery took 310 ms. It hides the one customer who
waited 30 seconds, and it moves for reasons nobody can act on.

A **histogram** counts observations into pre-declared buckets:

```text
   le=0.005   ██████████ 41
   le=0.01    ██████████████ 58
   le=0.05    ███████████████████ 99
   le=0.1     ███████████████████ 99
   ...
   le=+Inf    ████████████████████ 100
```

Percentiles are then estimated from the buckets at query time
(`histogram_quantile(0.99, ...)`). Two consequences worth knowing:

- **Buckets are chosen in advance.** A p99 that falls between `le=1` and `le=2` is reported by
  interpolation, so bucket boundaries determine resolution. Boundaries must straddle the values you
  care about.
- **Histograms aggregate; percentiles do not.** Two replicas each reporting "p99 = 200 ms" cannot be
  combined — the p99 of the union is not the average of the p99s. Bucket counts *can* be summed,
  which is exactly why Prometheus stores buckets rather than quantiles.

### 1.3 Cardinality: the failure mode of metrics systems

Every distinct combination of label values is a **separate time series**, each with its own memory
and its own storage.

```text
   hookrelay_deliveries_total{result="success"}                       → 1 series
   hookrelay_deliveries_total{result, endpoint_id}                    → 1 × endpoints
   hookrelay_deliveries_total{result, endpoint_id, event_type}        → 1 × endpoints × types
```

With 10 000 endpoints and 20 event types, that last line is **600 000 series** from one metric —
and it grows every time a customer registers an endpoint. This is how monitoring systems fall over,
and the cause is always the same: a label whose value space is unbounded and controlled by users.

BLUEPRINT.md §23 states the rule directly — no `endpoint_id`, `delivery_id` or `event_id` as labels.
The tension is real, because those are exactly the identifiers you want during an incident. The
resolution is that **metrics answer "how much and how bad", logs answer "which one"**:

```text
   metrics  →  "delivery success rate dropped to 60%, p99 is 14s"
   logs     →  "...for endpoint 7c9e6679, deliveries 018f…, 018f…, 018f…"
```

One caveat this phase has to handle: BLUEPRINT.md §23 lists `hookrelay_circuit_breaker_state`, which
naively means one series *per endpoint* — the very thing the rule forbids. It is published instead
as a count of endpoints in each state (`{state="open"} 3`), which is three series regardless of how
many endpoints exist, and is the number an operator actually reacts to.

### 1.4 Pull, not push

Prometheus **scrapes** `/actuator/prometheus` on a schedule. The application never sends anything.

This inverts a detail that matters operationally: a process that is up but wedged still answers a
scrape with stale-but-present values, while a process that has died stops answering and Prometheus
records `up == 0`. Failure to report is itself a signal, which push-based systems have to
reconstruct with heartbeats.

It also means the worker needs an HTTP server. It has not had one — phase 3 deliberately set
`web-application-type: none`, since it is queue-driven background work. That changes here: without a
scrape endpoint the worker's metrics do not exist, and the same server carries the Kubernetes
liveness and readiness probes in phase 7.

### 1.5 What to measure: RED

For a request-driven system, the **RED** method names three things worth having for every operation:

- **R**ate — how many per second
- **E**rrors — how many are failing
- **D**uration — how long they take

Applied to delivery: `deliveries_total` (rate and errors, split by result) and
`delivery_duration_seconds` (duration). Applied to ingest: `ingest_latency_seconds` and the accepted
counter. Everything else on the list is a saturation signal — queue depth, in-flight requests,
outbox lag — which is the **USE** method's contribution: utilisation, saturation, errors.

### 1.6 Two different latencies, and why reporting one is a lie

BLUEPRINT.md §24 insists these stay separate, and the reason is worth making concrete:

```text
   attempt duration        how long the HTTP request took
   end-to-end latency      accepted (202) → delivered
```

A delivery that fails seven times over four hours and succeeds on the eighth attempt in 40 ms has an
excellent attempt duration and a terrible end-to-end latency. Only the second reflects what the
customer experienced. A dashboard showing only attempt duration would have looked perfect
throughout.

End-to-end latency is measurable here because `events.created_at` is recorded at ingest, so the
worker can compute the difference when a delivery finally succeeds.

### 1.7 Structured logs and correlation

A log line is only useful during an incident if the lines for one delivery can be found together.
That needs two things: machine-parseable output (JSON, not a formatted sentence), and a correlation
identifier on every line.

**MDC** (mapped diagnostic context) is a per-thread map that the logging framework merges into every
event. Setting `delivery_id` once at the top of processing means every subsequent line — including
ones logged deep inside a helper that knows nothing about deliveries — carries it, and it must be
cleared in a `finally` block, because worker threads are pooled and reused.

This is the other half of §1.3: the identifiers deliberately kept out of metric labels live here,
where high cardinality costs nothing because logs are not a time-series database.

---

## 2. The problem this phase solves

1. Expose a Prometheus endpoint from both services.
2. Instrument delivery, ingest, retries, the DLQ, the circuit breakers, and queue depth — without
   unbounded label cardinality.
3. Keep attempt duration and end-to-end latency distinct.
4. Emit structured logs carrying delivery/event/endpoint identifiers.
5. Provision Prometheus and Grafana with a dashboard that answers the operational questions.

Not in this phase: alerting rules, distributed tracing (BLUEPRINT.md §2 makes it a non-goal),
per-tenant metrics.

---

## 3. Design options

### 3.1 Where queue depth comes from

| Option | Trade-off |
|---|---|
| RabbitMQ's own Prometheus plugin | Authoritative, no application code. Another component to run, and its metric names are not ours. |
| **A gauge in the worker polling `RabbitAdmin`** | One small class; queue depth sits beside delivery metrics in the same dashboard. Costs a management-API call per queue per poll. |

**Chosen: a gauge in the worker**, polling every 5 seconds. Ten queues (deliveries, deferred, DLQ,
seven retry tiers) is ten series — bounded and small. Phase 8's KEDA autoscaler reads RabbitMQ
directly and does not depend on this.

### 3.2 Structured logging

Spring Boot 3.4 added built-in structured logging (`logging.structured.format.console`), so no
Logstash encoder dependency is needed. It is left **off by default** — a developer running the app
locally wants readable lines — and switched on in the container image in phase 7, where a log
collector is the consumer.

### 3.3 Circuit breaker state without per-endpoint series

Published as `hookrelay_circuit_breakers{state}` — a count of endpoints in each state. Three series
instead of one per endpoint, and "how many endpoints are we currently shedding?" is the question an
operator actually asks.

---

## 4. Chosen design

### 4.1 Metrics

**Worker**

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `hookrelay_deliveries_total` | counter | `result` | Are we delivering? What is the success rate? |
| `hookrelay_delivery_duration_seconds` | histogram | — | How slow are customer endpoints? |
| `hookrelay_end_to_end_latency_seconds` | histogram | — | **The real SLO** — accepted to delivered |
| `hookrelay_attempts_total` | counter | `attempt_no` | How much capacity is going into retries? |
| `hookrelay_attempt_failures_total` | counter | `error_class` | Timeouts? 5xx? Breaker? Capacity? |
| `hookrelay_dlq_total` | counter | `reason` | What is being given up on, and why? |
| `hookrelay_deferrals_total` | counter | — | Are endpoints hitting their concurrency caps? |
| `hookrelay_circuit_breakers` | gauge | `state` | How many endpoints are being shed? |
| `hookrelay_worker_inflight` | gauge | — | Saturated, or idle and blocked? |
| `hookrelay_queue_depth` | gauge | `queue` | Are we falling behind? |

**API**

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `hookrelay_ingest_latency_seconds` | histogram | — | Is accepting events fast? |
| `hookrelay_events_total` | counter | `result` | Accepted, duplicate, or conflict? |
| `hookrelay_fanout_deliveries_total` | counter | — | Deliveries created per event |
| `hookrelay_outbox_lag_seconds` | gauge | — | Is the publisher wedged? *(phase 2)* |
| `hookrelay_outbox_published_total` | counter | — | *(phase 2)* |

Every label above has a small, bounded value space: `result` (3), `attempt_no` (8), `error_class`
(~10), `reason` (2), `state` (3), `queue` (10).

### 4.2 Histogram buckets

Delivery durations span milliseconds to the 15-second timeout; end-to-end latency spans milliseconds
to hours, because of the retry ladder. They need different boundaries, chosen to straddle the values
that matter rather than left at a default.

### 4.3 The dashboard

Six panels, each answering one question: delivery rate by result · success rate · delivery duration
p50/p95/p99 · end-to-end latency p50/p95/p99 · queue depth by queue · saturation (in-flight,
breakers open, outbox lag). Provisioned as code in `infra/grafana`, not clicked together by hand.

### 4.4 Tests

| Test | Asserts |
|---|---|
| `/actuator/prometheus` exposed | both services |
| delivery counters | success and failure increment the right `result` |
| attempt/error counters | `attempt_no` and `error_class` recorded |
| duration histogram | observations recorded |
| end-to-end latency | recorded on success, and larger than the attempt duration after a retry |
| DLQ counter | `reason` recorded |
| breaker gauge | counts endpoints per state |
| queue depth gauge | reflects real queue contents |
| ingest metrics | latency and `result` for 202/200/409 |
| **no high-cardinality labels** | no metric carries a UUID-valued label |
| MDC | log events carry `delivery_id` |

---

## 5. Implementation plan

1. `micrometer-registry-prometheus` in both services; a minimal web server in the worker.
2. `DeliveryMetrics` and `QueueDepthMetrics` in the worker; wire into the processor and publisher.
3. `IngestMetrics` in the API.
4. MDC in `DeliveryProcessor`, cleared in `finally`.
5. Prometheus and Grafana in `docker-compose`, provisioned from `infra/`.
6. Tests, then the §6 experiment.
7. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — what does one high-cardinality label actually cost?

BLUEPRINT.md §23 forbids `endpoint_id` as a metric label. That rule is easy to state, easy to break
by accident, and the damage is invisible until the metrics system falls over — so it is worth a
number rather than an assertion.

**Method.** Two real `PrometheusMeterRegistry` instances, the same counter, the same traffic: 50
endpoints × 3 results. One tagged `{result}`, the other `{result, endpoint_id}`. Both scraped
exactly as Prometheus would scrape them, and the output measured.

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

At a 5-second scrape interval that is roughly **35 MB per minute** of scrape traffic, from **one
counter**, growing every time a customer registers an endpoint. Add `event_type` and it multiplies
again.

The bounded version costs 3 series and 232 bytes *no matter how many customers exist* — the metric's
size is a function of the design, not of the business.

**Reproduce:** `CardinalityCostTest`.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=CardinalityCostTest -Dsurefire.failIfNoSpecifiedTests=false test
```

`MetricsIntegrationTest#noHighCardinalityLabels` is the permanent guard: it scans every registered
`hookrelay*` meter and fails if any label value looks like a UUID, so the rule cannot be broken
quietly by a future change.

---

## 7. Failures

**1. `RabbitAdmin` could not be injected, and had only worked by luck.** Every worker context failed
with `No qualifying bean of type 'RabbitAdmin'` the moment `QueueDepthMetrics` needed one at startup.

Spring Boot's `RabbitAutoConfiguration` declares that bean with the **return type `AmqpAdmin`**, so
before it is instantiated Spring only knows it as an `AmqpAdmin` and a `RabbitAdmin` injection point
does not match. Earlier tests autowired `RabbitAdmin` successfully because something else had
already instantiated it by then — the injection worked for a reason that had nothing to do with
being correct. Fixed by declaring the concrete type in `RabbitTopology`, where the topology is
already owned; the auto-configured bean backs off on `@ConditionalOnMissingBean`.

**2. The cardinality guard flagged its own histogram.** `labelsAreBounded` failed on
`le` — the histogram bucket boundary Micrometer adds for the SLOs declared in `DeliveryMetrics`.
It is a genuine label, but its value space is fixed by the code that declares the buckets, not by
users, so it belongs on the allowed list. Worth the false positive: a guard that only ever passes is
not evidence of anything.

**3. The "synthetic failure is not timed" test never exercised the path it named.** It set
`max_concurrency = 1` and called `process(deliveryId, 999)` expecting a `CAPACITY` outcome — but
nothing was holding the permit, so `tryAcquire` succeeded and the delivery went out over HTTP as
normal. The deferral branch only runs when the permit is genuinely unavailable. Fixed by acquiring
the permit in the test first.

**4. Two arms in one test method double-counted the receiver.** Restructuring the noisy-neighbour
test to run both configurations in a single method (see §8) made the second arm report `n=20` and a
**negative p50**: the receiver accumulates requests across both arms, so the first arm's timestamps
were being measured against the second arm's start time. Fixed by clearing the receivers between
arms. A negative latency is at least an obvious kind of wrong.

---

## 8. Lessons learned

**An average latency describes nothing that happened.** 99 deliveries at 10 ms and one at 30 s give a
mean of 310 ms — a value no delivery experienced, which hides the customer who waited half a minute.
Histograms are more work up front, because bucket boundaries have to be chosen before you have the
data, and they are the only thing that answers "how bad is it for the worst-off caller".

**Cardinality is a design property, not an operational one.** `{result}` costs 3 series whether the
platform has ten customers or ten thousand. `{result, endpoint_id}` costs 3 × customers — its size is
a function of business success, which is precisely the wrong thing to couple monitoring cost to. The
split that resolves it is worth stating as a rule: **metrics answer "how much and how bad", logs
answer "which one"**.

**A rule needs a guard or it decays.** The cardinality rule was written in the blueprint in phase 0
and would have been broken the first time someone wanted per-endpoint delivery rates. A test that
scans every meter for UUID-shaped label values makes it enforceable rather than aspirational.

**Two things called "latency" can differ by four hours.** A delivery that fails seven times and
succeeds on the eighth in 40 ms has excellent attempt duration and terrible end-to-end latency. Only
the second is what the customer experienced, and a dashboard showing only the first would have looked
perfect throughout the incident. They need separate histograms with entirely different bucket ranges
— reusing the attempt buckets would have put every retried delivery in `+Inf`.

**Do not record a zero for work that did not happen.** `CIRCUIT_OPEN` and `CAPACITY` outcomes never
make an HTTP request. Timing them at zero would drag the duration distribution toward zero exactly
when the system is unhealthy — the metric would look *better* as things got worse.

**Prefer a ratio to a threshold when the environment is not controlled.** The noisy-neighbour test
asserted an absolute p99 bound and failed inside the full suite, where cached Spring contexts change
how many workers are consuming. Running both arms back to back in one method and asserting the
*ratio* between them makes the comparison the measurement, and the assertion holds in both
environments while still catching a broken mechanism.

# Phase 5 — Endpoint Isolation and Destination Security

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** one slow or dead endpoint stops degrading delivery to healthy ones, and a
customer-supplied URL stops being a way to reach inside the cluster.

**Definition-of-Done items this phase closes:**
- Endpoint concurrency is isolated.
- Circuit breakers protect failing endpoints.
- SSRF protection works at registration and delivery time.
- DNS rebinding is considered.
- Redirects cannot bypass SSRF protection.

---

## 1. Concepts

### 1.1 The noisy neighbour

A worker pool has a fixed number of threads. Delivery is I/O-bound, so a thread spends almost all of
its time waiting for a customer's server. That is fine until one customer's server is slow:

```text
   worker with 4 threads, one queue

   thread 1  ──► endpoint A (30s timeout) ....................... blocked
   thread 2  ──► endpoint A (30s timeout) ....................... blocked
   thread 3  ──► endpoint A (30s timeout) ....................... blocked
   thread 4  ──► endpoint A (30s timeout) ....................... blocked

   endpoint B's deliveries: waiting. B is healthy. B responds in 12ms.
```

Nothing is broken. No error is logged. Every metric except latency looks normal. And every other
customer's webhooks are now minutes late because one endpoint went dark.

This is the fifth row of the README's table — *"one customer is dead, others fine → the dead one
starves the healthy ones"* — and it is the failure that most distinguishes a real delivery platform
from a queue with an HTTP client attached.

### 1.2 Why blocking on a semaphore does not fix it

The obvious fix is a per-endpoint concurrency limit: at most *N* in-flight requests per endpoint.
The obvious implementation is a semaphore the worker **blocks** on.

That does not fix anything, and understanding why determines the whole design:

```text
   thread 1  ──► endpoint A  (holds the 1 permit, 30s request)
   thread 2  ──► endpoint A  → semaphore.acquire() ............. BLOCKED for 30s
   thread 3  ──► endpoint A  → semaphore.acquire() ............. BLOCKED for 30s
   thread 4  ──► endpoint A  → semaphore.acquire() ............. BLOCKED for 30s
```

The limit is respected — only one request is in flight — and the pool is just as dead. The thread
is consumed whether it is waiting on a socket or waiting on a lock. **The scarce resource is the
worker thread, not the outbound request.**

So the acquire must be non-blocking. If no permit is available, the delivery is *put back* and the
thread immediately moves to the next message:

```text
   thread 2  ──► endpoint A  → tryAcquire() fails → defer, ack, next message   (microseconds)
```

### 1.3 Deferral is not an attempt

A delivery put back because the worker was busy has not been *tried*. Nothing was sent, the customer
saw nothing, and nothing failed. If a deferral consumed one of the eight attempts, a busy period
would exhaust the retry ladder and dead-letter deliveries that were never attempted at all.

So a deferral must not increment `attempt_count`, which has a consequence for the ordering inside
the worker: the semaphore has to be checked **before** the attempt is claimed, not after.

Deferred messages go to a dedicated short-delay queue rather than onto `retry.5s`. Mixing 2-second
deferrals into a queue holding 4–6-second retries would reintroduce exactly the head-of-line
blocking phase 4 measured. The deferral queue uses a single fixed TTL, so every message in it has
the same delay and no message can block another.

**Deferral must be bounded**, or a permanently saturated endpoint would cycle forever. After 50
deferrals (~100 seconds of waiting) the delivery is recorded as a real retryable failure with error
class `CAPACITY` and enters the normal retry ladder, which is already bounded.

### 1.4 Circuit breakers

Concurrency limits cap the damage from a slow endpoint. They do not stop us from calling an endpoint
that is definitely down — every delivery still pays the full timeout before failing.

A **circuit breaker** remembers that an endpoint is failing and stops calling it for a while:

```text
        ┌──────────┐  N consecutive failures   ┌────────┐
        │  CLOSED  │ ─────────────────────────►│  OPEN  │
        │ (normal) │                           │ (fail  │
        └──────────┘                           │  fast) │
             ▲                                 └───┬────┘
             │                        cooldown     │
             │ probe succeeds              elapses │
             │                                     ▼
             │                              ┌─────────────┐
             └──────────────────────────────│  HALF_OPEN  │
                     probe fails            │ (one probe) │
                     → back to OPEN         └─────────────┘
```

`HALF_OPEN` is the part that makes it a breaker rather than a mute button: after the cooldown,
exactly **one** delivery is allowed through as a probe. If it succeeds the endpoint is healthy again
and the breaker closes. If it fails, the breaker reopens and the cooldown restarts. Recovery is
automatic and costs one request, not a flood.

**What happens to a delivery that hits an open breaker?** Two defensible answers, and the choice
matters:

- *Defer it*, like the semaphore. But an endpoint that is down stays down for minutes or hours, so
  deferral loops until the deferral cap converts it into a failure anyway — with a great deal of
  pointless queue traffic in between.
- **Record it as a retryable failure** with error class `CIRCUIT_OPEN` and no HTTP call. This is
  chosen. It is honest — the delivery *did* fail, we simply declined to try — the attempt row
  records exactly why, and it is bounded by the retry ladder that already exists.

The cost is real and worth stating: a breaker that is open during attempts 2 and 3 burns them
without contacting the endpoint. Given the backoff ladder reaches 2 minutes by attempt 4 and the
cooldown is 30 seconds, an endpoint that recovers will be probed well before the attempts run out.

**Worker-local, deliberately.** Each worker keeps its own breaker state. With *W* workers, an
endpoint gets *W* independent breakers, so it takes `W × threshold` failures to shut it off
everywhere. A shared breaker needs Redis and a distributed state machine, which BLUEPRINT.md §32
puts in "optional future improvements" for good reason — this is one of the places where the
distributed version is dramatically more complex than the local one for a modest gain.

### 1.5 SSRF: the endpoint URL is attacker-controlled

Every other input to this system is validated. The endpoint URL is *supposed* to be arbitrary — that
is the product. And the worker that fetches it runs **inside the cluster**, where the network looks
very different than it does from the internet:

```text
   http://169.254.169.254/latest/meta-data/iam/security-credentials/
                                    ↑ cloud instance metadata: IAM credentials
   http://10.0.0.0/8, 172.16/12, 192.168/16
                                    ↑ every internal service, unauthenticated
   http://kubernetes.default.svc/api/v1/namespaces/default/secrets
                                    ↑ the cluster API
   http://127.0.0.1:5432 · http://localhost:15672
                                    ↑ our own database and broker
```

**Server-Side Request Forgery** is exactly this: persuading a server to make a request its caller
could not make. A webhook platform is an unusually good SSRF primitive because making arbitrary
outbound requests *is its job* — and it will sign them, retry them, and store the response body.

The defence is an address blocklist applied to the **resolved IP**, not the hostname. Checking the
hostname is useless: `http://evil.example.com/` looks fine and resolves to `127.0.0.1`.

Blocked: loopback, link-local (which covers `169.254.169.254`), private ranges, unique-local IPv6,
carrier-grade NAT, multicast, broadcast, `0.0.0.0/8`, and IPv4-mapped IPv6 addresses that decode to
any of the above.

Also rejected: non-`http(s)` schemes (`file://`, `gopher://`), and userinfo in the URL
(`http://user@internal-host/`), which is a classic way to confuse naive host parsing.

### 1.6 DNS rebinding, and an honest account of what is and is not fixed

Validating at registration time alone is defeated by changing DNS afterwards:

```text
   registration:  attacker.com  →  93.184.216.34   (public — passes)
   ...
   delivery:      attacker.com  →  169.254.169.254 (private — but nobody re-checks)
```

So validation happens **again at delivery time, on the freshly resolved address**. That closes the
registration-time hole.

It does not close everything, and the residual gap should be named rather than glossed:

```text
   worker:  resolve attacker.com          → 93.184.216.34   ✓ allowed
                    ↓  (microseconds)
   HttpClient: resolve attacker.com       → 169.254.169.254 ✗ never checked
                    ↓
            connects to the internal address
```

This is a **time-of-check to time-of-use** gap. Two lookups happen, and only the first is validated.
Closing it properly means connecting to the specific IP that was validated, which
`java.net.http.HttpClient` does not expose — the URL determines the lookup. Pinning by rewriting the
URL to the literal IP works for `http` but breaks TLS certificate verification for `https`, which is
a worse trade.

What narrows it in practice: the JVM caches successful DNS lookups (`networkaddress.cache.ttl`), so
the second resolution almost always returns the first's cached answer, and the window is
microseconds wide. That is a mitigation, not a guarantee, and it is recorded here as a known
limitation rather than claimed as protection.

**Redirects were already handled** in phase 3 — `HttpClient.Redirect.NEVER`, and a 3xx classified
`PERMANENT`. Without it, all of the above is bypassed by a public URL that answers `302 Location:
http://169.254.169.254/`, because the redirect is followed by the HTTP library, below the layer
where any of these checks live.

---

## 2. The problem this phase solves

1. Cap in-flight requests per endpoint without blocking worker threads.
2. Put deferred deliveries back without consuming an attempt, and bound the deferral loop.
3. Stop calling endpoints that are consistently failing, and probe for recovery automatically.
4. Reject destinations that resolve to internal addresses, at registration and at delivery.
5. Measure that a slow endpoint no longer degrades a healthy one.

Not in this phase: distributed (Redis-backed) semaphores or breakers, per-tenant rate limiting.

---

## 3. Design options

### 3.1 Semaphore acquisition

| Option | Trade-off |
|---|---|
| Blocking `acquire()` | Respects the limit and keeps the thread hostage — the failure this phase exists to fix (§1.2). |
| `tryAcquire(timeout)` | A bounded compromise; still holds the thread for the timeout, for no benefit. |
| **`tryAcquire()`, defer on failure** | The thread is released in microseconds. Costs a requeue and a short delay. |

**Chosen: non-blocking, defer on failure.**

### 3.2 What "without isolation" means for the experiment

The experiment needs a baseline. Rather than a feature flag or a temporarily reverted commit, the
baseline uses the system's own configuration: an endpoint with `max_concurrency` set very high has a
semaphore that never blocks, which *is* the unisolated behaviour. The comparison runs entirely
through supported configuration, and both arms exercise the shipped code path.

### 3.3 Where the SSRF check lives

`SsrfGuard` goes in `common`: the API needs it at registration, the worker needs it at delivery, and
the two must not drift. Registration rejects what it can prove is bad; a hostname that fails to
resolve at registration is *allowed* (DNS is allowed to be temporarily broken) because delivery-time
validation is the real gate.

---

## 4. Chosen design

### 4.1 Processing order

Deferral must not consume an attempt, so the checks move ahead of the claim:

```text
   receive DeliveryMessage
        │
        ├─ load delivery ─── already SUCCEEDED? ──────────────► ack, done
        │
        ├─ circuit breaker OPEN? ──► record CIRCUIT_OPEN attempt, retry ladder, ack
        │
        ├─ tryAcquire endpoint permit ── fails? ──► defer (no attempt), ack
        │        │                                  after 50 deferrals → CAPACITY failure
        │        │
        │        ├─ SSRF check on resolved address ─ blocked? ─► DEAD + DLQ, ack
        │        ├─ claim attempt (atomic)
        │        ├─ sign, POST, record, route          ← phases 3 and 4
        │        └─ release permit  (always, in a finally block)
        │
        └─ ack
```

### 4.2 Topology addition

```text
   deferred delivery ──► hookrelay.retry ──► deliveries.deferred
                                                    │  fixed TTL 2s, no consumer
                                                    └──► hookrelay ──► deliveries
```

A single fixed TTL, not a jittered one: every message in the queue then has the same delay, so
deferrals cannot block each other. This is the head-of-line lesson from phase 4 applied by
construction rather than by measurement.

### 4.3 Defaults

| Setting | Default | Why |
|---|---|---|
| `endpoints.max_concurrency` | 5 | Per endpoint, per worker. |
| deferral delay | 2s | Fixed. |
| max deferrals | 50 | ≈100s of waiting, then a `CAPACITY` failure onto the retry ladder. |
| breaker failure threshold | 5 consecutive | |
| breaker cooldown | 30s | Shorter than the 2m gap before attempt 4, so recovery is probed in time. |

### 4.4 Tests

| Test | Asserts |
|---|---|
| permit limits in-flight requests | with `max_concurrency=1`, never two concurrent requests |
| deferral does not consume an attempt | `attempt_count` unchanged, no attempt row |
| deferral is bounded | after the cap, a `CAPACITY` attempt is recorded |
| permit released after success and after failure | subsequent deliveries proceed |
| breaker opens after N consecutive failures | 6th delivery records `CIRCUIT_OPEN`, no HTTP call |
| breaker half-opens after cooldown | exactly one probe is allowed |
| probe success closes it | traffic resumes |
| probe failure reopens it | still no HTTP calls |
| success resets the failure count | 4 failures then a success then 4 failures stays closed |
| SSRF: blocked literals rejected | loopback, private, link-local, metadata, IPv4-mapped IPv6 |
| SSRF: public addresses allowed | |
| SSRF at registration | `POST /v1/endpoints` rejects `http://127.0.0.1/` |
| SSRF at delivery | a hostname resolving to a blocked address → `DEAD`, no request |
| non-http scheme, userinfo | rejected |
| **noisy neighbour** | the §6 experiment |

---

## 5. Implementation plan

1. `SsrfGuard` in `common`; wire into `EndpointService` at registration.
2. `deliveries.deferred` queue in `RabbitTopology`; `RetryPublisher.defer`.
3. `EndpointSemaphores` and `CircuitBreakerRegistry` in the worker.
4. Reorder `DeliveryProcessor` per §4.1; add `CIRCUIT_OPEN`, `CAPACITY`, `SSRF_BLOCKED` outcomes.
5. Tests; then the §6 experiment.
6. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — does a slow endpoint actually starve a healthy one?

**Method.** Two endpoints share the worker pool (4 listener threads, prefetch 8). The slow endpoint
takes 2 seconds per request and **succeeds**, so the circuit breaker stays closed and concurrency is
the only variable. The healthy endpoint answers instantly.

A backlog of **24 slow deliveries** is published first, then **10 healthy ones** — the realistic
shape of the failure, where one customer accumulates a queue and then goes slow. Latency is measured
from publishing the healthy batch to the request arriving at the healthy endpoint.

The baseline is not a feature flag: an endpoint whose `max_concurrency` is 10,000 has a semaphore
that never blocks, which *is* the unisolated behaviour, so both arms run the shipped code path.

**Result** (representative run; range across three runs in brackets):

| Measurement | Without isolation | With isolation | Improvement |
|---|---:|---:|---:|
| Healthy endpoint **p50** | **12 644 ms** [12.3–16.3 s] | **72 ms** [63–192 ms] | **176×** |
| Healthy endpoint p95 | 12 661 ms | 2 067 ms | 6.1× |
| Healthy endpoint **p99** | **12 661 ms** | **2 067 ms** [2.07–2.11 s] | **6.1×** |
| Healthy endpoint max | 12 661 ms | 2 067 ms | |

**The unisolated number is exactly what the mechanism predicts:** 24 slow requests × 2 s ÷ 4 worker
threads = 12 s. Every healthy delivery sat behind the slow endpoint's backlog in the consumers'
prefetch buffers. Nothing failed, nothing was logged, and every metric except latency looked normal —
which is what makes this failure mode worth engineering against.

**The residual 2 s at p99 with isolation is not noise, and it is not fixable by this mechanism.** A
semaphore stops a healthy delivery *queueing* behind the slow endpoint; it cannot preempt a slow
request already in flight on the thread that then picks the healthy one up. One in-flight 2-second
request is precisely the worst case, and 2 067 ms is that worst case. p50 of 72 ms shows the common
case is unaffected.

**Reproduce — standalone, not in the full suite:**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=NoisyNeighbourTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Inside the full suite the same test reports p50 ≈ 3 200 ms for the unisolated arm rather than
≈ 12 600 ms. That is measurement contamination, not a better result — see §7.

---

## 7. Failures

**1. The first version of the experiment measured nothing, and pointed the wrong way.** Slow and
healthy deliveries were published *interleaved*, which produced:

```text
   WITHOUT ISOLATION  p50=237ms   p95=259ms   p99=259ms
   WITH ISOLATION     p50=50ms    p95=4076ms  p99=4076ms
```

The baseline never starved, and isolation looked actively worse at the tail. The cause is that
RabbitMQ round-robins messages across consumers: alternating slow and healthy handed them to
different threads, so the work partitioned itself by accident and no thread was ever starved. A
noisy-neighbour experiment in which the neighbour is never noisy measures the variance of the test
harness.

Publishing the slow backlog first — the realistic shape — reproduced the failure immediately.

**2. The test HTTP server was single-threaded, which inflated the baseline for the wrong reason.**
After the redesign the unisolated arm reported ~44 s, against ~12 s predicted by the mechanism. The
extra time was the receiver: `HttpServer` uses a **single-threaded** default executor, so a receiver
told to delay 2 s serialised all 24 requests, and the measurement partly reflected the test server's
own concurrency limit rather than worker-pool starvation.

Giving the receiver a real thread pool brought the baseline to 12.3 s — matching
24 × 2 s ÷ 4 threads almost exactly. A number that agrees with an independent prediction is worth far
more than a large number.

**3. Spring's test-context cache silently multiplies the worker pool.** Standalone, the unisolated
arm measures ~12.6 s; inside the full suite, ~3.2 s. Every worker test class with a distinct
`@TestPropertySource` gets its own cached `ApplicationContext`, each with its own listener container,
and *all of them stay alive for the JVM* consuming the same `deliveries` queue. The effective pool is
several times four threads.

This was initially misdiagnosed as leftover in-flight work, and a quiescence step was added in
`@AfterEach` to drain each arm before the next — worth keeping, since purging a queue does not
reclaim messages consumers have already prefetched, but it was not the cause. The real fix is to run
the measurement standalone, which the test now says in its javadoc.

**4. Two constructors meant Spring could not build `CircuitBreakerRegistry`.** Every worker context
failed with `No default constructor found`, because the package-private test-seam constructor (which
injects a `Clock`) made the choice ambiguous. Fixed with an explicit `@Autowired` on the real one.

**5. A new package in `common` was invisible to component scanning.** `DestinationPolicy` landed in
`common.net`, but both applications listed `common.messaging` explicitly in
`scanBasePackageClasses`. Every API context failed to start. Fixed with a `CommonModule` marker
interface at the root of the module, so any future package is included automatically — enumerating
sub-packages is a trap that only fires when someone adds the next one.

---

## 8. Lessons learned

**A concurrency limit you *block* on is not isolation.** The instinctive `semaphore.acquire()`
respects the limit perfectly and starves the pool just as thoroughly, because the scarce resource is
the worker thread, not the outbound request. Non-blocking `tryAcquire` plus putting the delivery back
is what actually frees the thread — and it changes the processing order, since a deferral must not
consume an attempt and therefore has to happen before the claim.

**Isolation bounds the damage; it does not eliminate it, and the residual is worth quoting.** p99
stayed at one full slow-request duration because a semaphore cannot preempt work already in flight.
"Healthy endpoint p99 fell from 12.7 s to 2.1 s, which is exactly one in-flight slow request" is a
more useful and more defensible claim than "isolation fixes noisy neighbours".

**An experiment that confirms your expectation is the one to distrust.** All three measurement bugs
here — accidental partitioning, the single-threaded receiver, the multiplied context pool — produced
plausible-looking numbers. What caught two of them was having an independent prediction to compare
against (24 × 2 s ÷ 4 = 12 s). A measurement with no predicted value cannot be checked, only
believed.

**Check the resolved address, never the hostname.** `evil.example.com` is a perfectly ordinary name
that can resolve to `127.0.0.1`, so string-matching "localhost" or "169.254." accomplishes nothing.
And the check has to run again at delivery, on a fresh lookup, or an attacker simply changes DNS
after registering a public address.

**Name the gap you did not close.** DNS rebinding is *narrowed* here, not eliminated: validation and
the HTTP client each resolve the name, and only the first lookup is checked. Closing it means
connecting to the validated IP, which `java.net.http.HttpClient` will not do without breaking TLS
verification. Writing that down is worth more than a security control that quietly implies a
guarantee it does not provide.

**Every security control needs a documented way to turn it off for local development, and it should
be loud.** The test receiver and every developer's local setup live on loopback — exactly what the
guard refuses. `allow-private-destinations` defaults to false and logs a warning when enabled; the
alternative is developers discovering they can disable it by deleting the check.

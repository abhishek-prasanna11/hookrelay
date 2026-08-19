# Phase 3 — Delivery Worker

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** a signed HTTP POST actually arrives at a customer's endpoint, the attempt is
recorded, and the delivery reaches a terminal state — with no message lost if the worker is killed
mid-flight. Retries and the DLQ are phase 4; a failure here is recorded and the message is released,
not rescheduled.

**Definition-of-Done items this phase closes:**
- Workers perform signed HTTP delivery.
- Delivery IDs remain stable across retries.
- Worker crashes result in safe message redelivery.
- HTTP timeouts are bounded.
- Response bodies are bounded.

---

## 1. Concepts

### 1.1 Consuming a queue: prefetch

A consumer does not ask for one message at a time — the round trip would dominate. The broker
*pushes* messages ahead of time, up to a limit called **prefetch** (`basic.qos`). Those messages are
held by the consumer, unacknowledged, until it works through them.

```text
   prefetch = 1                          prefetch = 250

   Worker holds 1 unacked message.       Worker holds 250 unacked messages.
   Idle between messages waiting for     Never starves. But if it dies, 250
   the next. Safe, slow.                 messages wait for the ack timeout,
                                         and a second worker sits idle while
                                         one worker hoards the backlog.
```

Prefetch is a queue-fairness knob, not a throughput knob. Too high and one worker claims work it
cannot get to while others idle — which directly undermines the autoscaling in phase 8, since
adding replicas will not help if the existing one is holding everything. This project uses a small
prefetch (**8**) precisely because delivery is slow, I/O-bound work: holding many messages buys
nothing and costs distribution.

### 1.2 Acknowledgement: the whole reliability story of a consumer

When a consumer takes a message, the broker does not delete it. It marks it *unacknowledged* and
waits. Three outcomes:

| Outcome | Meaning |
|---|---|
| `basic.ack` | Consumer is done. Broker deletes the message. |
| `basic.nack` / `basic.reject` with requeue | Consumer failed. Broker puts it back. |
| Connection drops with no ack | Broker assumes the consumer died and **redelivers to someone else**. |

The third line is the entire crash-safety mechanism, and it only works if acknowledgement is
manual. With **automatic** acknowledgement, the broker considers a message delivered the moment it
writes it to the socket — so a worker that receives 8 messages and dies has lost all 8, silently,
before it did any work at all. Auto-ack is the default in several clients and it is the single
most common way to lose messages in a queue-based system.

**The rule this phase enforces: acknowledge last.** Do the work, commit the result to PostgreSQL,
*then* ack.

### 1.3 The redelivery window, and why it cannot be closed

Even with manual acks, there is an irreducible gap:

```text
   HTTP 200 received from the customer
        ↓
   delivery marked SUCCEEDED in PostgreSQL
        ↓
   ✗ WORKER KILLED HERE
        ↓
   basic.ack never sent
        ↓
   broker redelivers the message to another worker
```

The customer has already been called. The message is coming back. This is the same ambiguity as
phase 1's lost HTTP response and phase 2's confirm timeout — a third instance of the same shape.

There is no ordering of "ack" and "commit" that removes it. Acking first would turn a duplicate
into a *lost* delivery, which is strictly worse. So the window stays, and it is handled rather than
prevented:

- **Worker-side:** before attempting, re-read the delivery. If it is already `SUCCEEDED`, ack and
  do nothing. This closes the window in the common case.
- **Receiver-side:** the delivery carries the same `X-HookRelay-Delivery-Id` on every attempt, and
  the receiver is contractually expected to deduplicate on it. This is the only thing that works in
  *all* cases, because the worker could equally have died before writing `SUCCEEDED`.

### 1.4 Claiming an attempt atomically

The re-read above is a check-then-act — the same shape phase 1 measured 16 duplicate events from.
Two workers processing a redelivered message could both read `PENDING` and both call the customer.

The fix is the same as phase 1's: let the database arbitrate, with a single statement that reads and
writes at once:

```sql
UPDATE deliveries
   SET status = 'IN_FLIGHT',
       attempt_count = attempt_count + 1,
       updated_at = now()
 WHERE id = :id
   AND status <> 'SUCCEEDED'
RETURNING attempt_count
```

One statement does three jobs. It refuses to proceed if the delivery already succeeded (zero rows
returned → ack and skip). It takes a row lock for its duration, so a concurrent worker on the same
row blocks and then sees the updated state. And `RETURNING attempt_count` hands back an attempt
number that is unique per increment even under concurrency — which is what makes
`delivery_attempts`'s `UNIQUE (delivery_id, attempt_no)` a safety net rather than a landmine.

### 1.5 HMAC signatures: proving who sent the webhook

A customer receiving `POST /hook` has no idea it came from us. Anyone who learns the URL can post to
it. Some real integrations "solve" this with a shared secret in a header:

```text
   X-Secret: hunter2          ← sent in full, on every request, to a URL the
                                customer chose. Any proxy, any log, any
                                misconfigured TLS terminator now has it.
```

An **HMAC** (hash-based message authentication code) proves possession of the secret without ever
transmitting it. Sender and receiver share a key; the sender sends
`HMAC-SHA256(key, message)` alongside the message, and the receiver recomputes it. Matching means
the sender held the key *and* the body was not altered in transit — a plain hash would prove neither.

**Replay is the remaining hole.** A captured request is a valid request forever: same body, same
signature. The fix is to put a timestamp *inside the signed string*, so it cannot be changed
without invalidating the signature:

```text
   X-HookRelay-Signature: t=1755624000,v1=<hex HMAC-SHA256(secret, "1755624000.{body}")>
```

The receiver rejects anything whose `t` is outside a tolerance window (±5 minutes). An attacker can
replay the request byte for byte, but only for five minutes, and cannot extend that without the key.
This is the scheme Stripe publishes, and the `v1=` prefix is there so a future `v2` can be added
without breaking every receiver.

**Comparison must be constant-time.** A normal string comparison returns as soon as two bytes
differ, so how long it takes leaks how many leading bytes were correct. That is enough to recover a
signature byte by byte given enough attempts. `MessageDigest.isEqual` compares every byte regardless.

### 1.6 Sign the bytes you actually send

This is the trap flagged in phase 1 and it is subtle enough to be worth stating precisely.

The payload was stored as `jsonb`, which **normalizes**: PostgreSQL reorders object keys, strips
insignificant whitespace, and canonicalizes numbers. So the JSON that comes back out is not
byte-identical to what the producer sent.

If the signature is computed over one serialization and the body is written from another, every
signature fails verification — and it fails *at the customer*, intermittently, in a way that looks
like a key problem rather than a serialization problem.

The rule: **serialize the body exactly once, into a `byte[]`, then sign those bytes and send those
same bytes.** Never sign a `String` that is re-serialized on its way to the socket.

### 1.7 Timeouts, and why an unbounded read is a denial of service

A customer endpoint that accepts a connection and then never responds will hold a worker thread
forever. With no timeout, a handful of such endpoints removes the entire worker pool from service —
no crash, no error, just a system that has quietly stopped delivering anything.

Two bounds are needed and they are different:

- **Connect timeout** — how long to wait for the TCP handshake. Catches unroutable hosts and dropped
  SYNs.
- **Request timeout** — the total budget for the whole exchange. Catches the endpoint that accepts
  the connection, sends a byte every 20 seconds, and never finishes.

Java's `java.net.http.HttpClient` exposes exactly these two: `connectTimeout` on the client and
`timeout` on the request. It has no separate socket-read timeout, so the request timeout is what
bounds a slow trickle.

### 1.8 Bounding the response body

The response is also attacker-controlled. `BodyHandlers.ofString()` and `ofByteArray()` buffer the
entire body into memory, so an endpoint returning a 2 GB response takes the worker down with an
`OutOfMemoryError`.

Java 21 has no built-in "read at most N bytes" body handler, so the body is taken as an
`InputStream` and at most 513 bytes are read — 512 to keep (BLUEPRINT.md §16) plus one to detect
that truncation occurred — then the stream is closed, which discards the rest without reading it.

### 1.9 Classifying a failure

Not every failure deserves a retry. Retrying a `400 Bad Request` eight times over eleven hours
achieves nothing except load on a customer who has already told us the request is wrong.

| Class | Examples | Why |
|---|---|---|
| `SUCCESS` | 2xx | Delivered. |
| `RETRYABLE` | timeout, connect refused, DNS failure, TLS failure, 5xx, 429, 408 | The endpoint is unavailable or overloaded — a later attempt has a real chance. |
| `PERMANENT` | other 4xx (400, 401, 403, 404, 422) | The endpoint understood and refused. Repeating it will not change the answer. |

Two deliberate placements. **429 is retryable**, not permanent — it is explicitly a "try later"
signal. **3xx is permanent**, because redirects are not followed at all (§3.4).

---

## 2. The problem this phase solves

1. Consume `deliveries` with manual acknowledgement and bounded prefetch.
2. Claim an attempt atomically, skipping deliveries that already succeeded.
3. Build the webhook body once, sign it, POST it with bounded timeouts.
4. Read a bounded amount of the response.
5. Classify the outcome, record a `delivery_attempts` row, move the delivery to a terminal state.
6. Acknowledge only after all of that is committed.

Not in this phase: retries, backoff, delay queues, the DLQ, per-endpoint concurrency, circuit
breakers, SSRF validation. A retryable failure here records `FAILED` and releases the message; phase
4 replaces that release with routing to a delay queue.

---

## 3. Design options

### 3.1 HTTP client

| Option | Trade-off |
|---|---|
| **`java.net.http.HttpClient` (JDK)** | No dependency. Connect and request timeouts built in. Redirect policy is explicit and can be set to `NEVER`, which is what phase 5 needs. Response streaming available, so the body can be bounded. |
| Spring `RestClient` / `WebClient` | Nicer ergonomics, more configuration surface. `WebClient` drags in Reactor for what is a blocking call per message. |
| Apache HttpClient 5 | Most control (separate socket-read timeout, connection pool tuning). An extra dependency for control this phase does not need. |

**Chosen: the JDK client.** It has the two timeouts, an explicit redirect policy, and streaming
bodies — everything this phase and phase 5 require — with nothing to add to the build.

### 3.2 Where the worker's writes go

The worker's hot path is: claim the attempt, insert an attempt row, update the delivery. All three
are single-statement operations, and the claim needs `UPDATE ... RETURNING`, which JPA does not
express naturally.

**Chosen: `JdbcTemplate` for the worker's writes**, JPA repositories for reads. The three writes are
plain SQL that says exactly what it does, with no entity lifecycle in the way of a path that runs
once per delivery attempt.

### 3.3 What the webhook body looks like

```json
{
  "id": "0198f2c1-...-7a3b",
  "event_id": "0198f2c0-...-1c4d",
  "event_type": "payment.succeeded",
  "created_at": "2026-08-19T15:02:47Z",
  "data": { "order_id": "A-1", "amount": 4200 }
}
```

The producer's payload is nested under `data` rather than merged at the top level, so a payload
containing its own `id` or `event_type` cannot collide with the envelope.

Headers:

| Header | Purpose |
|---|---|
| `X-HookRelay-Delivery-Id` | Stable across retries. The deduplication key. |
| `X-HookRelay-Event-Id` | |
| `X-HookRelay-Event-Type` | Lets a receiver route without parsing the body. |
| `X-HookRelay-Attempt` | Attempt number, for the receiver's logs. |
| `X-HookRelay-Signature` | `t=<unix>,v1=<hex>` |
| `Content-Type` | `application/json` |
| `User-Agent` | `HookRelay/<version>` |

### 3.4 Redirects

**Not followed** (`HttpClient.Redirect.NEVER`), and a 3xx is classified `PERMANENT`.

The reason is security, not laziness. In phase 5 the destination is validated against private
address ranges; if the client silently followed redirects, a customer could register a public URL
that passes validation and 302 to `169.254.169.254`, and the check would have been bypassed by the
HTTP library. Following redirects safely means re-validating every hop, so the default is to refuse.

### 3.5 What happens to a message on failure, in this phase

There is no retry infrastructure yet, and the options are all temporarily wrong in different ways:

| Option | Problem |
|---|---|
| `nack` with requeue | Immediate infinite hot loop against a dead endpoint. |
| `nack` without requeue | Message dropped with no DLQ declared yet — silent loss. |
| **`ack` and record `FAILED`** | Honest: the attempt is durably recorded and visible in the API, and the delivery stops. |

**Chosen: ack, record the attempt, set `FAILED` (retryable) or `DEAD` (permanent).** A `FAILED`
delivery in this phase is a delivery that will not be retried *yet* — phase 4 replaces the ack with
a publish to a delay queue. Stated plainly here so `FAILED` is not mistaken for a terminal state.

### 3.6 Demonstrating real delivery

A test needs a real HTTP server that records what arrived and verifies the signature independently.

**Chosen: a small embedded server** in the test sources built on the JDK's `com.sun.net.httpserver`,
plus a standalone **`tools/webhook_receiver.py`** for manual runs and for the load tests in phase 8.
Writing the verification side twice, in two languages, from the published header format is the real
check that the signing scheme is specified rather than merely implemented.

---

## 4. Chosen design

### 4.1 Modules

`worker` becomes the third Maven module, depending on `common`. It is a separate Spring Boot
application with no web server: it consumes from RabbitMQ and talks to PostgreSQL, and does not run
Flyway — the API owns the schema (`spring.flyway.enabled: false`, `ddl-auto: validate`).

### 4.2 Processing one message

```text
   receive DeliveryMessage{deliveryId}
        │
        ├─ claim attempt  (UPDATE ... WHERE status <> 'SUCCEEDED' RETURNING attempt_count)
        │     └─ 0 rows → already SUCCEEDED → ack, done
        │
        ├─ load delivery, endpoint, event
        │     └─ endpoint or event missing → record, mark DEAD, ack
        │
        ├─ build body bytes  (once)
        ├─ sign(timestamp, bodyBytes) with the endpoint secret
        ├─ POST  (connect timeout, request timeout, no redirects)
        ├─ read at most 513 response bytes, close
        │
        ├─ classify → SUCCESS | RETRYABLE | PERMANENT
        ├─ INSERT delivery_attempts
        ├─ UPDATE deliveries → SUCCEEDED | FAILED | DEAD
        │
        └─ ack        ← always last
```

### 4.3 Signature

```text
   signed string = "<unix_seconds>" + "." + <exact body bytes>
   header        = X-HookRelay-Signature: t=<unix_seconds>,v1=<lowercase hex HMAC-SHA256>
```

Lives in `common` so the worker and the reference receivers agree by construction.

### 4.4 Configuration

| Property | Default |
|---|---|
| `hookrelay.delivery.connect-timeout-ms` | 5000 |
| `hookrelay.delivery.request-timeout-ms` | 15000 |
| `hookrelay.delivery.max-response-bytes` | 512 |
| `hookrelay.delivery.signature-tolerance-seconds` | 300 (receiver side) |
| `spring.rabbitmq.listener.simple.acknowledge-mode` | `manual` |
| `spring.rabbitmq.listener.simple.prefetch` | 8 |
| `spring.rabbitmq.listener.simple.concurrency` | 4 |

### 4.5 Tests

| Test | Asserts |
|---|---|
| successful delivery | receiver got one request; delivery `SUCCEEDED`; one attempt row with status 200 |
| envelope shape | `id`/`event_id`/`event_type`/`created_at`/`data`, payload nested under `data` |
| headers present | delivery id, event id, event type, attempt number |
| **signature verifies** | recomputed independently by the receiver over the received bytes |
| signature covers the body | a tampered body fails verification |
| signature covers the timestamp | a changed `t` fails verification |
| stable delivery id | id in the header equals `deliveries.id` |
| 500 response | classified retryable, delivery `FAILED`, attempt records 500 |
| 400 response | classified permanent, delivery `DEAD` |
| 429 response | classified **retryable**, not permanent |
| 302 response | not followed; classified permanent; receiver's redirect target never called |
| connection refused | attempt recorded with `error_class`, no response status |
| slow endpoint | request timeout fires; attempt recorded as `TIMEOUT` |
| huge response body | at most 512 bytes retained; worker survives |
| **already SUCCEEDED** | redelivered message is acked with no second HTTP call |
| attempt numbering | second attempt records `attempt_no = 2` |

---

## 5. Implementation plan

1. `worker` module and its `pom.xml`; add to the parent's `<modules>`.
2. `WebhookSignature` in `common` — sign and verify, constant-time.
3. `WebhookEnvelope` in `common` — the body shape.
4. `DeliveryStore` (JdbcTemplate) — claim, record attempt, complete.
5. `WebhookSender` — JDK `HttpClient`, bounded timeouts, bounded response read.
6. `FailureClassifier`.
7. `DeliveryProcessor` — the orchestration in §4.2.
8. `DeliveryListener` — `@RabbitListener`, manual ack.
9. Test receiver on `com.sun.net.httpserver`, and `tools/webhook_receiver.py`.
10. The test table above; then the §6 experiment.
11. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — what does a worker crash actually cost?

§1.3 claims the redelivery window cannot be closed, only handled, by two mechanisms: the worker-side
`SUCCEEDED` check and the receiver-side delivery-id contract. That implies the two mechanisms cover
*different* crashes, so the experiment injects both.

**Method.** 10 deliveries against the test receiver. Every delivery's first processing crashes; the
broker redelivers; the run continues until all 10 are `SUCCEEDED` and the queue is empty. The two
configurations differ only in *where* the crash lands relative to the database commit.

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

**Nothing is lost in either case** — which is the guarantee that matters, and it is the manual
acknowledgement that provides it.

**A** is the window most people picture, and the worker-side guard closes it completely: the
redelivered message finds the delivery already `SUCCEEDED`, `claimAttempt` returns empty, and the
customer is never called twice.

**B is why the delivery-id contract exists.** When the crash lands before the commit, the database
has no record that the customer was called. The redelivery legitimately re-attempts, and the
customer receives the identical webhook a second time — **10 duplicate calls out of 10**. No amount
of server-side care prevents this; the only thing that saves the receiver is that both requests
carry the same `X-HookRelay-Delivery-Id`. This is the third appearance of the same ambiguity, after
phase 1's lost HTTP response and phase 2's confirm timeout.

**The unplanned finding: `delivery_attempts` undercounts real HTTP calls.** Configuration B made 20
requests and recorded 10 attempt rows. An attempt that crashes before its row is written leaves no
trace at all, so the audit trail is *"attempts we know completed"*, not *"requests the customer
received"*. That is an honest limitation of any record written after the side effect, and it is
worth knowing before phase 10 uses attempt rows to reason about a chaos run — the row count is a
lower bound, not a measurement.

**Reproduce.** The permanent regression tests are
`DeliveryListenerTest#redeliveryDoesNotCallTwice` (configuration A's property, through the real
broker) and `DeliveryProcessorTest#alreadySucceededIsSkipped`:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,worker -Dtest=DeliveryListenerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

---

## 7. Failures

**1. The worker had no `ObjectMapper` bean, and the reason is a conditional two libraries away.**
Every worker test failed at context startup with `No qualifying bean of type 'ObjectMapper'`. Spring
Boot's `JacksonAutoConfiguration` is conditional on `Jackson2ObjectMapperBuilder`, which lives in
`spring-web` — and the worker has no web stack, so nothing auto-configured a mapper even though
`jackson-databind` was on the classpath.

The interesting part was where *not* to fix it. Declaring the bean in `common` would have solved it
for the worker and silently broken the API: a user-defined `ObjectMapper` makes
`JacksonAutoConfiguration` back off, so the API's `spring.jackson` settings would have been
discarded and its wire format would have flipped from snake_case to camelCase — with every test
still passing, because the API's tests read fields through `JsonNode`. The mapper is declared in the
worker module instead, with a comment explaining why it is not shared.

**2. Migrations had to move from `api` to `common`.** The worker's tests need a schema, and the
worker does not depend on the API. Rather than duplicate the SQL, `db/migration` now lives in
`common`. The *authority* to apply it is unchanged: the API runs Flyway, the worker sets
`spring.flyway.enabled: false` and only validates its mappings. Two services racing to migrate the
same database is a deadlock waiting for a deploy.

**3. The response-body cap collided with its own truncation marker.** Appending `...[truncated]` to
exactly 512 retained bytes produces 526 characters, which the
`CHECK (length(response_body) <= 512)` constraint rejects — the insert would have failed on
precisely the hostile input the cap exists to survive. The marker is now written *inside* the
budget. The constraint caught this immediately, which is the argument for having put it in the
schema rather than trusting the worker's arithmetic.

---

## 8. Lessons learned

**"Handled" is not one mechanism, and the experiment is what proves it.** Before running it, "the
worker-side check plus the receiver-side contract handle redelivery" read as belt and braces. The
measurement shows they cover disjoint cases: crash after the commit and the worker-side check is
sufficient; crash before it and the worker-side check is *useless*, and only the receiver's
deduplication prevents double processing. Anyone reasoning about which one they could drop needed
this table.

**A record written after a side effect can only ever be a lower bound.** `delivery_attempts` says
10 when the customer got 20. Nothing is wrong with the code — the row simply cannot be written
before the thing it describes has happened, and the process can die in between. Worth remembering
whenever an audit table is treated as ground truth.

**Auto-configuration conditions cross library boundaries in ways that make "just add the bean"
dangerous.** The missing `ObjectMapper` had an obvious one-line fix in the shared module that would
have changed the API's public wire format without failing a single test. The general shape: adding a
bean does not only *provide* something, it can *withdraw* an auto-configuration somewhere else
entirely.

**Sign the bytes, not the object.** The envelope is serialized once into a `byte[]` that is both
signed and transmitted. Because the payload round-trips through `jsonb` — which reorders keys and
strips whitespace — signing any re-serialization would produce failures at the customer that look
like a key problem rather than a serialization one. `DeliveryProcessorTest#signatureVerifies`
verifies over the bytes the receiver actually received, so it would catch a regression here.

**Writing the receiver twice is what makes the scheme a specification.** `tools/webhook_receiver.py`
implements verification independently in Python, and both it and `WebhookSignatureTest` are pinned
to the same golden vector rather than to each other's behaviour. Implementing only the sending side
proves the algorithm is self-consistent, which is not the property customers need.

**429 is retryable and 3xx is permanent, and both are easy to get backwards.** The 4xx/5xx split is
the intuitive rule and it is wrong at both ends: 429 explicitly means "try later", and a redirect is
an instruction this client will never follow, so repeating it changes nothing.

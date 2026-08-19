# Phase 1 — Ingest API + PostgreSQL

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 (Experiment, Failures, Lessons) are
> filled in *after* implementation, from what actually happened.

**Goal of this phase:** a producer can `POST` an event, get `202 Accepted`, and that event plus one
delivery row per matching endpoint is durably in PostgreSQL — atomically, and immune to duplicate
submissions. Nothing is delivered yet. Nothing touches RabbitMQ yet.

**Definition-of-Done items this phase closes:**
- Events are durably persisted before 202 is returned.
- Duplicate submissions are prevented with database-enforced idempotency.
- Idempotency-key payload mismatches are rejected.
- Event fan-out is durable.

---

## 1. Concepts

Everything here is built from zero. If a term is used later in the project, it is defined here.

### 1.1 What a webhook actually is

A **webhook** is a reversed API call. Normally *you* call someone else's server when you want
something. With a webhook, you register a URL with a provider, and the *provider* calls *you* when
something happens.

```text
   Normal API (pull)                    Webhook (push)

   You ──── GET /orders ────► Them      Them ──── POST /your/url ────► You
       ◄─── 200 [orders] ────                ◄─── 200 ──────────────
```

The vocabulary:

- **Producer** — the system where something happened (here: whoever calls our API).
- **Event** — the immutable record of that happening. `{"type": "payment.succeeded", ...}`.
- **Endpoint** — a customer-registered URL that wants to receive certain event types.
- **Subscription** — which event types an endpoint wants.
- **Delivery** — one attempt-tracked obligation to get one event to one endpoint. This is the unit
  of work the whole system is built around.

The key insight, and the reason `deliveries` is its own table: **an event is a fact, a delivery is
a task.** One fact can create many tasks. The fact never changes; each task has its own lifecycle,
its own failures, its own retry count.

### 1.2 Why the request handler must not do the HTTP call

The naive implementation:

```java
void handleEvent(Event e) {
    db.save(e);
    httpClient.post(customerUrl, e.payload());   // ← everything wrong lives here
}
```

Seven independent failures, each of which this project exists to fix:

| Failure | Result in the naive version |
|---|---|
| Customer's server is down | Event lost forever |
| Customer's server takes 30s | *Your* request handler blocks 30s |
| Your process crashes mid-POST | Event lost, nobody knows |
| Customer returns 500 | No retry, event lost |
| One customer is dead, others fine | Dead one starves the healthy ones |
| Network fails *after* they processed it | You retry, they double-process |
| You deploy a new version | In-flight deliveries dropped |

The fix, in one sentence: **write the work down durably, return immediately, and let a separate
process do the risky part.** Phase 1 is the "write the work down durably, return immediately" half.

### 1.3 What "durable" actually means

"Saved to the database" is doing a lot of work in that sentence. Precisely:

When PostgreSQL commits a transaction, it appends the change to the **Write-Ahead Log (WAL)** and
calls `fsync()` on it — forcing the operating system to push the bytes out of its page cache and
onto physical storage — *before* it reports success. The data pages themselves may still be dirty
in memory. That's fine: on crash, PostgreSQL replays the WAL and reconstructs them.

So "committed" means: *the machine can lose power right now and the write survives.*

This is exactly the guarantee `202 Accepted` is promising. Returning 202 before the commit would be
a lie — we'd be telling the producer "I have your event" when a power cut would erase it. **The
order matters and it is not negotiable: commit, then respond.**

### 1.4 Atomicity: why all the inserts are one transaction

An event with three matching endpoints produces four rows: one `events`, three `deliveries`. If we
wrote them one at a time and crashed after two, we'd have an event that will be delivered to two of
its three endpoints — silently, forever, with no record that a third was ever owed.

A **transaction** makes the group all-or-nothing. The *A* in ACID:

```text
   BEGIN
     INSERT events        (1 row)
     INSERT deliveries    (3 rows)
   COMMIT          ← all 4 become visible at the same instant, or none do
```

There is no intermediate state any other connection can observe. This is why fan-out belongs in the
same transaction as the event, and it's why the blueprint puts fan-out in the API (§4) rather than
having a worker discover subscriptions later.

### 1.5 Idempotency, and why HTTP POST is not idempotent

An operation is **idempotent** if doing it twice has the same effect as doing it once.

`GET /orders/5` — idempotent. `DELETE /orders/5` — idempotent (the second one finds nothing to do).
`POST /orders` — **not** idempotent. It creates a new order each time.

This matters because networks lose responses, not just requests:

```text
   Producer                          HookRelay
      │                                  │
      │──── POST /v1/events ────────────►│
      │                                  │  event committed ✓
      │         ✗ response lost ─────────│
      │                                  │
   (timeout — did it work? no way to know)
      │                                  │
      │──── POST /v1/events (retry) ────►│  ← must NOT create a second event
```

The producer genuinely cannot tell "my request never arrived" from "the response never came back."
Its only safe move is to retry. So *we* have to make the retry safe.

The mechanism is an **idempotency key**: a unique token the producer generates per logical
operation and sends on every retry of it. We remember the key. Second time we see it, we return the
original result instead of doing the work again.

### 1.6 Check-then-act, and why the database must arbitrate

The obvious implementation of an idempotency key is wrong:

```java
if (!repo.existsByIdempotencyKey(key)) {   // ← check
    repo.save(event);                       // ← act
}
```

With two API pods handling the producer's original request and its retry simultaneously:

```text
   time    Pod A                        Pod B
   ────────────────────────────────────────────────────────
    t1     exists(key)? → false
    t2                                  exists(key)? → false
    t3     INSERT event  ✓
    t4                                  INSERT event  ✓   ← duplicate
```

Both pods checked before either wrote. The window between "check" and "act" is where the bug lives,
and no amount of application-level care closes it — the two pods share no memory.

The fix is to stop asking and let the database refuse:

```sql
UNIQUE (tenant_id, idempotency_key)
```

Now `t4` raises a unique-violation. Pod B catches it, re-reads the row Pod A wrote, and returns
*that*. The constraint is evaluated inside the database's own locking, so there is no window.

**The general principle, which recurs throughout this project: when correctness depends on "only
one of these may exist," express it as a database constraint, not an `if`.** Application checks are
advisory; constraints are enforcement.

### 1.7 Why `request_hash` exists

An idempotency key alone has a hole. Suppose a producer bug reuses key `abc123` for a genuinely
different event:

```text
   POST { key: abc123, type: "payment.succeeded", amount: 100 }   → event created
   POST { key: abc123, type: "payment.refunded",  amount: 999 }   → ???
```

If we only check the key, we'd silently return the *first* event and throw the second away. The
producer thinks the refund was recorded. It wasn't. That's data loss disguised as deduplication —
far worse than an error.

So we store a hash of the request body alongside the key and compare:

- same key + **same** hash → genuine retry → return the original event, `200 OK`
- same key + **different** hash → producer bug → refuse loudly, `409 Conflict`

This is what Stripe's API does, and the reasoning is exactly this: silent wrong behaviour is worse
than a loud error.

### 1.8 Fan-out

**Fan-out** is turning one input into many outputs. Here: one event → one delivery per endpoint
whose subscription matches.

```text
   Event  {type: "payment.succeeded", tenant: T}
     │
     ├─► Endpoint A  (active, subscribes to payment.*)      → delivery
     ├─► Endpoint B  (active, subscribes to payment.succeeded) → delivery
     ├─► Endpoint C  (active, subscribes to user.created)   → no match, no row
     └─► Endpoint D  (INACTIVE, subscribes to payment.*)    → no row
```

Per blueprint §8, this is evaluated **once, at accept time**, and frozen. If endpoint C subscribes
to `payment.succeeded` tomorrow, it does not retroactively receive today's event. If endpoint A is
disabled tomorrow, the deliveries already created for it still exist. Replay is a non-goal.

Freezing it is a deliberate simplification with a real benefit: a delivery row is a complete,
self-contained obligation. A worker handling it never has to re-evaluate whether it still should.

### 1.9 Why Testcontainers and not H2

The temptation is to test against H2 (an in-memory Java database) because it's fast and needs no
Docker. It is a trap for this project specifically, because the things we most need to test are the
things H2 emulates differently or not at all:

- `jsonb` — Postgres-specific type
- `text[]` array columns and containment operators
- the exact unique-violation error and SQLSTATE we catch in §1.6
- `SELECT ... FOR UPDATE SKIP LOCKED` (needed in phase 2)
- real transaction isolation and locking semantics

A test suite that passes on H2 and fails in production has actively lied to us. **Testcontainers**
runs a real PostgreSQL in a throwaway Docker container for the test run. Slower to start, correct.

---

## 2. The problem this phase solves

Given a `POST /v1/events` from a producer, and a set of registered endpoints:

1. Validate the request.
2. Decide whether this is a new event or a duplicate submission — correctly, under concurrency.
3. Persist the event.
4. Compute the matching endpoints and create one delivery row per match.
5. Commit 3 and 4 atomically.
6. Return `202 Accepted` with the event id — and only after the commit.

Plus the supporting surface: register an endpoint, list endpoints, look up a delivery's status.

Explicitly **not** in this phase: RabbitMQ, the outbox table, any HTTP delivery, retries, signing.
Deliveries created here sit in `PENDING` and nothing picks them up yet. That is correct and
expected at the end of phase 1.

---

## 3. Design options

### 3.1 Build layout

The blueprint's §31 structure has `api/` and `worker/` as siblings. Both need the same JPA
entities, repositories, and enums, so something has to hold the shared code.

| Option | How | Trade-off |
|---|---|---|
| **A. Maven multi-module: `common` + `api` + `worker`** | Three modules, two Spring Boot apps, two Docker images | Clean separation; each Deployment gets its own image, resource limits, probes, dependency set. One extra module vs the blueprint's diagram. |
| B. One app, mode switch | Single jar, `--hookrelay.mode=api\|worker`, one image, two Deployments | Simplest build and CI (one image to push). But the worker image carries the whole web stack, and "separate services" becomes a runtime flag rather than a real boundary. |
| C. Two fully independent projects | Duplicate the entities in both | No shared-module coupling; guaranteed drift between two copies of the schema mapping. Rejected. |

**Chosen: A.** The blueprint's whole §4 argument is that api and worker are genuinely different
services with different scaling characteristics — the build should reflect that, and phase 7 gets a
better Kubernetes story from two independently-sized images. `common/` is a deviation from the
§31 diagram; flagging it rather than silently adding it.

Maven over Gradle, matching the toolchain already used elsewhere in this portfolio.

### 3.2 Where does fan-out happen?

| Option | Trade-off |
|---|---|
| **In the API, inside the event transaction** | Deliveries are durable the instant the event is. A worker's job is unambiguous: deliver *this* row. Cost: ingest latency scales with fan-out width. |
| In a worker, after the fact | Constant ingest latency. But now there's a window where the event exists and its obligations don't, and something must guarantee the fan-out eventually runs — which is a second reliability problem, identical to the one the outbox solves. |

**Chosen: in the API** — blueprint §4, already settled. Noted here because the cost is real: an
event matching 500 endpoints does 500 inserts before returning 202. Mitigated with a batch insert,
and measured in phase 8's load test (§25 explicitly asks for events/sec *and* deliveries/sec
separately, precisely because of this).

### 3.3 Subscription matching: `text[]` column vs join table

The blueprint specifies `endpoints.event_types text[]`.

- `text[]` + GIN index — matching is `WHERE active AND event_types @> ARRAY['payment.succeeded']`.
  One table, one query, no joins.
- A `subscriptions` join table — more normalized, easier to add per-subscription settings later.

**Chosen: `text[]`**, per the blueprint. It's one query, and per-subscription settings are not in
scope. Wildcard matching (`payment.*`) is deliberately deferred — exact string match only in phase
1, so the query stays a single indexed containment check.

### 3.4 Primary keys: UUIDv4 vs UUIDv7

`deliveries.id` is public (it becomes `X-HookRelay-Delivery-Id`), so it must not be a guessable
sequential integer. That rules out `bigserial` for the public-facing tables.

But random UUIDv4 has a real cost as a primary key. A B-tree index keeps entries sorted; random
keys mean every insert lands in a random page, so the whole index must stay hot in memory and pages
split constantly. Under the insert volume phase 8 aims for (≈10,000 deliveries/sec), this matters.

**UUIDv7** puts a millisecond timestamp in the high bits, so values are time-ordered. Inserts append
to the right edge of the index, like a sequence, while staying unguessable.

**Chosen: UUIDv7**, generated in the application (we need the id before insert anyway, to build the
outbox row in phase 2). Java 21 has no built-in v7 generator; a ~20-line helper in `common` is
enough, and it becomes a nice thing to be able to explain.

### 3.5 Schema migrations

**Chosen: Flyway.** Plain SQL files, versioned, applied in order, recorded in a
`flyway_schema_history` table. `spring.jpa.hibernate.ddl-auto` is set to `validate` — Hibernate
checks that the entities match the migrated schema and refuses to start otherwise, but never
alters the database itself. Schema changes only ever happen through a numbered migration file.

### 3.6 `delivery_status`: a native PostgreSQL ENUM vs `text` + CHECK

The blueprint's data model writes `status enum(PENDING, IN_FLIGHT, ...)`. PostgreSQL can express
that literally, with `CREATE TYPE delivery_status AS ENUM (...)`. That was the original plan here
and it was changed during implementation.

The problem is adding a value later. `ALTER TYPE ... ADD VALUE` could not run inside a transaction
block at all before PostgreSQL 12, and even now the new value cannot be *used* in the same
transaction that added it. Flyway runs each migration in a transaction, so a future migration that
adds a status and backfills rows with it has to be split apart and marked non-transactional — a
sharp edge waiting in a migration nobody will be thinking hard about. It also needs
`@JdbcTypeCode(SqlTypes.NAMED_ENUM)` on the Hibernate side to map cleanly.

**Chosen: `text` with a `CHECK` constraint**, mapped with `@Enumerated(EnumType.STRING)`. It gives
the identical guarantee — the database rejects any value outside the set — while a future status is
a one-line change to the CHECK. This is a deviation from the blueprint's literal wording; the
integrity property the blueprint was actually asking for is preserved.

### 3.7 Payload storage: `jsonb` vs `text`

**Chosen: `jsonb`**, per the blueprint. It is parsed and validated on write (malformed JSON is
rejected by the database, not just by us), stored in a binary form, and queryable. `text` would
store whatever bytes we hand it, including invalid JSON.

Note for later: the *signed* body in phase 3 must be the exact bytes we send, and `jsonb`
normalizes (reorders keys, strips whitespace). So the HMAC must be computed over the serialized
form we actually transmit, not over the original request bytes. Recording that here so phase 3
doesn't get it wrong.

---

## 4. Chosen design

### 4.1 Schema — `V1__init.sql`

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE endpoints (
    id              uuid PRIMARY KEY,
    tenant_id       uuid        NOT NULL,
    url             text        NOT NULL,
    secret          text        NOT NULL,
    event_types     text[]      NOT NULL,
    active          boolean     NOT NULL DEFAULT true,
    max_concurrency int         NOT NULL DEFAULT 5,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT endpoints_max_concurrency_positive CHECK (max_concurrency > 0)
);

-- fan-out query: active endpoints for a tenant subscribing to this event type
CREATE INDEX endpoints_fanout_idx
    ON endpoints USING gin (event_types)
    WHERE active;
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id) WHERE active;

CREATE TABLE events (
    id              uuid PRIMARY KEY,
    tenant_id       uuid        NOT NULL,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT events_idempotency_uq UNIQUE (tenant_id, idempotency_key)
);

CREATE TYPE delivery_status AS ENUM (
    'PENDING', 'IN_FLIGHT', 'SUCCEEDED', 'FAILED', 'DEAD'
);

CREATE TABLE deliveries (
    id              uuid PRIMARY KEY,
    event_id        uuid        NOT NULL REFERENCES events(id),
    endpoint_id     uuid        NOT NULL REFERENCES endpoints(id),
    status          delivery_status NOT NULL DEFAULT 'PENDING',
    attempt_count   int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT deliveries_event_endpoint_uq UNIQUE (event_id, endpoint_id)
);

CREATE INDEX deliveries_status_idx ON deliveries (status);
CREATE INDEX deliveries_event_idx  ON deliveries (event_id);

CREATE TABLE delivery_attempts (
    id              bigserial PRIMARY KEY,
    delivery_id     uuid        NOT NULL REFERENCES deliveries(id),
    attempt_no      int         NOT NULL,
    started_at      timestamptz NOT NULL,
    duration_ms     int         NOT NULL,
    response_status int,
    error_class     text,
    response_body   text,
    CONSTRAINT delivery_attempts_uq UNIQUE (delivery_id, attempt_no),
    CONSTRAINT delivery_attempts_body_len CHECK (length(response_body) <= 512)
);

CREATE INDEX delivery_attempts_delivery_idx ON delivery_attempts (delivery_id);
```

Two constraints worth calling out, both of which encode a rule from §1.6 rather than trusting code:

- `deliveries_event_endpoint_uq` — an event can owe an endpoint *at most one* delivery. If phase 2's
  at-least-once outbox publisher ever causes the fan-out to run twice, the database refuses instead
  of silently double-delivering.
- `delivery_attempts_body_len` — blueprint §16's 512-byte cap enforced in the schema, so a bug in
  the worker's truncation logic surfaces as a failed insert rather than an unbounded column.

### 4.2 API surface

```text
POST   /v1/endpoints          register an endpoint
GET    /v1/endpoints          list endpoints for the tenant
GET    /v1/endpoints/{id}     fetch one
DELETE /v1/endpoints/{id}     deactivate (soft — sets active=false)

POST   /v1/events             accept an event      ← the important one
GET    /v1/events/{id}        fetch event + its deliveries

GET    /v1/deliveries/{id}    delivery status + attempt history
```

`POST /v1/events`:

```http
POST /v1/events
Idempotency-Key: 5f3a...           (required)
X-Tenant-Id: 7c9e...               (stand-in for auth; see §4.5)
Content-Type: application/json

{ "event_type": "payment.succeeded",
  "payload": { "order_id": "A-1", "amount": 4200 } }
```

```http
202 Accepted
{ "event_id": "018f...", "deliveries_created": 3 }
```

Responses:

| Status | When |
|---|---|
| `202 Accepted` | New event accepted, fan-out committed |
| `200 OK` | Duplicate: same key, same `request_hash` — returns the original `event_id` |
| `409 Conflict` | Same key, **different** `request_hash` (§1.7) |
| `400 Bad Request` | Missing `Idempotency-Key`, unknown/blank `event_type`, malformed JSON |
| `422 Unprocessable` | Well-formed but invalid (e.g. payload exceeds the size cap) |

`202` vs `200` is a deliberate signal: the producer can tell "I created this" from "this already
existed" without parsing anything.

**`deliveries_created: 0` is a success, not an error.** An event nobody subscribes to is a valid
event. Returning an error would push producers toward not sending events, which is the opposite of
what we want.

### 4.3 The ingest transaction

```text
   POST /v1/events
        │
        ├─ validate headers + body                       (no DB)
        ├─ request_hash = sha256(tenant | type | canonical_payload)
        │
        ├─ BEGIN ─────────────────────────────────────────────────┐
        │    INSERT INTO events (...)                             │
        │      └─ unique violation? ──► rollback, go to (D)       │
        │                                                          │
        │    SELECT id, max_concurrency FROM endpoints             │
        │      WHERE tenant_id = ? AND active                      │
        │        AND event_types @> ARRAY[?]                       │
        │                                                          │
        │    INSERT INTO deliveries (...)  ← batch, one per match  │
        │  COMMIT ────────────────────────────────────────────────┘
        │
        └─ 202 Accepted
```

Duplicate path (D):

```text
   (D) unique violation on events_idempotency_uq
        │
        ├─ SELECT * FROM events WHERE tenant_id=? AND idempotency_key=?
        │
        ├─ request_hash matches?  ──► 200 OK  + original event_id
        └─ request_hash differs?  ──► 409 Conflict
```

Three details that are easy to get wrong:

1. **Catch the violation, don't pre-check.** Spring surfaces it as
   `DataIntegrityViolationException`. The catch block must distinguish *which* constraint fired —
   inspect the underlying `PSQLException`'s constraint name rather than assuming. Catching every
   integrity violation as "duplicate idempotency key" would misreport an FK failure as a 200.

2. **The re-read must be a separate transaction.** Once PostgreSQL raises an error inside a
   transaction, that transaction is aborted — every subsequent statement fails with
   `current transaction is aborted` until rollback. So: roll back, then read in a new transaction.
   This is a Postgres-specific behaviour that does not reproduce on H2 (§1.9).

3. **`request_hash` must be computed over a canonical form.** `{"a":1,"b":2}` and `{"b":2,"a":1}`
   are the same event; if the hash differs by key order, an honest retry that re-serializes its
   payload gets a spurious 409. Canonicalize: sorted keys, no insignificant whitespace, before
   hashing.

### 4.4 Fan-out cost

One event matching N endpoints does N inserts inside the request. Batched via
`rewriteBatchedInserts=true` on the JDBC URL plus a single multi-row insert, so it's one round trip
rather than N. The relationship between ingest latency and fan-out width is exactly what phase 8
measures.

### 4.5 Tenancy and auth — scoped out, honestly

Every table carries `tenant_id`, and the API reads it from an `X-Tenant-Id` header. There is **no
authentication in phase 1** — anyone who can reach the API can claim any tenant.

This is a deliberate scope decision, not an oversight: blueprint §2 lists OAuth endpoint
authentication as a non-goal, and the project's four engineering stories are about delivery
reliability, not auth. Recorded here so it's stated plainly rather than discovered later, and so
the README says so too. If it ever needs closing, an API-key filter resolving key → `tenant_id` is
the minimal fix.

### 4.6 Module layout

```text
hookrelay/
├── pom.xml                  parent, dependencyManagement, Java 21
├── common/
│   └── src/main/java/.../common/
│       ├── domain/          Event, Endpoint, Delivery, DeliveryAttempt, DeliveryStatus
│       ├── repo/            Spring Data repositories
│       └── util/            Uuid7, CanonicalJson, Hashing
├── api/
│   └── src/
│       ├── main/java/.../api/
│       │   ├── web/         controllers, DTOs, exception handler
│       │   └── service/     IngestService, EndpointService
│       ├── main/resources/db/migration/V1__init.sql
│       └── test/java/       Testcontainers integration tests
└── worker/                  (empty until phase 3)
```

Flyway migrations live in `api` — the API owns the schema and is the only thing that runs
migrations. The worker validates against it but never changes it.

### 4.7 Tests

Integration tests against real PostgreSQL via Testcontainers, with a single shared container reused
across the class:

| Test | Asserts |
|---|---|
| accept event, no endpoints | 202, event row exists, 0 deliveries |
| accept event, 3 matching endpoints | 202, 3 delivery rows, all `PENDING` |
| inactive endpoint excluded | not in fan-out |
| non-matching event type excluded | not in fan-out |
| duplicate key, same body | 200, same `event_id`, **no new deliveries** |
| duplicate key, different body | 409, nothing written |
| missing `Idempotency-Key` | 400 |
| malformed JSON payload | 400 |
| key reused across *different* tenants | both succeed — the constraint is composite |
| **20 concurrent POSTs, same key** | exactly 1 event row, exactly N deliveries, 19 responses are 200 |
| payload key order differs on retry | 200, not 409 (canonicalization works) |

The concurrency test is the one that matters. It is the only test that would actually catch a
regression back to check-then-act (§1.6), and it must run against real Postgres to mean anything.

---

## 5. Implementation plan

1. Parent `pom.xml`, Java 21, Spring Boot 3.x BOM. Modules `common`, `api`, `worker`.
2. `common`: entities, enums, repositories, `Uuid7`, `CanonicalJson`, `Hashing`.
3. `api`: `V1__init.sql`, Flyway wired, `ddl-auto: validate`.
4. `docker-compose.yml` with PostgreSQL for local development.
5. `EndpointService` + `POST/GET/DELETE /v1/endpoints`.
6. `IngestService` — the transaction and the duplicate path of §4.3.
7. `POST /v1/events`, `GET /v1/events/{id}`, `GET /v1/deliveries/{id}`.
8. `@RestControllerAdvice` mapping exceptions to the §4.2 status codes.
9. Testcontainers suite, §4.7, concurrency test last.
10. Update `REFERENCE.md` with the API and config.

---

## 6. Experiment — does the constraint actually matter?

The claim in §1.6 is that the database constraint, not the application code, is what makes ingest
idempotency correct under concurrency. That is testable, so it was tested rather than asserted.

**Method.** Twenty threads submit the byte-identical request with the same `Idempotency-Key`
simultaneously, released together from a `CountDownLatch`, against a real PostgreSQL. Run twice:

- **A — check-then-act.** `events_idempotency_uq` removed from `V1__init.sql` (replaced with a plain
  non-unique index so lookups still work), and `IngestService.ingest` given the pre-check the design
  deliberately avoids: `if (findByTenantIdAndIdempotencyKey(...).isPresent()) return duplicate;`
- **B — as shipped.** Unique constraint present, no pre-check, violation caught and resolved.

**Result.**

| Measurement | A: check-then-act, no constraint | B: unique constraint (shipped) | Correct |
|---|---:|---:|---:|
| Event rows created | **16** | **1** | 1 |
| Delivery rows created | **32** | **2** | 2 |
| HTTP 202 (created) | 16 | 1 | 1 |
| HTTP 200 (duplicate observed) | 0 | 19 | 19 |
| HTTP 5xx | **4** | **0** | 0 |
| Distinct event ids returned to callers | 16 | 1 | 1 |

Sixteen of twenty threads passed the existence check before any of them had committed, so sixteen
duplicate events were created, and the fan-out ran sixteen times instead of once. In production this
is not a tidy "duplicate row" problem: it means a customer's endpoint receives the same webhook
sixteen times, and sixteen different `event_id`s are handed back to callers who each believe theirs
is authoritative.

**The unplanned part of the result — corruption compounds.** The 4 HTTP 5xx responses in run A were
not the writes failing. They were *reads* failing:

```
NonUniqueResultException: Query did not return a unique result: 9 results were returned
```

`EventRepository.findByTenantIdAndIdempotencyKey` returns `Optional<Event>`, whose contract is "at
most one." That contract was only ever true because of the unique constraint. Once duplicates
exist, the lookup path that is supposed to *recover* from a duplicate submission throws instead —
so the system loses its ability to deduplicate at exactly the moment it most needs to. The
signature `Optional<Event>` is, in effect, the constraint restated in Java, and it silently becomes
a lie the instant the database stops enforcing it.

**Reproduce.** Revert the two files as described, then:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl common,api -Dtest=IngestIntegrationTest#concurrentIdenticalSubmissionsCreateExactlyOneEvent -Dsurefire.failIfNoSpecifiedTests=false test
```

The shipped configuration keeps this permanently as
`IngestIntegrationTest#concurrentIdenticalSubmissionsCreateExactlyOneEvent`. It is the only test in
the phase that would catch a regression back to check-then-act, and it is meaningless against
anything but a real database — which is the concrete justification for §1.9's rejection of H2.

---

## 7. Failures

**Nothing broke during implementation.** The module compiled on the first attempt and all 37 tests
passed on their first run. Recording that plainly rather than inventing a struggle — but it is worth
being suspicious of, so: the suite was verified to be capable of failing by running it against the
deliberately-broken configuration in §6, where it reported 16 events instead of 1. The tests do
detect the thing they claim to detect.

Two things were changed from the pre-implementation design once the code was real:

1. **`delivery_status` as a native PostgreSQL ENUM → `text` + CHECK.** See §3.6. The migration
   ergonomics of `ALTER TYPE ... ADD VALUE` under Flyway's transactional migrations were the
   deciding factor, not the Hibernate mapping.
2. **The `common` module.** Flagged in §3.1 as a deviation from the blueprint's §31 repository
   layout, which lists only `api/` and `worker/`. Both need the same entities and repositories, and
   duplicating them would guarantee drift.

The one genuine discovery was the `NonUniqueResultException` cascade in §6 — that was not
anticipated when the experiment was designed. The experiment was set up to measure duplicate rows;
it also revealed that duplicate rows break the recovery path, which is a strictly worse failure
than the one being measured.

---

## 8. Lessons learned

**A `UNIQUE` constraint and an `Optional<T>` return type are the same claim, written twice.** One
is enforced, the other is assumed. When the enforced one is removed, the assumed one does not
degrade gracefully — it throws. Any repository method returning `Optional` is implicitly relying on
a database constraint, and it is worth knowing which one.

**"Check, then act" is not a code smell here, it is a correctness bug, and the difference is
invisible on a single node.** Run the 20-thread test against one API process and it still fails —
16 duplicates — because the window is between two *database* operations, not two threads in one
JVM. There is no amount of Java-level synchronization that closes it, because the second replica
does not share your locks.

**Canonicalization is what makes `request_hash` safe to enforce.** Hashing raw request bytes would
have been simpler and would have rejected honest retries whose JSON serializer emitted keys in a
different order — converting a safety mechanism into a source of spurious 409s. The
`payloadKeyOrderDoesNotCauseSpuriousConflict` and `arrayOrderIsSignificant` tests pin both halves:
object key order must not matter, array order must.

**Flush early to fail cheaply.** `IngestTransactions` flushes the event insert *before* running
fan-out. Ordering it the other way is equally correct — the transaction rolls back either way — but
it means doing N delivery inserts before discovering the event was a duplicate. Under a retry storm,
that is the difference between wasted work proportional to fan-out width and none.

**Carrying `next_attempt_at` from phase 1 is a trap worth labelling now.** The column exists because
the blueprint's data model has it, but from phase 4 the retry schedule lives in RabbitMQ's delay
queues. It is recorded in `Delivery`'s javadoc as observability, not a scheduler, so that phase 4
does not accidentally grow a database poller competing with the broker for the same deliveries.

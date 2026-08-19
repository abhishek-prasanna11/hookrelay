-- HookRelay phase 1: ingest schema.
--
-- Constraints here are not decoration. Each one encodes a correctness rule that would otherwise
-- have to be enforced by application code, which cannot hold across multiple API replicas.

CREATE TABLE endpoints (
    id              uuid        PRIMARY KEY,
    tenant_id       uuid        NOT NULL,
    url             text        NOT NULL,
    secret          text        NOT NULL,
    event_types     text[]      NOT NULL,
    active          boolean     NOT NULL DEFAULT true,
    max_concurrency int         NOT NULL DEFAULT 5,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT endpoints_max_concurrency_positive CHECK (max_concurrency > 0),
    CONSTRAINT endpoints_event_types_not_empty    CHECK (cardinality(event_types) > 0)
);

-- Fan-out reads active endpoints for one tenant that subscribe to one event type. GIN supports the
-- array containment operator (@>) that query uses; a btree index would not.
CREATE INDEX endpoints_event_types_idx ON endpoints USING gin (event_types);
CREATE INDEX endpoints_tenant_idx      ON endpoints (tenant_id) WHERE active;

CREATE TABLE events (
    id              uuid        PRIMARY KEY,
    tenant_id       uuid        NOT NULL,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),

    -- The whole of ingest idempotency rests on this one line. Two API replicas racing on the same
    -- key are arbitrated here, inside PostgreSQL's own locking, rather than by an application-level
    -- existence check that has a window between "check" and "act".
    CONSTRAINT events_idempotency_uq UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC);

CREATE TABLE deliveries (
    id              uuid        PRIMARY KEY,
    event_id        uuid        NOT NULL REFERENCES events (id),
    endpoint_id     uuid        NOT NULL REFERENCES endpoints (id),
    status          text        NOT NULL DEFAULT 'PENDING',
    attempt_count   int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT deliveries_status_valid CHECK (
        status IN ('PENDING', 'IN_FLIGHT', 'SUCCEEDED', 'FAILED', 'DEAD')
    ),
    CONSTRAINT deliveries_attempt_count_non_negative CHECK (attempt_count >= 0),

    -- An event owes each endpoint at most one delivery. This is a safety net for phase 2: the
    -- outbox publisher is at-least-once, so if a duplicate publication ever caused fan-out to run
    -- twice, the database refuses rather than silently double-delivering.
    CONSTRAINT deliveries_event_endpoint_uq UNIQUE (event_id, endpoint_id)
);

CREATE INDEX deliveries_status_idx   ON deliveries (status);
CREATE INDEX deliveries_event_idx    ON deliveries (event_id);
CREATE INDEX deliveries_endpoint_idx ON deliveries (endpoint_id);

CREATE TABLE delivery_attempts (
    id              bigserial   PRIMARY KEY,
    delivery_id     uuid        NOT NULL REFERENCES deliveries (id),
    attempt_no      int         NOT NULL,
    started_at      timestamptz NOT NULL,
    duration_ms     int         NOT NULL,
    response_status int,
    error_class     text,
    response_body   text,

    CONSTRAINT delivery_attempts_uq        UNIQUE (delivery_id, attempt_no),
    CONSTRAINT delivery_attempts_no_valid  CHECK (attempt_no > 0),

    -- Blueprint section 16 caps retained response bodies at 512 bytes so a hostile endpoint cannot
    -- grow our storage without bound. Enforcing it here means a truncation bug in the worker fails
    -- the insert instead of writing an unbounded value.
    CONSTRAINT delivery_attempts_body_len  CHECK (length(response_body) <= 512)
);

CREATE INDEX delivery_attempts_delivery_idx ON delivery_attempts (delivery_id, attempt_no);

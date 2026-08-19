-- HookRelay phase 2: the transactional outbox.
--
-- The database and RabbitMQ cannot be written atomically. Rather than accepting a window where one
-- succeeded and the other did not, the *intent* to publish is recorded here, in the same
-- transaction as the deliveries it describes. A separate publisher drains it afterwards and can
-- retry forever, because the row survives until the broker confirms the message.

CREATE TABLE outbox_events (
    id            uuid        PRIMARY KEY,
    delivery_id   uuid        NOT NULL REFERENCES deliveries (id),
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz,
    attempt_count int         NOT NULL DEFAULT 0,
    last_error    text,

    -- One queue message per delivery. Together with deliveries_event_endpoint_uq from V1, a
    -- re-executed fan-out cannot produce a second message for work that is already queued.
    CONSTRAINT outbox_delivery_uq UNIQUE (delivery_id),
    CONSTRAINT outbox_attempt_count_non_negative CHECK (attempt_count >= 0)
);

-- Partial index. The poll query only ever looks at unpublished rows, so only those are indexed:
-- the index stays small and fast regardless of how many published rows are waiting to be purged.
-- A full index would carry every published row for no benefit.
CREATE INDEX outbox_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

-- Supports the purge, which scans by published_at.
CREATE INDEX outbox_published_idx
    ON outbox_events (published_at)
    WHERE published_at IS NOT NULL;

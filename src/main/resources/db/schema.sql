CREATE TABLE IF NOT EXISTS shipments (
    id           UUID PRIMARY KEY,
    reference    TEXT NOT NULL UNIQUE,
    destination  TEXT NOT NULL,
    booked_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id             BIGSERIAL PRIMARY KEY,
    message_id     UUID NOT NULL UNIQUE,
    aggregate_id   UUID NOT NULL,
    message_type   TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INT NOT NULL DEFAULT 0,
    last_error     TEXT
);

-- The relay only ever asks for unpublished rows, so the index only carries
-- those. Once a row is published it drops out of the index instead of
-- growing it forever.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;

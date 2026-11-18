-- Initialization for the device fault log platform (runs once on an empty data dir).
--
-- 1) device_fault_logs: hot table that keeps receiving writes while the index
--    is being built online. request_id is the idempotency key used by the
--    reliable-write API (NULL for plain /logs writes); its UNIQUE index is
--    what makes concurrent identical submissions and receipt-loss retries
--    insert exactly one row.
-- 2) index_build_jobs:  ledger used by the stateless API instances to
--    coordinate online index builds (single-winner claim + fencing).

CREATE TABLE IF NOT EXISTS device_fault_logs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    severity    TEXT        NOT NULL DEFAULT 'error',
    message     TEXT        NOT NULL,
    request_id  TEXT
);

-- Preload historical fault logs so that the online index build is non-trivial.
INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)
SELECT 'dev-' || lpad((floor(random() * 2000))::int::text, 5, '0'),
       now() - (random() * interval '180 days'),
       (ARRAY['info', 'warn', 'error', 'critical'])[floor(random() * 4 + 1)],
       'preloaded fault event #' || g
FROM generate_series(1, 4000000) AS g;

-- Idempotency key for reliable writes. NULLs (plain /logs rows) are distinct
-- and never conflict. Created after the preload so the bulk insert does not
-- have to maintain it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fault_logs_request_id
    ON device_fault_logs (request_id);

-- One row per managed index. The UNIQUE constraint on index_name is what
-- guarantees that concurrent build requests across all API instances produce
-- exactly one claimed build.
CREATE TABLE IF NOT EXISTS index_build_jobs (
    id          UUID PRIMARY KEY,
    index_name  TEXT        NOT NULL UNIQUE,
    status      TEXT        NOT NULL CHECK (status IN ('building', 'ready', 'failed')),
    builder     TEXT,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

VACUUM ANALYZE device_fault_logs;

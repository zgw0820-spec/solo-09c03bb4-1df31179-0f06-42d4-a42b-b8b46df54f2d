"""Database connection helpers.

The app intentionally uses short-lived connections instead of a pool: request
volume is modest, and the index-build path needs plain autocommit connections
anyway (CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction block).
"""

import os
import time

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ops:ops@localhost:5432/faultlogs"
)

# Fail fast when the database is down: the reliable-write path depends on a
# quick connect failure to fall back to durable staging within seconds.
CONNECT_TIMEOUT = float(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "5"))

# Serializes the idempotent schema migration across instances at startup.
MIGRATION_LOCK_KEY = 861_227_002


def connect(autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, autocommit=autocommit,
                           connect_timeout=CONNECT_TIMEOUT)


def wait_for_schema(timeout_s: int = 240) -> None:
    """Block until the init script has created the schema.

    The Postgres container reports healthy via pg_isready before the init
    scripts have finished, so API instances must tolerate the tables not
    existing yet for a while.
    """
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while True:
        attempt += 1
        try:
            with connect() as conn:
                conn.execute("SELECT 1 FROM index_build_jobs LIMIT 1")
                conn.execute("SELECT 1 FROM device_fault_logs LIMIT 1")
            return
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError("database schema not ready in time") from exc
            time.sleep(min(5.0, 0.5 * attempt))


def migrate() -> None:
    """Idempotently add what the reliable-write path needs.

    Fresh databases get this from db/init.sql; the guarded statements here
    upgrade data directories created by an older release. The advisory lock
    serializes concurrent startup migrations across API instances.
    """
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_KEY,))
        try:
            conn.execute(
                "ALTER TABLE device_fault_logs "
                "ADD COLUMN IF NOT EXISTS request_id TEXT"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_fault_logs_request_id "
                "ON device_fault_logs (request_id)"
            )
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_KEY,))

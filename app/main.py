"""FastAPI surface: fault-log writes/queries + online index build operations."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from . import index_manager as im
from . import reliable
from .db import connect, migrate, wait_for_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Wait for the init script, then make tracked state consistent with the
    # actual index before serving traffic.
    wait_for_schema()
    migrate()
    im.reconcile()
    reliable.start_backfill()
    log.info("instance %s ready", im.INSTANCE_ID)
    yield
    reliable.stop_backfill()


app = FastAPI(
    title="Device Fault Log Ops",
    version="1.0.0",
    description=(
        "Fault-log ingestion plus coordinated online (CONCURRENTLY) index "
        "builds across multiple stateless API instances."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "instance": im.INSTANCE_ID}


# ---------------------------------------------------------------------------
# Fault logs
# ---------------------------------------------------------------------------

class LogIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4000)
    severity: str = Field(default="error", max_length=16)
    occurred_at: Optional[datetime] = None


@app.post("/logs", status_code=201, tags=["logs"])
def create_log(entry: LogIn):
    with connect() as conn:
        row = conn.execute(
            """
            INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)
            VALUES (%s, COALESCE(%s, now()), %s, %s)
            RETURNING id, occurred_at
            """,
            (entry.device_id, entry.occurred_at, entry.severity, entry.message),
        ).fetchone()
    return {"id": row[0], "occurred_at": row[1]}


@app.get("/logs", tags=["logs"])
def list_logs(
    device_id: str,
    start: datetime,
    end: datetime,
    limit: int = Query(100, ge=1, le=1000),
):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, device_id, occurred_at, severity, message
            FROM device_fault_logs
            WHERE device_id = %s AND occurred_at >= %s AND occurred_at <= %s
            ORDER BY occurred_at DESC
            LIMIT %s
            """,
            (device_id, start, end, limit),
        ).fetchall()
    return {
        "count": len(rows),
        "items": [
            {"id": r[0], "device_id": r[1], "occurred_at": r[2],
             "severity": r[3], "message": r[4]}
            for r in rows
        ],
    }


@app.get("/logs/plan", tags=["logs"])
def logs_plan(device_id: str, start: datetime, end: datetime):
    """EXPLAIN the device/time-range query (shows whether the index is used)."""
    with connect() as conn:
        rows = conn.execute(
            """
            EXPLAIN (COSTS FALSE)
            SELECT id, device_id, occurred_at, severity, message
            FROM device_fault_logs
            WHERE device_id = %s AND occurred_at >= %s AND occurred_at <= %s
            ORDER BY occurred_at DESC
            LIMIT 100
            """,
            (device_id, start, end),
        ).fetchall()
    return {"plan": [r[0] for r in rows]}


# ---------------------------------------------------------------------------
# Reliable writes (request_id idempotency + outage staging)
# ---------------------------------------------------------------------------

class ReliableLogIn(BaseModel):
    request_id: str = Field(
        min_length=1, max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
        description="Client-chosen idempotency key (safe as a file name).",
    )
    device_id: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4000)
    severity: str = Field(default="error", max_length=16)
    occurred_at: Optional[datetime] = Field(
        default=None,
        description="Fixed at acceptance time when omitted; the backfill "
                    "never rewrites it to the recovery time.",
    )


@app.post("/logs/reliable", tags=["reliable"],
          responses={201: {"description": "Persisted in the database"},
                     202: {"description": "Durably staged for backfill"},
                     409: {"description": "request_id exists with different content"},
                     503: {"description": "Neither database nor staging succeeded"}})
def create_log_reliable(entry: ReliableLogIn, response: Response):
    """Reliably accept one fault log, even during a PostgreSQL outage.

    The record is acknowledged (201/202) only after it is durably persisted:
    inserted into PostgreSQL, or fsynced to the shared staging directory for
    automatic backfill. If both fail, the request is rejected with 503.

    Idempotent on `request_id`: concurrent identical submissions and retries
    after a lost receipt insert exactly one row; the same `request_id` with
    different content is rejected with 409.
    """
    rec = reliable.new_record(entry.request_id, entry.device_id, entry.message,
                              entry.severity, entry.occurred_at)
    code, body = reliable.accept(rec)
    response.status_code = code
    return body


@app.get("/logs/reliable", tags=["reliable"])
def reliable_overview():
    """Staging-queue overview: genuinely pending vs already-persisted
    leftovers awaiting cleanup, conflicts, and backfill loop health."""
    return reliable.overview()


@app.get("/logs/reliable/{request_id}", tags=["reliable"],
         responses={404: {"description": "Unknown request_id"},
                    503: {"description": "State unknown (database unreachable)"}})
def reliable_status(request_id: str, response: Response):
    """Status of one reliable write: `persisted` (in the database, visible to
    the normal query API), `staged` (waiting for backfill), `conflicted`
    (staged copy contradicts the database row) or `not_found`."""
    code, body = reliable.get_request_status(request_id)
    response.status_code = code
    return body


# ---------------------------------------------------------------------------
# Online index build operations
# ---------------------------------------------------------------------------

@app.post("/indexes/build", tags=["index"])
def build_index():
    """Request the (device_id, occurred_at) index build.

    Idempotent: concurrent callers collapse onto a single build; once the
    catalog confirms the index is valid, callers get 'ready'.
    """
    return im.request_build()


@app.get("/indexes/status", tags=["index"])
def index_status():
    return im.get_status()


@app.post("/indexes/cleanup", tags=["index"])
def index_cleanup():
    """Drop invalid leftover indexes (only when no build is active)."""
    return im.cleanup()

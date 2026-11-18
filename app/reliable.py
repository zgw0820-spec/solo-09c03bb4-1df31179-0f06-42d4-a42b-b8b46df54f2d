"""Reliable fault-log ingestion with request_id idempotency and outage staging.

Goal: the monitoring gateway must be able to submit fault logs even while
PostgreSQL is briefly unavailable, and the on-duty operator must find those
records through the normal query API once the database is back.

Accept path (POST /logs/reliable):

  1. ``occurred_at`` is fixed at acceptance time (UTC now when the client did
     not supply one). The value is stored with the record and reused verbatim
     by the backfill -- a recovered database never rewrites it to the
     recovery time.
  2. Try the database first: ``INSERT ... ON CONFLICT (request_id) DO
     NOTHING``. The UNIQUE index on request_id makes concurrent identical
     submissions (across all instances) and receipt-loss retries insert
     exactly one row. A conflicting row with *different* content is rejected
     with 409.
  3. If the database is unreachable, durably stage the record to the shared
     staging directory: write tmp file, fsync, atomic hard-link to the final
     name, fsync the directory. Only after that do we answer 202 accepted.
     The link is atomic and never overwrites, so two instances staging the
     same request_id concurrently collapse onto one file; different content
     for the same request_id is rejected with 409.
  4. If the database insert failed AND durable staging failed -> 503.

Idempotency identity: the client-supplied fields (request_id, device_id,
severity, message, and occurred_at ONLY when the client supplied it). A
retry that omitted occurred_at therefore matches the originally accepted
record -- it gets the original acceptance timestamp back instead of a
spurious 409. A stored record is never modified by a replay.

Backfill (background thread in every instance, restart-safe because the
staging directory is a persistent shared volume):

  scan staged files -> INSERT ... ON CONFLICT -> on success, or when the row
  already exists with identical content (a previous instance inserted it but
  died before removing the staged copy), remove the file. A staged record
  whose request_id exists in the database with *different* content is
  quarantined to ``<request_id>.json.conflict.<ts>`` and surfaced via the
  status API -- it is never silently applied and never retried forever.

Both instances run the loop against the same directory; inserts are
idempotent and unlink ignores missing files, so no locking is needed.
"""

import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone

from .db import connect
from .index_manager import INSTANCE_ID

log = logging.getLogger("reliable")

STAGING_DIR = os.environ.get("STAGING_DIR", "/staging")
BACKFILL_INTERVAL = float(os.environ.get("BACKFILL_INTERVAL_SECONDS", "2"))
TMP_MAX_AGE = float(os.environ.get("STAGING_TMP_MAX_AGE_SECONDS", "60"))

REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

INSERT_SQL = """
INSERT INTO device_fault_logs (request_id, device_id, occurred_at, severity, message)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (request_id) DO NOTHING
RETURNING id
"""
SELECT_SQL = """
SELECT id, device_id, occurred_at, severity, message
FROM device_fault_logs
WHERE request_id = %s
"""


class ContentConflict(Exception):
    """request_id already exists with different content (db or staging)."""

    def __init__(self, where: str, existing: dict):
        self.where = where
        self.existing = existing
        super().__init__(f"request_id already exists in {where} with different content")


class StagingError(Exception):
    """The record could not be durably staged."""


class CorruptFile(StagingError):
    """A staged file failed to parse or its hash does not match."""


# ---------------------------------------------------------------------------
# Record canonicalisation
# ---------------------------------------------------------------------------

def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def new_record(request_id: str, device_id: str, message: str,
               severity: str, occurred_at) -> dict:
    """Build the canonical record; occurred_at is fixed HERE (acceptance)."""
    return {
        "request_id": request_id,
        "device_id": device_id,
        "occurred_at": _utc(occurred_at) if occurred_at is not None
                       else datetime.now(timezone.utc),
        "occurred_at_provided": occurred_at is not None,
        "severity": severity,
        "message": message,
    }


def identity_payload(rec: dict) -> dict:
    """The fields idempotency compares: everything the client supplied.
    occurred_at only counts when the client supplied it -- otherwise it is a
    server-assigned acceptance timestamp and must not turn a receipt-loss
    retry into a false conflict."""
    p = {
        "request_id": rec["request_id"],
        "device_id": rec["device_id"],
        "severity": rec["severity"],
        "message": rec["message"],
    }
    if rec.get("occurred_at_provided"):
        p["occurred_at"] = _utc(rec["occurred_at"]).isoformat()
    return p


def content_hash(rec: dict) -> str:
    blob = json.dumps(identity_payload(rec), sort_keys=True,
                      separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def stored_payload(rec: dict) -> dict:
    """The full record as persisted/staged (occurred_at always present)."""
    return {
        "request_id": rec["request_id"],
        "device_id": rec["device_id"],
        "occurred_at": _utc(rec["occurred_at"]).isoformat(),
        "severity": rec["severity"],
        "message": rec["message"],
    }


def _row_matches(rec: dict, row) -> bool:
    """row: (id, device_id, occurred_at, severity, message)"""
    if row[1] != rec["device_id"] or row[3] != rec["severity"] \
            or row[4] != rec["message"]:
        return False
    if rec.get("occurred_at_provided"):
        return _utc(row[2]) == _utc(rec["occurred_at"])
    return True


def _row_to_record(request_id: str, row) -> dict:
    return {
        "request_id": request_id,
        "device_id": row[1],
        "occurred_at": _utc(row[2]).isoformat(),
        "severity": row[3],
        "message": row[4],
    }


# ---------------------------------------------------------------------------
# Database insert (idempotent on request_id)
# ---------------------------------------------------------------------------

def db_insert(rec: dict):
    """Insert the record; returns ("inserted"|"duplicate", row_id, occurred_iso).

    Raises ContentConflict if request_id exists with different content,
    or any psycopg error if the database is unreachable/broken.
    """
    params = (rec["request_id"], rec["device_id"], _utc(rec["occurred_at"]),
              rec["severity"], rec["message"])
    with connect() as conn:
        row = conn.execute(INSERT_SQL, params).fetchone()
        if row:
            return "inserted", row[0], _utc(rec["occurred_at"]).isoformat()
        existing = conn.execute(SELECT_SQL, (rec["request_id"],)).fetchone()
    if existing is None:
        # ON CONFLICT reported a conflict but the row is invisible: extremely
        # unlikely (no deletes on this table); treat as a transient db error.
        raise RuntimeError("unique conflict on request_id but row not found")
    if _row_matches(rec, existing):
        return "duplicate", existing[0], _utc(existing[2]).isoformat()
    raise ContentConflict("database",
                          _row_to_record(rec["request_id"], existing))


# ---------------------------------------------------------------------------
# Durable staging on the shared directory
# ---------------------------------------------------------------------------

def _ensure_dir() -> None:
    try:
        os.makedirs(STAGING_DIR, exist_ok=True)
    except OSError as exc:
        raise StagingError(f"staging dir {STAGING_DIR} not usable: {exc}") from exc


def _fsync_dir() -> None:
    fd = os.open(STAGING_DIR, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _final_path(request_id: str) -> str:
    return os.path.join(STAGING_DIR, request_id + ".json")


def _staged_file_bytes(rec: dict) -> bytes:
    doc = stored_payload(rec)
    doc["occurred_at_provided"] = bool(rec.get("occurred_at_provided"))
    doc["content_hash"] = content_hash(rec)
    doc["staged_at"] = datetime.now(timezone.utc).isoformat()
    doc["staged_by"] = INSTANCE_ID
    doc["format"] = 1
    return (json.dumps(doc, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _load_file(path: str) -> dict:
    """Parse a staged file back into a record; CorruptFile on any problem."""
    try:
        with open(path, "rb") as f:
            doc = json.loads(f.read().decode("utf-8"))
        rec = {
            "request_id": doc["request_id"],
            "device_id": doc["device_id"],
            "occurred_at": datetime.fromisoformat(doc["occurred_at"]),
            "occurred_at_provided": bool(doc["occurred_at_provided"]),
            "severity": doc["severity"],
            "message": doc["message"],
        }
    except Exception as exc:
        raise CorruptFile(f"{path}: unreadable staged record: {exc}") from exc
    if doc.get("content_hash") and doc["content_hash"] != content_hash(rec):
        raise CorruptFile(f"{path}: content hash mismatch")
    return rec


def stage_record(rec: dict):
    """Durably stage the record; returns ("created"|"exists_same", occurred_iso).

    Never overwrites an existing staged record: the atomic hard-link fails if
    the final name exists, and the existing file must hold identical content.
    """
    _ensure_dir()
    rid = rec["request_id"]
    final = _final_path(rid)
    tmp = os.path.join(STAGING_DIR, f".{rid}.{uuid.uuid4().hex}.tmp")
    data = _staged_file_bytes(rec)
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, final)  # atomic, O_EXCL semantics
            created = True
        except FileExistsError:
            created = False
    except OSError as exc:
        raise StagingError(f"could not stage {rid}: {exc}") from exc
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

    if created:
        try:
            _fsync_dir()
        except OSError as exc:
            # The file itself is fsynced; a lost directory entry would lose
            # the record, so treat this as a failure and clean up.
            try:
                os.unlink(final)
            except OSError:
                pass
            raise StagingError(f"could not fsync staging dir: {exc}") from exc
        return "created", _utc(rec["occurred_at"]).isoformat()

    # Already staged (possibly by the other instance): content must match.
    existing = _load_file(final)  # CorruptFile propagates
    if content_hash(existing) != content_hash(rec):
        raise ContentConflict("staging", stored_payload(existing))
    return "exists_same", _utc(existing["occurred_at"]).isoformat()


def discard_staged(request_id: str, expected_hash: str = None) -> bool:
    """Best-effort removal of a staged copy; only removes the file whose
    content matches expected_hash (when given). Never raises."""
    path = _final_path(request_id)
    try:
        if expected_hash is not None:
            if content_hash(_load_file(path)) != expected_hash:
                return False
        os.unlink(path)
        try:
            _fsync_dir()
        except OSError:
            pass
        return True
    except (OSError, StagingError):
        return False


def _quarantine(path: str, suffix: str) -> str:
    target = f"{path}.{suffix}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    try:
        os.replace(path, target)
        _fsync_dir()
    except OSError:
        log.exception("could not quarantine %s", path)
    return target


def _sweep_tmp_files() -> int:
    """Remove orphaned tmp files (crash between write and link)."""
    removed = 0
    now = time.time()
    try:
        names = os.listdir(STAGING_DIR)
    except OSError:
        return 0
    for name in names:
        if not name.endswith(".tmp"):
            continue
        path = os.path.join(STAGING_DIR, name)
        try:
            if now - os.path.getmtime(path) > TMP_MAX_AGE:
                os.unlink(path)
                removed += 1
        except OSError:
            pass
    return removed


# ---------------------------------------------------------------------------
# Accept path
# ---------------------------------------------------------------------------

def accept(rec: dict):
    """Returns (http_status_code, response_body)."""
    base = {"request_id": rec["request_id"], "handled_by": INSTANCE_ID}

    db_error = None
    try:
        outcome, row_id, occurred_iso = db_insert(rec)
        # A staged copy may be left over from an earlier outage: the row is
        # safely in the database now, so drop the file (never raises).
        discard_staged(rec["request_id"], expected_hash=content_hash(rec))
        return 201, {**base, "state": "persisted", "id": row_id,
                     "occurred_at": occurred_iso,
                     "deduplicated": outcome == "duplicate",
                     "stored": "database"}
    except ContentConflict as exc:
        return 409, {**base, "state": "conflict",
                     "detail": str(exc), "existing": exc.existing}
    except Exception as exc:  # database unreachable / broken
        db_error = f"{type(exc).__name__}: {exc}"
        log.warning("db insert failed for %s, falling back to staging: %s",
                    rec["request_id"], db_error)

    try:
        result, occurred_iso = stage_record(rec)
        return 202, {**base, "state": "staged", "id": None,
                     "occurred_at": occurred_iso,
                     "deduplicated": result == "exists_same",
                     "stored": "staging",
                     "detail": "database unreachable; record durably staged "
                               "and will be backfilled automatically",
                     "db_error": db_error}
    except ContentConflict as exc:
        return 409, {**base, "state": "conflict",
                     "detail": str(exc), "existing": exc.existing}
    except StagingError as exc:
        log.error("staging failed for %s: %s", rec["request_id"], exc)
        return 503, {**base, "state": "rejected",
                     "detail": "neither database insert nor durable staging "
                               "succeeded; the record was NOT accepted",
                     "db_error": db_error, "staging_error": str(exc)}


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def backfill_once() -> dict:
    stats = {"scanned": 0, "inserted": 0, "already_persisted": 0,
             "conflicted": 0, "corrupt": 0, "db_errors": 0,
             "tmp_swept": 0, "db_reachable": True}
    try:
        names = sorted(os.listdir(STAGING_DIR))
    except OSError as exc:
        stats["db_reachable"] = None
        stats["staging_error"] = str(exc)
        return stats

    for name in names:
        if not name.endswith(".json"):
            continue
        stats["scanned"] += 1
        path = os.path.join(STAGING_DIR, name)
        try:
            rec = _load_file(path)
        except CorruptFile as exc:
            log.error("quarantining corrupt staged file: %s", exc)
            _quarantine(path, "corrupt")
            stats["corrupt"] += 1
            continue
        try:
            outcome, _row_id, _occ = db_insert(rec)
        except ContentConflict:
            log.error("staged %s conflicts with the database row; quarantining",
                      rec["request_id"])
            _quarantine(path, "conflict")
            stats["conflicted"] += 1
            continue
        except Exception as exc:
            stats["db_reachable"] = False
            stats["db_errors"] += 1
            if stats["db_errors"] == 1:
                log.warning("backfill: database not ready (%s); will retry", exc)
            continue
        # Inserted by us, or already present with identical content (another
        # instance inserted it but died before cleanup): safe to remove.
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            log.exception("could not remove staged file %s", path)
        stats["inserted" if outcome == "inserted" else "already_persisted"] += 1
    stats["tmp_swept"] = _sweep_tmp_files()
    return stats


_loop_stop = threading.Event()
_loop_thread = None
_last_run = {"at": None, "stats": None}


def _backfill_loop() -> None:
    log.info("backfill loop started (dir=%s, interval=%.1fs, instance=%s)",
             STAGING_DIR, BACKFILL_INTERVAL, INSTANCE_ID)
    while not _loop_stop.is_set():
        try:
            stats = backfill_once()
            _last_run["at"] = datetime.now(timezone.utc).isoformat()
            _last_run["stats"] = stats
            if stats.get("inserted") or stats.get("conflicted") \
                    or stats.get("corrupt"):
                log.info("backfill cycle: %s", stats)
        except Exception:
            log.exception("backfill cycle failed")
        _loop_stop.wait(BACKFILL_INTERVAL)


def start_backfill() -> None:
    global _loop_thread
    _ensure_dir()
    _loop_stop.clear()
    _loop_thread = threading.Thread(target=_backfill_loop, name="log-backfill",
                                    daemon=True)
    _loop_thread.start()


def stop_backfill() -> None:
    _loop_stop.set()
    if _loop_thread:
        _loop_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Status queries
# ---------------------------------------------------------------------------

def _list_staging():
    staged, conflicted, corrupt = [], [], []
    try:
        names = os.listdir(STAGING_DIR)
    except OSError:
        return staged, conflicted, corrupt, "staging dir unreadable"
    for name in sorted(names):
        if name.endswith(".json"):
            staged.append(name[:-len(".json")])
        elif ".json.conflict." in name:
            conflicted.append(name.split(".json.conflict.")[0])
        elif ".json.corrupt." in name:
            corrupt.append(name.split(".json.corrupt.")[0])
    return staged, conflicted, corrupt, None


def get_request_status(request_id: str):
    """Per-request status. Database wins over staging: a record that is
    already persisted must never be reported as pending."""
    db_error = None
    row = None
    try:
        with connect() as conn:
            row = conn.execute(SELECT_SQL, (request_id,)).fetchone()
    except Exception as exc:
        db_error = f"{type(exc).__name__}: {exc}"

    staged, conflicted, corrupt, dir_error = _list_staging()
    has_staged = request_id in staged
    has_conflict = request_id in conflicted

    if row:
        code, body = 200, {
            "state": "persisted",
            "id": row[0],
            "record": _row_to_record(request_id, row),
            "staged_copy_remaining": has_staged,
            "conflicted_staged_copy": has_conflict,
        }
    elif has_staged:
        rec = None
        try:
            rec = stored_payload(_load_file(_final_path(request_id)))
        except StagingError:
            pass
        code, body = 200, {
            "state": "staged",
            "id": None,
            "record": rec,
            "detail": "durably staged; waiting for database backfill",
        }
    elif has_conflict:
        code, body = 200, {
            "state": "conflicted",
            "id": None,
            "detail": "a staged copy conflicts with the database row and was "
                      "quarantined; manual resolution required",
        }
    elif db_error:
        code, body = 503, {
            "state": "unknown",
            "detail": "database unreachable and no staged copy found; "
                      "the record may or may not be persisted",
            "db_error": db_error,
        }
    else:
        code, body = 404, {"state": "not_found",
                           "detail": "no such request_id in database or staging"}
    body.update({"request_id": request_id, "handled_by": INSTANCE_ID})
    if db_error:
        body["db_reachable"] = False
        body.setdefault("db_error", db_error)
    if dir_error:
        body["staging_error"] = dir_error
    return code, body


def overview() -> dict:
    staged, conflicted, corrupt, dir_error = _list_staging()
    db_error = None
    persisted_ids = set()
    try:
        with connect() as conn:
            if staged:
                rows = conn.execute(
                    "SELECT request_id FROM device_fault_logs "
                    "WHERE request_id = ANY(%s)",
                    (staged,),
                ).fetchall()
                persisted_ids = {r[0] for r in rows}
            else:
                conn.execute("SELECT 1")
    except Exception as exc:
        db_error = f"{type(exc).__name__}: {exc}"

    pending, awaiting_cleanup = [], []
    for rid in staged:
        (awaiting_cleanup if rid in persisted_ids else pending).append(rid)

    return {
        "instance": INSTANCE_ID,
        "staging_dir": STAGING_DIR,
        "db_reachable": db_error is None,
        "db_error": db_error,
        "staging_error": dir_error,
        # Staged and NOT in the database: genuinely waiting for backfill.
        "pending_count": len(pending),
        "pending_request_ids": pending[:100],
        # Staged file still present although the row is already persisted:
        # a leftover the backfill is about to remove -- NOT pending work.
        "awaiting_cleanup_count": len(awaiting_cleanup),
        "awaiting_cleanup_request_ids": awaiting_cleanup[:100],
        "conflicted_request_ids": conflicted,
        "corrupt_request_ids": corrupt,
        "backfill": {
            "interval_seconds": BACKFILL_INTERVAL,
            "last_run_at": _last_run["at"],
            "last_stats": _last_run["stats"],
        },
    }

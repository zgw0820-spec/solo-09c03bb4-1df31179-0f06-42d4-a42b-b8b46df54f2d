#!/usr/bin/env python3
"""End-to-end self-test for the fault-log online-index platform.

Runs against two live API instances and a real PostgreSQL and verifies:

  P0  both instances up, Swagger reachable, schema present, clean slate
  P1  log write via instance 1 is readable via instance 2
  P2  12 concurrent build requests across both instances -> exactly ONE build;
      log writes keep flowing during the build; the in-progress (temporarily
      invalid) catalog entry is never deleted mid-build
  P3  further build requests are idempotent ('ready', same job, catalog-checked)
  P4a instance crash while the server-side build finishes anyway -> orphan
      claim -> failed -> next request converges to ready (no rebuild)
  P4b instance crash with the build truly interrupted -> invalid leftover ->
      orphan detection after grace -> cleanup + retry succeeds; late old-job
      writes are fenced off
  P5  startup reconcile marks the orphaned build failed; restarts keep the
      tracked state consistent with the actual index
  P6  the device/time-range query plan uses the new index

Reliable-write path (request_id idempotency + outage staging):

  R0  reliable write with db up -> persisted; visible via the original query
      API; replay after 'lost receipt' deduplicates; different content -> 409
  R1  concurrent identical submissions across both instances insert exactly
      one row; concurrent different content for one request_id -> one winner,
      the rest 409
  R2  db outage: writes are 202-accepted only after durable staging,
      occurred_at is fixed at acceptance; re-submits while down are
      idempotent; after recovery the backfill inserts them (readable through
      the original GET /logs) with the acceptance-time occurred_at
  R3  staged records survive a hard restart of both instances and are
      backfilled after recovery
  R4  db row present + leftover staged copy: status says persisted (never
      pending), backfill removes the copy without duplicating the row
  R5  staged copy contradicting the db row is quarantined (.conflict), never
      overwritten, surfaced by the status APIs
  R6  db down AND staging unwritable -> 503 (local mode only)
  R7  client-supplied occurred_at survives the staging/backfill round trip

Modes:
  local   spawns/kills two local uvicorn processes (used in this sandbox)
  docker  drives `docker compose` services api1/api2

Requires psycopg (project venv). Usage:
  python3 tests/selftest.py --mode local --dsn "host=/tmp/... dbname=faultlogs user=ops"
  python3 tests/selftest.py --mode docker --dsn "postgresql://ops:ops@localhost:5433/faultlogs"
"""

import argparse
import itertools
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import psycopg

GRACE = float(os.environ.get("ORPHAN_GRACE_SECONDS", "15"))
INDEX_NAME = "idx_fault_logs_device_occurred"
MARKER = "test-" + uuid.uuid4().hex[:10]

RESULTS = []
app_reliable = None  # imported in main() once --workdir is known


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""),
          flush=True)
    if not ok:
        raise SystemExit(f"aborting: {name} failed {detail}")


def info(msg):
    print(f"  .. {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP / SQL helpers
# ---------------------------------------------------------------------------

def http(method, base, path, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"raw": raw}
        return e.code, payload


def http_raw(base, path, timeout=30):
    with urllib.request.urlopen(base + path, timeout=timeout) as r:
        return r.status, r.read().decode(errors="replace")


class Db:
    def __init__(self, dsn):
        self.dsn = dsn

    def q(self, sql, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.execute(sql, params or ())
            if cur.description is None:  # DDL/DML without RETURNING
                return []
            return cur.fetchall()

    def rowcount(self, sql, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            return conn.execute(sql, params or ()).rowcount


def index_catalog(db):
    rows = db.q(
        """
        SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid)
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s AND n.nspname = 'public' AND c.relkind = 'i'
        """,
        (INDEX_NAME,),
    )
    return rows  # [] or [(indisvalid, indisready, def)]


def wait_health(base, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, _ = http("GET", base, "/health", timeout=5)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def wait_state(bases, want, timeout=240):
    """Poll /indexes/status (round-robin) until state == want."""
    deadline = time.time() + timeout
    i = 0
    last = None
    while time.time() < deadline:
        code, st = http("GET", bases[i % len(bases)], "/indexes/status", timeout=30)
        i += 1
        if code == 200:
            last = st.get("state")
            if last == want:
                return st
        time.sleep(0.5)
    raise SystemExit(f"aborting: state never became {want!r} (last={last!r})")


# ---------------------------------------------------------------------------
# Instance control
# ---------------------------------------------------------------------------

class LocalInstance:
    def __init__(self, name, port, dsn, workdir, logdir, staging_dir):
        self.name, self.port, self.dsn, self.workdir = name, port, dsn, workdir
        self.staging_dir = staging_dir
        self.logpath = os.path.join(logdir, f"{name}.log")
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["DATABASE_URL"] = self.dsn
        env["ORPHAN_GRACE_SECONDS"] = str(GRACE)
        env["STAGING_DIR"] = self.staging_dir
        env["BACKFILL_INTERVAL_SECONDS"] = "1"
        self._log = open(self.logpath, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=self.workdir, env=env,
            stdout=self._log, stderr=subprocess.STDOUT)
        info(f"{self.name} started pid={self.proc.pid} (log: {self.logpath})")

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()  # SIGKILL: simulate a hard crash
            self.proc.wait()
        info(f"{self.name} killed")


class DockerInstance:
    def __init__(self, service, workdir):
        self.service, self.workdir = service, workdir

    def _compose(self, *args):
        subprocess.run(["docker", "compose", *args], cwd=self.workdir, check=True)

    def start(self):
        self._compose("start", self.service)

    def kill(self):
        self._compose("kill", self.service)


# ---------------------------------------------------------------------------
# Outage simulation + staging-dir access
# ---------------------------------------------------------------------------

def freeze_db(ctx):
    """Simulate a PostgreSQL outage (connections hang/refused until thaw)."""
    if ctx["mode"] == "docker":
        subprocess.run(["docker", "compose", "stop", "db"],
                       cwd=ctx["workdir"], check=True)
    else:
        # Stop postmaster AND backends (all have comm 'postgres').
        subprocess.run(["pkill", "-STOP", "-x", "postgres"], check=True)
    info("database frozen (outage simulated)")


def unfreeze_db(ctx):
    if ctx["mode"] == "docker":
        subprocess.run(["docker", "compose", "start", "db"],
                       cwd=ctx["workdir"], check=True)
    else:
        subprocess.run(["pkill", "-CONT", "-x", "postgres"], check=True)
    info("database restored")


def staging_names(ctx):
    """File names currently in the shared staging directory."""
    if ctx["mode"] == "docker":
        out = subprocess.run(
            ["docker", "compose", "exec", "-T", "api1", "ls", "-1", "/staging"],
            cwd=ctx["workdir"], capture_output=True, text=True)
        return out.stdout.split()
    try:
        return os.listdir(ctx["staging_dir"])
    except FileNotFoundError:
        return []


def write_staged_file(ctx, rec):
    """Plant a staged record exactly as the app would (crash-leftover
    simulation), bypassing the API."""
    data = app_reliable._staged_file_bytes(rec)
    name = rec["request_id"] + ".json"
    if ctx["mode"] == "docker":
        tmp = os.path.join(ctx["logdir"], name)
        with open(tmp, "wb") as f:
            f.write(data)
        subprocess.run(["docker", "compose", "cp", tmp, f"api1:/staging/{name}"],
                       cwd=ctx["workdir"], check=True)
        return
    path = os.path.join(ctx["staging_dir"], name)
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def wait_staging_gone(ctx, prefix, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not any(n.startswith(prefix) for n in staging_names(ctx)):
            return True
        time.sleep(0.5)
    return False


def wait_staging_present(ctx, marker, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if any(marker in n for n in staging_names(ctx)):
            return True
        time.sleep(0.5)
    return False


def wait_reliable_state(bases, rid, want, timeout=90):
    """Poll GET /logs/reliable/<rid> (round-robin) until state == want."""
    deadline = time.time() + timeout
    i, last = 0, None
    while time.time() < deadline:
        code, st = http("GET", bases[i % len(bases)],
                        f"/logs/reliable/{rid}", timeout=30)
        i += 1
        if code == 200:
            last = st.get("state")
            if last == want:
                return st
        time.sleep(0.5)
    raise SystemExit(f"aborting: {rid} never became {want!r} (last={last!r})")


def reset_staging(ctx):
    """Start each run with an empty shared staging directory."""
    if ctx["mode"] == "docker":
        for svc in ("api1",):
            subprocess.run(["docker", "compose", "exec", "-T", svc,
                            "sh", "-c", "find /staging -mindepth 1 -delete"],
                           cwd=ctx["workdir"], check=True)
        return
    shutil.rmtree(ctx["staging_dir"], ignore_errors=True)
    os.makedirs(ctx["staging_dir"], exist_ok=True)


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def operator_reset(db):
    """Simulate the DBA wiping managed state so a fresh build is needed."""
    db.q(f'DROP INDEX CONCURRENTLY IF EXISTS "{INDEX_NAME}"')
    db.q("DELETE FROM index_build_jobs")


def parallel_builds(bases, n_per_instance):
    futs, results = [], []
    with ThreadPoolExecutor(max_workers=n_per_instance * len(bases)) as ex:
        for _ in range(n_per_instance):
            for base in bases:
                futs.append(ex.submit(http, "POST", base, "/indexes/build"))
        for f in futs:
            results.append(f.result())
    return results


def phase_p0(ctx):
    print("\n== P0: setup ==")
    if ctx["mode"] == "docker":
        subprocess.run(["docker", "compose", "up", "-d", "--build"],
                       cwd=ctx["workdir"], check=True)
    else:  # local
        subprocess.run(["pkill", "-f", "uvicorn app.main:app"],
                       capture_output=True)  # leftovers from previous runs
        time.sleep(1)
        for inst in ctx["instances"].values():
            inst.start()

    # schema gate
    deadline = time.time() + 300
    while True:
        try:
            ctx["db"].q("SELECT 1 FROM index_build_jobs LIMIT 1")
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("schema never appeared")
            time.sleep(2)
    info("schema present")

    operator_reset(ctx["db"])
    info("operator reset done (index dropped if existed, jobs cleared)")

    for name, base in ctx["bases"].items():
        check(f"P0 {name} healthy", wait_health(base), base)
        code, body = http_raw(base, "/docs")
        check(f"P0 {name} Swagger UI", code == 200 and "swagger" in body.lower())

    reset_staging(ctx)
    info("staging dir reset")

    for name, base in ctx["bases"].items():
        code, st = http("GET", base, "/indexes/status")
        check(f"P0 {name} initial state not_started",
              code == 200 and st["state"] == "not_started", str(st.get("state")))


def phase_p1(ctx):
    print("\n== P1: write via api1, read via api2 ==")
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    code, r = http("POST", b1, "/logs",
                   {"device_id": MARKER, "message": "p1 sanity", "severity": "error"})
    check("P1 write log", code == 201 and r.get("id"), f"id={r.get('id')}")
    code, r = http("GET", b2,
                   f"/logs?device_id={MARKER}&start=2000-01-01T00:00:00Z"
                   f"&end=2100-01-01T00:00:00Z")
    check("P1 read log back via other instance",
          code == 200 and r["count"] >= 1
          and any(it["message"] == "p1 sanity" for it in r["items"]))


def phase_p2(ctx):
    print("\n== P2: concurrent build requests -> single build; writes flow ==")
    bases = list(ctx["bases"].values())
    responses = parallel_builds(bases, 6)

    codes = [c for c, _ in responses]
    states = [r.get("state") for _, r in responses]
    job_ids = {r.get("job_id") for _, r in responses}
    check("P2 all requests HTTP 200", all(c == 200 for c in codes), str(codes))
    check("P2 states subset of started/building",
          set(states) <= {"started", "building"}, str(states))
    check("P2 exactly one 'started'", states.count("started") == 1, str(states))
    check("P2 exactly one distinct job id", len(job_ids) == 1, str(job_ids))
    job_id = next(iter(job_ids))

    saw = {"max_active": 0, "invalid_seen": False, "gone_after_invalid": False,
           "writes_ok": 0, "writes_fail": 0}
    deadline = time.time() + 240
    i = 0
    final = None
    while time.time() < deadline:
        code, st = http("GET", bases[i % 2], "/indexes/status", timeout=30)
        i += 1
        cat = st.get("catalog", {})
        saw["max_active"] = max(saw["max_active"], st.get("active_build_backends", 0))
        if cat.get("exists") and not cat.get("valid"):
            saw["invalid_seen"] = True
        if saw["invalid_seen"] and not cat.get("exists"):
            saw["gone_after_invalid"] = True
        if st.get("state") == "building":
            c, _ = http("POST", bases[i % 2], "/logs",
                        {"device_id": MARKER, "message": "written during build"})
            if c == 201:
                saw["writes_ok"] += 1
            else:
                saw["writes_fail"] += 1
        if st.get("state") == "ready":
            final = st
            break
        time.sleep(0.25)
    check("P2 build reached ready", final is not None)
    check("P2 never more than one active build backend",
          saw["max_active"] == 1, f"max_active={saw['max_active']}")
    check("P2 in-progress invalid catalog entry observed", saw["invalid_seen"])
    check("P2 in-progress entry never deleted mid-build",
          not saw["gone_after_invalid"])
    check("P2 writes flowed during build",
          saw["writes_ok"] > 0 and saw["writes_fail"] == 0,
          f"ok={saw['writes_ok']} fail={saw['writes_fail']}")
    check("P2 ready job matches claimed job",
          final and final.get("job_id") == job_id,
          f"{final and final.get('job_id')} vs {job_id}")

    cat = index_catalog(ctx["db"])
    check("P2 catalog: exactly one index, valid+ready, correct definition",
          len(cat) == 1 and cat[0][0] and cat[0][1]
          and "(device_id, occurred_at)" in cat[0][2],
          str(cat))

    code, r = http("GET", bases[0],
                   f"/logs?device_id={MARKER}&start=2000-01-01T00:00:00Z"
                   f"&end=2100-01-01T00:00:00Z&limit=1000")
    check("P2 logs written during build are queryable",
          code == 200 and r["count"] >= saw["writes_ok"],
          f"count={r.get('count')} writes={saw['writes_ok']}")
    return job_id


def phase_p3(ctx, job_id):
    print("\n== P3: idempotent ready ==")
    bases = list(ctx["bases"].values())
    responses = parallel_builds(bases, 2)
    states = [r.get("state") for _, r in responses]
    ids = {r.get("job_id") for _, r in responses}
    check("P3 all report ready", set(states) == {"ready"}, str(states))
    check("P3 same job id, no rebuild", ids == {job_id}, str(ids))
    cat = index_catalog(ctx["db"])
    check("P3 still exactly one valid index", len(cat) == 1 and cat[0][0])


def phase_p4(ctx):
    print("\n== P4a: crash, server-side build completes -> converge ready ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    operator_reset(db)

    code, r = http("POST", b1, "/indexes/build")
    check("P4a build started on api1", r.get("state") == "started", str(r))
    job_a = r["job_id"]
    deadline = time.time() + 60
    cat = []
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P4a invalid in-progress index present before kill",
          len(cat) == 1 and not cat[0][0], str(cat))

    ctx["instances"]["api1"].kill()
    # The backend does not notice the dead client (client_connection_check_
    # interval=0) and finishes the CONCURRENTLY build server-side.
    deadline = time.time() + 90
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and cat[0][0]:
            break
        time.sleep(0.5)
    check("P4a server-side build completed despite client crash",
          len(cat) == 1 and cat[0][0], str(cat))

    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    code, st = http("GET", b2, "/indexes/status")
    check("P4a orphan claim converged to failed",
          st.get("state") == "failed", str(st.get("state")))
    check("P4a catalog shows the (valid) finished index",
          st["catalog"]["exists"] and st["catalog"]["valid"])

    code, r = http("POST", b2, "/indexes/build")
    check("P4a next build request converges to ready without rebuild",
          r.get("state") == "ready", str(r.get("state")))
    cat = index_catalog(db)
    check("P4a catalog still exactly one valid index",
          len(cat) == 1 and cat[0][0], str(cat))

    print("\n== P4b: crash mid-build (interrupted) -> orphan -> cleanup -> retry ==")
    ctx["instances"]["api1"].start()  # api1 was killed in P4a
    check("P4b api1 healthy again", wait_health(b1))
    operator_reset(db)
    code, st = http("GET", b2, "/indexes/status")
    check("P4b reset -> not_started", st.get("state") == "not_started")

    code, r = http("POST", b1, "/indexes/build")
    check("P4b build started on api1", code == 200 and r.get("state") == "started",
          str(r))
    job_b = r["job_id"]

    # wait until the in-progress (invalid) catalog entry exists
    deadline = time.time() + 60
    cat = []
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P4b invalid in-progress index present before kill",
          len(cat) == 1 and not cat[0][0], str(cat))

    ctx["instances"]["api1"].kill()
    # Make the interruption deterministic: terminate the build backend itself
    # (simulates e.g. db restart / OOM), aborting CREATE INDEX CONCURRENTLY.
    rows = db.q(
        "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() "
        "AND query ~* '^\\s*create\\s+index' AND query LIKE %s",
        (f"%{INDEX_NAME}%",))
    check("P4b terminated in-flight build backend",
          len(rows) >= 1 and all(r[1] for r in rows), str(rows))
    time.sleep(1)
    cat = index_catalog(db)
    check("P4b interrupted build leaves invalid leftover index",
          len(cat) == 1 and not cat[0][0], str(cat))
    rows = db.q("SELECT status FROM index_build_jobs WHERE id = %s",
                (uuid.UUID(job_b),))
    check("P4b job row still 'building' right after crash",
          rows and rows[0][0] == "building", str(rows))

    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    code, st = http("GET", b2, "/indexes/status")
    check("P4b orphan detected -> failed", st.get("state") == "failed",
          str(st.get("state")))
    check("P4b failed state keeps invalid leftover visible",
          st["catalog"]["exists"] and not st["catalog"]["valid"])
    check("P4b no active build backend", st.get("active_build_backends") == 0)

    n = db.rowcount(
        "UPDATE index_build_jobs SET status = 'ready', error = 'late write' "
        "WHERE id = %s AND status = 'building'", (uuid.UUID(job_b),))
    check("P4b fencing: late write to old job touches 0 rows", n == 0)

    code, r = http("POST", b2, "/indexes/cleanup")
    check("P4b explicit cleanup cleaned+dropped invalid leftover",
          code == 200 and r.get("cleaned") and r.get("dropped_invalid_index"),
          str(r))
    cat = index_catalog(db)
    check("P4b invalid leftover gone after cleanup", len(cat) == 0, str(cat))
    code, st = http("GET", b2, "/indexes/status")
    check("P4b state not_started after cleanup",
          st.get("state") == "not_started", str(st.get("state")))

    responses = parallel_builds([b2], 8)
    states = [r.get("state") for _, r in responses]
    ids = {r.get("job_id") for _, r in responses}
    check("P4b retry: exactly one new job", len(ids) == 1, str(ids))
    job_c = next(iter(ids))
    check("P4b retry: new job differs from crashed one", job_c != job_b)
    check("P4b retry: one started, rest building",
          states.count("started") == 1
          and set(states) <= {"started", "building"}, str(states))

    final = wait_state([b2], "ready", timeout=240)
    check("P4b retry reached ready", final.get("job_id") == job_c)
    cat = index_catalog(db)
    check("P4b catalog: single valid index after cleanup+rebuild",
          len(cat) == 1 and cat[0][0] and cat[0][1], str(cat))

    n = db.rowcount(
        "UPDATE index_build_jobs SET status = 'failed' "
        "WHERE id = %s AND status = 'building'", (uuid.UUID(job_b),))
    check("P4b fencing: crashed job cannot overwrite new result", n == 0)

    ctx["instances"]["api1"].start()
    check("P4b api1 back healthy", wait_health(b1))
    code, st = http("GET", b1, "/indexes/status")
    check("P4b restarted api1 reports ready (consistent with catalog)",
          st.get("state") == "ready", str(st.get("state")))


def phase_p5(ctx):
    print("\n== P5: startup reconcile + restart consistency ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    operator_reset(db)

    code, r = http("POST", b2, "/indexes/build")
    check("P5 build started on api2", r.get("state") == "started", str(r))
    job_c = r["job_id"]
    deadline = time.time() + 60
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P5 invalid in-progress index present", bool(cat) and not cat[0][0])

    ctx["instances"]["api2"].kill()
    # Deterministic interruption (as in P4b): the build must not finish
    # server-side, so terminate the backend running CREATE INDEX.
    rows = db.q(
        "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() "
        "AND query ~* '^\\s*create\\s+index' AND query LIKE %s",
        (f"%{INDEX_NAME}%",))
    check("P5 terminated in-flight build backend",
          len(rows) >= 1 and all(r[1] for r in rows), str(rows))
    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    ctx["instances"]["api2"].start()  # startup reconcile should mark job C failed
    check("P5 api2 back healthy", wait_health(b2))
    st = wait_state([b1, b2], "failed", timeout=60)
    check("P5 reconcile marked orphaned job failed",
          st.get("job_id") == job_c, str(st.get("job_id")))

    code, r = http("POST", b1, "/indexes/build")
    check("P5 rebuild started after reconcile", r.get("state") == "started", str(r))
    wait_state([b1], "ready", timeout=240)
    cat = index_catalog(db)
    check("P5 catalog valid after rebuild", len(cat) == 1 and cat[0][0])

    for name in ("api1", "api2"):
        ctx["instances"][name].kill()
        ctx["instances"][name].start()
        check(f"P5 {name} healthy after restart", wait_health(ctx["bases"][name]))
        code, st = http("GET", ctx["bases"][name], "/indexes/status")
        check(f"P5 {name} state consistent with actual index after restart",
              st.get("state") == "ready", str(st.get("state")))


def phase_p6(ctx):
    print("\n== P6: query plan uses the index ==")
    b1 = ctx["bases"]["api1"]
    code, r = http("GET", b1,
                   "/logs/plan?device_id=dev-00001&start=2000-01-01T00:00:00Z"
                   "&end=2100-01-01T00:00:00Z")
    plan = " | ".join(r.get("plan", []))
    check("P6 plan uses idx_fault_logs_device_occurred",
          code == 200 and INDEX_NAME in plan, plan[:200])


# ---------------------------------------------------------------------------
# Reliable-write phases
# ---------------------------------------------------------------------------

def _rel_body(rid, message, **kw):
    body = {"request_id": rid, "device_id": MARKER, "message": message,
            "severity": "warn"}
    body.update(kw)
    return body


def _db_rows_for(db, rid):
    return db.q("SELECT id, device_id, occurred_at, severity, message "
                "FROM device_fault_logs WHERE request_id = %s", (rid,))


def phase_r0(ctx):
    print("\n== R0: reliable write with db up; replay dedup; 409 ==")
    db, b1, b2 = ctx["db"], ctx["bases"]["api1"], ctx["bases"]["api2"]
    rid = "r0-" + MARKER
    body = _rel_body(rid, "r0 hello")

    code, r = http("POST", b1, "/logs/reliable", body)
    check("R0 write -> 201 persisted", code == 201
          and r.get("state") == "persisted" and r.get("id"),
          f"code={code} body={r}")
    check("R0 occurred_at fixed at acceptance", bool(r.get("occurred_at")))
    row_id = r["id"]

    code, r2 = http("GET", b2, f"/logs?device_id={MARKER}"
                    "&start=2000-01-01T00:00:00Z&end=2100-01-01T00:00:00Z"
                    "&limit=1000")
    check("R0 visible via original GET /logs on the other instance",
          code == 200 and any(it["id"] == row_id for it in r2["items"]))

    code, st = http("GET", b2, f"/logs/reliable/{rid}")
    check("R0 status persisted via api2",
          code == 200 and st.get("state") == "persisted"
          and st.get("id") == row_id, str(st.get("state")))

    code, r3 = http("POST", b2, "/logs/reliable", body)  # 'lost receipt' retry
    check("R0 replay deduplicated (same id)",
          code == 201 and r3.get("deduplicated") and r3.get("id") == row_id,
          f"code={code} body={r3}")
    rows = _db_rows_for(db, rid)
    check("R0 exactly one row in db", len(rows) == 1, str(len(rows)))

    code, r4 = http("POST", b1, "/logs/reliable", _rel_body(rid, "r0 DIFFERENT"))
    check("R0 same request_id different content -> 409",
          code == 409 and r4.get("state") == "conflict", f"code={code}")
    rows = _db_rows_for(db, rid)
    check("R0 409 did not modify the row",
          len(rows) == 1 and rows[0][4] == "r0 hello")

    code, ov = http("GET", b1, "/logs/reliable")
    check("R0 overview: db reachable, nothing pending",
          code == 200 and ov.get("db_reachable") and ov["pending_count"] == 0,
          str(ov.get("pending_request_ids")))

    code, spec = http("GET", b1, "/openapi.json")
    paths = spec.get("paths", {})
    check("R0 Swagger documents the reliable endpoints",
          code == 200 and "/logs/reliable" in paths
          and "/logs/reliable/{request_id}" in paths, str(sorted(paths)))


def phase_r1(ctx):
    print("\n== R1: concurrent submissions collapse to one row ==")
    db = ctx["db"]
    bases = [ctx["bases"]["api1"], ctx["bases"]["api2"]]

    rid = "r1-" + MARKER
    body = _rel_body(rid, "r1 concurrent identical")
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(http, "POST", bases[i % 2], "/logs/reliable", body)
                for i in range(12)]
        results = [f.result() for f in futs]
    codes = [c for c, _ in results]
    check("R1 identical: all 201", all(c == 201 for c in codes), str(codes))
    created = [r for c, r in results if not r.get("deduplicated")]
    check("R1 identical: exactly one actual insert",
          len(created) == 1, f"created={len(created)}")
    ids = {r.get("id") for _, r in results}
    check("R1 identical: all responses carry the same row id",
          len(ids) == 1, str(ids))
    check("R1 identical: exactly one row in db",
          len(_db_rows_for(db, rid)) == 1)

    rid2 = "r1x-" + MARKER
    body_a = _rel_body(rid2, "r1x content A")
    body_b = _rel_body(rid2, "r1x content B")
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(http, "POST", bases[i % 2], "/logs/reliable",
                          body_a if i % 2 == 0 else body_b)
                for i in range(10)]
        results = [f.result() for f in futs]
    codes = [c for c, _ in results]
    check("R1 conflicting: every request is 201 or 409",
          all(c in (201, 409) for c in codes), str(codes))
    n201, n409 = codes.count(201), codes.count(409)
    check("R1 conflicting: at least one winner and one rejected",
          n201 >= 1 and n409 >= 1, f"201={n201} 409={n409}")
    rows = _db_rows_for(db, rid2)
    check("R1 conflicting: exactly one row in db", len(rows) == 1)
    # The surviving row must belong to the content that got the 201s.
    winner_msg = rows[0][4]
    a_won = winner_msg == "r1x content A"
    exp_201 = sum(1 for i in range(10) if (i % 2 == 0) == a_won)
    check("R1 conflicting: winner's submissions all 201, loser all 409",
          n201 == exp_201 and n409 == 10 - exp_201,
          f"winner={winner_msg!r} 201={n201} expected={exp_201}")


def phase_r2(ctx):
    print("\n== R2: db outage -> durable staging -> auto backfill ==")
    db, b1, b2 = ctx["db"], ctx["bases"]["api1"], ctx["bases"]["api2"]
    bases = [b1, b2]
    rid = "r2-" + MARKER
    body = _rel_body(rid, "r2 written during outage")

    freeze_db(ctx)
    t_frozen = time.time()
    try:
        code, r = http("POST", b1, "/logs/reliable", body, timeout=60)
        check("R2 write during outage -> 202 staged (durable ack)",
              code == 202 and r.get("state") == "staged" and r.get("id") is None,
              f"code={code} body={r}")
        accepted_occurred = datetime.fromisoformat(r["occurred_at"])

        code, r2 = http("POST", b2, "/logs/reliable", body, timeout=60)
        check("R2 re-submit during outage is idempotent",
              code == 202 and r2.get("deduplicated"), f"code={code} body={r2}")

        code, r3 = http("POST", b2, "/logs/reliable",
                        _rel_body(rid, "r2 DIFFERENT"), timeout=60)
        check("R2 different content during outage -> 409",
              code == 409, f"code={code}")

        code, st = http("GET", b1, f"/logs/reliable/{rid}", timeout=60)
        check("R2 status while down: staged",
              code == 200 and st.get("state") == "staged", str(st.get("state")))
        code, ov = http("GET", b1, "/logs/reliable", timeout=60)
        check("R2 overview while down lists it as pending",
              code == 200 and rid in ov.get("pending_request_ids", []),
              str(ov.get("pending_request_ids")))
        check("R2 staged file present in shared dir",
              rid + ".json" in staging_names(ctx), str(staging_names(ctx)))
    finally:
        unfreeze_db(ctx)
        t_thawed = time.time()

    st = wait_reliable_state(bases, rid, "persisted", timeout=90)
    check("R2 backfilled after recovery", st.get("id") is not None,
          f"id={st.get('id')}")
    rows = _db_rows_for(db, rid)
    check("R2 exactly one row after backfill", len(rows) == 1)
    db_occurred = rows[0][2] if rows else None
    check("R2 occurred_at kept from acceptance (not recovery time)",
          db_occurred is not None and db_occurred == accepted_occurred
          and accepted_occurred.timestamp() <= t_thawed,
          f"db={db_occurred} accepted={accepted_occurred} thawed={t_thawed}")
    check("R2 acceptance time lies inside the outage window",
          t_frozen <= accepted_occurred.timestamp() <= t_thawed)

    code, r4 = http("GET", b2, f"/logs?device_id={MARKER}"
                    "&start=2000-01-01T00:00:00Z&end=2100-01-01T00:00:00Z"
                    "&limit=1000")
    check("R2 backfilled row readable via original GET /logs",
          code == 200 and any(it["message"] == "r2 written during outage"
                              for it in r4["items"]))
    check("R2 staged file cleaned after backfill",
          wait_staging_gone(ctx, rid + ".json"), str(staging_names(ctx)))
    code, ov = http("GET", b1, "/logs/reliable")
    check("R2 overview clean after backfill",
          code == 200 and rid not in ov.get("pending_request_ids", [])
          and rid not in ov.get("awaiting_cleanup_request_ids", []))


def phase_r3(ctx):
    print("\n== R3: staged records survive instance restart ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    rid = "r3-" + MARKER

    freeze_db(ctx)
    try:
        code, r = http("POST", b1, "/logs/reliable",
                       _rel_body(rid, "r3 staged before restart"), timeout=60)
        check("R3 staged during outage", code == 202, f"code={code}")
        # Hard-restart BOTH instances while the database is still down.
        for name in ("api1", "api2"):
            ctx["instances"][name].kill()
            ctx["instances"][name].start()
    finally:
        unfreeze_db(ctx)

    check("R3 api1 healthy after restart", wait_health(b1))
    check("R3 api2 healthy after restart", wait_health(b2))
    st = wait_reliable_state([b1, b2], rid, "persisted", timeout=90)
    check("R3 staged record backfilled after restart+recovery",
          st.get("id") is not None)
    check("R3 exactly one row", len(_db_rows_for(db, rid)) == 1)


def phase_r4(ctx):
    print("\n== R4: db row + leftover staged copy -> no dup, not pending ==")
    db, b1, b2 = ctx["db"], ctx["bases"]["api1"], ctx["bases"]["api2"]
    rid = "r4-" + MARKER
    rec = app_reliable.new_record(rid, MARKER, "r4 leftover staged copy",
                                  "warn", None)
    # Simulate: the insert succeeded but the instance died before removing
    # the staged copy -> row in db AND file in the shared dir.
    db.q("INSERT INTO device_fault_logs "
         "(request_id, device_id, occurred_at, severity, message) "
         "VALUES (%s, %s, %s, %s, %s)",
         (rid, MARKER, rec["occurred_at"], "warn", "r4 leftover staged copy"))
    db_id = _db_rows_for(db, rid)[0][0]
    write_staged_file(ctx, rec)
    info("planted leftover staged copy (backfill may clean it at any time)")

    # Deterministic invariants while the row is in the db, no matter when the
    # backfill cleans the leftover file:
    code, st = http("GET", b2, f"/logs/reliable/{rid}")
    check("R4 status says persisted (never pending)",
          code == 200 and st.get("state") == "persisted"
          and st.get("id") == db_id, str(st))
    info(f"staged_copy_remaining observed: {st.get('staged_copy_remaining')}")

    ever_pending, saw_awaiting = False, False
    deadline = time.time() + 5
    while time.time() < deadline:
        code, ov = http("GET", b1, "/logs/reliable")
        ever_pending |= rid in ov.get("pending_request_ids", [])
        saw_awaiting |= rid in ov.get("awaiting_cleanup_request_ids", [])
        if rid + ".json" not in staging_names(ctx):
            break  # cleaned before the next poll
        time.sleep(0.2)
    check("R4 leftover never reported as pending", not ever_pending)
    info(f"awaiting_cleanup listing observed: {saw_awaiting}")

    check("R4 backfill removed the leftover copy",
          wait_staging_gone(ctx, rid + ".json"), str(staging_names(ctx)))
    rows = _db_rows_for(db, rid)
    check("R4 still exactly one row (no duplicate write)",
          len(rows) == 1, str(len(rows)))


def phase_r5(ctx):
    print("\n== R5: staged copy contradicting db -> quarantined, not applied ==")
    db, b1 = ctx["db"], ctx["bases"]["api1"]
    rid = "r5-" + MARKER
    db.q("INSERT INTO device_fault_logs "
         "(request_id, device_id, occurred_at, severity, message) "
         "VALUES (%s, %s, now(), 'error', 'r5 db truth')", (rid, MARKER))
    rec = app_reliable.new_record(rid, MARKER, "r5 DIFFERENT staged content",
                                  "error", None)
    write_staged_file(ctx, rec)

    check("R5 conflicting staged copy quarantined",
          wait_staging_present(ctx, rid + ".json.conflict."),
          str(staging_names(ctx)))
    rows = _db_rows_for(db, rid)
    check("R5 db row untouched by the conflicting copy",
          len(rows) == 1 and rows[0][4] == "r5 db truth", str(rows))
    code, st = http("GET", b1, f"/logs/reliable/{rid}")
    check("R5 status: persisted + conflicted copy flagged",
          code == 200 and st.get("state") == "persisted"
          and st.get("conflicted_staged_copy") is True, str(st))
    code, ov = http("GET", b1, "/logs/reliable")
    check("R5 overview lists the conflict",
          code == 200 and rid in ov.get("conflicted_request_ids", []), str(ov))


def phase_r6(ctx):
    print("\n== R6: db down AND staging unwritable -> 503 ==")
    if ctx["mode"] != "local":
        check("R6 both stores unavailable -> 503 (local mode only)",
              True, "skipped in docker mode")
        return
    b1 = ctx["bases"]["api1"]
    rid = "r6-" + MARKER
    sdir = ctx["staging_dir"]
    os.chmod(sdir, 0o555)
    freeze_db(ctx)
    try:
        code, r = http("POST", b1, "/logs/reliable",
                       _rel_body(rid, "r6 nowhere to persist"), timeout=60)
        check("R6 db+staging both fail -> 503 rejected",
              code == 503 and r.get("state") == "rejected",
              f"code={code} body={r}")
        check("R6 nothing staged for the rejected record",
              rid + ".json" not in staging_names(ctx))
    finally:
        unfreeze_db(ctx)
        os.chmod(sdir, 0o755)
    code, r = http("POST", b1, "/logs/reliable",
                   _rel_body(rid, "r6 retry after recovery"), timeout=60)
    check("R6 retry after recovery succeeds",
          code == 201 and r.get("state") == "persisted", f"code={code}")


def phase_r7(ctx):
    print("\n== R7: client-supplied occurred_at survives staging round trip ==")
    db, b1, b2 = ctx["db"], ctx["bases"]["api1"], ctx["bases"]["api2"]
    rid = "r7-" + MARKER
    explicit = "2026-01-02T03:04:05.123456+00:00"
    freeze_db(ctx)
    try:
        code, r = http("POST", b1, "/logs/reliable",
                       _rel_body(rid, "r7 explicit ts", occurred_at=explicit),
                       timeout=60)
        check("R7 staged during outage", code == 202, f"code={code}")
    finally:
        unfreeze_db(ctx)
    wait_reliable_state([b1, b2], rid, "persisted", timeout=90)
    rows = _db_rows_for(db, rid)
    check("R7 explicit occurred_at preserved exactly",
          len(rows) == 1
          and rows[0][2] == datetime.fromisoformat(explicit),
          f"db={rows[0][2] if rows else None} expected={explicit}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "docker"], required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--api1", default="http://127.0.0.1:8001")
    ap.add_argument("--api2", default="http://127.0.0.1:8002")
    ap.add_argument("--workdir", default="/workspace")
    ap.add_argument("--logdir", default="/tmp/ops-logs")
    ap.add_argument("--staging-dir", default="/tmp/ops-staging")
    args = ap.parse_args()

    os.makedirs(args.logdir, exist_ok=True)
    # Reuse the app's staging-file format helpers for the leftover/conflict
    # simulations (R4/R5).
    sys.path.insert(0, args.workdir)
    global app_reliable
    import app.reliable as _app_reliable
    app_reliable = _app_reliable

    db = Db(args.dsn)
    if args.mode == "local":
        instances = {
            "api1": LocalInstance("api1", 8001, args.dsn, args.workdir,
                                  args.logdir, args.staging_dir),
            "api2": LocalInstance("api2", 8002, args.dsn, args.workdir,
                                  args.logdir, args.staging_dir),
        }
    else:
        instances = {"api1": DockerInstance("api1", args.workdir),
                     "api2": DockerInstance("api2", args.workdir)}
    ctx = {
        "mode": args.mode,
        "db": db,
        "instances": instances,
        "bases": {"api1": args.api1, "api2": args.api2},
        "workdir": args.workdir,
        "logdir": args.logdir,
        "staging_dir": args.staging_dir,
    }

    try:
        phase_p0(ctx)
        phase_p1(ctx)
        job_id = phase_p2(ctx)
        phase_p3(ctx, job_id)
        phase_p4(ctx)
        phase_p5(ctx)
        phase_p6(ctx)
        phase_r0(ctx)
        phase_r1(ctx)
        phase_r2(ctx)
        phase_r3(ctx)
        phase_r4(ctx)
        phase_r5(ctx)
        phase_r6(ctx)
        phase_r7(ctx)
    finally:
        passed = sum(1 for _, ok in RESULTS if ok)
        print(f"\n==== summary: {passed}/{len(RESULTS)} checks passed ====")
        if args.mode == "local":
            subprocess.run(["pkill", "-CONT", "-x", "postgres"],
                           capture_output=True)  # never leave the db frozen
            for inst in ctx["instances"].values():
                inst.kill()
    if not all(ok for _, ok in RESULTS):
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()

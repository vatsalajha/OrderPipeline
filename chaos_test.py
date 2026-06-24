#!/usr/bin/env python3
"""
chaos_test.py — automated correctness suite for the order pipeline.

Every scenario gets a FRESH database and a fresh set of services, drives one
specific condition from the build brief, then asserts the two invariants that
must hold no matter what:

  (i)  nothing lost:    orders_submitted == delivered + cancelled + dlq
                                              + in_flight + waiting
  (ii) nothing doubled:  counters.courier_dispatch == count(status='delivered')

Note on (i): in this schema status='failed' and "has a dead_letters row" are
the same set by construction (`_move_to_dlq` always does both together, and
`/dlq/replay` always undoes both together) — so "failed" and "dlq" are not
two separate buckets to add up, `dlq` already covers it. `in_flight` and
`waiting` are computed with the exact live predicates the dashboard/API use
(not derived by subtraction), so the equality is a real check, not a tautology.

Usage (stop honcho first — this starts its own services on the same ports):
  python3 chaos_test.py
"""
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

import httpx
from common.db import init_db
from config import DB_PATH, MAX_ATTEMPTS, LEASE_SECONDS, DOWNSTREAM_TIMEOUT

# Short lease so lease-expiry recovery (scenario 5) finishes quickly in a test.
TEST_LEASE_SECONDS = 10
# WORKER_COUNT=0: the API's own lifespan hook otherwise auto-spawns WORKER_COUNT
# (12, from .env) workers of its own on top of whatever this script spawns
# explicitly — every scenario needs exact control over which worker processes
# exist (scenario 14 sends signals to specific ones by id), so the API must
# never silently add its own.
TEST_ENV = {**os.environ, "LEASE_SECONDS": str(TEST_LEASE_SECONDS), "WORKER_COUNT": "0"}

API_URL = "http://localhost:8000"
COURIER_PORT = 8002
DEVNULL = subprocess.DEVNULL

SEP = "=" * 70


# ── harness ──────────────────────────────────────────────────────────────────

def _kill_port(port: int) -> None:
    r = subprocess.run(["lsof", "-ti", f":{port}"], capture_output=True, text=True)
    for s in r.stdout.strip().split("\n"):
        if s.strip():
            try:
                os.kill(int(s), signal.SIGKILL)
            except ProcessLookupError:
                pass


def _wait_ready(url: str, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(0.3)
    return False


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fresh_db() -> None:
    for suffix in ("", "-wal", "-shm"):
        p = ROOT / (DB_PATH + suffix)
        if p.exists():
            p.unlink()
    init_db()


def start_services(n_workers: int, env: dict | None = None) -> dict[str, subprocess.Popen]:
    env = env or TEST_ENV
    for port in (8000, 8001, 8002):
        _kill_port(port)
    time.sleep(0.3)

    procs: dict[str, subprocess.Popen] = {
        "restaurant": subprocess.Popen(
            ["uvicorn", "restaurant.main:app", "--host", "0.0.0.0", "--port", "8001"],
            cwd=ROOT, env=env, stdout=DEVNULL, stderr=DEVNULL,
        ),
        "courier": subprocess.Popen(
            ["uvicorn", "courier.main:app", "--host", "0.0.0.0", "--port", "8002"],
            cwd=ROOT, env=env, stdout=DEVNULL, stderr=DEVNULL,
        ),
        "api": subprocess.Popen(
            ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"],
            cwd=ROOT, env=env, stdout=DEVNULL, stderr=DEVNULL,
        ),
    }
    if not _wait_ready(f"{API_URL}/health"):
        stop_services(procs)
        raise RuntimeError("API did not come up in time")

    for i in range(n_workers):
        wid = f"tw{i + 1}"
        procs[wid] = subprocess.Popen(
            ["python3", "-m", "worker.worker", "--id", wid],
            cwd=ROOT, env=env, stdout=DEVNULL, stderr=DEVNULL,
        )
    time.sleep(0.3)
    return procs


def stop_services(procs: dict[str, subprocess.Popen]) -> None:
    for p in procs.values():
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs.values():
        try:
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


def submit_orders(n: int, tag: str) -> list[str]:
    ids: list[str] = []
    with httpx.Client() as http:
        for i in range(n):
            r = http.post(f"{API_URL}/orders", json={"item": f"{tag}-{i}"}, timeout=5)
            r.raise_for_status()
            ids.append(r.json()["order_id"])
    return ids


def set_chaos(port: int, mode: str, seconds: int = 60) -> None:
    with httpx.Client() as http:
        http.post(f"http://localhost:{port}/chaos", json={"mode": mode, "seconds": seconds}, timeout=5)


def wait_all_terminal(timeout: float, poll: float = 1.0) -> bool:
    deadline = time.time() + timeout
    conn = _conn()
    try:
        while time.time() < deadline:
            pending = conn.execute(
                "SELECT COUNT(*) n FROM orders WHERE status NOT IN ('delivered','cancelled','failed')"
            ).fetchone()["n"]
            if pending == 0:
                return True
            time.sleep(poll)
        return False
    finally:
        conn.close()


def compute_invariants(conn: sqlite3.Connection) -> dict:
    """Whole-table check — valid because every scenario starts from a fresh DB,
    so 'submitted' for the run == orders_submitted == everything in the table."""
    now = time.time()
    by_state = {
        r["status"]: r["n"]
        for r in conn.execute("SELECT status, COUNT(*) n FROM orders GROUP BY status").fetchall()
    }
    delivered = by_state.get("delivered", 0)
    cancelled = by_state.get("cancelled", 0)
    dlq = by_state.get("failed", 0)

    # Exact live predicates the dashboard/API use — not derived by subtraction,
    # so a leaked claim or a missing clear-on-commit would show up as a real mismatch.
    in_flight = conn.execute(
        "SELECT COUNT(*) n FROM orders WHERE claimed_by IS NOT NULL AND claimed_at > ?",
        (now - TEST_LEASE_SECONDS,),
    ).fetchone()["n"]
    waiting = conn.execute(
        """SELECT COUNT(*) n FROM orders
            WHERE status NOT IN ('delivered','cancelled','failed')
              AND (claimed_by IS NULL OR claimed_at <= ?)""",
        (now - TEST_LEASE_SECONDS,),
    ).fetchone()["n"]

    submitted = conn.execute("SELECT value FROM counters WHERE name='orders_submitted'").fetchone()["value"]
    dispatch = conn.execute("SELECT value FROM counters WHERE name='courier_dispatch'").fetchone()["value"]

    dlq_rows = conn.execute("SELECT COUNT(*) n FROM dead_letters").fetchone()["n"]
    double_delivered = conn.execute(
        """SELECT order_id FROM order_events WHERE to_status='delivered'
            GROUP BY order_id HAVING COUNT(*) > 1"""
    ).fetchall()

    accounted = delivered + cancelled + dlq + in_flight + waiting
    inv1_ok = submitted == accounted
    inv2_ok = dispatch == delivered and not double_delivered
    structural_ok = dlq == dlq_rows  # status='failed' count must equal dead_letters row count

    return {
        "ok": inv1_ok and inv2_ok and structural_ok,
        "inv1_ok": inv1_ok, "inv2_ok": inv2_ok, "structural_ok": structural_ok,
        "submitted": submitted, "accounted": accounted,
        "delivered": delivered, "cancelled": cancelled, "dlq": dlq,
        "in_flight": in_flight, "waiting": waiting, "dispatch": dispatch,
        "double_delivered_orders": len(double_delivered),
    }


def run_scenario(name: str, fn) -> dict:
    print(f"\n--- {name} ---", flush=True)
    t0 = time.time()
    try:
        ok, detail = fn()
    except Exception as e:
        ok, detail = False, f"EXCEPTION: {e!r}"
    dt = time.time() - t0
    print(f"  {'PASS' if ok else 'FAIL'}  ({dt:.1f}s)  {detail}", flush=True)
    return {"name": name, "passed": ok, "detail": detail, "seconds": round(dt, 1)}


# ── scenario 1 — happy path ──────────────────────────────────────────────────

def scenario_happy_path():
    fresh_db()
    procs = start_services(n_workers=4)
    try:
        ids = submit_orders(15, "happy")
        if not wait_all_terminal(30):
            return False, "orders did not all reach terminal within 30s"
        conn = _conn()
        try:
            inv = compute_invariants(conn)
        finally:
            conn.close()
        ok = inv["ok"] and inv["delivered"] == len(ids)
        detail = (f"delivered={inv['delivered']}/{len(ids)}  submitted={inv['submitted']}  "
                  f"dispatch={inv['dispatch']}  inv1={inv['inv1_ok']} inv2={inv['inv2_ok']} "
                  f"structural={inv['structural_ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 2 — ordering: exact legal sequence, no skips ───────────────────

LEGAL_CHAIN = [
    (None, "placed"), ("placed", "confirmed"), ("confirmed", "preparing"),
    ("preparing", "ready"), ("ready", "out_for_delivery"), ("out_for_delivery", "delivered"),
]


def scenario_ordering():
    fresh_db()
    procs = start_services(n_workers=3)
    try:
        ids = submit_orders(6, "order-seq")
        if not wait_all_terminal(30):
            return False, "orders did not all drain within 30s"
        conn = _conn()
        try:
            bad = []
            for oid in ids:
                evs = conn.execute(
                    "SELECT from_status, to_status FROM order_events WHERE order_id=? ORDER BY at, id",
                    (oid,),
                ).fetchall()
                seq = [(e["from_status"], e["to_status"]) for e in evs]
                if seq != LEGAL_CHAIN:
                    bad.append((oid, seq))
            inv = compute_invariants(conn)
        finally:
            conn.close()
        ok = (not bad) and inv["ok"]
        detail = f"{len(ids) - len(bad)}/{len(ids)} orders show the exact legal chain; inv_ok={inv['ok']}"
        if bad:
            detail += f"; example bad sequence: {bad[0]}"
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 3 — slow downstream ─────────────────────────────────────────────

def scenario_slow_downstream():
    """commit_advance() resets attempt_count=0 on every successful step, so a
    delivered order's FINAL attempt_count is always 0 no matter how many retries
    happened along the way — checking it only after drain proves nothing. Sample
    it live, during the run, instead."""
    fresh_db()
    procs = start_services(n_workers=4)
    try:
        set_chaos(COURIER_PORT, "slow", 120)
        ids = submit_orders(10, "slow")

        conn = _conn()
        try:
            max_attempt_seen = 0
            deadline = time.time() + 90
            while time.time() < deadline:
                pending = conn.execute(
                    "SELECT COUNT(*) n FROM orders WHERE status NOT IN ('delivered','cancelled','failed')"
                ).fetchone()["n"]
                m = conn.execute("SELECT COALESCE(MAX(attempt_count),0) n FROM orders").fetchone()["n"]
                max_attempt_seen = max(max_attempt_seen, m)
                if pending == 0:
                    break
                time.sleep(0.3)
            else:
                return False, "orders did not all drain within 90s under a slow courier"

            inv = compute_invariants(conn)
        finally:
            conn.close()
        ok = inv["ok"] and inv["delivered"] == len(ids) and max_attempt_seen > 0
        detail = (f"delivered={inv['delivered']}/{len(ids)}  max_attempt_count_seen_live={max_attempt_seen} "
                  f"(retries exercised: {max_attempt_seen > 0})  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 4 — down then recover, then DLQ replay ──────────────────────────

def scenario_down_then_replay():
    fresh_db()
    procs = start_services(n_workers=4)
    try:
        set_chaos(COURIER_PORT, "down", 90)
        ids = submit_orders(6, "dlqr")

        conn = _conn()
        dlq_n = 0
        deadline = time.time() + 60
        while time.time() < deadline:
            dlq_n = conn.execute("SELECT COUNT(*) n FROM dead_letters").fetchone()["n"]
            if dlq_n >= len(ids):
                break
            time.sleep(1)
        conn.close()
        if dlq_n < len(ids):
            return False, f"only {dlq_n}/{len(ids)} orders reached the DLQ within 60s"

        set_chaos(COURIER_PORT, "normal", 0)
        with httpx.Client() as http:
            r = http.post(f"{API_URL}/dlq/replay", json={}, timeout=10)
        replayed = r.json().get("replayed", 0)

        # The circuit breaker adds latency to recovery by design: if it's still
        # within its cooldown window when replay happens, every order has to
        # wait out the remainder before the first probe; if that probe (or a
        # later one) happens to hit baseline TRANSIENT_ERROR_RATE noise, it's
        # back to open for another full cooldown cycle. One cycle (~10s) is
        # typical; budget for several unlucky ones rather than the old
        # pre-breaker "recovery is instant" assumption.
        if not wait_all_terminal(60):
            return False, "orders did not all drain within 60s after replay"

        conn = _conn()
        try:
            inv = compute_invariants(conn)
            dlq_left = conn.execute("SELECT COUNT(*) n FROM dead_letters").fetchone()["n"]
        finally:
            conn.close()
        ok = inv["ok"] and inv["delivered"] == len(ids) and dlq_left == 0
        detail = (f"dlq_hit={dlq_n}/{len(ids)}  replayed={replayed}  "
                  f"delivered={inv['delivered']}/{len(ids)}  dlq_left={dlq_left}  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 5 — worker crash mid-rush, x5 ───────────────────────────────────

def _single_worker_crash_run(iteration: int):
    fresh_db()
    procs = start_services(n_workers=5)
    try:
        ids = submit_orders(50, f"crash{iteration}")
        time.sleep(0.4)  # let some orders get claimed before we kill a worker
        procs["tw3"].send_signal(signal.SIGTERM)

        if not wait_all_terminal(60):
            return False, "orders did not all drain within 60s after the crash"
        conn = _conn()
        try:
            inv = compute_invariants(conn)
        finally:
            conn.close()
        return inv["ok"], (f"submitted={len(ids)} delivered={inv['delivered']} "
                            f"dlq={inv['dlq']} inv_ok={inv['ok']}")
    finally:
        stop_services(procs)


def scenario_worker_crash_x5():
    results = []
    for i in range(1, 6):
        ok, detail = _single_worker_crash_run(i)
        print(f"    iter {i}: {'PASS' if ok else 'FAIL'} — {detail}", flush=True)
        results.append(ok)
    n_pass = sum(results)
    return all(results), f"{n_pass}/5 iterations passed"


# ── scenario 6 — concurrent claim, no double-claim ───────────────────────────

def scenario_no_double_claim():
    fresh_db()
    procs = start_services(n_workers=6)
    try:
        ids = submit_orders(40, "ndc")
        if not wait_all_terminal(40):
            return False, "orders did not all drain within 40s"
        conn = _conn()
        try:
            dup = conn.execute(
                """SELECT order_id, from_status, to_status, COUNT(*) n FROM order_events
                    GROUP BY order_id, from_status, to_status HAVING COUNT(*) > 1"""
            ).fetchall()
            inv = compute_invariants(conn)
        finally:
            conn.close()
        ok = (not dup) and inv["ok"]
        detail = (f"duplicate (order,from,to) step-pairs={len(dup)}  "
                  f"delivered={inv['delivered']}/{len(ids)}  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 7 — max retries -> DLQ, exactly MAX_ATTEMPTS, nothing stuck ─────

def scenario_max_retries_dlq():
    fresh_db()
    procs = start_services(n_workers=3)
    try:
        set_chaos(COURIER_PORT, "down", 120)
        ids = submit_orders(5, "deadend")

        conn = _conn()
        deadline = time.time() + 60
        while time.time() < deadline:
            n_failed = conn.execute(
                "SELECT COUNT(*) n FROM orders WHERE status='failed'"
            ).fetchone()["n"]
            if n_failed >= len(ids):
                break
            time.sleep(1)

        ph = ",".join("?" * len(ids))
        rows = conn.execute(f"SELECT id, status, attempt_count FROM orders WHERE id IN ({ph})", ids).fetchall()
        dlq_ids = {r["order_id"] for r in conn.execute("SELECT order_id FROM dead_letters").fetchall()}
        inv = compute_invariants(conn)
        conn.close()

        not_failed = [dict(r) for r in rows if r["status"] != "failed"]
        wrong_attempts = [dict(r) for r in rows if r["status"] == "failed" and r["attempt_count"] != MAX_ATTEMPTS]
        not_in_dlq = [dict(r) for r in rows if r["status"] == "failed" and r["id"] not in dlq_ids]

        ok = not (not_failed or wrong_attempts or not_in_dlq) and inv["ok"]
        detail = (f"failed={len(rows) - len(not_failed)}/{len(ids)}  "
                  f"wrong_attempt_count={len(wrong_attempts)}  not_in_dlq={len(not_in_dlq)}  "
                  f"inv_ok={inv['ok']}")
        if not_failed:
            detail += f"  stuck_non_terminal={not_failed}"
        if wrong_attempts:
            detail += f"  example_wrong_attempts={wrong_attempts[0]} (want {MAX_ATTEMPTS})"
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 8 — idempotency under a duplicate downstream call ──────────────

def scenario_idempotency():
    """Two layers: (1) the simulator's own idempotency cache dedupes a repeated
    call with the same key; (2) the REAL correctness backstop is commit_advance's
    `WHERE status=:expected` guard — even if a downstream call somehow fired
    twice, only the first commit can ever land, so courier_dispatch can't double-
    increment. Layer 2 is what actually matters; layer 1 is just a nice-to-have
    that avoids redoing the (simulated) side effect."""
    fresh_db()
    _kill_port(8002)
    time.sleep(0.2)
    courier = subprocess.Popen(
        ["uvicorn", "courier.main:app", "--host", "0.0.0.0", "--port", "8002"],
        cwd=ROOT, env=TEST_ENV, stdout=DEVNULL, stderr=DEVNULL,
    )
    try:
        if not _wait_ready("http://localhost:8002/health"):
            return False, "courier simulator did not come up"

        body = {"order_id": "idem-probe", "step": "out_for_delivery", "idempotency_key": "idem-probe:delivered"}
        with httpx.Client() as http:
            r1 = http.post("http://localhost:8002/dispatch", json=body, timeout=30)
            r2 = http.post("http://localhost:8002/dispatch", json=body, timeout=10)
        layer1_ok = (r1.status_code == 200 and r2.status_code == 200
                     and not r1.json().get("idempotent") and r2.json().get("idempotent") is True)

        from worker.worker import commit_advance
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            now = time.time()
            oid = "idem-commit-probe"
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO orders (id,status,attempt_count,next_attempt_at,customer_id,item,created_at,updated_at)
                   VALUES (?, 'out_for_delivery', 0, 0, NULL, 'idem-probe', ?, ?)""",
                (oid, now, now),
            )
            conn.execute("COMMIT")

            before = conn.execute("SELECT value FROM counters WHERE name='courier_dispatch'").fetchone()["value"]
            first = commit_advance(conn, oid, "out_for_delivery", "delivered", now)
            after_first = conn.execute("SELECT value FROM counters WHERE name='courier_dispatch'").fetchone()["value"]
            second = commit_advance(conn, oid, "out_for_delivery", "delivered", now)  # duplicate commit attempt
            after_second = conn.execute("SELECT value FROM counters WHERE name='courier_dispatch'").fetchone()["value"]
        finally:
            conn.close()

        layer2_ok = (first is True and after_first == before + 1
                     and second is False and after_second == after_first)

        ok = layer1_ok and layer2_ok
        detail = (f"layer1(simulator cache)={layer1_ok}  "
                  f"layer2(commit guard: dispatch {before}->{after_first}->{after_second}, "
                  f"second_commit_applied={second})={layer2_ok}")
        return ok, detail
    finally:
        try:
            courier.terminate()
            courier.wait(timeout=5)
        except Exception:
            try:
                courier.kill()
            except Exception:
                pass


# ── scenario 9 — lease tuning ─────────────────────────────────────────────────

def scenario_lease_tuning():
    """claim_next() takes `now` as an explicit parameter, so the lease boundary
    can be proven exactly without waiting in real time: simulate t0 (claim),
    t0+LEASE-1 (must still be held), and t0+LEASE+1 (must be reclaimable)."""
    fresh_db()
    from worker.worker import claim_next

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        oid = "lease-test-order"
        t0 = 1_000_000.0
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """INSERT INTO orders (id,status,attempt_count,next_attempt_at,customer_id,item,created_at,updated_at)
               VALUES (?, 'ready', 0, 0, NULL, 'lease-probe', ?, ?)""",
            (oid, t0, t0),
        )
        conn.execute("COMMIT")

        claimed_a = claim_next(conn, "worker-A", t0)
        if claimed_a is None or claimed_a["id"] != oid:
            return False, "worker A failed to claim the seeded order"

        claimed_b_early = claim_next(conn, "worker-B", t0 + LEASE_SECONDS - 1)
        if claimed_b_early is not None:
            return False, f"premature double-claim: reclaimed at t0+{LEASE_SECONDS - 1}s (still within lease)"

        claimed_b_late = claim_next(conn, "worker-B", t0 + LEASE_SECONDS + 1)
        if claimed_b_late is None or claimed_b_late["id"] != oid:
            return False, f"order was NOT reclaimable at t0+{LEASE_SECONDS + 1}s (past lease expiry)"

        margin = LEASE_SECONDS - DOWNSTREAM_TIMEOUT
        ok = margin >= 5
        detail = (f"LEASE_SECONDS={LEASE_SECONDS}s  DOWNSTREAM_TIMEOUT={DOWNSTREAM_TIMEOUT}s  margin={margin}s — "
                  f"no double-claim within the lease window, reclaimable right after expiry; "
                  f"a killed worker's order recovers within ~{LEASE_SECONDS}s")
        return ok, detail
    finally:
        conn.close()


# ── scenario 10 — heavy burst, zero lock errors ──────────────────────────────

def scenario_heavy_burst():
    """2000 orders via the real /load/start batched-insert path, 16 workers,
    everything's stderr captured so we can prove zero 'database is locked' /
    OperationalError surfaced anywhere, and that no worker silently died.
    TIME_SCALE is sped up 5x for this scenario only — that only shortens
    simulated downstream sleep time, it doesn't change anything about the
    claim/commit transaction boundaries actually being stress-tested."""
    fresh_db()
    n_workers = 16
    burst_env = {**TEST_ENV, "TIME_SCALE": "0.01"}
    for port in (8000, 8001, 8002):
        _kill_port(port)
    time.sleep(0.3)

    log_dir = Path(tempfile.mkdtemp(prefix="burst_logs_", dir=ROOT))
    procs: dict[str, subprocess.Popen] = {}
    handles = []
    log_paths: dict[str, Path] = {}

    def _spawn_logged(name: str, cmd: list[str]) -> None:
        lp = log_dir / f"{name}.log"
        f = open(lp, "w")
        procs[name] = subprocess.Popen(cmd, cwd=ROOT, env=burst_env, stdout=f, stderr=subprocess.STDOUT)
        handles.append(f)
        log_paths[name] = lp

    try:
        _spawn_logged("restaurant", ["uvicorn", "restaurant.main:app", "--host", "0.0.0.0", "--port", "8001"])
        _spawn_logged("courier", ["uvicorn", "courier.main:app", "--host", "0.0.0.0", "--port", "8002"])
        _spawn_logged("api", ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"])
        if not _wait_ready(f"{API_URL}/health"):
            return False, "API did not come up in time"
        for i in range(n_workers):
            _spawn_logged(f"bw{i + 1}", ["python3", "-m", "worker.worker", "--id", f"bw{i + 1}"])
        time.sleep(0.3)

        with httpx.Client() as http:
            r = http.post(f"{API_URL}/load/start", json={"rate": 2000, "count": 2000}, timeout=10)
            r.raise_for_status()

        # /load/start returns immediately — it only kicks off an async batched-insert
        # task. Must wait for the 2000 rows to actually land before checking drain,
        # or an empty table satisfies "nothing pending" trivially and instantly.
        submit_conn = _conn()
        deadline = time.time() + 30
        submitted_ok = False
        try:
            while time.time() < deadline:
                n = submit_conn.execute("SELECT COUNT(*) n FROM orders").fetchone()["n"]
                if n >= 2000:
                    submitted_ok = True
                    break
                time.sleep(0.2)
        finally:
            submit_conn.close()
        if not submitted_ok:
            return False, "load generator did not finish submitting all 2000 orders within 30s"

        if not wait_all_terminal(180):
            return False, "2000-order burst did not fully drain within 180s"

        conn = _conn()
        try:
            inv = compute_invariants(conn)
        finally:
            conn.close()

        for f in handles:
            f.flush()
            f.close()
        handles = []

        lock_hits = []
        for name, lp in log_paths.items():
            text = lp.read_text(errors="replace")
            hits = text.count("database is locked") + text.count("OperationalError")
            if hits:
                lock_hits.append((name, hits))

        dead_workers = [name for name in (f"bw{i + 1}" for i in range(n_workers))
                        if procs[name].poll() is not None]

        ok = inv["ok"] and inv["delivered"] == 2000 and not lock_hits and not dead_workers
        detail = (f"delivered={inv['delivered']}/2000  lock_errors_found={sum(c for _, c in lock_hits)}  "
                  f"dead_workers={len(dead_workers)}/{n_workers}  inv_ok={inv['ok']}")
        if lock_hits:
            detail += f"  hit_in={lock_hits}"
        if dead_workers:
            detail += f"  dead={dead_workers}"
        return ok, detail
    finally:
        for f in handles:
            try:
                f.close()
            except Exception:
                pass
        for p in procs.values():
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        shutil.rmtree(log_dir, ignore_errors=True)


# ── scenario 11 — a slow downstream doesn't block other orders ──────────────

def scenario_slow_does_not_block():
    """If a downstream call held any shared lock, orders still in the
    restaurant-only phase (placed->confirmed->preparing->ready, never touches
    courier) would queue up behind courier's slow calls. They shouldn't —
    claim/commit are short independent transactions per order."""
    fresh_db()
    procs = start_services(n_workers=6)
    try:
        set_chaos(COURIER_PORT, "slow", 120)
        t_submit = time.time()
        ids = submit_orders(15, "noblock")

        conn = _conn()
        try:
            # Generous but still discriminating: baseline TRANSIENT_ERROR_RATE
            # noise can occasionally chain 2-3 retries on a restaurant step
            # (a few seconds of backoff, unrelated to courier at all) — that's
            # normal variance, not blocking. What we're ruling out is
            # SERIALIZATION behind courier's 6-18s/call slowness, which would
            # push this well past 30s for 15 orders, not just over it.
            deadline = time.time() + 30
            cleared_at = None
            while time.time() < deadline:
                still_pre_ready = conn.execute(
                    "SELECT COUNT(*) n FROM orders WHERE status IN ('placed','confirmed','preparing')"
                ).fetchone()["n"]
                if still_pre_ready == 0:
                    cleared_at = time.time()
                    break
                time.sleep(0.1)
            if cleared_at is None:
                return False, "orders never cleared the restaurant-only steps within 30s — looks blocked"
            restaurant_clear_s = cleared_at - t_submit

            if not wait_all_terminal(60):
                return False, "orders did not all eventually drain (courier slow) within 60s"
            inv = compute_invariants(conn)
        finally:
            conn.close()

        ok = inv["ok"] and inv["delivered"] == len(ids) and restaurant_clear_s < 20.0
        detail = (f"all {len(ids)} orders cleared restaurant-only steps in {restaurant_clear_s:.1f}s "
                  f"while courier was slow (6-18s/call)  delivered={inv['delivered']}/{len(ids)}  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 12 — cancellation at various lifecycle stages ──────────────────

def scenario_cancellation():
    fresh_db()
    procs = start_services(n_workers=4)
    conn = _conn()
    try:
        def status_of(oid):
            r = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
            return r["status"] if r else None

        def wait_for_status(oid, targets, timeout):
            deadline = time.time() + timeout
            while time.time() < deadline:
                s = status_of(oid)
                if s in targets:
                    return s
                time.sleep(0.1)
            return status_of(oid)

        with httpx.Client() as http:
            early_id = submit_orders(1, "cancel-early")[0]
            http.post(f"{API_URL}/orders/{early_id}/cancel", json={"reason": "early"}, timeout=5)

            mid_id = submit_orders(1, "cancel-mid")[0]
            wait_for_status(mid_id, {"preparing", "ready"}, timeout=20)
            http.post(f"{API_URL}/orders/{mid_id}/cancel", json={"reason": "mid"}, timeout=5)

            late_id = submit_orders(1, "cancel-late")[0]
            wait_for_status(late_id, {"out_for_delivery", "delivered"}, timeout=30)
            http.post(f"{API_URL}/orders/{late_id}/cancel", json={"reason": "late"}, timeout=10)

            done_id = submit_orders(1, "cancel-too-late")[0]
            wait_for_status(done_id, {"delivered"}, timeout=20)
            r_too_late = http.post(f"{API_URL}/orders/{done_id}/cancel", json={"reason": "toolate"}, timeout=5)

        if not wait_all_terminal(20):
            return False, "probe orders did not all reach terminal"

        bad = []
        if status_of(early_id) != "cancelled":
            bad.append(f"early status={status_of(early_id)} (want cancelled)")
        if status_of(mid_id) != "cancelled":
            bad.append(f"mid status={status_of(mid_id)} (want cancelled)")
        if status_of(done_id) != "delivered":
            bad.append(f"done status={status_of(done_id)} (want delivered, unchanged)")
        if r_too_late.status_code != 409:
            bad.append(f"cancel-after-delivered returned {r_too_late.status_code}, want 409")

        def reason_for(oid):
            r = conn.execute(
                "SELECT reason FROM order_events WHERE order_id=? AND to_status='cancelled'", (oid,)
            ).fetchone()
            return r["reason"] if r else None

        late_final = status_of(late_id)
        compensation_fired = False
        if late_final == "cancelled":
            r = reason_for(late_id)
            compensation_fired = bool(r and "compensation=cancel-dispatch:ok" in r)
            if not compensation_fired:
                bad.append(f"late cancelled but no compensation recorded (reason={r})")
        elif late_final != "delivered":
            bad.append(f"late ended in unexpected status={late_final}")
        # if late_final == "delivered": the courier finished the delivery before our
        # cancel landed (a real race, not a bug) — acceptable, no compensation expected.

        for oid, label in [(early_id, "early"), (mid_id, "mid")]:
            r = reason_for(oid)
            if r and "compensation" in r:
                bad.append(f"{label} unexpectedly has a compensation marker: {r}")

        dup_comp = conn.execute(
            "SELECT order_id, COUNT(*) n FROM order_events WHERE reason LIKE '%compensation=cancel-dispatch%' "
            "GROUP BY order_id HAVING COUNT(*) > 1"
        ).fetchall()
        if dup_comp:
            bad.append(f"duplicate compensation events: {[dict(r) for r in dup_comp]}")

        inv = compute_invariants(conn)
        ok = (not bad) and inv["ok"]
        detail = (f"early=cancelled  mid=cancelled  late={late_final}(compensation_fired={compensation_fired})  "
                  f"too_late_rejected={r_too_late.status_code == 409}  inv_ok={inv['ok']}")
        if bad:
            detail += f"  ISSUES={bad}"
        return ok, detail
    finally:
        conn.close()
        stop_services(procs)


# ── scenario 13 — illegal transition is rejected ─────────────────────────────

def scenario_illegal_transition_rejected():
    """commit_advance() is the only place that writes the forward-path status —
    it must refuse a (from,to) pair that isn't a legal edge in TRANSITIONS,
    independent of whatever the caller claims, and leave the row untouched."""
    fresh_db()
    from worker.worker import commit_advance
    from common.state_machine import is_valid_transition

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        now = time.time()
        oid = "illegal-transition-probe"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """INSERT INTO orders (id,status,attempt_count,next_attempt_at,customer_id,item,created_at,updated_at)
               VALUES (?, 'placed', 0, 0, NULL, 'illegal-probe', ?, ?)""",
            (oid, now, now),
        )
        conn.execute("COMMIT")

        sanity = is_valid_transition("placed", "out_for_delivery")
        rejected = False
        try:
            commit_advance(conn, oid, "placed", "out_for_delivery", now)
        except ValueError:
            rejected = True

        status_after = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()["status"]

        # A legal transition from the SAME starting point must still work —
        # proves we rejected the illegal jump specifically, not all writes.
        legal_ok = commit_advance(conn, oid, "placed", "confirmed", now)
        status_after_legal = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()["status"]

        ok = (not sanity) and rejected and status_after == "placed" and legal_ok and status_after_legal == "confirmed"
        detail = (f"illegal placed->out_for_delivery rejected={rejected}  "
                  f"state_unchanged_after_reject={status_after == 'placed'}  "
                  f"legal placed->confirmed still works={legal_ok} (status now {status_after_legal})")
        return ok, detail
    finally:
        conn.close()


# ── scenario 14 — graceful SIGTERM vs hard SIGKILL ───────────────────────────

def scenario_graceful_vs_hard_shutdown():
    """SIGTERM lets the worker finish its current cycle (which already
    releases the claim as a side effect of commit/retry/dlq) then exit — the
    held order should be picked up again almost immediately. SIGKILL can't be
    caught at all; recovery for that case is lease expiry, which is much
    slower by design — it's the backstop, not the common path."""
    fresh_db()
    procs = start_services(n_workers=3)
    try:
        conn = _conn()

        def wait_claimed(oid, timeout):
            deadline = time.time() + timeout
            while time.time() < deadline:
                row = conn.execute("SELECT claimed_by FROM orders WHERE id=?", (oid,)).fetchone()
                if row and row["claimed_by"]:
                    return row["claimed_by"]
                time.sleep(0.02)
            return None

        def wait_claim_changed(oid, original_worker, timeout):
            deadline = time.time() + timeout
            while time.time() < deadline:
                row = conn.execute("SELECT claimed_by FROM orders WHERE id=?", (oid,)).fetchone()
                if row and row["claimed_by"] != original_worker:
                    return time.time()
                time.sleep(0.05)
            return None

        oid_a = submit_orders(1, "graceful-probe")[0]
        worker_a = wait_claimed(oid_a, timeout=5)
        if worker_a is None:
            return False, "no worker claimed the graceful-probe order"
        t_term = time.time()
        procs[worker_a].send_signal(signal.SIGTERM)
        procs[worker_a].wait(timeout=10)
        changed_at = wait_claim_changed(oid_a, worker_a, timeout=5)
        graceful_release_s = (changed_at - t_term) if changed_at else None
        # The spawned services run with TEST_LEASE_SECONDS (via TEST_ENV), not
        # the imported LEASE_SECONDS (that's chaos_test.py's own process env,
        # used only by scenario 9's in-process white-box test) — use the one
        # that actually governs these subprocesses' behavior.
        graceful_ok = graceful_release_s is not None and graceful_release_s < (TEST_LEASE_SECONDS / 2)

        survivors = [wid for wid in procs if wid.startswith("tw") and wid != worker_a]

        oid_b = submit_orders(1, "hardkill-probe")[0]
        worker_b = wait_claimed(oid_b, timeout=10)
        if worker_b is None or worker_b not in survivors:
            return False, f"unexpected claimant for hardkill-probe: {worker_b} (survivors={survivors})"
        t_kill = time.time()
        procs[worker_b].kill()  # SIGKILL — cannot be caught, simulates a hard crash
        procs[worker_b].wait(timeout=5)

        row = conn.execute("SELECT claimed_by FROM orders WHERE id=?", (oid_b,)).fetchone()
        still_held_immediately = row["claimed_by"] == worker_b

        changed_at_b = wait_claim_changed(oid_b, worker_b, timeout=TEST_LEASE_SECONDS + 10)
        hard_release_s = (changed_at_b - t_kill) if changed_at_b else None
        hard_ok = hard_release_s is not None and hard_release_s >= TEST_LEASE_SECONDS - 1

        conn.close()
        ok = graceful_ok and still_held_immediately and hard_ok
        detail = (f"graceful SIGTERM: reclaimed in {graceful_release_s:.2f}s "
                  f"(< lease/2={TEST_LEASE_SECONDS / 2}s) -> {graceful_ok}.  "
                  f"hard SIGKILL: held immediately after={still_held_immediately}, "
                  f"reclaimed after {hard_release_s if hard_release_s is None else round(hard_release_s, 1)}s "
                  f"(>= TEST_LEASE_SECONDS-1={TEST_LEASE_SECONDS - 1}s) -> {hard_ok}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 15 — named load profiles ────────────────────────────────────────

def scenario_load_profiles():
    """Every profile is a script over the same /load/start (-> LoadGenerator ->
    _insert_batch) path the manual dashboard buttons use — this runs each of
    the four once, back to back in one stack, checking invariants after every
    single one drains, not just at the very end.

    The four profiles together submit ~5000 orders. TIME_SCALE is sped up 5x
    (same trick as the burst test) so drain keeps pace with submission within
    a sane test budget — that only shortens simulated downstream sleep time,
    it has no effect on the profiles' own (rate, seconds) staging, which is
    real load-generator pacing and is exactly what's being verified here."""
    fresh_db()
    profile_env = {**TEST_ENV, "TIME_SCALE": "0.01"}
    procs = start_services(n_workers=16, env=profile_env)
    try:
        conn = _conn()
        try:
            with httpx.Client() as http:
                results = []
                for name in ("lunch_rush", "steady_evening", "promo_spike", "flash_outage"):
                    r = http.post(f"{API_URL}/load/profile", json={"name": name}, timeout=10)
                    r.raise_for_status()
                    total_seconds = r.json()["total_seconds"]

                    deadline = time.time() + total_seconds + 30
                    finished = False
                    while time.time() < deadline:
                        st = http.get(f"{API_URL}/load/profile", timeout=5).json()
                        if not st["running"]:
                            finished = True
                            break
                        time.sleep(1)
                    if not finished:
                        return False, f"profile {name!r} never finished within {total_seconds + 30}s"

                    if not wait_all_terminal(180):
                        return False, f"orders from profile {name!r} did not fully drain within 180s"

                    inv = compute_invariants(conn)
                    results.append((name, inv))
                    if not inv["ok"]:
                        return False, f"invariants broke after profile {name!r}: {inv}"
        finally:
            conn.close()

        ok = all(inv["ok"] for _, inv in results)
        detail = "  ".join(f"{name}=OK(delivered={inv['delivered']})" for name, inv in results)
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 16 — latency metrics update live during a rush ─────────────────

def scenario_latency_metrics():
    """p50/p95/SLA-breach are computed from existing created_at/updated_at —
    confirm they show real samples WHILE a rush is still draining, not only
    after the fact, and that the numbers are internally sane. TIME_SCALE sped
    up 5x (same as the burst/profile scenarios) so 500 orders on 6 workers
    drains in a sane test budget — that only shortens simulated downstream
    sleep, it has no bearing on whether latency is computed correctly."""
    fresh_db()
    procs = start_services(n_workers=6, env={**TEST_ENV, "TIME_SCALE": "0.01"})
    try:
        with httpx.Client() as http:
            r = http.post(f"{API_URL}/load/rush", timeout=5)
            r.raise_for_status()

            saw_live_samples = False
            deadline = time.time() + 40
            while time.time() < deadline:
                m = http.get(f"{API_URL}/metrics", timeout=5).json()
                if m["latency"]["sample_n"] > 0:
                    saw_live_samples = True
                    break
                time.sleep(1)

            if not wait_all_terminal(90):
                return False, "rush did not fully drain within 90s"

            m = http.get(f"{API_URL}/metrics", timeout=5).json()

        lat = m["latency"]
        conn = _conn()
        try:
            inv = compute_invariants(conn)
        finally:
            conn.close()

        sane = lat["p50_s"] > 0 and lat["p95_s"] >= lat["p50_s"] and lat["sample_n"] >= 400
        ok = saw_live_samples and sane and inv["ok"]
        detail = f"saw_live_samples_during_rush={saw_live_samples}  final_latency={lat}  inv_ok={inv['ok']}"
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 17 — poison order doesn't block or slow other orders ───────────

def scenario_poison_order():
    """A poison order always fails at whichever downstream it's currently at
    — it should retry, exhaust attempts, and DLQ, WITHOUT slowing or blocking
    the normal orders mixed in alongside it."""
    fresh_db()
    procs = start_services(n_workers=6)
    try:
        with httpx.Client() as http:
            r = http.post(f"{API_URL}/orders", json={"item": "poison-probe", "poison": True}, timeout=5)
            poison_id = r.json()["order_id"]
        normal_ids = submit_orders(40, "normal-mix")

        t0 = time.time()
        if not wait_all_terminal(60):
            return False, "mix did not fully drain within 60s"
        elapsed = time.time() - t0

        conn = _conn()
        try:
            poison_row = conn.execute(
                "SELECT status, attempt_count FROM orders WHERE id=?", (poison_id,)
            ).fetchone()
            ph = ",".join("?" * len(normal_ids))
            normal_rows = conn.execute(f"SELECT status FROM orders WHERE id IN ({ph})", normal_ids).fetchall()
            normal_delivered = sum(1 for r in normal_rows if r["status"] == "delivered")
            inv = compute_invariants(conn)
        finally:
            conn.close()

        poison_ok = poison_row["status"] == "failed" and poison_row["attempt_count"] == MAX_ATTEMPTS
        normal_ok = normal_delivered == len(normal_ids)
        # The poison order's OWN backoff sequence to MAX_ATTEMPTS is ~30-35s
        # (2+4+8+16s + jitter) regardless of blocking — that's just how long 5
        # attempts take, not evidence of anything stuck. The bound here is
        # about ruling out catastrophic blocking (which would push this into
        # the hundreds of seconds as 40 orders queue up behind one worker),
        # not about being faster than the poison order's own natural timing.
        speed_ok = elapsed < 70.0

        ok = poison_ok and normal_ok and speed_ok and inv["ok"]
        detail = (f"poison: status={poison_row['status']} attempts={poison_row['attempt_count']} "
                  f"(want failed/{MAX_ATTEMPTS})  normal: delivered={normal_delivered}/{len(normal_ids)}  "
                  f"drain={elapsed:.1f}s  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 18 — delivery zones: a zone-scoped outage stays scoped ─────────

def scenario_zones():
    """A courier outage scoped to ONE zone shouldn't affect the others.
    Cancellation/DLQ/zone are all dimensions on the same orders table, not
    separate bookkeeping, so global invariants must hold throughout too."""
    fresh_db()
    procs = start_services(n_workers=8)
    try:
        with httpx.Client() as http:
            r = http.post(f"{API_URL}/chaos/courier", json={"mode": "down", "seconds": 30, "zone": "north"}, timeout=5)
            r.raise_for_status()

            ids_by_zone = {}
            for zone in ("north", "south", "east", "west"):
                ids_by_zone[zone] = []
                for i in range(8):
                    rr = http.post(f"{API_URL}/orders", json={"item": f"{zone}-{i}", "zone": zone}, timeout=5)
                    ids_by_zone[zone].append(rr.json()["order_id"])

        if not wait_all_terminal(60):
            return False, "zoned orders did not fully drain within 60s"

        conn = _conn()
        try:
            results = {}
            for zone, ids in ids_by_zone.items():
                ph = ",".join("?" * len(ids))
                rows = conn.execute(f"SELECT status FROM orders WHERE id IN ({ph})", ids).fetchall()
                results[zone] = {
                    "delivered": sum(1 for r in rows if r["status"] == "delivered"),
                    "failed": sum(1 for r in rows if r["status"] == "failed"),
                }
            inv = compute_invariants(conn)
        finally:
            conn.close()

        # The other 3 zones should be essentially unaffected — all 8 delivered.
        # north (the outage zone) may have some DLQ; what matters is nothing
        # is LOST (every one of its 8 reaches some terminal state).
        others_ok = all(results[z]["delivered"] == 8 for z in ("south", "east", "west"))
        north_accounted = (results["north"]["delivered"] + results["north"]["failed"]) == 8

        ok = others_ok and north_accounted and inv["ok"]
        detail = f"results={results}  others_unaffected={others_ok}  inv_ok={inv['ok']}"
        return ok, detail
    finally:
        stop_services(procs)


# ── scenario 19 — circuit breaker opens, stops hammering, recovers ──────────

def scenario_circuit_breaker():
    """With courier down, the breaker should OPEN — proven by the simulator's
    own request counter plateauing, not just a state label — then recover
    through half-open once courier comes back. Needs a steady trickle of
    fresh orders throughout: a breaker only gets evaluated when there's
    traffic to evaluate it against, same as any real implementation — if
    every pending order DLQs via its own backoff before fresh traffic shows
    up, there's nothing left to carry a probe through."""
    fresh_db()
    procs = start_services(n_workers=4)
    try:
        outage_seconds = 25
        set_chaos(COURIER_PORT, "down", outage_seconds)
        t_start = time.time()

        conn = _conn()
        try:
            opened = False
            n_during_open_start = n_during_open_later = None
            state = "closed"
            deadline = t_start + outage_seconds + 30
            while time.time() < deadline:
                submit_orders(1, "cb-trickle")
                row = conn.execute("SELECT state FROM circuit_breakers WHERE downstream='courier'").fetchone()
                state = row["state"]
                if state == "open" and not opened:
                    opened = True
                    with httpx.Client() as http:
                        n_during_open_start = http.get("http://localhost:8002/health", timeout=5).json()["request_count"]
                if opened and n_during_open_later is None and time.time() - t_start > outage_seconds * 0.6:
                    with httpx.Client() as http:
                        n_during_open_later = http.get("http://localhost:8002/health", timeout=5).json()["request_count"]
                if state == "closed" and opened:
                    break
                time.sleep(1)
            recovered = state == "closed" and opened

            if not wait_all_terminal(60):
                return False, "orders did not all drain within 60s after breaker recovery"
            inv = compute_invariants(conn)
        finally:
            conn.close()

        plateaued = (
            n_during_open_start is not None and n_during_open_later is not None
            and (n_during_open_later - n_during_open_start) <= 3
        )
        ok = opened and plateaued and recovered and inv["ok"]
        detail = (f"opened={opened}  courier_request_count_during_open: "
                  f"{n_during_open_start}->{n_during_open_later} (plateaued={plateaued})  "
                  f"recovered_to_closed={recovered}  inv_ok={inv['ok']}")
        return ok, detail
    finally:
        stop_services(procs)


# ── main ──────────────────────────────────────────────────────────────────────

SCENARIOS = [
    ("1. Happy path", scenario_happy_path),
    ("2. Ordering — legal sequence, no skips", scenario_ordering),
    ("3. Slow downstream", scenario_slow_downstream),
    ("4. Down then recover + DLQ replay", scenario_down_then_replay),
    ("5. Worker crash mid-rush (x5)", scenario_worker_crash_x5),
    ("6. Concurrent — no double-claim", scenario_no_double_claim),
    ("7. Max retries -> DLQ, none stuck forever", scenario_max_retries_dlq),
    ("8. Idempotency under duplicate downstream call", scenario_idempotency),
    ("9. Lease tuning", scenario_lease_tuning),
    ("10. Heavy burst (2000 orders), zero lock errors", scenario_heavy_burst),
    ("11. Slow downstream doesn't block other orders", scenario_slow_does_not_block),
    ("12. Cancellation at various lifecycle stages", scenario_cancellation),
    ("13. Illegal transition is rejected", scenario_illegal_transition_rejected),
    ("14. Graceful SIGTERM vs hard SIGKILL", scenario_graceful_vs_hard_shutdown),
    ("15. Named load profiles", scenario_load_profiles),
    ("16. Latency metrics update live during a rush", scenario_latency_metrics),
    ("17. Poison order doesn't block throughput", scenario_poison_order),
    ("18. Delivery zones — outage stays scoped", scenario_zones),
    ("19. Circuit breaker opens and recovers", scenario_circuit_breaker),
]


def main() -> None:
    print(SEP)
    print("  order-pipeline correctness suite")
    print(SEP)
    print(f"  LEASE={TEST_LEASE_SECONDS}s  MAX_ATTEMPTS={MAX_ATTEMPTS}\n")

    results = [run_scenario(name, fn) for name, fn in SCENARIOS]

    print(f"\n{SEP}")
    print("  SUMMARY")
    print(SEP)
    for r in results:
        mark = "PASS" if r["passed"] else "FAIL"
        print(f"  [{mark}]  {r['name']:<42} {r['seconds']:>6}s  {r['detail']}")
    n_pass = sum(1 for r in results if r["passed"])
    print(SEP)
    print(f"  {n_pass}/{len(results)} scenarios passed")
    print(SEP)

    sys.exit(0 if n_pass == len(results) else 1)


if __name__ == "__main__":
    main()

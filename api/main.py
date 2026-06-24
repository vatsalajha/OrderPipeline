import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import random
import signal
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from common.db import get_conn, init_db
from common.state_machine import is_valid_transition
from config import (
    LEASE_SECONDS, RESTAURANT_URL, COURIER_URL, WORKER_COUNT, HEARTBEAT_INTERVAL,
    SLA_THRESHOLD_SECONDS, ZONES,
)

PROJECT_ROOT = Path(__file__).parent.parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    WORKER_POOL.start(WORKER_COUNT)
    try:
        yield
    finally:
        await LOAD_PROFILE.stop()
        await LOAD.stop()
        await CANCEL_CHAOS.stop()
        WORKER_POOL.shutdown()


app = FastAPI(title="Order Pipeline API", lifespan=lifespan)

DASHBOARD = Path(__file__).parent.parent / "dashboard" / "index.html"


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD.read_text()


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

class OrderRequest(BaseModel):
    customer_id: str | None = None
    item: str | None = None
    poison: bool = False
    zone: str | None = None


@app.post("/orders", status_code=202)
def create_order(req: OrderRequest):
    order_id = str(uuid.uuid4())
    now = time.time()
    zone = req.zone if req.zone in ZONES else random.choice(ZONES)
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO orders
                (id, status, attempt_count, next_attempt_at, customer_id, item, created_at, updated_at, poison, zone)
            VALUES (?, 'placed', 0, 0, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, req.customer_id, req.item, now, now, int(req.poison), zone),
        )
        conn.execute(
            "INSERT INTO order_events(order_id, from_status, to_status, reason, at) VALUES (?,NULL,'placed','created',?)",
            (order_id, now),
        )
        conn.execute(
            "UPDATE counters SET value = value + 1 WHERE name = 'orders_submitted'"
        )
        conn.commit()
    finally:
        conn.close()
    return {"order_id": order_id, "status": "placed"}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Order not found")
        events = conn.execute(
            "SELECT * FROM order_events WHERE order_id = ? ORDER BY at", (order_id,)
        ).fetchall()
        return {"order": dict(row), "events": [dict(e) for e in events]}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Cancellation (section 13 stretch #4) — a guarded transition like any other,
# legal from any non-terminal state. Cancelling after the courier was already
# dispatched (out_for_delivery) needs a compensating action, not just a status
# flip — that's the mini-saga: the status change is the authoritative,
# guarded write; the compensation is a best-effort side call that happens
# after, outside any transaction, same as a normal downstream call. If it
# fails we record that rather than retry — a production version would put it
# on a durable retry queue instead of a synchronous inline call.
# ---------------------------------------------------------------------------

class CancelRequest(BaseModel):
    reason: str | None = None


def _compensate_courier_dispatch(order_id: str) -> dict:
    idem_key = f"{order_id}:cancel-dispatch"
    outcome = {"called": True, "ok": False}
    try:
        with httpx.Client() as http:
            r = http.post(
                f"{COURIER_URL}/cancel-dispatch",
                json={"order_id": order_id, "idempotency_key": idem_key},
                timeout=5,
            )
        outcome["ok"] = r.status_code == 200
        outcome["status_code"] = r.status_code
    except Exception as e:
        outcome["ok"] = False
        outcome["error"] = str(e)
    return outcome


def _cancel_order(order_id: str, reason: str) -> dict:
    now = time.time()
    conn = get_conn()
    try:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Order not found")
        current = row["status"]
        if not is_valid_transition(current, "cancelled"):
            raise HTTPException(status_code=409, detail=f"order already terminal ({current})")
        result = conn.execute(
            "UPDATE orders SET status='cancelled', claimed_by=NULL, claimed_at=NULL, updated_at=? "
            "WHERE id=? AND status=?",
            (now, order_id, current),
        )
        if result.rowcount == 0:
            conn.commit()
            raise HTTPException(status_code=409, detail="order status changed concurrently, retry")
        conn.commit()
    finally:
        conn.close()

    compensation = None
    event_reason = reason
    if current == "out_for_delivery":
        compensation = _compensate_courier_dispatch(order_id)
        event_reason = f"{reason}; compensation=cancel-dispatch:{'ok' if compensation['ok'] else 'failed'}"

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO order_events(order_id, from_status, to_status, reason, at) VALUES (?,?,'cancelled',?,?)",
            (order_id, current, event_reason, time.time()),
        )
        conn.commit()
    finally:
        conn.close()

    return {"order_id": order_id, "status": "cancelled", "was": current, "compensation": compensation}


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: str, req: CancelRequest = CancelRequest()):
    return _cancel_order(order_id, reason=req.reason or "customer_cancelled")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    conn = get_conn()
    try:
        rows = conn.execute("SELECT status, COUNT(*) n FROM orders GROUP BY status").fetchall()
        counts = {r["status"]: r["n"] for r in rows}
        dlq = conn.execute("SELECT COUNT(*) n FROM dead_letters").fetchone()["n"]
        total = conn.execute("SELECT COUNT(*) n FROM orders").fetchone()["n"]
        return {"status": "ok", "total_orders": total, "order_counts": counts, "dlq_size": dlq}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Latency / SLA, per-zone, circuit breaker, and DLQ-reason helpers — shared
# by /metrics and the WS snapshot so both surfaces report identical numbers.
# All computed from existing timestamps/columns; nothing new to track.
# ---------------------------------------------------------------------------

LATENCY_WINDOW_SECONDS = 300  # rolling window — "live during a rush", not an all-time average


def _latency_metrics(conn, now: float) -> dict:
    rows = conn.execute(
        "SELECT created_at, updated_at FROM orders WHERE status='delivered' AND updated_at >= ?",
        (now - LATENCY_WINDOW_SECONDS,),
    ).fetchall()
    latencies = sorted(r["updated_at"] - r["created_at"] for r in rows)

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = min(len(latencies) - 1, int(round(p * (len(latencies) - 1))))
        return round(latencies[idx], 2)

    sla_breaches_in_flight = conn.execute(
        "SELECT COUNT(*) n FROM orders WHERE status NOT IN ('delivered','cancelled','failed') AND ? - created_at > ?",
        (now, SLA_THRESHOLD_SECONDS),
    ).fetchone()["n"]

    return {
        "p50_s": pct(0.50),
        "p95_s": pct(0.95),
        "sla_threshold_s": SLA_THRESHOLD_SECONDS,
        "sla_breaches_in_flight": sla_breaches_in_flight,
        "sla_breaches_delivered_recent": sum(1 for x in latencies if x > SLA_THRESHOLD_SECONDS),
        "window_s": LATENCY_WINDOW_SECONDS,
        "sample_n": len(latencies),
    }


def _zone_metrics(conn, now: float) -> dict:
    zones: dict[str, dict] = {z: {"by_state": {}, "throughput_5s": 0} for z in ZONES}
    for r in conn.execute("SELECT zone, status, COUNT(*) n FROM orders WHERE zone IS NOT NULL GROUP BY zone, status"):
        zones.setdefault(r["zone"], {"by_state": {}, "throughput_5s": 0})["by_state"][r["status"]] = r["n"]
    for r in conn.execute(
        """SELECT o.zone AS zone, COUNT(*) n FROM order_events e JOIN orders o ON o.id = e.order_id
            WHERE e.to_status='delivered' AND e.at >= ? GROUP BY o.zone""",
        (now - 5.0,),
    ):
        if r["zone"] in zones:
            zones[r["zone"]]["throughput_5s"] = r["n"]
    return zones


def _circuit_breaker_status(conn) -> dict:
    return {
        r["downstream"]: {
            "state": r["state"],
            "consecutive_failures": r["consecutive_failures"],
            "opened_at": r["opened_at"],
        }
        for r in conn.execute("SELECT downstream, state, consecutive_failures, opened_at FROM circuit_breakers")
    }


def _recent_dlq(conn, limit: int = 10) -> list:
    rows = conn.execute(
        """SELECT d.order_id, d.last_status, d.reason, d.at, o.poison, o.zone
            FROM dead_letters d JOIN orders o ON o.id = d.order_id
            ORDER BY d.at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Metrics (section 10) — per-state counts, in-flight, due-backlog, retries,
# DLQ size, per-worker last-seen heartbeat, downstream states, latency/SLA,
# per-zone breakdown, circuit breaker state, and the two reconciliation
# invariants (section 12).
# ---------------------------------------------------------------------------

def _build_metrics() -> dict:
    """Sync — gathers everything from SQLite. Wrapped by /metrics in an executor."""
    conn = get_conn()
    try:
        now = time.time()

        rows = conn.execute(
            "SELECT status, COUNT(*) n FROM orders GROUP BY status"
        ).fetchall()
        by_state = {r["status"]: r["n"] for r in rows}

        in_flight = conn.execute(
            "SELECT COUNT(*) n FROM orders WHERE claimed_by IS NOT NULL AND claimed_at > ?",
            (now - LEASE_SECONDS,),
        ).fetchone()["n"]

        due_backlog = conn.execute(
            """
            SELECT COUNT(*) n FROM orders
             WHERE status NOT IN ('delivered','cancelled','failed')
               AND next_attempt_at <= ?
               AND (claimed_by IS NULL OR claimed_at <= ?)
            """,
            (now, now - LEASE_SECONDS),
        ).fetchone()["n"]

        waiting = conn.execute(
            """
            SELECT COUNT(*) n FROM orders
             WHERE status NOT IN ('delivered','cancelled','failed')
               AND (claimed_by IS NULL OR claimed_at <= ?)
            """,
            (now - LEASE_SECONDS,),
        ).fetchone()["n"]

        total_retries = conn.execute(
            "SELECT COALESCE(SUM(attempt_count),0) n FROM orders WHERE attempt_count > 0"
        ).fetchone()["n"]

        dlq_size = conn.execute("SELECT COUNT(*) n FROM dead_letters").fetchone()["n"]

        submitted = conn.execute(
            "SELECT value FROM counters WHERE name='orders_submitted'"
        ).fetchone()["value"]
        dispatch = conn.execute(
            "SELECT value FROM counters WHERE name='courier_dispatch'"
        ).fetchone()["value"]

        wrows = conn.execute(
            "SELECT id, pid, started_at, last_seen FROM workers ORDER BY id"
        ).fetchall()
        stale_after = HEARTBEAT_INTERVAL * 3
        workers = []
        for w in wrows:
            age = round(now - w["last_seen"], 2)
            workers.append({
                "id": w["id"],
                "pid": w["pid"],
                "last_seen_age_s": age,
                "uptime_s": round(now - w["started_at"], 1),
                "alive": age <= stale_after,
            })

        delivered = by_state.get("delivered", 0)
        cancelled = by_state.get("cancelled", 0)
        failed = by_state.get("failed", 0)
        recon_rhs = delivered + cancelled + failed + in_flight + waiting

        return {
            "ts": now,
            "orders_by_state": by_state,
            "in_flight": in_flight,
            "due_backlog": due_backlog,
            "waiting": waiting,
            "total_retries": total_retries,
            "dlq_size": dlq_size,
            "latency": _latency_metrics(conn, now),
            "zones": _zone_metrics(conn, now),
            "circuit_breakers": _circuit_breaker_status(conn),
            "recent_dlq": _recent_dlq(conn),
            "workers": {"alive": sum(1 for w in workers if w["alive"]),
                        "total": len(workers), "heartbeats": workers},
            "reconciliation": {
                "no_orders_lost": {
                    "ok": submitted == recon_rhs,
                    "submitted": submitted,
                    "accounted": recon_rhs,
                    "breakdown": {"delivered": delivered, "cancelled": cancelled,
                                  "failed_dlq": failed, "in_flight": in_flight,
                                  "waiting": waiting},
                },
                "no_double_processing": {
                    "ok": dispatch == delivered,
                    "courier_dispatch": dispatch,
                    "delivered": delivered,
                },
            },
        }
    finally:
        conn.close()


@app.get("/metrics")
async def metrics():
    loop = asyncio.get_running_loop()
    async with httpx.AsyncClient() as http:
        m, ds = await asyncio.gather(
            loop.run_in_executor(None, _build_metrics),
            _downstream_health(http),
        )
    m["downstreams"] = ds
    return m


# ---------------------------------------------------------------------------
# Chaos proxy — browser talks to /chaos/* so no CORS issues
# ---------------------------------------------------------------------------

class ChaosRequest(BaseModel):
    mode: str = "normal"
    seconds: int = 60
    zone: str | None = None  # courier only — see /chaos/courier


@app.post("/chaos/restaurant")
async def chaos_restaurant(req: ChaosRequest):
    async with httpx.AsyncClient() as http:
        r = await http.post(f"{RESTAURANT_URL}/chaos", json=req.model_dump(), timeout=5)
        return r.json()


@app.post("/chaos/courier")
async def chaos_courier(req: ChaosRequest):
    async with httpx.AsyncClient() as http:
        r = await http.post(f"{COURIER_URL}/chaos", json=req.model_dump(), timeout=5)
        return r.json()


# ---------------------------------------------------------------------------
# Worker pool supervisor — the API owns the worker subprocesses
# ---------------------------------------------------------------------------
# Why not honcho? honcho terminates the whole process group the moment ANY
# managed process exits. /worker/kill must SIGTERM a single worker WITHOUT
# bringing the stack down, so the API spawns/owns the workers itself. A killed
# worker's in-flight order is reclaimed automatically by lease expiry — no
# reaper needed (see section 6). Restart is one click: POST /worker/start.

class WorkerPool:
    def __init__(self):
        self._procs: list[dict] = []   # {id, pid, proc, started_at}
        self._seq = 0

    def _spawn(self) -> dict:
        self._seq += 1
        wid = f"worker-{self._seq}"
        proc = subprocess.Popen(
            ["python3", "-m", "worker.worker", "--id", wid],
            cwd=str(PROJECT_ROOT),
        )
        rec = {"id": wid, "pid": proc.pid, "proc": proc, "started_at": time.time()}
        self._procs.append(rec)
        return rec

    def start(self, n: int) -> list[str]:
        return [self._spawn()["id"] for _ in range(n)]

    def _alive(self) -> list[dict]:
        return [p for p in self._procs if p["proc"].poll() is None]

    def kill(self, n: int) -> list[str]:
        """SIGTERM up to n live workers (newest first). Returns killed ids."""
        alive = sorted(self._alive(), key=lambda p: p["started_at"], reverse=True)
        killed = []
        for rec in alive[:n]:
            try:
                rec["proc"].send_signal(signal.SIGTERM)
                killed.append(rec["id"])
            except ProcessLookupError:
                pass
        return killed

    def status(self) -> dict:
        # Reap finished procs so poll() state is current, then report.
        workers = []
        for p in self._procs:
            alive = p["proc"].poll() is None
            workers.append({
                "id": p["id"],
                "pid": p["pid"],
                "alive": alive,
                "uptime": round(time.time() - p["started_at"], 1) if alive else None,
            })
        live = [w for w in workers if w["alive"]]
        return {"alive": len(live), "total": len(workers), "workers": workers}

    def shutdown(self) -> None:
        for p in self._procs:
            if p["proc"].poll() is None:
                try:
                    p["proc"].send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    pass


WORKER_POOL = WorkerPool()


class WorkerKillRequest(BaseModel):
    n: int = 1


@app.post("/worker/kill")
def worker_kill(req: WorkerKillRequest):
    killed = WORKER_POOL.kill(req.n)
    return {
        "killed": killed,
        "count": len(killed),
        "pool": WORKER_POOL.status(),
        "note": "Their in-flight orders are reclaimed by lease expiry "
                f"(~{LEASE_SECONDS}s). Restart with POST /worker/start "
                "or the '+ Worker' dashboard button.",
    }


class WorkerStartRequest(BaseModel):
    n: int = 1


@app.post("/worker/start")
def worker_start(req: WorkerStartRequest):
    started = WORKER_POOL.start(req.n)
    return {"started": started, "count": len(started), "pool": WORKER_POOL.status()}


@app.get("/workers")
def workers():
    return WORKER_POOL.status()


# ---------------------------------------------------------------------------
# Load generator — non-blocking background task inside the API
# ---------------------------------------------------------------------------

def _insert_batch(n: int, tag: str) -> None:
    """Insert n 'placed' orders + their creation events in one transaction.
    Sync (runs in a thread) so it never blocks the event loop."""
    now = time.time()
    conn = get_conn()
    try:
        orders, events = [], []
        for _ in range(n):
            oid = str(uuid.uuid4())
            orders.append((oid, tag, now, now, random.choice(ZONES)))
            events.append((oid, now))
        conn.executemany(
            "INSERT INTO orders (id, status, attempt_count, next_attempt_at, "
            "customer_id, item, created_at, updated_at, zone) "
            "VALUES (?, 'placed', 0, 0, NULL, ?, ?, ?, ?)",
            orders,
        )
        conn.executemany(
            "INSERT INTO order_events(order_id, from_status, to_status, reason, at) "
            "VALUES (?, NULL, 'placed', 'created', ?)",
            events,
        )
        conn.execute(
            "UPDATE counters SET value = value + ? WHERE name = 'orders_submitted'",
            (n,),
        )
        conn.commit()
    finally:
        conn.close()


class LoadGenerator:
    """Drips `count` orders into the pipeline at `rate`/sec, non-blocking.
    Batches per 100ms tick so a 500-order rush is ~50 small writes, not 500."""
    TICK = 0.1

    def __init__(self):
        self.task: asyncio.Task | None = None
        self.running = False
        self.submitted = 0
        self.target = 0
        self.rate = 0

    async def _run(self, rate: float, count: int, tag: str):
        loop = asyncio.get_running_loop()
        per_tick = max(1, round(rate * self.TICK))
        try:
            while self.submitted < count:
                n = min(per_tick, count - self.submitted)
                await loop.run_in_executor(None, _insert_batch, n, tag)
                self.submitted += n
                await asyncio.sleep(self.TICK)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False

    async def start(self, rate: float, count: int, tag: str = "load"):
        await self.stop()
        self.running = True
        self.submitted = 0
        self.target = count
        self.rate = rate
        self.task = asyncio.create_task(self._run(rate, count, tag))

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.running = False

    def status(self) -> dict:
        return {
            "running": self.running,
            "submitted": self.submitted,
            "target": self.target,
            "rate": self.rate,
        }


LOAD = LoadGenerator()


class LoadStartRequest(BaseModel):
    rate: float = 5
    count: int = 200


@app.post("/load/start")
async def load_start(req: LoadStartRequest):
    await LOAD.start(req.rate, req.count, tag="load")
    return {"status": "started", **LOAD.status()}


@app.post("/load/rush")
async def load_rush():
    # Preset dinner-rush spike: ~500 orders in ~10s.
    await LOAD.start(rate=50, count=500, tag="rush")
    return {"status": "rush", **LOAD.status()}


@app.post("/load/stop")
async def load_stop():
    await LOAD.stop()
    return {"status": "stopped", **LOAD.status()}


# ---------------------------------------------------------------------------
# Named load profiles — each one is just a SCRIPT of (rate, seconds) stages
# fed through the same LOAD.start()/_insert_batch() path the manual buttons
# use, optionally interleaved with scheduled /chaos calls. No profile ever
# inserts an order directly; they only sequence the existing primitives.
# ---------------------------------------------------------------------------

LOAD_PROFILES: dict[str, dict] = {
    "lunch_rush": {
        "label": "Lunch Rush",
        "stages": [
            {"rate": 40, "seconds": 20},   # sharp spike
            {"rate": 15, "seconds": 20},   # taper
            {"rate": 5,  "seconds": 20},   # tail off
        ],
    },
    "steady_evening": {
        "label": "Steady Evening",
        "stages": [
            {"rate": 8, "seconds": 120},   # sustained moderate rate for a couple minutes
        ],
    },
    "promo_spike": {
        "label": "Promo Spike (3x)",
        "stages": [
            {"rate": 15, "seconds": 90},   # 3x the default manual rate (5/s), sustained
        ],
    },
    "flash_outage": {
        "label": "Flash Outage",
        "stages": [
            {"rate": 30, "seconds": 50},   # heavy load runs the whole time
        ],
        "chaos": [
            # courier goes down 15s into the load, recovers 20s later (at t=35s),
            # leaving 15s of load still running post-recovery to show the drain.
            {"target": "courier", "mode": "down", "at": 15, "seconds": 20},
        ],
    },
}


class LoadProfileRunner:
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.running = False
        self.name: str | None = None
        self.started_at: float = 0.0
        self.total_seconds: float = 0.0

    async def _run_chaos_step(self, step: dict, http: httpx.AsyncClient) -> None:
        await asyncio.sleep(step["at"])
        url = f"{RESTAURANT_URL}/chaos" if step["target"] == "restaurant" else f"{COURIER_URL}/chaos"
        await http.post(url, json={"mode": step["mode"], "seconds": step["seconds"]}, timeout=5)
        await asyncio.sleep(step["seconds"])
        await http.post(url, json={"mode": "normal", "seconds": 0}, timeout=5)

    async def _run(self, profile: dict) -> None:
        try:
            async with httpx.AsyncClient() as http:
                chaos_tasks = [
                    asyncio.create_task(self._run_chaos_step(step, http))
                    for step in profile.get("chaos", [])
                ]
                for stage in profile["stages"]:
                    rate, seconds = stage["rate"], stage["seconds"]
                    count = max(1, round(rate * seconds))
                    await LOAD.start(rate, count, tag=f"profile:{self.name}")
                    deadline = time.time() + seconds + 10
                    while LOAD.running and time.time() < deadline:
                        await asyncio.sleep(0.2)
                for t in chaos_tasks:
                    if not t.done():
                        await t
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False

    async def start(self, name: str) -> None:
        await self.stop()
        profile = LOAD_PROFILES[name]
        self.running = True
        self.name = name
        self.started_at = time.time()
        self.total_seconds = sum(s["seconds"] for s in profile["stages"])
        self.task = asyncio.create_task(self._run(profile))

    async def stop(self) -> None:
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        await LOAD.stop()
        self.running = False
        self.name = None

    def status(self) -> dict:
        return {
            "running": self.running,
            "name": self.name,
            "label": LOAD_PROFILES[self.name]["label"] if self.name else None,
            "elapsed": round(time.time() - self.started_at, 1) if self.running else 0,
            "total_seconds": self.total_seconds,
        }


LOAD_PROFILE = LoadProfileRunner()


class LoadProfileRequest(BaseModel):
    name: str


@app.post("/load/profile")
async def load_profile_start(req: LoadProfileRequest):
    if req.name not in LOAD_PROFILES:
        raise HTTPException(status_code=400, detail=f"unknown profile {req.name!r}")
    await LOAD_PROFILE.start(req.name)
    return {"status": "started", **LOAD_PROFILE.status()}


@app.post("/load/profile/stop")
async def load_profile_stop():
    await LOAD_PROFILE.stop()
    return {"status": "stopped"}


@app.get("/load/profile")
def load_profile_status():
    return LOAD_PROFILE.status()


@app.get("/load/profiles")
def load_profiles_list():
    return {name: {"label": p["label"]} for name, p in LOAD_PROFILES.items()}


# ---------------------------------------------------------------------------
# Cancel-chaos — load-generator option that randomly cancels a small % of
# currently in-flight orders, modeling a customer changing their mind mid-flight.
# Sampling from ALL non-terminal orders (not just freshly-placed ones) means
# some land early and some land after out_for_delivery, exercising the
# compensation path in the mini-saga above.
# ---------------------------------------------------------------------------

class CancelChaos:
    TICK = 0.5

    def __init__(self):
        self.task: asyncio.Task | None = None
        self.running = False
        self.cancelled_count = 0

    def _cancel_random_batch(self, pct: float) -> int:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT id FROM orders WHERE status NOT IN ('delivered','cancelled','failed')"
            ).fetchall()
        finally:
            conn.close()
        ids = [r["id"] for r in rows]
        k = max(0, round(len(ids) * pct / 100))
        if k == 0:
            return 0
        n = 0
        for oid in random.sample(ids, min(k, len(ids))):
            try:
                _cancel_order(oid, reason="random_chaos_cancel")
                n += 1
            except Exception:
                pass  # lost the race with a worker advancing/finishing it — fine, skip
        return n

    async def _run(self, pct: float, seconds: float):
        loop = asyncio.get_running_loop()
        deadline = time.time() + seconds
        try:
            while time.time() < deadline:
                n = await loop.run_in_executor(None, self._cancel_random_batch, pct)
                self.cancelled_count += n
                await asyncio.sleep(self.TICK)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False

    async def start(self, pct: float, seconds: float):
        await self.stop()
        self.running = True
        self.cancelled_count = 0
        self.task = asyncio.create_task(self._run(pct, seconds))

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.running = False


CANCEL_CHAOS = CancelChaos()


class CancelChaosRequest(BaseModel):
    percent: float = 5
    seconds: float = 30


@app.post("/load/cancel-chaos")
async def load_cancel_chaos(req: CancelChaosRequest):
    await CANCEL_CHAOS.start(req.percent, req.seconds)
    return {"status": "started", "percent": req.percent, "seconds": req.seconds}


@app.post("/load/cancel-chaos/stop")
async def load_cancel_chaos_stop():
    await CANCEL_CHAOS.stop()
    return {"status": "stopped", "cancelled_count": CANCEL_CHAOS.cancelled_count}


# ---------------------------------------------------------------------------
# DLQ replay (stretch) — once a downstream recovers, reset dead-lettered orders
# back to their last good state so the workers resume them from where they
# failed. Nothing is re-done from scratch: last_status is the non-terminal
# state the order was stuck in when retries were exhausted.
# ---------------------------------------------------------------------------

@app.post("/dlq/replay")
def dlq_replay():
    now = time.time()
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT order_id, last_status FROM dead_letters"
        ).fetchall()
        restored = []
        for r in rows:
            res = conn.execute(
                "UPDATE orders SET status=?, attempt_count=0, claimed_by=NULL, "
                "claimed_at=NULL, next_attempt_at=0, updated_at=? "
                "WHERE id=? AND status='failed'",
                (r["last_status"], now, r["order_id"]),
            )
            if res.rowcount:
                conn.execute(
                    "INSERT INTO order_events(order_id, from_status, to_status, reason, at) "
                    "VALUES (?, 'failed', ?, 'dlq_replay', ?)",
                    (r["order_id"], r["last_status"], now),
                )
                restored.append({"order_id": r["order_id"], "restored_to": r["last_status"]})
        if restored:
            ids = [o["order_id"] for o in restored]
            conn.execute(
                f"DELETE FROM dead_letters WHERE order_id IN ({','.join('?' * len(ids))})",
                ids,
            )
        conn.commit()
        return {"replayed": len(restored), "orders": restored}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# WebSocket snapshot
# ---------------------------------------------------------------------------

def _build_snapshot() -> dict:
    """Sync — called from run_in_executor so it can block on SQLite."""
    conn = get_conn()
    try:
        now = time.time()

        rows = conn.execute(
            "SELECT status, COUNT(*) n FROM orders GROUP BY status"
        ).fetchall()
        funnel = {r["status"]: r["n"] for r in rows}
        total_orders = sum(funnel.values())

        in_flight = conn.execute(
            "SELECT COUNT(*) n FROM orders WHERE claimed_by IS NOT NULL AND claimed_at > ?",
            (now - LEASE_SECONDS,),
        ).fetchone()["n"]

        # waiting = non-terminal AND not currently in-flight (claimed OR lease expired).
        # Includes both due-backlog and orders sleeping in retry backoff, so that
        #   submitted == terminal + in_flight + waiting   holds exactly.
        waiting = conn.execute(
            """
            SELECT COUNT(*) n FROM orders
             WHERE status NOT IN ('delivered','cancelled','failed')
               AND (claimed_by IS NULL OR claimed_at <= ?)
            """,
            (now - LEASE_SECONDS,),
        ).fetchone()["n"]

        submitted = conn.execute(
            "SELECT value FROM counters WHERE name='orders_submitted'"
        ).fetchone()["value"]

        due_backlog = conn.execute(
            """
            SELECT COUNT(*) n FROM orders
             WHERE status NOT IN ('delivered','cancelled','failed')
               AND next_attempt_at <= ?
               AND (claimed_by IS NULL OR claimed_at <= ?)
            """,
            (now, now - LEASE_SECONDS),
        ).fetchone()["n"]

        dlq_size = conn.execute(
            "SELECT COUNT(*) n FROM dead_letters"
        ).fetchone()["n"]

        total_retries = conn.execute(
            "SELECT COALESCE(SUM(attempt_count),0) n FROM orders WHERE attempt_count > 0"
        ).fetchone()["n"]

        # Deliveries committed in the last 5 s — this is the throughput signal
        throughput = conn.execute(
            "SELECT COUNT(*) n FROM order_events WHERE to_status='delivered' AND at >= ?",
            (now - 5.0,),
        ).fetchone()["n"]

        dispatch_count = conn.execute(
            "SELECT value FROM counters WHERE name='courier_dispatch'"
        ).fetchone()["value"]

        recent = conn.execute(
            """
            SELECT id, status, item, customer_id, attempt_count, updated_at, poison, zone
            FROM orders ORDER BY updated_at DESC LIMIT 20
            """
        ).fetchall()

        return {
            "ts": now,
            "funnel": funnel,
            "total_orders": total_orders,
            "submitted": submitted,
            "waiting": waiting,
            "in_flight": in_flight,
            "due_backlog": due_backlog,
            "dlq_size": dlq_size,
            "total_retries": total_retries,
            "throughput": throughput,
            "dispatch_count": dispatch_count,
            "recent_orders": [dict(r) for r in recent],
            "latency": _latency_metrics(conn, now),
            "zones": _zone_metrics(conn, now),
            "circuit_breakers": _circuit_breaker_status(conn),
            "recent_dlq": _recent_dlq(conn),
        }
    finally:
        conn.close()


async def _downstream_health(http: httpx.AsyncClient) -> dict:
    async def check(url: str) -> dict:
        try:
            r = await http.get(f"{url}/health", timeout=1.0)
            return r.json()
        except Exception:
            return {"effective_mode": "unreachable"}

    restaurant, courier = await asyncio.gather(
        check(RESTAURANT_URL),
        check(COURIER_URL),
    )
    return {"restaurant": restaurant, "courier": courier}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    loop = asyncio.get_running_loop()
    async with httpx.AsyncClient() as http:
        try:
            while True:
                snap, ds = await asyncio.gather(
                    loop.run_in_executor(None, _build_snapshot),
                    _downstream_health(http),
                )
                snap["downstreams"] = ds
                snap["workers"] = WORKER_POOL.status()
                snap["load"] = LOAD.status()
                snap["load_profile"] = LOAD_PROFILE.status()
                await ws.send_json(snap)
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

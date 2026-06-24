import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import random
import signal
import sqlite3
import time
import logging

import httpx

from config import (
    DB_PATH, LEASE_SECONDS, MAX_ATTEMPTS, WORKER_POLL_INTERVAL,
    DOWNSTREAM_TIMEOUT, RESTAURANT_URL, COURIER_URL, HEARTBEAT_INTERVAL,
    CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_COOLDOWN_SECONDS,
)
from common.db import init_db
from common.state_machine import next_status, is_valid_transition, DOWNSTREAM_FOR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)


class RetriableError(Exception):
    """Timeout, 5xx, 429 — worth retrying with backoff."""

class FatalError(Exception):
    """4xx (non-429) — not worth retrying; go straight to DLQ."""


# ---------------------------------------------------------------------------
# DB connection — isolation_level=None so we issue BEGIN IMMEDIATE ourselves.
# ---------------------------------------------------------------------------
def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ---------------------------------------------------------------------------
# Step A — claim
# ---------------------------------------------------------------------------
def claim_next(conn, worker_id: str, now: float):
    """
    Atomically grab the next eligible order.
    Eligible = non-terminal AND due AND (free OR lease expired).
    Lease expiry is crash recovery: a dead worker's order becomes claimable
    again automatically — no separate reaper needed.
    """
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        """
        SELECT id, status, attempt_count, poison, zone FROM orders
         WHERE status NOT IN ('delivered', 'cancelled', 'failed')
           AND next_attempt_at <= ?
           AND (claimed_by IS NULL OR claimed_at <= ?)
         ORDER BY next_attempt_at, created_at
         LIMIT 1
        """,
        (now, now - LEASE_SECONDS),
    ).fetchone()
    if row is None:
        conn.execute("ROLLBACK")
        return None
    conn.execute(
        "UPDATE orders SET claimed_by=?, claimed_at=?, updated_at=? WHERE id=?",
        (worker_id, now, now, row["id"]),
    )
    conn.execute("COMMIT")
    return row


# ---------------------------------------------------------------------------
# Step B — downstream call (real HTTP)
# ---------------------------------------------------------------------------
def call_downstream(
    http: httpx.Client,
    current_status: str,
    order_id: str,
    target: str,
    log: logging.Logger,
    poison: bool = False,
    zone: str | None = None,
) -> None:
    """
    Call the right downstream for this transition.
    placed->confirmed is internal — no HTTP call.
    Raises RetriableError for transient failures, FatalError for permanent ones.
    """
    svc = DOWNSTREAM_FOR.get(current_status)
    if svc is None:
        return  # placed -> confirmed: internal, no downstream

    url = f"{RESTAURANT_URL}/process" if svc == "restaurant" else f"{COURIER_URL}/dispatch"
    # Idempotency key: unique per (order, target state).
    # A worker that retries after a crash sends the same key; the downstream
    # returns the cached 200 instead of double-processing.
    idem_key = f"{order_id}:{target}"

    log.debug(f"{order_id[:8]}  calling {svc} ({current_status}->{target})")

    body = {"order_id": order_id, "step": current_status, "idempotency_key": idem_key, "poison": poison}
    if svc == "courier":
        body["zone"] = zone

    try:
        resp = http.post(
            url,
            json=body,
            timeout=DOWNSTREAM_TIMEOUT,
        )
    except httpx.TimeoutException:
        raise RetriableError(f"{svc} timed out after {DOWNSTREAM_TIMEOUT}s")
    except httpx.RequestError as e:
        raise RetriableError(f"{svc} unreachable: {e}")

    if resp.status_code in (500, 503, 429):
        raise RetriableError(f"{svc} returned {resp.status_code}")
    if resp.status_code >= 400:
        raise FatalError(f"{svc} returned non-retriable {resp.status_code}")
    # 2xx → success


# ---------------------------------------------------------------------------
# Circuit breaker — shared across all worker processes via SQLite, since the
# point is that ALL workers stop hammering a downstream together, not just
# the one that happened to notice failures. closed -> open after K consecutive
# failures; open -> half_open after a cooldown lets exactly ONE worker probe;
# half_open -> closed on a successful probe, or back to open (fresh cooldown)
# on a failed one. A success in 'closed' just resets the failure counter.
#
# Design note: being OPEN does NOT exempt an order from its own retry/backoff
# schedule — it just makes that attempt instant (no network call, no paying
# DOWNSTREAM_TIMEOUT) instead of slow. So orders still reach MAX_ATTEMPTS/DLQ
# at the same pace if the outage is genuinely permanent; what the breaker buys
# is that every worker stops blocking on a doomed 6s call the moment enough
# failures are seen, freeing them up for healthy orders immediately.
# ---------------------------------------------------------------------------
def breaker_gate(conn, downstream: str, worker_id: str, now: float) -> bool:
    """True => caller should make the real network call. False => fail fast,
    treat as an immediate retriable failure without ever touching the network."""
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT state, opened_at, half_open_probe_claimed_by FROM circuit_breakers WHERE downstream=?",
        (downstream,),
    ).fetchone()

    if row["state"] == "closed":
        conn.execute("COMMIT")
        return True

    if row["state"] == "open":
        if now - (row["opened_at"] or 0) >= CIRCUIT_BREAKER_COOLDOWN_SECONDS:
            conn.execute(
                "UPDATE circuit_breakers SET state='half_open', half_open_probe_claimed_by=? WHERE downstream=?",
                (worker_id, downstream),
            )
            conn.execute("COMMIT")
            return True  # this worker performs the probe call
        conn.execute("COMMIT")
        return False

    # half_open: only the worker holding the probe slot gets to call.
    if row["half_open_probe_claimed_by"] is None:
        conn.execute(
            "UPDATE circuit_breakers SET half_open_probe_claimed_by=? WHERE downstream=?",
            (worker_id, downstream),
        )
        conn.execute("COMMIT")
        return True
    conn.execute("COMMIT")
    return False


def breaker_record_success(conn, downstream: str) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE circuit_breakers SET state='closed', consecutive_failures=0, "
        "opened_at=NULL, half_open_probe_claimed_by=NULL WHERE downstream=?",
        (downstream,),
    )
    conn.execute("COMMIT")


def breaker_record_failure(conn, downstream: str, now: float) -> None:
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT state, consecutive_failures FROM circuit_breakers WHERE downstream=?",
        (downstream,),
    ).fetchone()
    if row["state"] == "half_open":
        # the probe failed — back to open, fresh cooldown, release the probe slot
        conn.execute(
            "UPDATE circuit_breakers SET state='open', opened_at=?, half_open_probe_claimed_by=NULL "
            "WHERE downstream=?",
            (now, downstream),
        )
    else:
        new_failures = row["consecutive_failures"] + 1
        if new_failures >= CIRCUIT_BREAKER_THRESHOLD:
            conn.execute(
                "UPDATE circuit_breakers SET state='open', consecutive_failures=?, opened_at=? WHERE downstream=?",
                (new_failures, now, downstream),
            )
        else:
            conn.execute(
                "UPDATE circuit_breakers SET consecutive_failures=? WHERE downstream=?",
                (new_failures, downstream),
            )
    conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Step C — commit
# ---------------------------------------------------------------------------
def commit_advance(
    conn, order_id: str, from_status: str, to_status: str, now: float
) -> bool:
    """
    Advance the order one step.
    WHERE status=:from is the idempotency guard: if another worker already
    advanced this order (after our lease expired), rowcount==0 and we no-op.
    is_valid_transition is the state-machine guard: it catches a caller asking
    for an illegal jump (e.g. placed->out_for_delivery) before it ever reaches
    the database, regardless of what the row's current status actually is.
    """
    if not is_valid_transition(from_status, to_status):
        raise ValueError(f"illegal transition {from_status!r} -> {to_status!r}")
    conn.execute("BEGIN IMMEDIATE")
    result = conn.execute(
        """
        UPDATE orders
           SET status=?, claimed_by=NULL, claimed_at=NULL,
               attempt_count=0, updated_at=?
         WHERE id=? AND status=?
        """,
        (to_status, now, order_id, from_status),
    )
    if result.rowcount == 0:
        conn.execute("ROLLBACK")
        return False
    conn.execute(
        """
        INSERT INTO order_events(order_id, from_status, to_status, reason, at)
        VALUES (?, ?, ?, 'downstream_ok', ?)
        """,
        (order_id, from_status, to_status, now),
    )
    if to_status == "delivered":
        conn.execute(
            "UPDATE counters SET value = value + 1 WHERE name = 'courier_dispatch'"
        )
    conn.execute("COMMIT")
    return True


# ---------------------------------------------------------------------------
# Retry / DLQ helpers
# ---------------------------------------------------------------------------
def schedule_retry(
    conn,
    order_id: str,
    current_status: str,
    attempt_count: int,
    now: float,
    reason: str,
) -> None:
    """
    Increment attempt count and back off.
    Backoff = min(2^n + jitter, 60s).
    Jitter avoids thundering-herd when many orders retry a recovering service
    simultaneously — without it they'd all hit at the same second.
    After MAX_ATTEMPTS, move to dead_letters.
    """
    new_attempts = attempt_count + 1
    if new_attempts >= MAX_ATTEMPTS:
        _move_to_dlq(conn, order_id, current_status, reason, now, attempt_count=new_attempts)
    else:
        backoff = min(2 ** new_attempts + random.random(), 60.0)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE orders
               SET attempt_count=?, claimed_by=NULL, claimed_at=NULL,
                   next_attempt_at=?, updated_at=?
             WHERE id=?
            """,
            (new_attempts, now + backoff, now, order_id),
        )
        conn.execute("COMMIT")


def move_to_dlq_immediately(
    conn, order_id: str, current_status: str, reason: str, now: float, attempt_count: int
) -> None:
    """Fatal error — skip retries entirely."""
    _move_to_dlq(conn, order_id, current_status, reason, now, attempt_count=attempt_count + 1)


def _move_to_dlq(
    conn, order_id: str, current_status: str, reason: str, now: float, attempt_count: int
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE orders SET status='failed', attempt_count=?, claimed_by=NULL, claimed_at=NULL, updated_at=? WHERE id=?",
        (attempt_count, now, order_id),
    )
    conn.execute(
        "INSERT INTO order_events(order_id, from_status, to_status, reason, at) VALUES (?,?,'failed',?,?)",
        (order_id, current_status, reason, now),
    )
    conn.execute(
        "INSERT OR REPLACE INTO dead_letters(order_id, last_status, reason, at) VALUES (?,?,?,?)",
        (order_id, current_status, reason, now),
    )
    conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def _register(conn, worker_id: str, now: float) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO workers(id, pid, started_at, last_seen) VALUES (?,?,?,?)",
        (worker_id, os.getpid(), now, now),
    )


def _heartbeat(conn, worker_id: str, now: float) -> None:
    conn.execute("UPDATE workers SET last_seen=? WHERE id=?", (now, worker_id))


# ---------------------------------------------------------------------------
# Graceful shutdown — SIGTERM sets a flag instead of killing the process
# outright. The main loop only checks it between cycles, so a claim that's
# already in flight always runs to its natural conclusion first (commit,
# retry-with-backoff, or DLQ — all three already clear claimed_by as a normal
# side effect), and only THEN does the worker exit instead of polling again.
# That makes "release the claim" and "finish the current step" the same
# outcome here, not a choice between two code paths.
#
# SIGKILL can't be caught by any userspace code — there is no graceful path
# for a hard crash, which is exactly why lease expiry exists as the backstop.
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _handle_sigterm(signum, frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True


def main(worker_id: str) -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    init_db()
    log = logging.getLogger(worker_id)
    log.info("started")
    conn = _get_conn()
    http = httpx.Client()  # one client reused across all requests

    _register(conn, worker_id, time.time())
    last_hb = time.time()

    while not _shutdown_requested:
        now = time.time()
        # Liveness heartbeat — lets /metrics tell a hung worker from a healthy one.
        if now - last_hb >= HEARTBEAT_INTERVAL:
            _heartbeat(conn, worker_id, now)
            last_hb = now

        order = claim_next(conn, worker_id, now)

        if order is None:
            time.sleep(WORKER_POLL_INTERVAL)
            continue

        order_id = order["id"]
        current_status = order["status"]
        attempt_count = order["attempt_count"]
        poison = bool(order["poison"])
        zone = order["zone"]
        target = next_status(current_status)

        if target is None:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE orders SET claimed_by=NULL, claimed_at=NULL WHERE id=?",
                (order_id,),
            )
            conn.execute("COMMIT")
            continue

        svc = DOWNSTREAM_FOR.get(current_status)

        if svc is not None and not breaker_gate(conn, svc, worker_id, now):
            log.warning(f"{order_id[:8]}  breaker OPEN for {svc} — failing fast ({current_status})")
            schedule_retry(conn, order_id, current_status, attempt_count, now, f"circuit breaker open for {svc}")
            continue

        try:
            call_downstream(http, current_status, order_id, target, log, poison=poison, zone=zone)
            # Stamp the commit at completion time (not claim time) so the
            # per-order timeline reflects how long each step actually took.
            advanced = commit_advance(conn, order_id, current_status, target, time.time())
            if svc is not None:
                breaker_record_success(conn, svc)
            if advanced:
                log.info(f"{order_id[:8]}  {current_status} -> {target}")
            else:
                log.warning(f"{order_id[:8]}  no-op (status already changed)")

        except RetriableError as e:
            if svc is not None:
                breaker_record_failure(conn, svc, time.time())
            log.warning(
                f"{order_id[:8]}  retry {attempt_count + 1}/{MAX_ATTEMPTS}  "
                f"({current_status}): {e}"
            )
            schedule_retry(conn, order_id, current_status, attempt_count, now, str(e))

        except FatalError as e:
            log.error(f"{order_id[:8]}  fatal — moving to DLQ ({current_status}): {e}")
            move_to_dlq_immediately(conn, order_id, current_status, str(e), now, attempt_count)

        except Exception as e:
            if svc is not None:
                breaker_record_failure(conn, svc, time.time())
            log.error(f"{order_id[:8]}  unexpected error — retrying ({current_status}): {e}", exc_info=True)
            schedule_retry(conn, order_id, current_status, attempt_count, now, str(e))

    log.info("SIGTERM received — drained current cycle, exiting cleanly")
    conn.execute("DELETE FROM workers WHERE id=?", (worker_id,))
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", dest="worker_id", default=f"worker-{os.getpid()}")
    args = parser.parse_args()
    main(args.worker_id)

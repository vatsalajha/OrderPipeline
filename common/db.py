import os
import sqlite3
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    """Create tables using individual execute() calls.

    Deliberately NOT using executescript() — that issues an implicit COMMIT
    before running, which can fail with 'disk I/O error' when WAL/SHM files
    are left in a dirty state after a hard kill (e.g. chaos_test SIGKILL).
    Plain execute() + explicit commit is safe and idempotent.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id              TEXT PRIMARY KEY,
            status          TEXT NOT NULL,
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            claimed_by      TEXT,
            claimed_at      REAL,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            customer_id     TEXT,
            item            TEXT,
            created_at      REAL NOT NULL,
            updated_at      REAL NOT NULL,
            poison          INTEGER NOT NULL DEFAULT 0,
            zone            TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    TEXT NOT NULL,
            from_status TEXT,
            to_status   TEXT NOT NULL,
            reason      TEXT,
            at          REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dead_letters (
            order_id    TEXT PRIMARY KEY,
            last_status TEXT,
            reason      TEXT,
            at          REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS counters (
            name    TEXT PRIMARY KEY,
            value   INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workers (
            id          TEXT PRIMARY KEY,
            pid         INTEGER,
            started_at  REAL,
            last_seen   REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS circuit_breakers (
            downstream                  TEXT PRIMARY KEY,
            state                       TEXT NOT NULL DEFAULT 'closed',
            consecutive_failures        INTEGER NOT NULL DEFAULT 0,
            opened_at                   REAL,
            half_open_probe_claimed_by  TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_claimable "
        "ON orders(status, next_attempt_at, claimed_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_order ON order_events(order_id)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO counters(name, value) VALUES ('courier_dispatch', 0)"
    )
    # Independent tally of orders ever submitted — the LHS of the "nothing lost"
    # invariant. Bumped at insert time, compared against the live status buckets.
    conn.execute(
        "INSERT OR IGNORE INTO counters(name, value) VALUES ('orders_submitted', 0)"
    )
    for downstream in ("restaurant", "courier"):
        conn.execute(
            "INSERT OR IGNORE INTO circuit_breakers(downstream, state, consecutive_failures) "
            "VALUES (?, 'closed', 0)",
            (downstream,),
        )
    conn.commit()


def _purge_wal() -> None:
    """Remove orphaned WAL/SHM files — safe because WAL is a performance
    layer; the committed data is always in the main .db file."""
    for suffix in ("-wal", "-shm"):
        p = DB_PATH + suffix
        if os.path.exists(p):
            os.remove(p)


def init_db() -> None:
    try:
        conn = get_conn()
        _create_schema(conn)
        conn.close()
    except sqlite3.OperationalError:
        # Leftover WAL/SHM files from a hard kill can cause 'disk I/O error'
        # on the first open.  Remove them and retry once.
        _purge_wal()
        conn = get_conn()
        _create_schema(conn)
        conn.close()

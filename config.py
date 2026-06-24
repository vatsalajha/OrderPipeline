import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "pipeline.db")
# Must exceed the worst-case time a HEALTHY worker can legitimately hold a claim:
# DOWNSTREAM_TIMEOUT (6s, the client gives up regardless of how slow the server
# is) + worst-case busy_timeout wait on the subsequent commit's write lock (5s)
# = ~11s. 15s leaves ~4s of margin while still reclaiming a truly dead worker's
# order quickly enough to be visible in a live demo (vs. a default of 30s).
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "15"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
RESTAURANT_URL = os.getenv("RESTAURANT_URL", "http://localhost:8001")
COURIER_URL = os.getenv("COURIER_URL", "http://localhost:8002")
WORKER_POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL", "0.075"))
DOWNSTREAM_TIMEOUT = float(os.getenv("DOWNSTREAM_TIMEOUT", "6.0"))
# Worker pool is supervised by the API process (not honcho) so a /worker/kill
# can SIGTERM a single worker without honcho tearing down the whole group.
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))

# Workers write a liveness heartbeat to the `workers` table this often (seconds).
# Surfaced in /metrics as per-worker last-seen, so a hung (but not dead) worker
# is distinguishable from a healthy one.
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2.0"))

# --- Simulated processing time -------------------------------------------------
# Each lifecycle step takes a realistic, VARIABLE amount of time. We express the
# real-world durations in minutes, then compress them with TIME_SCALE so the demo
# runs in seconds while preserving the relative shape (cooking >> confirming,
# the drive is the longest leg, etc.).
#
#   sleep_seconds = uniform(lo_min, hi_min) * TIME_SCALE
#
# TIME_SCALE = seconds of real sleep per simulated MINUTE. Default 0.05 →
# a 10-30 min "preparing" step becomes 0.5-1.5 s. Bump it toward 1.0 to watch
# things unfold closer to real time; drop it for a faster demo.
TIME_SCALE = float(os.getenv("TIME_SCALE", "0.05"))

# (lo, hi) simulated minutes, keyed by the CURRENT status the worker is leaving.
STEP_DURATION_MINUTES = {
    "confirmed":        (1, 3),    # confirmed -> preparing   (kitchen accepts)
    "preparing":        (10, 30),  # preparing -> ready       (cooking)
    "ready":            (2, 8),    # ready -> out_for_delivery (courier pickup)
    "out_for_delivery": (15, 45),  # out_for_delivery -> delivered (the drive)
}

# In "slow" chaos mode, multiply the simulated duration by this factor so steps
# blow past DOWNSTREAM_TIMEOUT and exercise the retry/backoff path.
SLOW_FACTOR = float(os.getenv("SLOW_FACTOR", "8.0"))

# Probability a normal-mode call returns a transient (retriable) error.
TRANSIENT_ERROR_RATE = float(os.getenv("TRANSIENT_ERROR_RATE", "0.10"))

# An order whose placed->delivered latency exceeds this is an SLA breach —
# the dashboard surfaces both delivered orders that breached it and
# currently in-flight orders already past it.
SLA_THRESHOLD_SECONDS = float(os.getenv("SLA_THRESHOLD_SECONDS", "30"))

# Delivery zones — a dimension on top of the core pipeline; orders are
# randomly assigned one at creation unless specified.
ZONES = ["north", "south", "east", "west"]

# Circuit breaker per downstream: after this many CONSECUTIVE failures, open
# (workers fail fast instead of making the network call); after the cooldown,
# half-open allows exactly one probe call through to test recovery.
#
# Threshold must clear the baseline noise floor, not just "feel low": the
# failure counter is GLOBAL per downstream, shared across every concurrent
# worker, and TRANSIENT_ERROR_RATE (0.10) means a real 10% of calls fail even
# with nothing actually wrong. At high call volume (thousands of calls under
# a rush/burst), the expected number of trials before seeing K consecutive
# failures by pure chance is roughly 1/TRANSIENT_ERROR_RATE**K — at K=4 that's
# ~10,000 trials, well within a single 2000-order burst's call volume, so the
# breaker would trip on normal noise alone, not a real outage. At K=6 it's
# ~1,000,000 trials — negligible at any volume this system sees — while a
# genuine outage (near-100% failure) still trips it within ~6 calls either way.
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "6"))
CIRCUIT_BREAKER_COOLDOWN_SECONDS = float(os.getenv("CIRCUIT_BREAKER_COOLDOWN_SECONDS", "10"))

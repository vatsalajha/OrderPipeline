"""Shared downstream-simulator behaviour: variable, realistic per-step timing.

Both the restaurant and courier sims call process_step() to decide the outcome
of one downstream call. Durations come from config.STEP_DURATION_MINUTES scaled
by TIME_SCALE, so each step takes a realistic, *variable* amount of time
(cooking takes far longer than confirming; the delivery drive is the longest
leg) while the demo still runs in seconds.
"""
import random
import time

from config import (
    STEP_DURATION_MINUTES, TIME_SCALE, SLOW_FACTOR, TRANSIENT_ERROR_RATE,
)


def step_seconds(step: str, slow: bool = False) -> float:
    """Sleep duration (s) for one step: uniform(lo,hi) simulated minutes * scale."""
    lo, hi = STEP_DURATION_MINUTES.get(step, (1, 3))
    secs = random.uniform(lo, hi) * TIME_SCALE
    return secs * SLOW_FACTOR if slow else secs


def process_step(step: str, mode: str) -> tuple[bool, int, float]:
    """Decide the outcome of one downstream call.

    Returns (ok, status_code, slept_seconds). The caller handles idempotency
    caching and response shaping.

    - down  -> 503 immediately (no work done)
    - normal/slow -> TRANSIENT_ERROR_RATE chance of a retriable 5xx/429
      (rolled before the work, so the step did not complete), otherwise sleep
      the step's variable duration (x SLOW_FACTOR in slow mode) and succeed.
    """
    if mode == "down":
        return (False, 503, 0.0)

    if random.random() < TRANSIENT_ERROR_RATE:
        return (False, random.choice([500, 503, 429]), 0.0)

    secs = step_seconds(step, slow=(mode == "slow"))
    time.sleep(secs)
    return (True, 200, secs)

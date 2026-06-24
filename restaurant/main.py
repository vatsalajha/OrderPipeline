import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.sim import process_step

app = FastAPI(title="Restaurant Simulator")

# --- Chaos state (thread-safe) ---
_lock = threading.Lock()
_mode: str = "normal"       # "normal" | "slow" | "down"
_mode_until: float = 0.0    # epoch; ignored when mode=="normal"

# Idempotency cache: key -> True (only cache successes).
# Guarantees a repeated call after a worker crash returns the same 200
# instead of double-processing the order.
_seen: dict[str, bool] = {}
_request_count: int = 0


def _effective_mode() -> str:
    with _lock:
        m, u = _mode, _mode_until
    if m == "normal" or time.time() >= u:
        return "normal"
    return m


# --- Models ---

class ChaosRequest(BaseModel):
    mode: str = "normal"
    seconds: int = 60


class ProcessRequest(BaseModel):
    order_id: str
    step: str
    idempotency_key: str
    poison: bool = False


# --- Endpoints ---

@app.get("/health")
def health():
    now = time.time()
    with _lock:
        m, u = _mode, _mode_until
    effective = m if (m != "normal" and now < u) else "normal"
    return {"service": "restaurant", "configured_mode": m, "effective_mode": effective, "request_count": _request_count}


@app.post("/chaos")
def set_chaos(req: ChaosRequest):
    global _mode, _mode_until
    with _lock:
        _mode = req.mode
        _mode_until = time.time() + req.seconds if req.mode != "normal" else 0.0
    return {"mode": _mode, "seconds": req.seconds}


@app.post("/process")
def process(req: ProcessRequest):
    global _request_count
    _request_count += 1

    # Return the same 200 for a repeated idempotency key — crash recovery.
    if req.idempotency_key in _seen:
        return JSONResponse({"status": "ok", "idempotent": True})

    if req.poison:
        # Permanently bad data — this call can never succeed, regardless of
        # chaos mode. A real 503 (not a fabricated one) so the worker's normal
        # RetriableError/backoff/DLQ path handles it exactly like any other
        # downstream failure; only the cause is different (the data, not the
        # service).
        return JSONResponse({"error": "restaurant 503 (poison order)"}, status_code=503)

    ok, code, secs = process_step(req.step, _effective_mode())
    if not ok:
        return JSONResponse({"error": f"restaurant {code}"}, status_code=code)

    _seen[req.idempotency_key] = True
    return JSONResponse({"status": "ok", "step": req.step, "took_s": round(secs, 2)})

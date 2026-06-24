import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.sim import process_step

app = FastAPI(title="Courier Simulator")

_lock = threading.Lock()
_mode: str = "normal"
_mode_until: float = 0.0
_seen: dict[str, bool] = {}
_request_count: int = 0
# Per-zone chaos overrides, e.g. {"north": ("down", epoch_until)} — lets one
# zone's courier be down while the others keep flowing. Zone overrides take
# precedence over the global mode for that zone only.
_zone_modes: dict[str, tuple[str, float]] = {}


def _effective_mode(zone: str | None = None) -> str:
    now = time.time()
    if zone:
        with _lock:
            zm = _zone_modes.get(zone)
        if zm and zm[0] != "normal" and now < zm[1]:
            return zm[0]
    with _lock:
        m, u = _mode, _mode_until
    if m == "normal" or now >= u:
        return "normal"
    return m


class ChaosRequest(BaseModel):
    mode: str = "normal"
    seconds: int = 60
    zone: str | None = None


class DispatchRequest(BaseModel):
    order_id: str
    step: str
    idempotency_key: str
    poison: bool = False
    zone: str | None = None


class CancelDispatchRequest(BaseModel):
    order_id: str
    idempotency_key: str


@app.get("/health")
def health():
    now = time.time()
    with _lock:
        m, u = _mode, _mode_until
        zones = {
            z: mode for z, (mode, until) in _zone_modes.items()
            if mode != "normal" and now < until
        }
    effective = m if (m != "normal" and now < u) else "normal"
    return {
        "service": "courier", "configured_mode": m, "effective_mode": effective,
        "request_count": _request_count, "zone_overrides": zones,
    }


@app.post("/chaos")
def set_chaos(req: ChaosRequest):
    global _mode, _mode_until
    if req.zone:
        with _lock:
            _zone_modes[req.zone] = (req.mode, time.time() + req.seconds if req.mode != "normal" else 0.0)
        return {"mode": req.mode, "seconds": req.seconds, "zone": req.zone}
    with _lock:
        _mode = req.mode
        _mode_until = time.time() + req.seconds if req.mode != "normal" else 0.0
    return {"mode": _mode, "seconds": req.seconds}


@app.post("/dispatch")
def dispatch(req: DispatchRequest):
    global _request_count
    _request_count += 1

    if req.idempotency_key in _seen:
        return JSONResponse({"status": "ok", "idempotent": True})

    if req.poison:
        return JSONResponse({"error": "courier 503 (poison order)"}, status_code=503)

    ok, code, secs = process_step(req.step, _effective_mode(req.zone))
    if not ok:
        return JSONResponse({"error": f"courier {code}"}, status_code=code)

    _seen[req.idempotency_key] = True
    return JSONResponse({"status": "ok", "step": req.step, "took_s": round(secs, 2)})


@app.post("/cancel-dispatch")
def cancel_dispatch(req: CancelDispatchRequest):
    """Compensating action for the cancellation saga: tell the courier to stand
    down a dispatch that's already in flight. Idempotency-keyed like /dispatch
    so a retried compensation call doesn't double-fire whatever real-world
    'recall the driver' side effect this would trigger."""
    if req.idempotency_key in _seen:
        return JSONResponse({"status": "ok", "idempotent": True})
    _seen[req.idempotency_key] = True
    return JSONResponse({"status": "ok", "order_id": req.order_id, "action": "cancel-dispatch"})

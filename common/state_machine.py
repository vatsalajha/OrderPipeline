TRANSITIONS: dict[str, list[str]] = {
    "placed":           ["confirmed", "cancelled", "failed"],
    "confirmed":        ["preparing", "cancelled", "failed"],
    "preparing":        ["ready", "cancelled", "failed"],
    "ready":            ["out_for_delivery", "cancelled", "failed"],
    "out_for_delivery": ["delivered", "cancelled", "failed"],
    "delivered":        [],
    "cancelled":        [],
    "failed":           [],
}

# Error exits — never the normal forward path.
_ERROR = {"cancelled", "failed"}

# Which downstream each forward step requires.
# placed->confirmed is internal (no HTTP call).
DOWNSTREAM_FOR: dict[str, str] = {
    "confirmed":        "restaurant",  # confirmed -> preparing
    "preparing":        "restaurant",  # preparing -> ready
    "ready":            "courier",     # ready -> out_for_delivery
    "out_for_delivery": "courier",     # out_for_delivery -> delivered
}


def next_status(current: str) -> str | None:
    """Return the normal forward step (not cancelled/failed), or None if already terminal."""
    candidates = [s for s in TRANSITIONS.get(current, []) if s not in _ERROR]
    return candidates[0] if candidates else None


def is_valid_transition(current: str, target: str) -> bool:
    """The single chokepoint every status-changing write should consult:
    commit_advance() validates the forward path against this, and the
    cancellation endpoint validates 'cancelled' against this — both defer to
    TRANSITIONS instead of each hardcoding their own notion of legality."""
    return target in TRANSITIONS.get(current, [])

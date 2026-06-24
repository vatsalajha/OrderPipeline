# Verification — requirement → test → result

Every row maps a requirement from the build brief to the exact automated test
that proves it (`chaos_test.py`, scenario numbers below) and the one design
guarantee that makes it true rather than just "usually true." Full scenario
descriptions: `TESTING.md`. Run: `python3 chaos_test.py` (stop honcho first).

| Requirement (brief) | Proven by | Result | Guaranteed by |
|---|---|---|---|
| **Bursty load** — absorb a sudden flood without falling over | Scenario 10 (2000 orders via the real `/load/start` batched path, 16 workers) + Scenario 5 (rush + worker crash, ×5) + Scenario 15 (4 named real-world load profiles — lunch rush, steady evening, a 3x promo spike, a flash outage — ~5000 orders total) | **PASS** — 2000/2000 delivered with zero `database is locked` errors and 0/16 workers died; all 4 profiles drained with invariants holding after each one | Batched inserts (one transaction per 100ms tick, not one per order) + claim/commit as two independent short transactions per order, so contention serializes through `busy_timeout` instead of erroring. Profiles are scripts over this same path — they never bypass normal order submission |
| **Ordered lifecycle** — no skipping stages | Scenario 2 (every delivered order's `order_events` checked against the exact 6-event legal chain) + Scenario 13 (a forced illegal jump is rejected) | **PASS** | `next_status()` only ever returns the single legal forward step; `commit_advance()` now validates `is_valid_transition(from,to)` against `TRANSITIONS` before writing anything — the state machine table is the single chokepoint, not a convention every caller has to honor on faith |
| **Flaky downstreams** — slow / rate-limited / erroring is normal, not exceptional | Scenario 3 (slow courier, retries sampled live) + Scenario 4 (down → DLQ → recover → replay) + Scenario 11 (slow doesn't block other orders) | **PASS** | Retries with exponential backoff + jitter on retriable errors (timeout/5xx/429); `DOWNSTREAM_TIMEOUT` bounds a stuck call; no DB lock is ever held during the downstream call itself |
| **No orders lost** | Asserted in **every** scenario (invariant i); sharpest proof: Scenario 5 (worker crash mid-rush, ×5 independent runs) | **PASS** (19/19 scenarios, invariant held in all) | `orders_submitted` is an independent counter bumped at insert time; a crashed worker's lease expires and the claim query treats its order as eligible again automatically — recovery is intrinsic to the claim query, no reaper process |
| **No double processing** | Asserted in every scenario (invariant ii); sharpest proof: Scenario 6 (no duplicate `(order,from,to)` event pairs under concurrent load) + Scenario 8 (calling `commit_advance` twice for one transition, white-box) | **PASS** | `BEGIN IMMEDIATE` serializes claims so two workers can't grab the same order; `commit_advance`'s `WHERE status=:expected` guard makes a duplicate commit a no-op — `courier_dispatch` can only reach N if exactly N orders were actually committed delivered |
| **Recovery** — from a downstream outage and from a worker crash | Scenario 4 (DLQ replay) + Scenario 9 (lease boundary, simulated time) + Scenario 14 (graceful SIGTERM vs. hard SIGKILL) | **PASS** | Lease expiry (crash) is intrinsic to the claim query; `/dlq/replay` resets a dead-lettered order back to its `last_status` once a downstream recovers; graceful SIGTERM now drains the current cycle and exits instead of waiting out the lease — SIGKILL still relies on lease expiry, since no userspace code can catch it |
| **Observability** — live visibility into health and what's happening | Scenario 16 (latency/SLA metrics specifically); dashboard/health/metrics endpoints otherwise verified manually | **PASS** (latency) + **verified manually** (the rest): `/health`, `/metrics`, dashboard reconciliation banner, per-worker heartbeats, downstream health badges | `/metrics` independently re-derives every number live from SQLite each call; the WebSocket pushes the same snapshot every 1s so the dashboard never goes stale |
| **Cancellation** — "can also be cancelled" | Scenario 12 (cancel at 4 lifecycle points: early, mid, after dispatch, after delivery) | **PASS** — early/mid cancelled cleanly, post-dispatch cancel fired its compensation exactly once, post-delivery cancel correctly rejected (409) | Cancellation is a guarded transition through the same `is_valid_transition` chokepoint as the forward path, legal from any non-terminal state; cancelling after `out_for_delivery` fires a best-effort compensating call to the courier's `/cancel-dispatch`, recorded in the same event |

## Beyond the original brief

Four further capabilities were added after the above, layered on top without changing the core pipeline:

| Capability | Proven by | Result | Guaranteed by |
|---|---|---|---|
| **Live p50/p95 latency + SLA breach count** | Scenario 16 — fires a 500-order rush, asserts `/metrics` shows real samples *while it's still draining*, not just after | **PASS** | Computed from existing `created_at`/`updated_at` columns over a rolling 5-minute window — no new tracking, cheap by construction |
| **Poison orders don't block or slow other orders** | Scenario 17 — one `poison:true` order mixed into 40 normal ones | **PASS** — poison order DLQ'd at exactly `MAX_ATTEMPTS`, all 40 normal orders delivered | Same claim/commit independence that protects against any other slow/failing order — a poison order is just an order that always fails, nothing structurally different |
| **Zone-scoped outage stays scoped** | Scenario 18 — courier down for one zone only, 8 orders per zone | **PASS** — the outage zone partially DLQ'd, the other 3 zones delivered 8/8 untouched | Zone is a plain column on the same `orders` table; the courier simulator checks a per-zone chaos override before the global one, but everything else (claim, commit, invariants) is zone-blind |
| **Circuit breaker opens and recovers** | Scenario 19 — courier down with continuous order traffic | **PASS** — breaker opened (request count to courier plateaued, not just a state flag), recovered to closed once courier came back | Breaker state lives in SQLite, shared across all worker processes; an order's own retry/backoff still runs on schedule while open, just without the network call — see README for why this beats blind per-worker retries |

## Notes

- **Observability** has no dedicated automated scenario because there's nothing
  to assert against — it's a live dashboard/endpoint, verified by looking at it,
  not by a pass/fail check. Listed as a deliberate gap in automated coverage,
  not an oversight.
- Scenario 13 exists because of a real finding: before it, `commit_advance()`
  trusted its caller completely — nothing inside it checked that a `(from,to)`
  pair was actually a legal edge. In normal operation this never mattered
  (only `next_status()`-derived targets ever reached it), but it meant the
  state machine was enforced by *convention*, not by a hard chokepoint. Fixed
  by adding `is_valid_transition()` to `common/state_machine.py` and calling it
  from both `commit_advance()` and the cancellation endpoint — one table, two
  callers, no third place that could drift out of sync.
- Two other real bugs were found and fixed by this suite, not just confirmed:
  `_move_to_dlq` wasn't persisting the final `attempt_count` (off by one,
  caught by scenario 7), and `start_services()`'s test harness itself was
  silently running every scenario with the API's auto-spawned worker pool on
  top of the explicitly-spawned test workers (caught by scenario 14, fixed by
  setting `WORKER_COUNT=0` in the test environment).
- Two test-harness-only bugs (not system bugs) surfaced while re-verifying
  after the `WORKER_COUNT=0` fix: scenario 10 could report a false pass by
  checking "fully drained" before the async load generator had inserted
  anything; scenario 11's timing threshold had been tuned against the
  inflated (pre-fix) worker count and became too tight for the real one. Both
  fixed in `chaos_test.py`; see `TESTING.md` for detail.
- **A real system bug**, found by the 2000-order burst test (scenario 10)
  going from ~90s to over 1000s after the circuit breaker landed:
  `CIRCUIT_BREAKER_THRESHOLD=4` was too low relative to baseline
  `TRANSIENT_ERROR_RATE` (0.10) at realistic call volume — the breaker's
  failure counter is global per downstream, shared across every concurrent
  worker, so at thousands of calls "4 consecutive failures" happens often
  from pure noise, not a real outage, and each false trip throttles every
  order needing that downstream for a full cooldown. Fixed by raising the
  threshold to 6 (see the math in `config.py`'s comment) — confirmed by
  re-running the burst test clean afterward. A rate-based breaker (failure %
  over a rolling window, not raw consecutive count) would be the structurally
  correct fix; raising the threshold is a calibrated one. Noted in
  `INTERVIEW_PREP.md` question 8 as the top thing I'd revisit with more time.
- Scenario 4's post-replay drain timeout was widened from 30s to 60s for an
  unrelated reason: the breaker adding *recovery latency by design* (waiting
  out a cooldown, and occasionally a second one if a probe happens to hit
  baseline noise) means recovery is no longer near-instant the way it was
  pre-breaker. This is expected breaker behavior, not a bug — confirmed by
  re-running the scenario twice back to back, both clean.
- Final consolidated run after both fixes: 19/19 scenarios pass.

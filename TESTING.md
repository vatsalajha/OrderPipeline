# Testing reference

`chaos_test.py` is a single runnable script (no pytest dependency, consistent with
the zero-infra approach elsewhere) that proves the pipeline's correctness claims
against real services, not mocks. Stop honcho first — it starts its own services
on ports 8000-8002.

```bash
python3 chaos_test.py
```

Every scenario gets a **fresh database** and a **fresh set of services**, drives
one specific condition, then asserts the two invariants that must hold no matter
what:

- **(i) nothing lost:** `orders_submitted == delivered + cancelled + dlq + in_flight + waiting`
- **(ii) nothing doubled:** `counters.courier_dispatch == count(status='delivered')`

`in_flight`/`waiting` are computed with the exact live SQL predicates the
dashboard/API use (not derived by subtraction from the total), so the equality is
a genuine check — a leaked claim or a missing clear-on-commit would show up as a
real mismatch, not be hidden by the arithmetic. `failed` and "has a `dead_letters`
row" are the same set by construction in this schema (`_move_to_dlq` always does
both together), so they're not summed separately — that would double-count.

## Scenarios

| # | Name | What it proves | Notes |
|---|---|---|---|
| 1 | Happy path | N orders, healthy downstreams → all delivered, invariants hold | Baseline; TRANSIENT_ERROR_RATE still applies (failure is "normal" per the brief), so a stray retry is expected, not a bug |
| 2 | Ordering — legal sequence, no skips | Every delivered order's `order_events` shows the *exact* legal chain (`placed→confirmed→preparing→ready→out_for_delivery→delivered`), nothing skipped or out of order | Works because retries never insert events — only successful transitions and DLQ entries do |
| 3 | Slow downstream | All orders still complete under a slow courier; retries get exercised | `attempt_count` is **sampled live during the run**, not checked after drain — `commit_advance` resets it to 0 on every successful step, so checking it on an already-delivered order always reads 0 regardless of how many retries happened along the way |
| 4 | Down then recover + DLQ replay | Courier down → orders exhaust retries into the DLQ at their `last_status` → courier restored → `/dlq/replay` → all resume from where they died and complete | Mirrors the live demo exactly |
| 5 | Worker crash mid-rush (×5) | SIGTERM one worker mid-burst, repeated **5 independent times** (fresh stack each run) | Guards against a flaky one-off pass hiding a real race; all 5 must pass |
| 6 | Concurrent — no double-claim | Under load with several workers, no `(order_id, from_status, to_status)` pair appears twice in `order_events` | Direct proof that `BEGIN IMMEDIATE` claim serialization works, not just "probably works" |
| 7 | Max retries → DLQ, none stuck forever | After exactly `MAX_ATTEMPTS` failures, status is `failed`, `attempt_count == MAX_ATTEMPTS` exactly, and it's in `dead_letters` | Caught a real bug: `_move_to_dlq` wasn't persisting the final attempt count (stored `MAX_ATTEMPTS-1`) — fixed in `worker.py` |
| 8 | Idempotency under a duplicate downstream call | Two layers: the simulator's idempotency cache dedupes a repeated call; **the real backstop** is `commit_advance`'s `WHERE status=:expected` guard — calling it twice for the same transition only increments `courier_dispatch` once | White-box — calls `commit_advance` directly to prove the guard, independent of network/timing flakiness |
| 9 | Lease tuning | No premature double-claim within the lease window; reclaimable immediately after expiry | `claim_next()` takes `now` as a parameter, so the boundary is proven exactly with simulated timestamps — no real waiting |
| 10 | Heavy burst (2000 orders), zero lock errors | 2000 orders via the real `/load/start` batched-insert path, 16 workers, all delivered, **zero** `"database is locked"` / `OperationalError` in any process's output, no worker silently died | `TIME_SCALE` is sped up 5× for this scenario only (shortens simulated sleep, doesn't touch transaction boundaries) to keep wall-clock reasonable |
| 11 | Slow downstream doesn't block other orders | Orders still in the restaurant-only phase (never touches courier) clear in seconds even while courier is slow | If any shared lock were held during a downstream call, this would queue up instead |
| 12 | Cancellation at various lifecycle stages | Cancel early / mid-pipeline / after `out_for_delivery` (triggers compensation) / after `delivered` (rejected, 409) | Confirms the compensating `cancel-dispatch` call fires exactly once, only when genuinely needed |
| 13 | Illegal transition is rejected | Forces `placed → out_for_delivery` directly through `commit_advance`; must raise and leave the row untouched; a legal transition from the same starting point must still work afterward | Caught a real gap: `commit_advance` had no validation at all before this — it trusted every caller to only ever construct legal `(from,to)` pairs. Fixed by calling `is_valid_transition()` (re-added to `common/state_machine.py`) at the top of `commit_advance`, before any DB write |
| 14 | Graceful SIGTERM vs. hard SIGKILL | SIGTERM a worker mid-claim → its order is picked up again in well under a second. SIGKILL a different worker mid-claim → the order stays held until lease expiry (~10s in test config), not before | Proves the worker's new SIGTERM handler actually changes behavior (vs. just trusting the description): a graceful stop drains its current cycle (which already clears `claimed_by` as a normal side effect of commit/retry/dlq) and exits instead of leaving the lease to expire; SIGKILL can't be caught by any process, so lease expiry is correctly still the only backstop for a hard crash |
| 15 | Named load profiles | Runs `lunch_rush`, `steady_evening`, `promo_spike`, and `flash_outage` once each, back to back in one stack (~5000 orders total); invariants checked after every single profile drains, not just at the end | `flash_outage` legitimately sends a handful of orders to the DLQ (its scripted 20s courier outage catches whichever orders happen to be mid-retry) — that's the profile working as intended, not a failure; the invariants still hold because DLQ is accounted for, not lost |
| 16 | Latency metrics update live during a rush | Fires `/load/rush` (500 orders), polls `/metrics` and asserts `latency.sample_n > 0` *while the rush is still draining* — not only checked after; final p50/p95/sample_n must be internally sane | Caught its own timing miscalculation, not a system bug: at default `TIME_SCALE`, 500 orders on 6 workers realistically takes minutes to drain, not the 60s first budgeted — fixed the same way scenarios 10/15 did, by speeding up `TIME_SCALE` 5x for this scenario only |
| 17 | Poison order doesn't block throughput | One order flagged `poison:true` mixed into 40 normal ones; the poison order must reach `failed` at exactly `MAX_ATTEMPTS`, all 40 normal orders must still deliver, and total drain time must rule out catastrophic blocking | First run's "speed_ok" threshold (40s) was tighter than the poison order's own unavoidable backoff sequence (~30-35s for 5 attempts, independent of any blocking) — widened to 70s, which still clearly distinguishes "fine" from "actually stuck" |
| 18 | Delivery zones — outage stays scoped | 8 orders per zone (32 total), courier down for `zone=north` only; the other 3 zones must show 8/8 delivered while north's 8 are fully accounted for (delivered + DLQ) | Confirms zone is a pure dimension on the same `orders` table — global reconciliation invariants hold throughout, not separate per-zone bookkeeping that could drift |
| 19 | Circuit breaker opens and recovers | Courier down with a continuous trickle of fresh orders; breaker must open (proven by the courier simulator's own request counter barely moving — a state label alone wouldn't prove it stopped calling), then close again once courier recovers | Needs *continuous* traffic, not a one-shot batch — a finite batch can fully exhaust its own per-order retries and DLQ before any fresh request arrives to test recovery via the half-open probe, same as a real breaker only gets evaluated when there's traffic flowing through it |

**Harness bug found and fixed while building scenario 14:** `start_services()` starts the real `api` process, whose own lifespan hook auto-spawns `WORKER_COUNT` (12, from `.env`) workers of its own — on top of whatever workers this script spawns explicitly. Every scenario before 14 was silently running with ~12 extra hidden workers; correctness invariants were unaffected (more workers can only stress the claim/commit guards harder), but scenario 14 needs exact control over which worker processes exist to signal them individually. Fixed by setting `WORKER_COUNT=0` in `TEST_ENV` — the API never auto-spawns in test environments now, so worker counts stated per scenario are the real, total counts. As a direct consequence, scenario 5 alone went from ~100s to ~240s once the hidden extra workers were removed — the true stated worker count for each scenario is now what's actually running, not roughly double it.

**Two more harness bugs found in the same pass:** (1) scenario 10 checked "has everything drained" immediately after calling `/load/start`, which only kicks off an async batched-insert task and returns before any rows exist — an empty table trivially satisfies "nothing pending," so the check could pass instantly with 0 orders ever submitted. Fixed by waiting for the row count to actually reach the target before checking drain. (2) scenario 11's "restaurant-only orders clear within 5s" threshold was tuned against the inflated worker count from bug (above) and became too tight once that was fixed — baseline `TRANSIENT_ERROR_RATE` backoff noise could occasionally push a normal run past it. Widened to a threshold that still clearly distinguishes "concurrent" from "serialized behind a slow downstream" (the actual thing being tested) without being sensitive to ordinary retry variance.

**A real system bug, not a test bug, found by scenario 10 after the circuit breaker landed:** the same 2000-order run went from ~90s to over 1000s. `CIRCUIT_BREAKER_THRESHOLD=4` was too low relative to baseline `TRANSIENT_ERROR_RATE` (0.10) at realistic call volume — the failure counter is global per downstream, shared across every worker, so "4 consecutive failures" happens often from pure noise once there are thousands of calls, not just during a real outage, and every false trip throttles the whole downstream for a cooldown. Raised to 6 (see `config.py`'s comment for the math); confirmed by re-running the burst test clean. Scenario 4's post-replay drain timeout was separately widened 30s→60s — not a bug fix, just acknowledging the breaker adds real recovery latency by design (a cooldown wait, occasionally two if a probe hits baseline noise) that didn't exist pre-breaker.

## Known limitations of the suite

- Scenarios 1, 3, 8 rely on `TRANSIENT_ERROR_RATE`/random timing in a few places;
  they're designed to hold with overwhelming probability (documented inline where
  relevant), not with mathematical certainty.
- The suite runs scenarios **sequentially** (each needs the full 8000-8002 port
  range to itself) — a full run takes several minutes, dominated by scenario 5's
  five fresh-stack iterations and scenario 10's 2000-order burst.

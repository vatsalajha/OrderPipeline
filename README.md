# Order Pipeline

A food-delivery order pipeline — orders move placed → confirmed → preparing → ready → out_for_delivery → delivered under bursty load and flaky downstreams, with no lost orders and no double-processed orders. Built with Python + FastAPI + SQLite. Runs on one machine with a single command — no Docker, no external services.
honcho start is the docker-compose up equivalent here: one command starts the API, the restaurant simulator, and the courier simulator. The API supervises the worker pool directly — see Worker pool below for why.

---

## Run it

```bash
cd order-pipeline
pip install -r requirements.txt
cp .env.example .env          # defaults work as-is
honcho start
```

Open **http://localhost:8000** for the live dashboard.

**Smoke test** (second terminal):

```bash
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "c1", "item": "burger"}' | python -m json.tool
# → {"order_id": "...", "status": "placed"}

curl -s http://localhost:8000/orders/ORDER_ID | python -m json.tool   # order + its event timeline
curl -s http://localhost:8000/health | python -m json.tool           # per-state counts, DLQ size
sqlite3 pipeline.db "SELECT id, status, item FROM orders;"           # inspect the DB directly
```

---

## Architecture

```
[Load Generator] -POST /orders-> [ API (FastAPI, :8000) ]
                                         |
                                  SQLite (WAL mode)
                                  pipeline.db
                                  ├── orders         (state + lease fields)
                                  ├── order_events   (audit log)
                                  ├── dead_letters   (DLQ)
                                  ├── counters       (orders_submitted, courier_dispatch)
                                  └── workers        (heartbeats)
                                         |
                       [ Worker pool (N native procs, supervised by API) ]
                              claim -> call downstream -> commit
                                    /             \
                    [ Restaurant :8001 ]   [ Courier :8002 ]
                         (flaky sim)            (flaky sim)

Dashboard (vanilla JS + WebSocket) served by the API
```

**SQLite as store and queue — no separate broker.** "Pending work" = any non-terminal order that is due (`next_attempt_at <= now`). Workers poll with a short `BEGIN IMMEDIATE` transaction to atomically claim one order, call the downstream *outside* the lock, then commit the result — only advancing `WHERE status = :expected`. That guard makes a duplicate commit a no-op, so **at-least-once processing + idempotent transitions = exactly-once effect**.

**Crash recovery is intrinsic.** A dead worker's lease (`LEASE_SECONDS`) expires; the next claim query treats its order as eligible again automatically. No reaper process. The downstream call also carries an idempotency key (`order_id:target_status`), so a worker that retries after a crash gets a deduped response instead of double-firing the side effect.

**Two workers racing for the same order** can't happen: `BEGIN IMMEDIATE` takes SQLite's write lock up front, so the claim query serializes — only one worker's `UPDATE` wins.

### Tech stack

| Layer | Choice | Justification |
|---|---|---|
| API / workers / sims | **Python + FastAPI** | Fast to build, async, ideal for I/O-bound work against flaky HTTP services |
| Store **and** queue | **SQLite (WAL mode)** | One durable embedded store gives ACID transactions for atomic, idempotent transitions — and doubles as the work queue. Zero infrastructure, one command to run |
| Concurrency control | `BEGIN IMMEDIATE` + lease-based claiming | Serializes claims so two workers never grab the same order; short txns keep the single writer free |
| Dashboard | **Vanilla HTML/JS + Chart.js (CDN)** + WebSocket | Real-time, no build toolchain |
| Run | **honcho** (Procfile) | One command starts API + simulators natively; workers are supervised by the API itself (see *Worker pool*) |

**Rejected alternatives:**
- **Redis/Kafka broker + Postgres** — operationally heavier, overkill for a single-machine exercise. I deliberately matched the tool to the constraints.
- **In-memory queue** — loses orders on crash, which violates the core "nothing lost" requirement.
- **Holding a DB lock during the downstream call** — would serialize every worker behind SQLite's single-writer model while waiting on slow HTTP calls; hence claim-then-release.

---

## How to drive load

```bash
POST /load/start  {"rate": 5, "count": 100}   # 100 orders at 5/sec
POST /load/rush                               # preset spike: ~500 orders in 10s
POST /load/stop
```

All three are one-click buttons on the dashboard (▶ Load 5/s, ⚡ Dinner Rush, ■ Stop Load). Inserts are batched per 100ms tick so a 500-order rush is ~50 small writes, not 500 — watch the in-flight/backlog stats absorb and drain the spike without falling over.

## Load profiles

Named, scripted load patterns — each a sequence of `(rate, seconds)` stages run through the *same* `/load/start` → `LoadGenerator` → batched-insert path the manual buttons use (a profile never bypasses normal order submission, it only sequences it). One-click on the dashboard under **Load Profiles**; the running profile shows in a banner with elapsed/total time and a **■ Stop Profile** button.

```bash
POST http://localhost:8000/load/profile       {"name": "lunch_rush"}
GET  http://localhost:8000/load/profile                                # status: running, name, elapsed, total_seconds
POST http://localhost:8000/load/profile/stop
GET  http://localhost:8000/load/profiles                                # list of {name: label}
```

| Profile | Pattern |
|---|---|
| `lunch_rush` | Sharp spike (40/s, 20s) → taper (15/s, 20s) → tail off (5/s, 20s) — ~60s total |
| `steady_evening` | Sustained moderate rate (8/s) for 120s |
| `promo_spike` | 3× the manual default rate (15/s vs. the manual button's 5/s), sustained for 90s |
| `flash_outage` | Heavy load (30/s, 50s) **and** a scripted incident: courier automatically goes down 15s in, recovers 20s later — load keeps running through and after the outage |

`flash_outage` is the one with a scripted side effect: it schedules its own `/chaos/courier` calls (down, then back to normal) at fixed offsets, concurrently with the load stage, rather than requiring you to time the courier toggle by hand.

## How to trigger failures

```bash
POST http://localhost:8000/chaos/restaurant  {"mode": "down", "seconds": 30}   # restaurant outage
POST http://localhost:8000/chaos/courier     {"mode": "slow", "seconds": 60}   # courier slowdown
POST http://localhost:8000/worker/kill       {"n": 1}                          # SIGTERM one worker
```

(The API proxies `/chaos/*` to the simulators so the browser never has to cross origins.) Also one-click on the dashboard: **Restaurant/Courier ↓ Down / ~ Slow / ✓ Normal**, **✖ Kill Worker**, **+ Worker**.

`mode: "down"` fails every call to that downstream for the window; `"slow"` multiplies step duration by `SLOW_FACTOR` (default 8×) so calls blow past `DOWNSTREAM_TIMEOUT` and exercise the retry/backoff path. Either way: status never advances on failure (commit only happens on success), so the order just retries with backoff — no special-casing needed for "downstream is broken."

Other ways to induce failure, each with its own section below: a **poison order** (`POST /orders {"poison":true}`) that always fails regardless of chaos mode; a **zone-scoped courier outage** (`{"mode":"down","zone":"north"}`) that's localized instead of global; **cancellation** (including the random `/load/cancel-chaos`); and the **circuit breaker**, which reacts to all of the above rather than being triggered directly.

## Worker pool

The worker pool is supervised by the **API process**, not honcho. honcho terminates the whole process group the moment any managed process exits, so a `/worker/kill` on a honcho-managed worker would tear the entire stack down. Instead the API spawns `WORKER_COUNT` (default 12) native `python -m worker.worker` subprocesses and owns their lifecycle.

```bash
POST http://localhost:8000/worker/kill   {"n": 1}   # SIGTERM n live workers (newest first)
POST http://localhost:8000/worker/start  {"n": 1}   # spawn n replacement workers
GET  http://localhost:8000/workers                  # pool status (pid, alive, uptime)
```

A killed worker's in-flight order is reclaimed automatically by lease expiry (default 15s) — nothing is lost or double-processed, so restarting a worker is only about restoring throughput, not recovering orders.

### Lease tuning

`LEASE_SECONDS=15`. It has to be longer than the worst-case time a *healthy* worker can legitimately hold a claim, or a live worker gets its claim stolen out from under it: `DOWNSTREAM_TIMEOUT` (6s — the client gives up on a slow downstream regardless of how slow the server actually is) plus the worst-case `busy_timeout` wait on the following commit's write lock (5s) ≈ 11s. 15s leaves ~4s of margin while still being fast enough that a killed worker's order recovers within a demo-friendly window (the default of 30s was correct but sluggish on stage). Proven exactly — not just argued — by feeding `claim_next()` simulated timestamps at `lease-1` and `lease+1` seconds; see `chaos_test.py` scenario 9.

### Graceful shutdown vs. a hard crash

`SIGTERM` (what `/worker/kill` sends) is caught — it sets a flag instead of letting the OS kill the process outright. The main loop only checks that flag *between* cycles, so a claim already in flight always runs to its natural conclusion first (commit, retry-with-backoff, or DLQ — all three already clear `claimed_by` as a normal side effect), and only then does the worker exit instead of polling again. "Finish the step" and "release the claim" turn out to be the same outcome here, not two things to implement separately. The worker also deregisters its own heartbeat row on the way out, so `/metrics` reflects the loss immediately instead of waiting ~6s to go stale.

`SIGKILL` cannot be caught by any userspace code — there is no graceful path for a hard crash, which is exactly why lease expiry exists as the backstop. Demonstrated side by side in `chaos_test.py` scenario 14: a `SIGTERM`'d worker's order is reclaimed in well under a second; a `SIGKILL`'d worker's order sits untouched for the full lease window first.

## DLQ replay

Retries back off exponentially with jitter (`min(2^n + jitter, 60s)`, jitter avoids a thundering herd when many orders retry a recovering service at once). After `MAX_ATTEMPTS` (default 5), an order moves to `dead_letters` with its `last_status` — the non-terminal state it was stuck in when retries ran out.

Once the failing downstream recovers, **"↺ Replay DLQ"** on the dashboard (or `POST /dlq/replay`) resets every dead-lettered order's status back to that `last_status`, zeroes `attempt_count`, clears the claim, and sets `next_attempt_at=0` — so the next worker poll resumes it from where it died, not from `placed`. The DLQ row is deleted on successful restore.

```bash
POST http://localhost:8000/dlq/replay   {}
# → {"replayed": 6, "orders": [{"order_id": "...", "restored_to": "ready"}, ...]}
```

Verified live: downed the courier, submitted 6 orders, all 6 exhausted retries into the DLQ at `last_status=ready` (the step right before the failing `ready -> out_for_delivery` call). Restored courier, hit replay — all 6 went `failed -> ready -> out_for_delivery -> delivered` within ~2s, DLQ drained to 0, and both reconciliation invariants stayed green throughout.

## Cancellation

Cancellation is a guarded transition through the same chokepoint as the forward path (see *State machine guard* below), legal from any non-terminal state, and logged to `order_events` with `to_status='cancelled'`.

```bash
POST http://localhost:8000/orders/{id}/cancel   {"reason": "customer_changed_mind"}
# → 409 if the order is already terminal (delivered/cancelled/failed)
POST http://localhost:8000/load/cancel-chaos    {"percent": 5, "seconds": 30}   # randomly cancel a slice of in-flight orders
```

**The mini-saga:** the status flip to `cancelled` is the one authoritative, guarded write (`UPDATE ... WHERE status=:expected`, same pattern as every other transition) and happens first, completely independent of whether a courier is even involved. Only *after* that succeeds, if the order had already reached `out_for_delivery`, does a second, best-effort step fire — a synchronous call to the courier's `/cancel-dispatch` endpoint asking it to stand down, with the outcome folded into the same `order_events` row's reason rather than a separate event. The order's own state is never left dangling on a slow or failing compensation call, since the cancellation itself already succeeded by the time the compensation is even attempted; a production version would put that second step on a durable retry queue instead of an inline call.

`cancelled` was already counted in both reconciliation invariants and rendered in its own funnel color before this — only the *trigger* (an endpoint that actually sets it) was missing. Verified across all four lifecycle points in `chaos_test.py` scenario 12: cancel immediately after placing, cancel mid-pipeline, cancel after `out_for_delivery` (compensation fires exactly once), and cancel after `delivered` (correctly rejected, 409).

## State machine guard

`commit_advance()` (the only place that writes the forward-path status) and the cancellation endpoint both call `is_valid_transition(from, to)` — checked against the single `TRANSITIONS` table in `common/state_machine.py` — before writing anything. This used to not exist: nothing stopped a caller from constructing an illegal `(from, to)` pair like `placed -> out_for_delivery`; it only ever worked in practice because `next_status()` happened to be the only caller. Confirmed and closed by `chaos_test.py` scenario 13, which forces exactly that illegal jump directly through `commit_advance()` and asserts it's rejected with the row left untouched, then confirms a *legal* transition from the same starting point still works.

## Simulated processing time

Each lifecycle step takes a realistic, **variable** duration — cooking takes far longer than confirming, and the delivery drive is the longest leg. Durations are expressed in real-world minutes (`config.py` → `STEP_DURATION_MINUTES`) and compressed by `TIME_SCALE` (seconds of sleep per simulated minute, default 0.05) so the demo runs in seconds while preserving the relative shape:

| Step | Simulated | At TIME_SCALE=0.05 |
|---|---|---|
| confirmed → preparing | 1–3 min | 0.05–0.15 s |
| preparing → ready (cooking) | 10–30 min | 0.5–1.5 s |
| ready → out_for_delivery (pickup) | 2–8 min | 0.1–0.4 s |
| out_for_delivery → delivered (drive) | 15–45 min | 0.75–2.25 s |

The per-order timeline (click any row on the dashboard) shows the actual time each step took.

## Metrics & reconciliation

`GET /metrics` returns per-state counts, in-flight count, due-backlog, total retries, DLQ size, per-worker last-seen heartbeat, downstream states, and the two live invariants below. The dashboard shows these as a banner that stays green even after you down a downstream or kill workers — the proof the system loses nothing and doubles nothing:

- **No orders lost:** `submitted == delivered + cancelled + failed(DLQ) + in_flight + waiting`. `submitted` is an independent durable counter bumped at insert time; the right-hand side is the live partition of every order by status. They can only diverge if an order is actually lost.
- **No double processing:** `counters.courier_dispatch == delivered_count`. `courier_dispatch` is incremented exactly once per guarded delivery commit (inside the same transaction as the `status='delivered'` update, guarded by `WHERE status='out_for_delivery'`), so a double-delivery would push it above the delivered count.

## Latency & SLA

`/metrics` and the dashboard both report **p50/p95 placed→delivered latency** and an **SLA breach count**, computed entirely from columns that already exist (`created_at`, `updated_at`) — no new tracking needed. p50/p95 are taken over a rolling 5-minute window of recently-delivered orders, not an all-time average, so they reflect what's happening *right now* during a rush rather than smoothing it out. The SLA breach count (`SLA_THRESHOLD_SECONDS`, default 30s) covers two things: currently in-flight orders already older than the threshold (the live "at risk" signal), and recently-delivered orders that breached it on the way through. Verified live during an actual 500-order rush in `chaos_test.py` scenario 16 — latency samples appear on `/metrics` while the rush is still draining, not only after.

## Poison orders

`POST /orders {"item": "...", "poison": true}` creates an order that **always** fails at whichever downstream it's currently calling — both simulators check `poison` first and return a real 503 regardless of chaos mode, modeling permanently bad data (an address that can never geocode, an item that doesn't exist) rather than a transient service problem. It goes through the exact same retry → backoff → DLQ path as any other failure and ends up in `dead_letters` after `MAX_ATTEMPTS`, tagged in the dashboard's Dead Letters panel.

The interesting property isn't that it fails — it's that **it doesn't take anything else down with it**. Verified in `chaos_test.py` scenario 17: one poison order mixed into a batch of 40 normal ones reaches the DLQ on its own schedule while all 40 others deliver normally, because claim/commit are independent short transactions per order — a worker stuck retrying the poison order for one attempt cycle never holds a lock that any other order needs.

## Delivery zones

Every order is tagged with a `zone` (`north`/`south`/`east`/`west`, randomly assigned unless specified) — a dimension layered on top of the existing pipeline, not a change to it. The dashboard's **Delivery Zones** card shows per-zone active/delivered/DLQ counts and 5-second throughput; `/metrics` exposes the same breakdown under `zones`.

`POST /chaos/courier {"mode":"down","seconds":30,"zone":"north"}` takes down the courier for **one zone only** — the simulator keeps a per-zone mode override that's checked before the global one, so the other three zones' orders keep flowing through the same courier process untouched. The dashboard's "↓ Outage: one random zone's courier" button drives this live. Verified in `chaos_test.py` scenario 18: an 8-order batch per zone with `north` down shows `north` partially DLQ'd while the other three zones deliver 8/8 — and the global reconciliation invariants hold throughout, because zone is just another column on the same `orders` table, not separate bookkeeping.

## Circuit breaker

Each downstream (`restaurant`, `courier`) has a breaker stored in SQLite — shared across every worker process, since the point is that **all** workers stop hammering a failing downstream together, not just the one that happened to notice. `closed → open` after `CIRCUIT_BREAKER_THRESHOLD` (default 4) consecutive failures; `open → half_open` after `CIRCUIT_BREAKER_COOLDOWN_SECONDS` (default 10s) lets exactly one worker through to probe; a successful probe closes it, a failed one sends it back to `open` with a fresh cooldown. State is visible on the dashboard next to each downstream's health badge (`CB: closed/open/half_open`) and under `circuit_breakers` in `/metrics`.

**Important design choice:** being open does *not* exempt an order from its own retry/backoff schedule — it just makes that attempt instant (no network call, no paying `DOWNSTREAM_TIMEOUT`) instead of slow. So an order still reaches `MAX_ATTEMPTS`/DLQ at the same pace if an outage is genuinely permanent; what the breaker buys is that every worker stops blocking on a doomed 6-second call the instant enough failures are seen, freeing all of them up for healthy orders immediately rather than one at a time as each one's own timeout expires.

**Why this beats blind retries:** without a breaker, N workers independently keep retrying a dead downstream on their own schedules, each wasting a full `DOWNSTREAM_TIMEOUT` per attempt — with the downstream truly down, that's N × 6s of blocked worker capacity per round, repeated every retry cycle, because no worker "knows" the others are hitting the same wall. A breaker makes that shared knowledge explicit and global: the moment enough consecutive failures are seen, *every* worker fails fast immediately, and recovery is tested with exactly one probe call instead of N independent backoff timers slowly drifting back into alignment — the whole system notices the downstream is healthy again as fast as a single request-response, not as slow as the worst-aligned timer.

Verified in `chaos_test.py` scenario 19 — with courier down and a steady trickle of orders, the breaker opens (proven by the courier simulator's own request counter barely moving, not just a state label) and recovers to closed shortly after courier comes back, with invariants holding throughout. (A circuit breaker only gets evaluated when there's traffic flowing through it — a one-shot finite batch of orders can fully exhaust its own retries and DLQ before any fresh request arrives to test recovery; that's why the test keeps submitting orders continuously, the same way a real system would have continuous traffic.)

## Verifying correctness

`chaos_test.py` is a 19-scenario automated suite — each scenario gets a fresh database and fresh services, drives one specific condition from the brief (happy path, ordering, slow/down downstreams, worker crashes, concurrent claims, idempotency, lease boundaries, a 2000-order burst, cancellation, illegal transitions, graceful vs. hard worker shutdown, named load profiles, live latency metrics, poison orders, zone-scoped outages, and the circuit breaker), and asserts the two reconciliation invariants every time. Full scenario-by-scenario writeup: **`TESTING.md`**. Requirement → test → result mapping: **`VERIFICATION.md`**.

```bash
# stop honcho first — the suite starts its own services on the same ports
python3 chaos_test.py
```

---

## Key decisions & trade-offs

| Decision | Why | At scale / rejected |
|---|---|---|
| SQLite as store **and** queue | one durable embedded store, zero infra, full ACID | Postgres + Redis Streams / Kafka at scale |
| Claim-lease-commit (no lock during downstream call) | keeps SQLite's single writer free; workers proceed concurrently | holding the lock during the slow HTTP call would serialize all workers |
| Lease expiry for crash recovery | no extra process; recovery is intrinsic to the claim query | dedicated reaper / visibility timeout at scale |
| `BEGIN IMMEDIATE` for claim + commit | SQLite serializes writers; short critical section ensures no two workers claim the same order | optimistic concurrency (CAS) is an alternative but adds retry logic |
| At-least-once + guarded idempotent transition | simplest correct path to exactly-once *effect* | true exactly-once is expensive / often impossible |
| Backoff + jitter on retries | avoids thundering-herd when a downstream is recovering | fixed retry interval hammers the service |
| Native processes via honcho + API-supervised workers | fast iteration, easy to kill individual workers for the demo | Docker + compose, autoscaled, in prod |

### What I'd do differently at scale

I'd replace SQLite with **Postgres** for concurrent writers and connection pooling, and add a **real broker — Redis Streams or Kafka** — for durable, replayable delivery with consumer groups instead of poll-and-claim. The single-writer ceiling is the main thing SQLite would force me to outgrow first: every claim and commit goes through one writer, so throughput is bounded by how fast that writer can serialize short transactions, not by how many workers I run. The claim-lease-commit correctness model carries over cleanly though — it maps onto Kafka consumer groups + offset commits (lease ≈ session timeout, commit ≈ offset commit), so the migration is mostly an infra swap, not a redesign. I ran everything as native processes here (no containers) specifically because fast iteration and the ability to `kill -TERM` an individual worker for the live demo mattered more than prod-fidelity for a one-machine take-home; in production that becomes containerized + autoscaled.

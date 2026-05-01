# CE Instability Investigation — 2026-04-17

**Author:** GitHub Copilot (read-only investigation)
**Date:** 2026-04-17
**Scope:** Company Evaluator only. No code or config was modified. The running CE service was not restarted.
**Trigger:** BenTrade → CE polling fails after 4 consecutive retries during On-Demand Evaluator runs; CE launcher GUI status visibly cycles through "Starting…" / "Online (initializing)".

---

## TL;DR

CE is **not crashing.** The worker process (PID 16860) has been continuously alive since `2026-04-17 14:28:14` with no restart, no graceful shutdown, and no OS-level termination event today.

The user-visible "Starting…" / "Online (initializing)" status is the launcher GUI's own label for **"the backend did not answer my dashboard poll within 3 seconds and did not answer the fallback `/health` poll within 2 seconds."** No FastAPI endpoint ever returns those strings.

The reason the backend stops answering polls is that **concurrent on-demand jobs serialize on SQLite write locks** and saturate the event loop with `database is locked` retry loops for tens of seconds at a time. During those windows, every endpoint served by the same event loop — including the launcher's status poll, BenTrade's job poll, and CE's `/health` — exceeds its caller's timeout.

**Today's failure burst is fully explained by 12 concurrent AAPL on-demand jobs submitted between 13:33 and 13:41, producing 13 documented `sqlite3.OperationalError: database is locked` errors and stretching successful jobs from a normal ~80–115s to 398–646s.**

---

## 3a. Failure mode determination — **Mode B (primary), Mode C (label overlap)**

| Mode | Status | Evidence |
|---|---|---|
| **A — process exits & is auto-restarted** | **Ruled out for current symptoms** | Worker PID 16860 has been alive since 14:28:14 today. Only 2 service-startup markers in today's log (10:40 and 14:28), widely spaced. No "Shutting down Company Evaluator Service" markers today. The launcher's auto-restart path ([launcher.py lines 582–593](../../launcher.py#L582-L593)) is gated on `process.poll() != None` — it did not fire. **Note**: historical multi-restart clusters exist (15 starts on 4/16, 20+ on 4/14, 30+ on 4/15) but those align with active development sessions, not the current production failure pattern. |
| **B — process up but unresponsive under load** | **Confirmed** | PID stable. `/api/status/dashboard` measured at 650 ms baseline today; runs 5+ serial SQLite queries including a nested per-tier `COUNT(*) WHERE symbol IN (SELECT …)` ([api/routes_status.py lines 18–136](../../api/routes_status.py#L18-L136)). Under DB-lock contention, this endpoint times out against the launcher's 3 s window and BenTrade's poll window. |
| **C — endpoint legitimately returns "starting/initializing"** | **Confirmed as labeling-only** | Grep of all CE source for `"starting"` / `"initializing"` finds matches **only in [launcher.py](../../launcher.py)** as Tkinter GUI labels. No server endpoint emits these strings. The launcher uses lifecycle vocabulary ("Starting…") to describe what is actually an HTTP-responsiveness condition, which makes Mode B *look like* Mode A to the user. |

### Process snapshot at investigation time

| Field | Value |
|---|---|
| Worker PID | 16860 |
| Worker command | `C:\Users\benja\anaconda3\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8100 --log-level info` |
| Parent uvicorn PID | 8012 (`.venv\Scripts\python.exe`) |
| Launcher PIDs | 14604, 15672 (`CompanyEvaluatorLauncher.exe` from `dist\`) |
| Worker start time | 2026-04-17 14:28:14 |
| Worker RSS | 518 MB |
| Worker handles | 467 |
| Worker threads | 25 |

### Documentation gap (incidental finding)

[docs/APP_CONTEXT.md](../APP_CONTEXT.md) §12.4 states "the launcher is not part of daily operation; the service is typically run directly from `.venv`." This is **contradicted by the running production instance**, which is running under `CompanyEvaluatorLauncher.exe`. The launcher *is* in the loop and is the source of the misleading status labels Ben sees.

---

## 3b. Root-cause ranking

| Rank | Cause | Evidence | Notes |
|---|---|---|---|
| **PRIMARY** | **Unbounded on-demand concurrency colliding on SQLite writes** | 13 `sqlite3.OperationalError: database is locked` errors logged 13:38–13:46 today after the user submitted ~12 concurrent AAPL jobs between 13:33–13:41. Failing SQL captured in tracebacks: progress UPDATEs on `on_demand_jobs`, final UPDATE on `company_evaluations.raw_financials`, and the global `UPDATE … SET rank = sub.rn` window-function recompute. Successful jobs from the same burst completed in 398–646 s vs. normal 80–115 s — confirming write serialization. **A single on-demand job at 14:35 (PEGA) completed normally in 81.8 s.** | See "Architectural amplifiers" below for the three source-code factors that turn moderate concurrency into a lock storm. |
| Contributing | **SMB-backed aiosqlite tail latency** | SMB stat to NAS measured at 1–14 ms (5 samples) — **the SMB transport itself is healthy.** However, `busy_timeout=10000` ms ([db/database.py line 217](../../db/database.py#L217)) is exceeded repeatedly under contention, indicating WAL writes over SMB are slow enough that 10 s isn't enough headroom. WAL file = 21.6 MB (moderate). | Amplifies #1; would not on its own cause the failures observed today. |
| Contributing | **Launcher GUI labels mis-describe an HTTP-responsiveness condition as a process-lifecycle state** | [launcher.py lines 540–600](../../launcher.py#L540-L600): when `/api/status/dashboard` (3 s timeout) and `/health` (2 s timeout) both miss the poll window, the GUI displays "Starting…" or "Online (initializing)" while the process is in fact still alive and processing. This is the entire reason Ben perceives "restarts." | Pure UX issue — the label drives the user's mental model of "CE keeps crashing." |
| Ruled out (today) | **Shadow mode doubling provider load** | `DATA_SOURCE_OVERRIDES='{"default":"shadow"}'` is configured in [.env](../../.env#L35), but no shadow-related log entries and no provider 429s during the 13:33–13:47 failure window. Today's failures are not provider-driven. | Remains a general efficiency concern. |
| Ruled out | **LLM router has no cooldown** | 60+ `event=llm_call_ok` entries today, zero `bump_error` / `endpoint_disabled` events. Both LM Studio endpoints responded to direct probes (`localhost:1234` 200 OK in 2.0 s, `192.168.1.89:1234` 200 OK in 0.15 s). One 131 s slow call observed, but the call still succeeded. | Not implicated in current symptoms. |
| Ruled out (today) | **Concurrent on-demand + crawler** | `CRAWLER_ENABLED=false` in [.env](../../.env#L30); `/api/pipeline/status` confirms `running:false, status:"idle"`. No crawler-cycle markers in the 13:33–13:47 window. | Will become relevant again whenever the crawler is re-enabled. |
| Ruled out | **Lifespan startup blocking readiness** | Only 2 lifespan-start events today (10:40, 14:28). Each completes in seconds and is not invoked at the times the user is reporting failures. | Not in scope for current symptoms. |
| Indeterminate | **Memory / handle / connection leak** | Current snapshot is healthy (518 MB RSS, 467 handles, 25 threads) but worker uptime at investigation was only ~7 minutes. **Need 24+ h of periodic `psutil` samples** to confirm or rule out a slow leak. The historical multi-restart clusters on 4/13–4/15 *could* be masking a leak, but no trend data exists. | Worth instrumenting; not actionable from current evidence. |

### Architectural amplifiers (why moderate concurrency becomes a lock storm)

These are the three source-code factors that turn the primary cause into the observed failure pattern. Cited so the remediation menu in §3c can be evaluated against specific code paths.

1. **No concurrency limit on on-demand jobs.** [api/routes_on_demand.py line 44](../../api/routes_on_demand.py#L44) fires `asyncio.create_task(run_on_demand_analysis(...))` per request. No semaphore, no queue, no per-symbol de-duplication. 12 user clicks → 12 concurrent pipelines.
2. **Global rank recompute runs after every single evaluation.** [pipeline/evaluator.py `_update_rankings()` lines 460–488](../../pipeline/evaluator.py#L460-L488) issues an exclusive-write `UPDATE company_evaluations SET rank = sub.rn FROM (window-function subquery)`. With ~2000 rows in the table, this holds the SQLite writer lock long enough for SMB latency to push it past `busy_timeout=10000`. N concurrent on-demand jobs → N exclusive-write contentions, each one blocking the others.
3. **14-step progress heartbeats per job.** [pipeline/on_demand.py PIPELINE_STEPS](../../pipeline/on_demand.py#L24-L38) drives `_update_progress` 14× per job, each call a write. 12 concurrent jobs × 14 steps ≈ **168 competing UPDATEs** against the single SQLite writer slot, on top of the rank recomputes.

The **proximate** failure is whichever of these writes happens to need the lock when `busy_timeout` expires; the **root** failure is that all three patterns assume single-stream usage but the on-demand path makes them N-stream.

---

## 3c. Remediation options (ranked by evidence-supported impact)

Presented as a menu. **No recommendation; Ben to choose sequencing.**

### Option R1 — Cap on-demand concurrency with an `asyncio.Semaphore` (1-liner config + ~10 LOC)
- **What it does:** Wrap the body of `run_on_demand_analysis` (or the `asyncio.create_task` call site) with a module-level `asyncio.Semaphore(N)` where N is small (1, 2, or 3). Concurrent submissions queue instead of executing in parallel.
- **Size:** Small patch (~10 LOC, single file: [pipeline/on_demand.py](../../pipeline/on_demand.py) or [api/routes_on_demand.py](../../api/routes_on_demand.py)).
- **Addresses:** PRIMARY cause directly. Eliminates the lock-storm pattern.
- **Does not fix:** Crawler + on-demand contention (when crawler is re-enabled); the launcher labeling issue; `_update_rankings` per-call cost.
- **Runtime:** Can be hot-applied in code, but takes effect only on next CE restart. Restart needed.
- **Trade-off:** With `N=1`, on-demand becomes strictly serial — slower for users who legitimately want to evaluate two different symbols at once. `N=2` is a reasonable middle ground given the existing crawler concurrency setting.

### Option R2 — Per-symbol on-demand de-duplication (small patch)
- **What it does:** Before creating a new job, check `on_demand_jobs` for an existing `queued`/`running` job for the same symbol. If one exists, return its `job_id` instead of starting a second pipeline. Today's 12 AAPL submissions would have collapsed to 1.
- **Size:** Small patch in [pipeline/on_demand.py `create_job`](../../pipeline/on_demand.py#L52) (~15 LOC).
- **Addresses:** PRIMARY cause for the most common real-world trigger (user mashes the "Analyze" button or BenTrade retries cause duplicate submissions).
- **Does not fix:** Genuine concurrent submissions for different symbols.
- **Runtime:** Restart required.
- **Trade-off:** Changes API semantics — POST `/on-demand/analyze` returns an existing job instead of starting a new one. BenTrade may need to handle this (it almost certainly already does, since the response shape is the same).

### Option R3 — Move `_update_rankings()` out of the per-evaluation hot path (small-medium refactor)
- **What it does:** Stop calling `_update_rankings()` after every `evaluate_company`. Either (a) call it only at the end of a crawler cycle, or (b) replace the global recompute with a per-symbol relative-rank update, or (c) compute rank on read in `/api/companies/ranked` instead of materializing it.
- **Size:** Small patch for (a) or (c); medium refactor for (b).
- **Addresses:** Removes the largest single write-lock holder, which is the actual blocker for concurrent on-demand jobs.
- **Does not fix:** Frequent `_update_progress` heartbeats; SMB tail latency; concurrent submissions for different symbols.
- **Runtime:** Restart required.
- **Trade-off:** If on-demand jobs no longer recompute rank, the `/api/companies/ranked` view may show a stale `rank` for the just-evaluated company until the next crawler cycle. (a) preserves correctness with crawler running; (c) preserves correctness universally at the cost of one extra ORDER BY per ranked-list request.

### Option R4 — Reduce on-demand DB write frequency (small patch)
- **What it does:** Coalesce `_update_progress` writes — write only on milestone steps (e.g., 1, 5, 10, 14) instead of every step, or throttle to one write per N seconds per job.
- **Size:** Small patch in [pipeline/on_demand.py](../../pipeline/on_demand.py#L170-L199) (~10 LOC).
- **Addresses:** Reduces the 14×-per-job write pressure to ~4×.
- **Does not fix:** The rank recompute lock; root cause of unbounded concurrency.
- **Runtime:** Restart required.
- **Trade-off:** Coarser progress reporting in BenTrade's UI.

### Option R5 — Move SQLite from SMB to local disk (medium operational change)
- **What it does:** Change `DATABASE_URL` to a local-disk path. Run a one-time copy from NAS to local. Optionally keep a periodic backup to NAS.
- **Size:** 1-line config change + operational copy + decision about backup strategy.
- **Addresses:** Removes the SMB tail-latency contributor (#2). Substantially shortens the window during which `busy_timeout` matters.
- **Does not fix:** PRIMARY cause — concurrent jobs would still serialize on the local SQLite write lock, just faster.
- **Runtime:** Requires CE restart and DB relocation. Loses the single-source-of-truth-on-NAS property.
- **Trade-off:** Backup discipline becomes Ben's responsibility. Affects any other machine that reads the DB.

### Option R6 — Increase `busy_timeout` (1-liner)
- **What it does:** Change [db/database.py line 217](../../db/database.py#L217) from `PRAGMA busy_timeout=10000` to `30000` or `60000`.
- **Size:** 1 LOC.
- **Addresses:** Symptom-only — instead of failing with `database is locked`, jobs would block waiting longer. Today's burst would have produced extreme tail latencies but possibly fewer hard failures.
- **Does not fix:** Root cause; in fact, longer waits would worsen the `/api/status/dashboard` polling timeout problem and could make the launcher *more* likely to display "Starting…".
- **Runtime:** Restart required.
- **Trade-off:** Trades one symptom (errors) for another (longer hangs). Not recommended on its own; could be reasonable in combination with R1.

### Option R7 — Fix the launcher labeling (small UX patch)
- **What it does:** Distinguish in [launcher.py](../../launcher.py#L540-L600) between "process not running" (label = "Starting…") and "process running but slow" (label = "Busy" or "Slow response"). Use `process.poll()` together with the elapsed time of the failed poll to choose the label.
- **Size:** Small patch (~20 LOC, single file).
- **Addresses:** Eliminates the *user-perceived* "CE keeps crashing" symptom even when the underlying Mode B contention persists. Makes future debugging clearer.
- **Does not fix:** The actual unresponsiveness; doesn't help BenTrade's poll failures.
- **Runtime:** Restart of launcher only; backend untouched.
- **Trade-off:** None significant. Pure UX clarity improvement.

### Option R8 — Disable shadow mode (1-liner)
- **What it does:** Change [.env line 35](../../.env#L35) from `DATA_SOURCE_OVERRIDES='{"default":"shadow"}'` to a single-provider mode.
- **Size:** 1 LOC.
- **Addresses:** Reduces baseline outbound HTTP load by ~50%. Not implicated in today's specific failures.
- **Does not fix:** Anything related to the on-demand DB-lock storm.
- **Runtime:** Restart required.
- **Trade-off:** Loses cross-validation between Polygon and FMP. Should not be done as a stability fix; only if shadow mode's value has otherwise diminished.

### Combination notes (for Ben's sequencing)

- **Smallest credible fix:** R1 (`Semaphore(1)`) + R2 (per-symbol de-dup). Together these eliminate the documented failure pattern with ~25 LOC.
- **Smallest fix that also helps general performance:** R1 + R3(a) (move rank recompute out of the per-evaluation path).
- **Symptom-only triage** that doesn't require restart-now planning: R7 (launcher labels) — would make Ben's GUI no longer mislead him about CE restarting, even before the real fix lands.

---

## 3d. Open questions

| Question | What would resolve it |
|---|---|
| Is there a slow memory or handle leak in the worker process? | Periodic `psutil` snapshot (RSS, handles, threads, `net_connections()`) every 5 minutes for 24+ h, written to a local CSV. Could be a small cron-style background task or an external `psutil` script invoked by Task Scheduler. |
| Are the historical multi-restart clusters (4/13–4/15, 4/16) genuine crashes, or development-iteration restarts? | Cross-reference startup-marker timestamps against Windows Event Viewer (Application + System logs) entries for the worker PIDs at those times. If Event Viewer shows process termination events with non-zero exit codes, those clusters are real Mode A. If not, they are dev restarts. Would also benefit from launcher-side logging of "auto-restart fired" with cause. |
| When shadow mode is active, what is the actual provider-call multiplier in steady state? | Add a per-provider request counter to the data-source router and log totals at the end of each crawler cycle. Not implementable in this prompt (in-scope for a future one). |
| Is the WAL file ever checkpointed under SMB, or does it grow indefinitely? | Periodic `Get-Item …\company_eval.db-wal` size sampling. Today's snapshot was 21.6 MB at 14:35 with last activity 14:34:16; benign but a single sample. |
| Does BenTrade's polling actually call `/api/status/dashboard`, `/health`, or a third endpoint (e.g. `/api/on-demand/jobs/{job_id}`)? | The 4-retry ceiling pattern matters because *which* endpoint is being polled determines which fix path actually unblocks BenTrade. Reading BenTrade's polling code would answer this — out of scope for this CE-only investigation. |

---

## 3e. Low-risk observability improvements (suggestions for a future prompt)

These are *not* implemented in this prompt. They are proposed because they would make the next investigation much faster.

1. **Log structured event when an on-demand job is created**, including the count of currently-running on-demand jobs. Would let us see concurrency at-a-glance without reconstructing it from interleaved progress lines.
2. **Log a single line per `_update_rankings()` call** with row count and elapsed time. Today this only logs on success and at INFO level inside the function — but during a lock storm it's the most valuable timing measurement.
3. **Log `database is locked` retries (not just final failures).** Currently only the unrecoverable failure surfaces as an ERROR. The retry storm itself is invisible to the log reader.
4. **Launcher: log `auto-restart fired` events to file**, including the reason (`process.poll() returned exit_code=N`). Currently the launcher's restart logic exists but its decisions are only visible in the in-memory GUI state; the launcher log file at `%LOCALAPPDATA%\CompanyEvaluator\logs\company_evaluator_launcher.log` is stale (last write 4/16). Suggests the launcher's own logging is broken or not configured for the running build.
5. **Server-side request log with elapsed-ms suffix** for `/api/status/dashboard` and `/health`. Would let us correlate launcher "Starting…" displays with actual server-side response times.
6. **Crawler/shadow-mode marker pair**: log a structured `event=shadow_mode_call provider=primary symbol=X` and `event=shadow_mode_call provider=shadow symbol=X` at start of each call so shadow-mode load is greppable.
7. **Periodic worker self-snapshot** (every 5 min, INFO level): RSS, threads, open file descriptors, in-flight on-demand jobs, in-flight LLM calls, WAL file size. ~20 LOC, would close the leak open-question without external tooling.

---

## Observations captured but not acted on (per prompt — no fixes in this investigation)

These are findings that a future prompt may want to address. Listed here so they aren't lost.

- [docs/APP_CONTEXT.md](../APP_CONTEXT.md) §12.4 incorrectly claims "the launcher is not part of daily operation" — production is running under the launcher.
- The launcher log file at `%LOCALAPPDATA%\CompanyEvaluator\logs\company_evaluator_launcher.log` has not been written to since 2026-04-16 14:21:03, despite the launcher being actively running since 14:28 today. Suggests the launcher's logging configuration is broken in the current `dist\CompanyEvaluatorLauncher.exe` build.
- `RotatingFileHandler` is configured at [main.py line 46](../../main.py#L46) for 5 backups × 50 MB each, but no rotated backup files exist on disk despite the active log being at 40.7 MB / 50 MB threshold. May indicate rotation is silently failing (worth a one-line check on next investigation).
- LM Studio at `localhost:1234` returned `/v1/models` in 2061 ms — that's 10–14× slower than the remote `192.168.1.89:1234` endpoint (146 ms). Functional, but worth investigating separately; could indicate the local LM Studio instance is loaded with a model that doesn't fit comfortably or is sharing the GPU with something else.
- One LLM call today took 131 s ([analysis.llm_router log at 13:42:58](../../analysis/llm_router.py)); this is the kind of long blocking operation that, when overlaid with on-demand DB contention, makes the event loop appear dead to pollers.

---

## Appendix A — Raw evidence snippets

### A.1 Today's on-demand failure burst (verbatim from `company_evaluator.log`)

```
2026-04-17 13:34:43,580 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:32:50_AAPL_7b66) in 112.5s
2026-04-17 13:38:23,002 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:34:15_AAPL_85df): (sqlite3.OperationalError) database is locked
2026-04-17 13:40:28,645 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:38:10_AAPL_e18b): (sqlite3.OperationalError) database is locked
2026-04-17 13:41:09,569 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:35:53_AAPL_aa18): (sqlite3.OperationalError) database is locked
2026-04-17 13:41:09,574 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:33:57_AAPL_897c): (sqlite3.OperationalError) database is locked
2026-04-17 13:41:11,412 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:37:36_AAPL_548d): (sqlite3.OperationalError) database is locked
2026-04-17 13:43:21,384 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:38:48_AAPL_d9dc): (sqlite3.OperationalError) database is locked
2026-04-17 13:44:39,463 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:35:54_AAPL_5ce4): (sqlite3.OperationalError) database is locked
2026-04-17 13:44:39,469 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:40:30_AAPL_f6f5): (sqlite3.OperationalError) database is locked
2026-04-17 13:44:39,473 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:41:09_AAPL_3bed): (sqlite3.OperationalError) database is locked
2026-04-17 13:45:48,682 [ERROR] pipeline.on_demand: On-demand analysis failed: AAPL (job=ondemand_2026-04-17T17:40:29_AAPL_4fd5): (sqlite3.OperationalError) database is locked
2026-04-17 13:46:00,309 [ERROR] pipeline.on_demand: On-demand analysis failed: TXN (job=ondemand_2026-04-17T17:36:33_TXN_1cc4): (sqlite3.OperationalError) database is locked
2026-04-17 13:46:04,878 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:35:19_AAPL_c557) in 640.5s
2026-04-17 13:46:24,007 [ERROR] pipeline.on_demand: On-demand analysis failed: TXN (job=ondemand_2026-04-17T17:35:54_TXN_7328): (sqlite3.OperationalError) database is locked
2026-04-17 13:46:42,513 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:39:30_AAPL_559c) in 421.4s
2026-04-17 13:47:25,807 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:36:40_AAPL_3f51) in 610.1s
2026-04-17 13:47:26,586 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:36:34_AAPL_307f) in 589.8s
2026-04-17 13:47:27,959 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:36:34_AAPL_9d1e) in 645.9s
2026-04-17 13:47:59,827 [INFO] pipeline.on_demand: On-demand analysis complete: AAPL (job=ondemand_2026-04-17T17:41:12_AAPL_47c5) in 398.3s
```

Single on-demand job for comparison (no concurrency):

```
2026-04-17 14:35:11,587 [INFO] pipeline.on_demand: On-demand analysis complete: PEGA (job=ondemand_2026-04-17T18:33:42_PEGA_72e3) in 81.8s
```

### A.2 Failing SQL statements (from tracebacks)

```
UPDATE on_demand_jobs SET current_step=?, current_step_index=?, percent=?, completed_steps=?, ... WHERE on_demand_jobs.job_id = ?
UPDATE company_evaluations SET raw_financials=?, evaluated_at=? WHERE company_evaluations.symbol = ?
UPDATE company_evaluations SET rank = sub.rn FROM (SELECT symbol, ROW_NUMBER() OVER (ORDER BY composite_score DESC) AS rn FROM company_evaluations ...) sub WHERE company_evaluations.symbol = sub.symbol
```

All three originate from [pipeline/on_demand.py](../../pipeline/on_demand.py) and [pipeline/evaluator.py](../../pipeline/evaluator.py).

### A.3 NAS / SMB latency probe

```
Test-Path \\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db  → True
SMB stat samples: 14ms, 1ms, 1ms, 1ms, 1ms
DB sizes: company_eval.db = 475,746,304 B; .db-wal = 21,679,472 B; .db-shm = 32,768 B
```

### A.4 LM Studio probe

```
http://localhost:1234/v1/models    → 200 OK in 2061 ms (308 bytes)
http://192.168.1.89:1234/v1/models → 200 OK in 146 ms  (648 bytes)
```

### A.5 Pipeline status during investigation

```json
{"running":false,"paused":false,"status":"idle","cycle_number":0,
 "schedule_state":"market_open","crawler_should_run":false, ...}
```

Crawler was not running during today's failure burst. Crawler is therefore ruled out as a contributing factor for the *currently reported* symptoms (it would still be a factor whenever it is re-enabled).

---

*End of report.*

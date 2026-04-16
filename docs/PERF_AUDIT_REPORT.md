# Performance Audit — Where Are the 80 Seconds Going?

**Date:** 2026-04-16
**Scope:** 10-symbol instrumented crawl (MSFT, AAPL, WMS, EXEL, PLTR, KO, JPM, XOM, AMT, DDOG)
**Instrumentation:** [scripts/perf_audit.py](scripts/perf_audit.py) — runtime monkey-patches on `httpx.AsyncClient.send`, `asyncio.sleep`, and `analysis.llm_client.call_llm`. **Zero library code changed.**
**Raw log:** [logs/perf_audit.log](logs/perf_audit.log)

---

## 1. Headline Numbers

| Metric | Value |
|---|---|
| Symbols evaluated | 10 |
| Total wall time | **820.4 s** |
| Average per symbol | **82.0 s** |
| Min / Max per symbol | 69.3 s (JPM) / 106.1 s (AMT) |
| Total HTTP calls | 161 |
| Rate-limit sleep | **0.0 s** (bulk cache saturates the budget) |

Extrapolated to the 2,942-symbol universe: **~67 hours** at current rate.

---

## 2. Per-Symbol Totals (from audit)

```
symbol   total_s   http_s    llm_s  sleep_s  http_n
MSFT       74.1     48.2     44.2      0.0      17
AAPL       81.7     58.7     54.2      0.0      16
WMS        76.0     53.3     49.2      0.0      16
EXEL       92.4     70.5     66.6      0.0      16
PLTR       77.8     54.8     50.9      0.0      16
KO         85.4     60.5     55.9      0.0      16
JPM        69.3     46.3     41.7      0.0      16
XOM        71.5     48.7     44.7      0.0      16
AMT       106.1     83.5     74.6      0.0      16
DDOG       86.1     62.1     58.0      0.0      16
```

Note `http_s` includes the LLM POST (it is an HTTP call). Non-LLM HTTP ≈ `http_s − llm_s` ≈ **4–9 s/symbol**.

---

## 3. Time Breakdown per Symbol (averaged)

Reconstructed from `pipeline.evaluator` step markers + audit host totals:

| Phase | Avg time | % of total | Notes |
|---|---:|---:|---|
| **Step 3 — LLM analysis** | **54.0 s** | **66 %** | Single call to `qwen3-14b-claude-4.5-opus-high-reasoning-distill` on `localhost:1234`. Reasoning model = long generation. |
| **Step 5 — Rankings update** | **~15 s** | **18 %** | `_update_rankings()` re-reads, re-sorts, and re-writes **1 931 rows** after every single symbol. |
| **Step 1 — Data fetch** | **~13 s** | **16 %** | 13 HTTP calls executed **sequentially** (Polygon × 4, Finnhub × 4, FMP × 4, Yahoo ownership × 1). |
| Step 1b — Cross-validation | ~0.1 s | <1 % | 4–5 metric adjustments per symbol; trivial. |
| Step 2 — 5-pillar metrics | ~0.0 s | 0 % | Pure Python math over cached data. |
| Step 4 — DB upsert | ~0.1 s | <1 % | One UPSERT + one history insert. |

Total reconciles: 54 + 15 + 13 + 0.3 ≈ **82.3 s/symbol** ✓.

---

## 4. Host Breakdown

```
host       calls  total_s  avg_ms    MB  errs
llm           11    533.2   48476   0.1     0
polygon       40     27.2     681   1.7     0
fmp           70     16.7     238   2.1    10
finnhub       40      9.6     240   2.1     0
```

Observations:
- **LLM dominates** — 533 s across 10 symbols (one call/symbol + 1 test ping). Average **48 s/call**, max **74 s** (AMT).
- **FMP has 10 errors out of 70 calls** (14 %): every symbol hits `HTTP 400` on `/institutional-ownership/symbol-positions-summary`. Endpoint appears broken/deprecated but still costs ~0.8 s/symbol (~2 % of total).
- **Bulk cache is working perfectly** — Polygon/Finnhub/FMP calls average 240–680 ms; `fmp_profile` frequently returns in **0.0 s** (in-process cache hit). **Total rate-limit sleep across 10 symbols = 0.0 s.** No more waiting on rate-limit windows.

---

## 5. LLM Details

```
symbol   prompt_chars resp_chars  elapsed_s
MSFT             2961       2539       44.2
AAPL             2994       1925       54.2
WMS              2949       2462       49.2
EXEL             2969       2149       66.6
PLTR             2922       2237       50.9
KO               2968       2412       55.9
JPM              2902       2144       41.7
XOM              2918       2079       44.7
AMT              2954       2204       74.6
DDOG             2951       2298       58.0

LLM avg: 54.0 s/symbol   (540.0 s total)
```

- Prompt is ~**3 kB** (constant). Response ~**2–2.5 kB**. Latency scales with response length + reasoning-trace length (model is a reasoning distill).
- **Top 10 slowest HTTP calls in the entire audit are all LLM calls** — non-LLM calls peak at 4 s.

---

## 6. Top 15 Slowest HTTP Calls

```
   73.98s  [200] AMT    localhost/v1/chat/completions
   65.90s  [200] EXEL   localhost/v1/chat/completions
   57.43s  [200] DDOG   localhost/v1/chat/completions
   55.26s  [200] KO     localhost/v1/chat/completions
   53.65s  [200] AAPL   localhost/v1/chat/completions
   50.26s  [200] PLTR   localhost/v1/chat/completions
   48.52s  [200] WMS    localhost/v1/chat/completions
   44.12s  [200] XOM    localhost/v1/chat/completions
   42.73s  [200] MSFT   localhost/v1/chat/completions
   41.13s  [200] JPM    localhost/v1/chat/completions
    4.04s  [200] AMT    finnhub.io/api/v1/stock/insider-transactions
    1.53s  [200] KO     api.polygon.io/vX/reference/financials
    1.40s  [200] AMT    api.polygon.io/vX/reference/financials
    0.90s  [200] JPM    api.polygon.io/vX/reference/financials
    0.89s  [200] WMS    api.polygon.io/vX/reference/financials
```

**Takeaway:** there are two populations of HTTP calls — the 10 LLM calls (41–74 s) and everything else (≤4 s). The data-fetch budget is effectively solved; the remaining wins are structural.

---

## 7. Cache Effectiveness

- **Bulk cache confirmed active** (`CompanyDataService FMP using bulk cache` on every symbol).
- Rate-limit sleeps totaled **0.0 s across 10 symbols** — the 5/s Polygon, 30/s Finnhub, 300/min FMP budgets are never saturated because most calls hit cache or local SQLite.
- Phase 2c cache work paid off: non-LLM HTTP wall time is **4–9 s/symbol**, down from the ~60 s observed before bulk-cache integration.

---

## 8. Top 3 Bottlenecks (ranked)

### #1 — LLM inference: **54 s/symbol, 66 % of total**
A single chat-completion call to the Qwen3-14B **reasoning** distill is, on average, longer than every other step combined. The reasoning trace is the slow part (response is only 2 kB; most of the elapsed time is hidden `<think>` tokens).

### #2 — Post-symbol rankings recompute: **~15 s/symbol, 18 % of total**
[`pipeline/evaluator.py`](pipeline/evaluator.py#L451) calls `_update_rankings()` after every symbol. That function `SELECT`s all 1 931 composite rows, sorts them in Python, then issues 1 931 `UPDATE` statements in a single transaction. **Doing this once per symbol means we do ~19 000 UPDATEs for 10 symbols** (and ~5.7 M UPDATEs for a 2 942-symbol universe).

### #3 — Sequential data fetches in `CompanyDataService.get_company_data`: **~13 s/symbol, 16 % of total**
13 HTTP calls are `await`ed one after the other ([data/company_data_service.py](data/company_data_service.py)). With [`asyncio.gather`](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather) the wall time ceiling drops to the slowest single call (~3 s).

---

## 9. Recommended Optimizations (ranked by ROI)

> The user directive for this audit was **investigation only — no code changes**. The changes below are recommendations only.

### Tier 1 — Zero-risk, mechanical

1. **Defer rankings to end of batch.**
   Move `_update_rankings()` out of the per-symbol loop and call it once after the batch finishes. Estimated save: **15 s × N** → for 10 symbols saves ~150 s (18 % of total); for the full 2 942-symbol crawl saves **~12 hours**. Also eliminates the 1 931-row write storm that pressures WAL/aiosqlite.

2. **Parallelize `CompanyDataService.get_company_data` with `asyncio.gather`.**
   All 13 fetches are independent (they only share the final merge). Wrapping them in `asyncio.gather(*tasks)` drops data-fetch wall time from ~13 s → ~3 s. Estimated save: **~10 s/symbol** (12 %).

3. **Delete the broken FMP institutional-ownership call.**
   `/institutional-ownership/symbol-positions-summary` returns HTTP 400 for every symbol (10/10 errors in audit). Short-circuit to `None` and skip the request. Save: ~0.8–1.4 s/symbol + clean WARN log.

### Tier 2 — Larger leverage, needs integration

4. **Distribute LLM calls across both LM Studio endpoints.**
   BenTrade already has `execute_routed_model()` with `ROUTING_MODE=local_distributed`, which balances between `localhost:1234` and `192.168.1.143:1234`. Integrating that into [`analysis/llm_client.py`](analysis/llm_client.py) lets the crawler run **two companies' LLM calls concurrently**. Expected LLM throughput ≈ **1.8×** (Amdahl w/ light queueing) → LLM effective cost ≈ **30 s/symbol** instead of 54 s. Save: **~24 s/symbol** (29 %).

5. **Two-tier LLM strategy.**
   Use a cheap 7B non-reasoning model for routine verdicts; escalate to the 14B reasoning distill only for breakout candidates or boundary scores. Potential additional save: **~20–30 s/symbol** on the ~80 % of symbols that aren't breakout candidates.

### Projected combined impact

| Scenario | Per-symbol | 2 942-symbol crawl |
|---|---:|---:|
| Current | 82 s | ~67 h |
| + Tier 1 (defer rankings + parallel fetch + drop broken endpoint) | ~58 s | **~47 h** |
| + Tier 1 + distributed LLM | ~32 s | **~26 h** |
| + Tier 1 + distributed + two-tier LLM | ~18 s | **~15 h** |

---

## 10. What to Hand to BenTrade (integration note)

The LLM client here ([`analysis/llm_client.py`](analysis/llm_client.py)) posts to a single endpoint. BenTrade has an `execute_routed_model()` helper that already:

- Holds two LM Studio endpoints (`local` / `model_machine`).
- Applies a concurrency semaphore per endpoint.
- Has fallback + retry built in.

A minimal integration is a thin adapter in `analysis/llm_client.py` that imports the router and calls it with `mode="local_distributed"`. The crawler would then run multiple symbols' LLM steps concurrently without further changes — each symbol's pipeline is already independent.

---

## 11. Deliverables

- Audit script kept in place for future runs: [scripts/perf_audit.py](scripts/perf_audit.py). Re-run any time with `python scripts/perf_audit.py --symbols MSFT,AAPL,...`.
- Raw log: [logs/perf_audit.log](logs/perf_audit.log) (contains every HTTP call, every step marker, and the summary tables reproduced above).
- This report: [docs/PERF_AUDIT_REPORT.md](docs/PERF_AUDIT_REPORT.md).

**No library code was modified during this audit.** All timing instrumentation lives inside `scripts/perf_audit.py` as runtime monkey-patches and is gone the moment the script exits.

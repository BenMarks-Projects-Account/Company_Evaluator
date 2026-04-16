# Tier 1 Optimization Results — 82s → 60.6s per symbol

**Date:** 2026-04-16  
**Baseline tag:** `pre-tier1-optimizations` (commit `be909b8`)  
**Result tag:** `post-tier1-optimizations`  
**Scope:** Same 10-symbol audit (MSFT, AAPL, WMS, EXEL, PLTR, KO, JPM, XOM, AMT, DDOG)  
**Raw log:** `logs/perf_audit_post_tier1.log`

---

## Headline: 26% faster per symbol

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Total wall time | 820.4 s | **605.6 s** | −214.8 s |
| Average per symbol | 82.0 s | **60.6 s** | **−21.4 s (−26%)** |
| Min per symbol | 69.3 s (JPM) | 56.5 s (KO) | −12.8 s |
| Max per symbol | 106.1 s (AMT) | 67.1 s (AMT) | −39.0 s |
| HTTP calls total | 161 | 151 | −10 (institutional eliminated) |
| HTTP errors | 10 × HTTP 400 on `/institutional-ownership` | **0** | fixed |

---

## Per-symbol comparison

| Symbol | Before (s) | After (s) | Saved (s) |
|---|---:|---:|---:|
| MSFT | 74.1 | 58.9 | −15.2 |
| AAPL | 81.7 | 57.6 | −24.1 |
| WMS  | 76.0 | 57.0 | −19.0 |
| EXEL | 92.4 | 58.6 | −33.8 |
| PLTR | 77.8 | 58.4 | −19.4 |
| KO   | 85.4 | 56.5 | −28.9 |
| JPM  | 69.3 | 66.9 | −2.4  |
| XOM  | 71.5 | 58.3 | −13.2 |
| AMT  | 106.1| 67.1 | −39.0 |
| DDOG | 86.1 | 66.5 | −19.6 |

---

## Bucket breakdown

| Bucket | Before avg | After avg | Change |
|---|---:|---:|---:|
| Data fetch (Step 1)   | ~13.0 s | ~9.7 s   | −3.3 s (parallelized) |
| Rankings (Step 5)     | ~14.8 s | 1.2 s*   | **−13.6 s** |
| LLM (Step 3)          | 54.0 s  | 50.7 s   | −3.3 s (variance) |
| FMP institutional     | ~1.0 s  | 0.0 s    | −1.0 s (short-circuited) |

\*Rankings in batch mode: one end-of-batch call of 12.3 s amortized across 10 symbols = 1.2 s/sym. In real crawler the amortization is over 100 symbols (`RANKINGS_UPDATE_INTERVAL = 100`), so effective per-symbol cost ≈ 0.12 s.

---

## What changed

### Fix 1 — Defer rankings recompute (saved ~13.6 s/sym)
- `pipeline/evaluator.py`: Added `skip_rankings: bool = False` param to `evaluate_company()`.
- `pipeline/crawler.py`: Calls `evaluate_company(symbol, skip_rankings=True)`, then runs `_update_rankings()` every `RANKINGS_UPDATE_INTERVAL = 100` symbols and once at end of cycle / on shutdown.
- Log line `[SYM] Step 5/5: Rankings update deferred (batch mode)` confirms.

### Fix 2 — Parallelize data fetches (saved ~3.3 s/sym)
- `data/company_data_service.py`: `get_company_data()` now issues 12 independent fetches via `asyncio.gather(*tasks.values(), return_exceptions=True)` (Polygon financials Q/A, Polygon prices, Polygon details, Finnhub metrics / profile / insiders / recs, Yahoo ownership, FMP insider txns / insider stats / profile). Exceptions logged and mapped to `None` per-task.
- FMP financials fallback runs only when Polygon statements are empty — also gathered.
- Less than theoretical max (~10 s saved) because shared httpx connection pool and rate-limiter semaphore serialize calls within a host.

### Fix 3 — Short-circuit FMP `/institutional-ownership` (saved ~1.0 s/sym)
- `data/fmp_client.py`: `get_institutional_ownership()` returns `None` immediately. Original implementation preserved as `_get_institutional_ownership_raw()` for future re-enable when FMP fixes the plan gating.
- 10 prior HTTP 400 warnings per audit cycle → 0.

---

## Data integrity — verified

Composite scores for the 10 audit symbols match the pre-Tier-1 snapshot exactly (±0.0). See `scripts/verify_tier1_results.py`:

```
MSFT   composite=69.9  pre=69.9  delta=+0.0
AMT    composite=51.6  pre=51.6  delta=+0.0
DDOG   composite=39.3  pre=39.3  delta=+0.0
```

Pillar scores, rank ordering, and data quality unchanged. LLM recommendations are non-deterministic and not part of the integrity check.

---

## Next opportunities (Tier 2 — out of scope here)

- **LLM response streaming / smaller model (saves 20–30 s/sym):** LLM is now 83% of per-symbol time (50.7 s of 60.6 s). Any further meaningful speedup requires LLM optimization.
- **Cross-symbol LLM concurrency (saves 30–40% of batch time):** Run N LLM calls in flight concurrently while next N symbols' data fetches run in parallel.
- **Shared httpx client tuning (saves ~2 s/sym):** Raise per-host connection pool limit so parallel gather doesn't serialize within Finnhub/Polygon/FMP.

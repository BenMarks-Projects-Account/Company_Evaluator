# Company Evaluator — Application Context

> Drop this document into a new AI chat to give it full working context of the project. It captures **what the service does**, **how it's wired together**, **what infrastructure it runs on**, and **what decisions were made and why**. Keep it current after material changes.

**Last updated:** git HEAD `b225fcb` (Fix LLM router: point remote endpoint to BenTrade machine).
**Latest tag:** `post-tier2-llm` (7cc589a).

---

## 1. What This Application Does

The **Company Evaluator** is a standalone FastAPI microservice that evaluates publicly traded companies as medium-term investment candidates using an institutional-grade 5-pillar framework. It runs on its own machine, pulls financial data from three primary providers (FMP, Polygon, Finnhub) with Yahoo Finance as a fallback, scores companies on a 0–100 composite scale, and enriches the quantitative scores with LLM-written thesis / risk / catalyst narratives via a locally hosted LM Studio model.

The service is the data/scoring backend for a sibling app called **BenTrade** (on a different machine). BenTrade consumes this service's REST API to power its UI — ranked company lists, per-symbol evaluation detail, DCF / EVA / Comps valuations, entry-point analyses, and on-demand "research any ticker" jobs. The Company Evaluator itself has no UI; it is API-only.

Operationally, a background **crawler** walks a universe of ~2,900 active tickers on a nightly cadence (or during market hours when configured), evaluates each symbol end-to-end (data fetch → 5-pillar scoring → LLM analysis → DB persist), and keeps the `company_evaluations` table hot. Rankings are updated every 50 symbols using a SQL window function. Valuation models (DCF, EVA, Comps, Entry Point) and deeper analyses (Piotroski, EPV, transcript analysis) run on demand or as follow-ups for top-ranked names.

The design philosophy is: **compute core metrics from raw financial statements** (not pre-packaged ratios), keep every external call rate-limited and failure-isolated, cache aggressively via a dedicated FMP bulk-endpoints SQLite cache, and route LLM load across two physical machines to reduce wall-clock per symbol.

---

## 2. Architecture Overview

```
                    ┌─────────────────────────────────────────────┐
                    │   NAS  —  192.168.1.149  (SMB share)        │
                    │   \\192.168.1.149\CompanyEvaluatorData\     │
                    │      company_evaluator\db\                  │
                    │        company_eval.db        (main, 314MB) │
                    │        company_eval_bulk.db   (FMP, 33MB)   │
                    └──────────────┬──────────────────────────────┘
                                   │ SQLite over SMB
                                   │ (aiosqlite)
                                   │
   ┌───────────────────────────────┴─────────────────────────────┐
   │   Machine 2 — EVALUATOR   192.168.1.143                     │
   │   ┌─────────────────────────────────────────────────────┐   │
   │   │  FastAPI  (uvicorn, port 8100)                       │   │
   │   │  ├── api/routes_*.py       (14 routers)              │   │
   │   │  ├── pipeline/crawler.py   (nightly batch)           │   │
   │   │  ├── pipeline/on_demand.py (async job queue)         │   │
   │   │  ├── metrics/*.py          (5 pillars + composite)   │   │
   │   │  ├── analysis/*.py         (LLM, DCF, EVA, Comps,    │   │
   │   │  │                          EntryPoint, EPV, Piotroski)│ │
   │   │  ├── data/*.py             (clients + router)        │   │
   │   │  └── bulk/*.py             (FMP bulk cache layer)    │   │
   │   └─────────────────────────────────────────────────────┘   │
   │   ┌─────────────────────────────────────────────────────┐   │
   │   │  LM Studio  (port 1234)                              │   │
   │   │  Model: qwen3-14b-claude-4.5-opus-high-reasoning-   │   │
   │   │         distill                                      │   │
   │   └─────────────────────────────────────────────────────┘   │
   └──────────────┬──────────────────────────────────────────────┘
                  │ HTTP (LLM routing)
                  │
   ┌──────────────┴──────────────────────────────────────────────┐
   │   Machine 1 — BENTRADE    192.168.1.89                      │
   │   ┌─────────────────────────────────────────────────────┐   │
   │   │  BenTrade backend (FastAPI) — consumer of this API   │   │
   │   │  calls http://192.168.1.143:8100/api/...             │   │
   │   └─────────────────────────────────────────────────────┘   │
   │   ┌─────────────────────────────────────────────────────┐   │
   │   │  LM Studio  (port 1234)  — secondary LLM endpoint    │   │
   │   │  Same model loaded; used by Company Evaluator's      │   │
   │   │  router for the 2nd concurrent symbol.               │   │
   │   └─────────────────────────────────────────────────────┘   │
   └─────────────────────────────────────────────────────────────┘

            ┌──────────────────────────────────────────┐
            │   External APIs                          │
            │   • Polygon.io    (financials, prices)    │
            │   • Finnhub       (ratios, estimates,     │
            │                    insiders, peers)       │
            │   • FMP /stable   (bulk financials, DCF,  │
            │                    ratings, transcripts)  │
            │   • Yahoo Finance (fallback only)         │
            └──────────────────────────────────────────┘
```

### Machines

| Role | IP | Purpose | What runs |
| --- | --- | --- | --- |
| Evaluator | `192.168.1.143` | This service | FastAPI on 8100, LM Studio on 1234, crawler |
| BenTrade | `192.168.1.89` | Consumer + 2nd LLM | BenTrade backend, LM Studio on 1234 |
| NAS | `192.168.1.149` | Shared storage | SMB share hosting both SQLite DBs |

### Tech Stack

- **Runtime:** Python 3.11 in `.venv/` (CPython, Windows-hosted).
- **Web framework:** FastAPI ≥ 0.115 + uvicorn[standard] ≥ 0.30.
- **Persistence:** SQLite via `aiosqlite` 0.20 + SQLAlchemy 2.0 async ORM. Both DBs live on a NAS SMB share.
- **HTTP:** `httpx` ≥ 0.27 async throughout. The `openai` SDK is **not** used; LLM calls go directly to LM Studio's OpenAI-compatible `/v1/chat/completions` endpoint.
- **Data:** `pandas` 2.2 + `numpy` 2.1 for financial math.
- **Config:** `pydantic` 2.9 + `pydantic-settings` 2.5. `.env` loaded from project root.
- **Scheduling:** `apscheduler` 3.10 (market-hours aware crawler scheduler).
- **Fallback data:** `yfinance` (used only when Polygon/FMP both fail).
- **Process monitoring:** `psutil`.
- **Build/packaging:** PyInstaller spec at `CompanyEvaluatorLauncher.spec` produces a `build/` / `dist/` launcher executable (see `launcher.py`).

No cloud services, no Docker, no Kubernetes, no managed DB. Everything is local-network.

---

## 3. Project Structure

```
Company_Evaluator/
├── main.py                    (148)  FastAPI entry point, lifespan, 14 routers
├── launcher.py                (809)  PyInstaller-wrapped launcher (tray app/launcher)
├── config.py                  (137)  Pydantic Settings class; reads .env
├── CompanyEvaluatorLauncher.spec    PyInstaller build spec
├── requirements.txt
├── README.md
├── .env                              (gitignored) real secrets
├── .env.example                      committed template
│
├── analysis/                         LLM + valuation models + research tooling
│   ├── llm_client.py                legacy single-endpoint LLM client
│   ├── llm_router.py          (308) dual-endpoint LLM router (Tier 2)
│   ├── company_analyst.py     (110) builds LLM prompt + parses response
│   ├── prompts.py                   system prompt templates
│   ├── dcf_model.py           (509) discounted cash flow valuation
│   ├── eva_model.py           (679) economic value added model
│   ├── comps_model.py         (530) trading comparables
│   ├── entry_point.py         (947) entry-point analysis (largest file)
│   ├── epv_model.py           (446) earnings power value
│   ├── piotroski.py           (245) Piotroski F-score
│   ├── business_profile.py    (311) qualitative profile synthesis
│   ├── transcript_analyzer.py       earnings-call transcript analysis
│   ├── chart_service.py             chart data + indicator computation
│   ├── search_service.py            symbol search
│   └── research_prompt_template.py  "deep research" prompt generator
│
├── api/                              FastAPI routers (14 files, all prefixed /api)
│   ├── routes_companies.py    (437) /companies/ranked, /companies/{sym}, ...
│   ├── routes_pipeline.py           /pipeline/run, /pipeline/status, ...
│   ├── routes_admin.py        (358) /admin/*, /universe/*
│   ├── routes_status.py             /status/dashboard
│   ├── routes_entry_point.py        /entry-point/*
│   ├── routes_comps.py              /valuation/comps
│   ├── routes_dcf.py                /valuation/dcf
│   ├── routes_eva.py                /valuation/eva
│   ├── routes_analyses.py           /analyses/status
│   ├── routes_quote.py              /quote/{sym}
│   ├── routes_transcripts.py        /companies/{sym}/transcript-*
│   ├── routes_on_demand.py    (105) /on-demand/analyze, /on-demand/jobs/*
│   ├── routes_charts.py             /charts/{sym}
│   └── routes_search.py             /search/symbols
│
├── bulk/                             FMP bulk-endpoint cache layer
│   ├── bulk_cache.py          (182) SQLite cache wrapper (separate DB)
│   ├── bulk_fetcher.py        (226) downloads bulk endpoints from FMP
│   ├── bulk_parser.py               parses CSV/JSON bulk payloads
│   ├── bulk_refresh.py              orchestrates nightly refresh
│   ├── bulk_endpoints.py            endpoint registry
│   ├── cache_lookup.py        (230) symbol→bulk row lookup (indexed)
│   ├── cached_fmp_client.py   (148) transparent FMPClient wrapper
│   └── cycle_orchestrator.py  (310) combines refresh + crawler cycle
│
├── data/                             provider clients + routing
│   ├── polygon_client.py      (406) Polygon.io (financials, aggregates, indicators)
│   ├── finnhub_client.py      (201) Finnhub (ratios, estimates, insiders, peers)
│   ├── fmp_client.py          (580) FMP /stable (primary cross-validator + fallback)
│   ├── fmp_normalizer.py            FMP → unified schema
│   ├── company_data_service.py (600) orchestrates all providers per symbol
│   ├── data_source_router.py        per-call-site routing (polygon/fmp/shadow)
│   ├── universe.py                  hardcoded seed symbols
│   ├── universe_builder.py          composes universe from tiers
│   ├── universe_expansion.py  (401) discovers new symbols (screener-driven)
│   ├── smart_money_analyzer.py      insider/institutional trading summary
│   └── cache/                       on-disk ad-hoc caches
│
├── db/
│   ├── database.py            (289) SQLAlchemy models + init_db + get_session
│   └── company_eval.db              (legacy local; real DB is on NAS)
│
├── metrics/                          5-pillar scoring + helpers
│   ├── business_quality.py     (137) Pillar 1
│   ├── operational_health.py   (162) Pillar 2
│   ├── capital_allocation.py   (186) Pillar 3
│   ├── growth_quality.py       (140) Pillar 4
│   ├── valuation_expectations.py (138) Pillar 5
│   ├── composite.py            (129) weighted composite
│   ├── breakout.py             (427) technical breakout score
│   ├── cross_validator.py      (138) Polygon↔FMP reconciliation
│   ├── validation.py                 boundary-case guards
│   └── helpers.py              (93)  scale/clamp/weighted_avg
│
├── pipeline/
│   ├── crawler.py             (460) universe walker (batched concurrent)
│   ├── evaluator.py           (423) per-symbol pipeline
│   ├── on_demand.py           (780) background job queue for ad-hoc tickers
│   └── scheduler.py           (159) APScheduler (market-hours aware)
│
├── docs/
│   ├── APP_CONTEXT.md                this file
│   ├── FMP_INVESTIGATION_REPORT.md
│   └── FMP_BULK_INVESTIGATION_REPORT.md
│
├── logs/                             ad-hoc run reports (JSON)
├── scripts/                          one-off utilities
├── tests/                            pytest (minimal)
├── build/ dist/                      PyInstaller output (gitignored)
└── _*.py                             throwaway probes (gitignored, some committed)
```

**File-count summary:** ~70 Python modules outside `build/dist/.venv`. Largest file is `analysis/entry_point.py` at 947 lines.

---

## 4. Data Flow

### 4.1 Per-symbol evaluation pipeline (`pipeline/evaluator.py`)

For a single symbol (e.g. `AAPL`):

1. **Fetch raw data** via `data/company_data_service.py::CompanyDataService.get_company_data(symbol)`:
   - **Financials**: routed call (Polygon vs FMP vs shadow) → 12 quarterly + annual statements.
   - **Company details / profile**: Polygon + Finnhub + FMP merged by `_merge_profile`.
   - **Ratios (TTM)**: FMP `ratios_ttm` + Finnhub `basic_financials` (117-metric package).
   - **Estimates / targets**: Finnhub `eps_estimates` + `price_target`.
   - **Insiders**: Finnhub `insider_transactions` + FMP `insider_trading_statistics`.
   - **Peers**: Finnhub `company_peers` (fallback: FMP).
   - **Price history**: Polygon aggregates (routed; FMP fallback).
   - **Ownership**: Yahoo (on-demand only) via `_fetch_yahoo_ownership`.
   - Every call is wrapped in `_safe(...)` — individual failures never crash the pipeline; they degrade `data_quality` to `partial` / `poor`.

2. **Score 5 pillars** via `metrics/composite.py::compute_composite_score(data)`:
   - Pillar 1 Business Quality (30%)
   - Pillar 2 Operational & Financial Health (15%)
   - Pillar 3 Capital Allocation (20%)
   - Pillar 4 Growth Quality (20%)
   - Pillar 5 Valuation & Expectations (15%)
   - Each pillar returns `{score: 0-100, metrics: {...}}`. Composite = weighted average.
   - `breakout.py` adds a separate technical breakout score stored in `breakout_score` / `breakout_components`.

3. **Cross-validate** via `metrics/cross_validator.py` — compares Polygon vs FMP on key figures (revenue, net income, operating cash flow) to surface data-quality flags.

4. **LLM analysis** via `analysis/company_analyst.py` → `analysis/llm_router.py`:
   - Receives computed scores + metrics (not raw statements).
   - Emits JSON: `summary, recommendation, conviction (1-10), thesis, risks[], catalysts[]`.
   - Routed to one of 2 LM Studio endpoints (see §9).

5. **Persist** via `db/database.py` — UPSERT into `company_evaluations`, append snapshot to `evaluation_history`.

### 4.2 Crawler (`pipeline/crawler.py`)

Walks the active universe (priority-ordered: tier_1_large_mid → tier_2_breakout_zone → tier_3_small_cap → tier_4_ipo_discovery). Runs **2 symbols concurrently** (`llm_concurrent_symbols=2`) — this is the key Tier 2 optimization. Pauses `pause_between_symbols_sec` (2.0s) between batches. Persists progress state to a JSON file so restarts resume mid-cycle. Re-ranks every 50 symbols using a SQL window function (`ROW_NUMBER() OVER (ORDER BY composite_score DESC)`).

**Scheduler** (`pipeline/scheduler.py`) starts the crawler on service startup if `crawler_enabled=true`, and can be configured for market-hours only, continuous, or disabled. Current production: `CRAWLER_ENABLED=false` in `.env` (crawler started explicitly via API).

### 4.3 On-demand evaluation (`pipeline/on_demand.py`)

`POST /api/on-demand/analyze` → creates a job row in `on_demand_jobs`, returns `job_id`. A background task walks a multi-step pipeline (fetch → score → LLM → valuations → chart → transcript) and updates `current_step` / `percent` live. Results are streamed back via `GET /api/on-demand/jobs/{job_id}` and `GET /api/on-demand/jobs/{job_id}/result`.

### 4.4 Data provider priority

| Need | Primary | Cross-validator | Fallback |
| --- | --- | --- | --- |
| Financial statements | Polygon (or FMP via routing) | FMP | Yahoo |
| Pre-computed ratios | FMP `ratios_ttm` | Finnhub `basic_financials` | — |
| Company profile | Polygon + Finnhub + FMP merged | — | — |
| Price history | Polygon aggregates | FMP | Yahoo |
| EPS estimates | Finnhub | FMP analyst estimates | — |
| Insider trading | Finnhub | FMP | — |
| Peers | Finnhub | FMP `stock_peers` | — |
| Transcripts | FMP `earnings_transcript` | — | — |
| Symbol search | FMP `search_symbol` | — | — |

**Rate limits:** Polygon 100/s, Finnhub 30/s, FMP 300/min, Yahoo 1/s. Enforced by per-client token buckets (e.g. `_TokenBucketRateLimiter` in `fmp_client.py`).

### 4.5 Data source routing (`data/data_source_router.py`)

`.env` variable `DATA_SOURCE_OVERRIDES` is a JSON map of call-site → provider. Current production:

```
DATA_SOURCE_OVERRIDES='{"default":"shadow"}'
```

`shadow` mode runs both Polygon and FMP and records discrepancies without user-visible failure. `polygon` or `fmp` force a single provider. Call sites: `financials`, `price_history`, `company_details`. Resolved by `Settings.get_data_source(call_site_key)` (exact → default → polygon).

### 4.6 FMP bulk cache (`bulk/`)

FMP offers `/stable/*-bulk` endpoints that return the **entire US market** in a single CSV response (e.g. all income statements for fiscal year 2024). The bulk layer downloads these nightly and stores them in `company_eval_bulk.db`. `CachedFMPClient` transparently wraps `FMPClient` — per-symbol calls are satisfied from the bulk cache when possible (O(1) indexed lookup), falling back to the live API only for cache misses or stale data.

**Current bulk cache contents (as of snapshot):**

- Size: **32.9 MB** on disk.
- 47 tables, ~46 endpoint snapshots tracked in `_bulk_refresh_metadata`.
- Full financial history y2021–y2025 for all 4 statements + growth versions (~2,800 rows/year/table).
- TTM ratios and key metrics: 2,907 symbols each.
- DCF prebuilt values: 2,065 symbols.
- Earnings surprises: 29,819 total rows across 3 years.
- EOD snapshot: 2,885 symbols.
- Analyst ratings / upgrades / price targets: ~2,700 symbols each.
- `bulk_profile_p0..p3`: profile slices split into pages (p0=2,878 rows, p1-p3 handful).

Staleness threshold: 24 hours (`bulk_cache_stale_hours=24`). Auto-refresh on startup when `bulk_auto_refresh=true`.

---

## 5. Database Schema

### 5.1 Main DB — `\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db`

Size: **314.2 MB**. SQLAlchemy models in `db/database.py`. SQLite with WAL mode. URL form in `.env`:

```
DATABASE_URL=sqlite:////192.168.1.149/CompanyEvaluatorData/company_evaluator/db/company_eval.db
```

#### `company_evaluations` (1,962 rows) — primary evaluation snapshot (latest per symbol)

| Column | Type | Notes |
| --- | --- | --- |
| `symbol` | VARCHAR(10) PK | |
| `company_name` | VARCHAR(200) | |
| `sector` | VARCHAR(100) | |
| `industry` | VARCHAR(200) | |
| `market_cap` | FLOAT | USD |
| `pillar_1_business_quality` | FLOAT | 0–100 |
| `pillar_2_operational_health` | FLOAT | 0–100 |
| `pillar_3_capital_allocation` | FLOAT | 0–100 |
| `pillar_4_growth_quality` | FLOAT | 0–100 |
| `pillar_5_valuation` | FLOAT | 0–100 |
| `composite_score` | FLOAT | weighted 0–100 |
| `rank` | INTEGER | dense rank by composite; updated every 50 syms |
| `pillar_{1..5}_detail` | JSON | `{metrics: {...}, scores: {...}}` |
| `llm_summary` | TEXT | 1–2 sentence verdict |
| `llm_recommendation` | VARCHAR(20) | BUY / HOLD / AVOID etc. |
| `llm_conviction` | INTEGER | 1–10 |
| `llm_thesis` | TEXT | |
| `llm_risks` | JSON | list[str] |
| `llm_catalysts` | JSON | list[str] |
| `raw_financials` | JSON | selected raw line items for debugging |
| `evaluated_at` | DATETIME | |
| `data_freshness` | VARCHAR(20) | `good` / `partial` / `poor` |
| `evaluation_version` | VARCHAR(20) | schema version of the row |
| `errors` | JSON | per-step error list |
| `breakout_score` | REAL | technical breakout (separate from pillars) |
| `breakout_components` | TEXT | JSON-as-text for breakout sub-scores |

#### `evaluation_history` (6,397 rows) — per-run snapshots (append-only)

`id PK, symbol, composite_score, rank, llm_recommendation, evaluated_at, snapshot JSON`.

#### `universe_symbols` (2,994 rows total; **2,942 active**)

`symbol PK, company_name, source, added_at, active, priority, market_cap, market_cap_tier, sector, industry, exchange, last_price, avg_volume, last_screened_at, delisted_at, notes, tier, refresh_days, discovery_source, discovery_metadata`.

**Tier distribution (active):**

| Tier | Count |
| --- | --- |
| (null — legacy unclassified) | 772 |
| tier_1_large_mid | 421 |
| tier_2_breakout_zone | 1,378 |
| tier_3_small_cap | 354 |
| tier_4_ipo_discovery | 17 |

**Market cap tier distribution (active):** mega=27, large=594, mid=936, small=1,154, micro=116, plus legacy `large_cap`/`small_cap` buckets and `unknown`=44.

#### Valuation tables (keyed on `symbol`)

| Table | Rows | Notable columns |
| --- | --- | --- |
| `comps_analyses` | 51 | `fair_value_composite, upside_pct, peer_count, verdict_status, confidence, current_price_at_analysis` |
| `dcf_analyses` | 47 | `intrinsic_value, upside_pct, verdict, wacc, confidence` |
| `entry_point_analyses` | 51 | `recommendation, conviction, composite_score, suggested_entry, suggested_stop, risk_reward` |
| `eva_analyses` | 58 | `roic, wacc, value_spread, eva_annual, grade, score, confidence` |

Each of these also stores the full analysis blob as JSON text in `analysis_data`.

#### `on_demand_jobs` (33 rows)

`job_id PK, symbol, status, created_at, started_at, completed_at, current_step, current_step_index, total_steps, percent, completed_steps, error, result_json`. Populated by `pipeline/on_demand.py`.

#### `crawler_cycle_metrics` (0 rows — feature present, not yet populated)

Designed to log each crawl cycle: duration, symbols processed, cache hit rate, refresh attempt/success.

### 5.2 Bulk cache DB — `company_eval_bulk.db`

Separate SQLite file on the same NAS share. 32.9 MB, 47 tables. See §4.6 for contents. Owned by `bulk/bulk_cache.py`; never written by the main pipeline or ORM models.

---

## 6. Evaluation Model

### 6.1 The five pillars (weights in `metrics/composite.py::PILLAR_WEIGHTS`)

| # | Pillar | Weight | What it measures |
| --- | --- | --- | --- |
| 1 | Business Quality | 30% | ROIC, gross/operating/FCF margins, revenue stability, moat indicators |
| 2 | Operational & Financial Health | 15% | SG&A efficiency, debt/EBITDA, interest coverage, Altman Z, cash conversion |
| 3 | Capital Allocation | 20% | ROIC–WACC spread, buyback effectiveness, dividend sustainability, insider ownership |
| 4 | Growth Quality | 20% | Revenue CAGR, growth consistency, FCF growth, margin trajectory |
| 5 | Valuation & Expectations | 15% | EV/EBITDA vs history+peers, implied growth, earnings quality, accruals |

Pillars 1–4 score the company. Pillar 5 scores whether the market is mispricing the company. Each metric inside a pillar is normalized to 0–100 before being weighted-averaged into the pillar score.

### 6.2 Composite

```
composite = 0.30·P1 + 0.15·P2 + 0.20·P3 + 0.20·P4 + 0.15·P5
```

### 6.3 Rankings

Updated every 50 processed symbols inside the crawler via a single SQL `UPDATE ... FROM (SELECT symbol, ROW_NUMBER() OVER (ORDER BY composite_score DESC) rnk FROM company_evaluations WHERE composite_score IS NOT NULL)`. This replaced an earlier Python-side rank-all-in-memory approach — the window function is faster and atomic.

### 6.4 Standalone valuation models (separate pipelines)

- **DCF** (`analysis/dcf_model.py`, 509 lines) — 5-year projected FCF + terminal value; WACC computed from market data.
- **EVA** (`analysis/eva_model.py`, 679 lines) — economic profit = (ROIC − WACC) × invested capital; annualized value spread.
- **Comps** (`analysis/comps_model.py`, 530 lines) — peer multiples (EV/EBITDA, P/E, EV/Sales) with outlier filtering.
- **Entry Point** (`analysis/entry_point.py`, 947 lines) — combines breakout score, support/resistance, composite score, and LLM judgment to produce a trade-ready recommendation.
- **EPV** (`analysis/epv_model.py`, 446 lines) — Bruce Greenwald Earnings Power Value.
- **Piotroski F-score** (`analysis/piotroski.py`, 245 lines) — 9-point accounting-quality checklist.

### 6.5 LLM analysis

Runs **after** quantitative scoring. The LLM receives:
- All 5 pillar scores + their key metrics.
- Company profile (sector, industry, market cap).
- Select raw financials (for sanity-checking).

It does **not** receive the full statements. Output is strict JSON parsed by `analysis/company_analyst.py`; malformed responses are retried once then recorded as an error in the `errors` column.

---

## 7. API Endpoints

All routes are mounted under `/api` (see `main.py`). One extra top-level route: `GET /health`.

### Companies — `api/routes_companies.py`

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/companies/ranked` | Paginated ranked list |
| GET | `/api/companies/sectors` | Sector breakdown |
| GET | `/api/companies/data-quality-issues` | Rows with `data_freshness != good` |
| GET | `/api/companies/{symbol}/raw` | Raw stored financials JSON |
| GET | `/api/companies/{symbol}` | Full evaluation detail |
| POST | `/api/companies/{symbol}/evaluate` | Re-evaluate this symbol now |

### Pipeline — `api/routes_pipeline.py`

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/pipeline/status` | Crawler live status |
| POST | `/api/pipeline/run` | Start crawler cycle (full universe or subset) |
| POST | `/api/pipeline/stop` | Stop crawler |
| POST | `/api/pipeline/pause` | Pause crawler |
| POST | `/api/pipeline/resume` | Resume crawler |
| POST | `/api/pipeline/evaluate/{symbol}` | Evaluate single symbol synchronously |
| POST | `/api/universe/rerank` | Force global re-rank now |

### Admin — `api/routes_admin.py`

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/admin/config` | Current settings (redacted) |
| GET | `/api/admin/universe` | List universe |
| POST | `/api/admin/backfill/{symbol}` | Force data backfill |
| POST | `/api/admin/universe/add` | Add symbol (admin) |
| POST | `/api/admin/universe/remove` | Remove symbol |
| POST | `/api/universe/add` | Add symbol (public) |
| POST | `/api/universe/refresh` | Refresh one symbol's universe row |
| POST | `/api/universe/refresh-all` | Refresh whole universe metadata |
| GET | `/api/universe/refresh-status` | Refresh progress |
| GET | `/api/universe/stats` | Tier/cap distributions |
| GET | `/api/admin/fmp-status` | FMP client + bulk cache health |
| POST | `/api/admin/expand-universe` | Run screener-driven universe expansion |
| GET | `/api/admin/expand-universe/status` | Expansion job status |
| GET | `/api/admin/universe/stats` | (duplicate surface of `/universe/stats`) |

### Valuation — `routes_dcf.py`, `routes_eva.py`, `routes_comps.py`, `routes_entry_point.py`

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/api/valuation/dcf` | Run DCF for a symbol (body) |
| GET | `/api/valuation/dcf/{symbol}` | Latest stored DCF |
| POST | `/api/valuation/eva` | Run EVA |
| GET | `/api/valuation/eva/{symbol}` | Latest EVA |
| POST | `/api/valuation/comps` | Run comps |
| GET | `/api/valuation/comps/{symbol}` | Latest comps |
| POST | `/api/entry-point/analyze` | Run entry-point analysis |
| GET | `/api/entry-point/analysis/{symbol}` | Latest entry-point |

### On-demand jobs — `api/routes_on_demand.py`

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/api/on-demand/analyze` | Create job for any ticker (returns `job_id`) |
| GET | `/api/on-demand/jobs/{job_id}` | Job status + progress |
| GET | `/api/on-demand/jobs/{job_id}/result` | Final result JSON |
| DELETE | `/api/on-demand/jobs/{job_id}` | Cancel/delete job |
| GET | `/api/on-demand/research-prompt/{symbol}` | Return the deep-research LLM prompt for external use |

### Misc

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/status/dashboard` | Service-wide health dashboard |
| GET | `/api/analyses/status` | Coverage of valuation tables |
| GET | `/api/quote/{symbol}` | Live quote (routed) |
| GET | `/api/charts/{symbol}` | Chart payload (prices + indicators) |
| GET | `/api/search/symbols?q=...` | Symbol search |
| GET | `/api/companies/{symbol}/transcript-analysis` | Latest transcript analysis |
| GET | `/api/companies/{symbol}/transcript-list` | Available transcripts |
| GET | `/health` | Liveness probe |

BenTrade's consumer layer (`Market_Analysis_Backend/BenTrade/backend/app/api/routes_company_evaluator.py`) wraps a subset of these and exposes them on its own API.

---

## 8. Configuration

All settings live in `config.py` (`Settings(BaseSettings)`, `env_file=".env"`). Real values are in `.env` (gitignored); `.env.example` is the committed template.

### 8.1 Production `.env` (sanitized)

```
HOST=0.0.0.0
PORT=8100
DEBUG=false

DATABASE_URL=sqlite:////192.168.1.149/CompanyEvaluatorData/company_evaluator/db/company_eval.db

LLM_ENDPOINT=http://localhost:1234/v1/chat/completions
LLM_TIMEOUT=120
LLM_TEMPERATURE=0.0

POLYGON_API_KEY=***REDACTED***
FINNHUB_API_KEY=***REDACTED***
POLYGON_RATE_LIMIT=100.0
FINNHUB_RATE_LIMIT=30.0
YAHOO_RATE_LIMIT=1.0
YAHOO_ENABLED=true
FMP_API_KEY=***REDACTED***
FMP_ENABLED=true

UNIVERSE=sp500_top100
CRAWLER_ENABLED=false
CRAWLER_SCHEDULE=02:00
EVALUATION_BATCH_SIZE=10

DATA_SOURCE_OVERRIDES='{"default":"shadow"}'
```

### 8.2 Key settings (full `Settings` class captured in `config.py`)

**Server:** `host="0.0.0.0"`, `port=8100`, `debug=True` (overridden to `false` in prod).

**Database:** `database_url`; `sqlite_url_to_path()` resolves UNC paths via the `sqlite:////` (4-slash) prefix convention.

**LLM base:** `llm_endpoint`, `llm_model=""` (auto-detect via `/v1/models`), `llm_timeout=120`, `llm_temperature=0.0`.

**LLM routing:** `llm_routing_enabled=True`, `llm_local_url="http://localhost:1234"`, `llm_model_machine_url="http://192.168.1.89:1234"`, `llm_concurrent_symbols=2`.

**Data sources:** `polygon_rate_limit=100.0`, `finnhub_rate_limit=30.0`, `yahoo_rate_limit=1.0`, `yahoo_enabled=True`.

**FMP:** `fmp_enabled=False` (code default; prod overrides to `true`), `fmp_rate_limit_per_min=300`, `fmp_base_url="https://financialmodelingprep.com/stable"`, `enable_bulk_cache=True`, `bulk_cache_path=""` (auto-derived), `bulk_cache_stale_hours=24`, `bulk_auto_refresh=True`.

**Routing:** `data_source_overrides="{}"` as JSON string, parsed by `get_data_source(call_site_key)`; resolution order is exact key → `default` → hardcoded `polygon`.

**Pipeline:** `universe="sp500_top100"`, `crawler_enabled=False`, `crawler_schedule="02:00"`, `evaluation_batch_size=10`.

**Refresh:** `refresh_period_days=7`, `pause_between_symbols_sec=2.0`, `rankings_update_interval=50`.

Logs go to `%LOCALAPPDATA%\CompanyEvaluator\logs\company_evaluator.log` with 50 MB rotation × 5 files — not the OneDrive project folder, because OneDrive file locking breaks `RotatingFileHandler`.

---

## 9. LLM Routing

### 9.1 Architecture

Two LM Studio instances on two machines, both with the same model (`qwen3-14b-claude-4.5-opus-high-reasoning-distill`) loaded:

- **Local endpoint:** `http://localhost:1234` (the evaluator machine, 192.168.1.143).
- **Remote endpoint:** `http://192.168.1.89:1234` (BenTrade machine).

Module: `analysis/llm_router.py` (308 lines). Key types:

- `LLMEndpoint` dataclass — `name, base_url, model, latency_ms_total, call_count, error_count`; helpers `avg_latency`, `error_rate`, `completions_url`, `models_url`.
- `LLMRouter` class — `resolve_model`, `call_llm`, `_call_endpoint`, `_select_endpoint`, `_bump_error`, `health_check`, `get_stats`. Singleton via `get_router()`.

### 9.2 Selection policy

`_select_endpoint(exclude)` picks the endpoint with the **lowest current in-flight count**. Ties broken by avg latency. Endpoints with `error_rate` above threshold are excluded until a health-check passes. Retries fall through to the other endpoint.

### 9.3 Concurrency

`llm_concurrent_symbols=2` — the crawler dispatches 2 symbols concurrently. With 2 endpoints this maps cleanly: one LLM call per symbol typically lands on a different box.

### 9.4 Verified behavior

- Router isolation test (pre-tag `post-tier2-llm`): 2.99× speedup concurrent vs serial.
- 10-symbol perf audit: wall=508.2s, effective **50.8s per symbol**, 12 LLM calls split 6/6 across endpoints, 0 errors post-warmup.

### 9.5 IP fix (commit `b225fcb`)

Originally `llm_model_machine_url` was set to `192.168.1.143` — the evaluator machine itself — causing both endpoints to hit the same LM Studio. Corrected to `192.168.1.89` (BenTrade machine) in both `config.py:64` and `analysis/llm_router.py:90`. Router init now logs `endpoints=2 model_machine(http://192.168.1.89:1234), local(http://localhost:1234)`, both healthy.

---

## 10. Performance Profile

### 10.1 History (measured averages, full pipeline per symbol)

| Stage | Tag | Per-symbol avg | Notes |
| --- | --- | --- | --- |
| Pre-optimization | `pre-tier1-optimizations` / `post-phase-2c` (`be909b8`) | ~82 s | Single LLM endpoint, serial crawler |
| Tier 1 complete | `post-tier1-optimizations` (`fd5bc62`) | **60.6 s** (−26%) | Provider concurrency, bulk cache hot, rank SQL window |
| Tier 2 complete | `post-tier2-llm` (`7cc589a`) | **50.8 s** (effective, concurrency=2) | Dual-endpoint LLM routing; wall 508.2s for 10 symbols |

### 10.2 Where time goes (approximate, Tier 2)

- LLM call: ~30 s/symbol (single pass, thinking-model output).
- Data fetching (Polygon + FMP + Finnhub + bulk lookups): ~10–15 s/symbol when bulk cache is warm; substantially longer on cold cache or cache refresh.
- Scoring + DB write: <2 s/symbol.

### 10.3 Crawl throughput

At 50.8 s/symbol effective and a ~2,900-active universe: a full cycle is roughly **40–45 hours**. Tiers are crawled in priority order so tier_1_large_mid (421 symbols, ~6 hours) completes first.

---

## 11. BenTrade Integration

BenTrade lives in a separate repo at `C:\Users\benja\OneDrive\Desktop\GitHub_Projects\Market_Analysis_Backend\BenTrade\`. Relevant file: `backend/app/api/routes_company_evaluator.py` — a thin proxy layer that exposes Company Evaluator endpoints under its own URL space. Routes mirrored: `/connection`, `/ranked`, `/company/{symbol}`, `/status`, `/evaluate/{symbol}`, `/entry-point/analyze`, `/crawl`, `/entry-point/analysis/{symbol}`, `/quote/{symbol}`, `/valuation/{dcf,eva,comps}/{symbol}` (GET + POST), `/analyses/status`, `/universe/add`, `/companies/{symbol}/raw`, `/admin/fmp-status`, `/on-demand/analyze`, `/on-demand/jobs/{job_id}` (+`/result`, DELETE), `/charts/{symbol}`, `/search/symbols`.

BenTrade calls `http://192.168.1.143:8100/api/...`. CORS is wide open (`allow_origins=["*"]`) in this service — meant to be tightened in production but not yet.

Beyond the proxy, BenTrade also **hosts the second LM Studio endpoint** (port 1234 on 192.168.1.89) that the LLM router uses. This dual role — data consumer + LLM provider — is load-bearing for Tier 2 performance.

---

## 12. Known Issues and Pending Work

### 12.1 Data quality

- **772 active universe rows have `tier=NULL`** — legacy from pre-tier classification. `universe_expansion.py` is the intended tagger; a one-shot backfill is pending.
- **Legacy `market_cap_tier` values** `large_cap` / `small_cap` (old naming) coexist with `mega`/`large`/`mid`/`small`/`micro`. Need migration.
- **44 active rows with `market_cap_tier=unknown`** — need cap-size refresh.
- **Shadow mode is running in production** (`DATA_SOURCE_OVERRIDES='{"default":"shadow"}'`). Every call hits both Polygon and FMP. This is by design for discrepancy detection but increases rate-limit pressure on both providers. Discrepancies are currently logged, not aggregated/reported.

### 12.2 Pipeline

- **`crawler_cycle_metrics` table is empty** — the cycle-orchestrator wrote instrumentation but the write path into this table is unfinished (`bulk/cycle_orchestrator.py`).
- **Crawler is disabled in production** (`CRAWLER_ENABLED=false`). Runs are kicked off manually via `/api/pipeline/run`. Intent was to move to continuous market-hours crawling once perf hit target.
- **Valuation coverage is thin**: only 47–58 symbols across DCF/EVA/Comps/EntryPoint out of 1,962 evaluated. These models run on-demand and top-ranked-only; no backfill pass has run since the models stabilized.

### 12.3 LLM

- **Error handling on router**: `_bump_error` disables an endpoint temporarily, but there's no explicit cooldown timer — recovery relies on the next health check. If both endpoints trip simultaneously, `call_llm` fails hard.
- **Model name is auto-resolved** from `/v1/models` on first call. If the two endpoints load different model names, the router currently warns but does not fail. Ideally it would enforce model-name equality.

### 12.4 Infrastructure

- **NAS SMB as primary DB**: works, but `aiosqlite` over SMB has higher tail latency than local disk. WAL mode helps. No automated backup.
- **No monitoring/alerting** — logs only, rotated on the evaluator machine. No Prometheus/Grafana/Sentry wired up.
- **Launcher executable** (`launcher.py` 809 lines, `CompanyEvaluatorLauncher.spec`) is built but not part of daily operation; service is typically run directly from `.venv` during development.

### 12.5 Tests

- `tests/` directory exists but coverage is minimal. Development relies on `_test_*.py` and `_*.py` probe scripts at the project root. Many of these are committed but not runnable without manual setup.

---

## 13. Key Decisions and Conventions

### 13.1 Design decisions

- **Compute pillar metrics from raw statements, not pre-packaged ratios.** Finnhub's 117-metric bundle is used for cross-validation and non-core fields (beta, dividend yield) only.
- **Polygon + FMP as dual primaries**; Yahoo is fallback only because of its aggressive rate-limiting.
- **Shadow mode** (`DATA_SOURCE_OVERRIDES='{"default":"shadow"}'`) treats data-source selection as a runtime-routable concern, enabling side-by-side provider comparison without code changes.
- **FMP bulk cache** was introduced because per-symbol FMP calls at 300 req/min don't scale to a 2,900-symbol universe; bulk endpoints collapse thousands of calls into a handful of CSV downloads.
- **SQLite on SMB** rather than Postgres: single-writer workload, simple ops, no external DB to admin. The NAS colocates main DB + bulk cache.
- **Two-machine LLM routing** instead of buying a bigger GPU: idle BenTrade LLM gets used, roughly halving wall-clock on LLM-bound pipelines.
- **Crawler is opt-in** (`crawler_enabled=false` default) — prevents accidental universe-wide runs on service start during development.
- **Ranking is atomic SQL**, not Python — eliminates stale-rank races between the crawler and API reads.
- **LLM input is structured scores, not raw financials** — keeps prompts short, deterministic, and avoids hallucinated line items.

### 13.2 Code conventions

- **`_safe` wrapper** around every provider call in `company_data_service.py`. Partial-data success is a first-class outcome — `data_freshness` records the degree.
- **`async def` everywhere** in I/O paths. Sync code only in pure-compute metrics.
- **Per-client token-bucket rate limiters**. See `_TokenBucketRateLimiter` in `fmp_client.py`; analogous limits are enforced in each client.
- **Singletons** for shared clients (`get_router()`, `get_crawler()`, data clients). Lifespan-managed where async cleanup matters.
- **Routers prefixed `/api`** on registration in `main.py`. No versioning yet.
- **Throwaway probes** prefixed `_` (e.g. `_test_piotroski.py`, `_inspect_db.py`). These are runnable snippets, not library code.
- **Logs** go to `%LOCALAPPDATA%\CompanyEvaluator\logs\` — never OneDrive.

### 13.3 Git conventions

- **Tags as phase markers.** Before and after every material change: `pre-<name>` / `post-<name>`. Sequence: `pre/post-phase-2a`, `pre/post-phase-2b`, `pre/post-phase-2c`, `pre/post-tier1-optimizations`, `pre/post-tier2-llm`. This makes rollback and perf-delta measurement trivial.
- **Small, scoped commits** with a summary line that includes the perf delta when relevant (e.g. `Tier 1 performance optimizations: 82s -> 60.6s per symbol (-26%)`).
- **Main branch only**; no feature branching.

### 13.4 Copilot / AI-collaboration conventions (project-specific)

Prompts follow this structure:

- **Scope header** — exactly what's in/out of scope.
- **STOP gates** — checkpoints where the AI must wait for the user to verify before proceeding.
- **Do NOT sections** — explicit negative constraints (e.g. "do NOT refactor unrelated code", "do NOT create new files unless necessary").
- **Git tagging cadence** — always `git tag pre-<name>` before starting a phase and `git tag post-<name>` at the end.
- **Secrets redaction** — API keys displayed as `***REDACTED***` in any generated artifact (including this document).
- **No time estimates** — the AI does not speculate on duration.

See `.github/copilot-instructions.md` for the full baseline instruction set.

---

## 14. Quick Reference

### 14.1 Paths

| Thing | Path |
| --- | --- |
| Project root | `C:\Users\benja\OneDrive\Desktop\GitHub_Projects\Company_Evaluator\` |
| Venv | `.venv\Scripts\python.exe` |
| Main DB | `\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db` |
| Bulk cache DB | `\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval_bulk.db` |
| Logs | `%LOCALAPPDATA%\CompanyEvaluator\logs\company_evaluator.log` |
| Config | `config.py` + `.env` |
| BenTrade repo | `C:\Users\benja\OneDrive\Desktop\GitHub_Projects\Market_Analysis_Backend\BenTrade\` |

### 14.2 Ports and addresses

| Service | Address |
| --- | --- |
| Company Evaluator API | `http://192.168.1.143:8100` |
| LM Studio (evaluator) | `http://localhost:1234` / `http://192.168.1.143:1234` |
| LM Studio (BenTrade) | `http://192.168.1.89:1234` |
| BenTrade API | `http://192.168.1.89:8000` (approximate; see BenTrade repo) |

### 14.3 Common commands

Run the service (dev):

```powershell
.\.venv\Scripts\python.exe main.py
```

Run a single-symbol evaluation (CLI, bypassing API):

```powershell
.\.venv\Scripts\python.exe -c "import asyncio; from pipeline.evaluator import evaluate_symbol; print(asyncio.run(evaluate_symbol('AAPL')))"
```

Trigger evaluation via API:

```powershell
curl -X POST http://localhost:8100/api/pipeline/evaluate/AAPL
```

Start a crawler cycle:

```powershell
curl -X POST http://localhost:8100/api/pipeline/run -H "Content-Type: application/json" -d "{\"symbols\": null}"
```

Inspect main DB (read-only):

```powershell
.\.venv\Scripts\python.exe _inspect_db.py
```

Git phase tag:

```powershell
git tag pre-<name>
# ...work...
git tag post-<name>
```

### 14.4 Key model/schema identifiers

- LLM model name: `qwen3-14b-claude-4.5-opus-high-reasoning-distill`
- Universe default: `sp500_top100`
- Active universe size: **2,942 symbols** across 4 tiers
- Evaluated symbols: **1,962** with composite score
- Evaluation history snapshots: **6,397**
- Latest git commit: `b225fcb` — "Fix LLM router: point remote endpoint to BenTrade machine (192.168.1.89)"
- Latest tag: `post-tier2-llm` (`7cc589a`)

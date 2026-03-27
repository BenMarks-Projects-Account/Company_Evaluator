Company Evaluator — Copilot Instructions
Project Overview
This is the BenTrade Company Evaluator — a standalone FastAPI microservice that evaluates public companies as medium-term investment candidates using a 5-pillar institutional-grade framework. It runs independently from BenTrade on Machine 2 (the model machine), crawls companies overnight, and exposes a REST API for ranked results.
Architecture

Runtime: Python 3.11+, FastAPI, SQLite (async via aiosqlite + SQLAlchemy)
Port: 8100 (separate from BenTrade on 8000)
LLM: LM Studio on localhost:1234 (same machine)
Data Sources: Polygon.io (primary financials + price), Finnhub (primary ratios + estimates), Yahoo Finance (fallback ONLY)
Database: SQLite at db/company_eval.db — stores evaluations, history, raw data
Universe: S&P 500 top 100 companies (expandable)

Project Structure
company-eval/
├── main.py                          # FastAPI entry point, port 8100
├── config.py                        # Pydantic settings from .env
├── db/
│   ├── database.py                  # SQLAlchemy models + init_db()
│   └── company_eval.db              # SQLite (auto-created)
├── data/
│   ├── polygon_client.py            # Financial statements from SEC XBRL
│   ├── finnhub_client.py            # 117 ratios, estimates, insiders, peers
│   ├── yahoo_client.py              # FALLBACK ONLY — do not use as primary
│   ├── company_data_service.py      # Orchestrates Polygon + Finnhub + Yahoo fallback
│   └── universe.py                  # Company universe definitions
├── metrics/
│   ├── business_quality.py          # Pillar 1: ROIC, margins, FCF, moat
│   ├── operational_health.py        # Pillar 2: efficiency, debt, Altman Z
│   ├── capital_allocation.py        # Pillar 3: ROIC-WACC, buybacks, insiders
│   ├── growth_quality.py            # Pillar 4: organic CAGR, consistency
│   ├── valuation_expectations.py    # Pillar 5: EV/EBITDA vs peers, accruals
│   └── composite.py                 # Weighted composite scoring
├── analysis/
│   ├── llm_client.py                # LM Studio HTTP client
│   ├── company_analyst.py           # LLM analysis per company
│   └── prompts.py                   # System prompts for analysis
├── pipeline/
│   ├── evaluator.py                 # Main pipeline: data → metrics → score → LLM
│   ├── crawler.py                   # Overnight batch processor
│   └── scheduler.py                 # APScheduler for nightly runs
└── api/
    ├── routes_companies.py          # GET /companies/ranked, /companies/{symbol}
    ├── routes_pipeline.py           # POST /pipeline/run, /pipeline/evaluate/{symbol}
    └── routes_admin.py              # Config, universe management
5-Pillar Evaluation Framework
PillarWeightWhat It Measures1. Business Quality30%ROIC, margins, FCF yield, revenue stability, moat indicators2. Operational & Financial Health15%SG&A efficiency, debt/EBITDA, interest coverage, Altman Z, cash conversion3. Capital Allocation20%ROIC-WACC spread, buyback effectiveness, dividend sustainability, insider ownership4. Growth Quality20%Revenue CAGR, growth consistency, FCF growth, margin trajectory5. Valuation & Expectations15%EV/EBITDA vs history+peers, implied growth, earnings quality, accruals
Pillars 1-4 judge the company fundamentals. Pillar 5 judges whether the market is over/under-pricing those fundamentals.
Data Source Priority
Data NeedPrimaryFallbackFinancial Statements (IS, BS, CF)Polygon Financials APIYahoo FinancePre-computed Ratios (117 metrics)Finnhub basic_financials—Company ProfileFinnhub company_profile2Polygon ticker detailsEPS EstimatesFinnhub eps_estimates—Price TargetsFinnhub price_target—Insider TransactionsFinnhub insider_transactions—Price HistoryPolygon aggregates—Peer CompaniesFinnhub company_peers—
Yahoo Finance is FALLBACK ONLY. Do NOT use Yahoo as a primary data source. It rate-limits aggressively and is unreliable. Only call Yahoo when Polygon fails for a specific company's financials.
Non-Negotiable Rules

Yahoo is fallback only — never add Yahoo as a primary source. Polygon and Finnhub handle everything.
Rate limiting on ALL data sources — Polygon (5 req/s), Finnhub (30 req/s), Yahoo (1 req/s). Never bypass rate limits.
Individual fetch failures must not crash the pipeline — if one data source fails for a company, continue with partial data and note it in data_quality.
All financial math must use raw numbers from statements — do not trust pre-computed ratios for critical metrics like ROIC. Compute from revenue, operating income, total assets, etc.
Finnhub's 117 ratios are useful for SUPPLEMENTARY metrics and cross-validation — use them for things like beta, dividend yield, PE ratios, but compute core pillar metrics from Polygon's financial statements.
SQLite is the database — do not add PostgreSQL, MySQL, or any external DB dependency. SQLite is sufficient for this workload.
LLM calls go to localhost:1234 (LM Studio on the same machine). Do not add cloud LLM providers.
This is a standalone project — do not import from BenTrade. Build your own clients, models, and utilities.
All monetary values are in USD — do not add currency conversion.
Pillar scores are 0-100 — each metric within a pillar normalizes to 0-100, then pillar score is weighted average of its metrics.
Composite score = weighted average of 5 pillar scores using the weights in the framework table.
LLM analysis runs AFTER quantitative scoring — the LLM receives the computed scores and metrics as input, not raw financial data.

API Endpoints
EndpointMethodDescription/healthGETService health check/api/companies/rankedGETRanked list of all evaluated companies/api/companies/{symbol}GETFull evaluation detail for one company/api/pipeline/evaluate/{symbol}POSTEvaluate a single company on demand/api/pipeline/runPOSTTrigger batch evaluation (full universe or symbol list)/api/pipeline/statusGETCurrent pipeline/crawler status/api/admin/configGETService configuration/api/admin/universeGETCurrent universe symbol list
Machine Setup

Machine 2 (192.168.1.143): Hosts this service + LM Studio
Machine 1 (localhost): Hosts BenTrade, calls this service's API for company data
Both machines are on the same local network

Development Workflow

Make changes to Python files
The server auto-reloads (DEBUG=true in .env)
Test with curl: curl http://localhost:8100/api/pipeline/evaluate/AAPL
Check logs in terminal for errors
Database at db/company_eval.db can be inspected with any SQLite viewer

Key Dependencies

fastapi + uvicorn: Web framework
sqlalchemy + aiosqlite: Async SQLite ORM
httpx: Async HTTP client for Polygon/Finnhub/LM Studio
pandas + numpy: Financial calculations
yfinance: Yahoo Finance fallback only
apscheduler: Overnight crawler scheduling
pydantic + pydantic-settings: Configuration and data validation
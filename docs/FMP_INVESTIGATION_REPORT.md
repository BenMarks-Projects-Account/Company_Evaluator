# FMP Ultimate Investigation Report — Phase 0 + Phase 1

**Date:** April 16, 2026  
**Scope:** Investigation only — no code changes  
**Purpose:** Guide Phase 2+ data provider consolidation decisions

---

## EXECUTIVE SUMMARY

FMP's `/stable/` API provides **28+ working endpoints** covering financial statements, ratios, price data, company profiles, analyst estimates, earnings history, insider trading, transcripts, and more. **All legacy `/api/v3/` and `/api/v4/` endpoints are blocked (403)** for post-Aug-2025 subscriptions — only `/stable/` paths work.

The current evaluator uses **3 data providers** (Polygon, Finnhub, FMP) with complex merge/fallback logic across **4 major reconciliation points**. FMP alone can replace ~70% of what Polygon and Finnhub provide, while adding **8 new data capabilities** not currently used.

**Key gaps in FMP stable:** No institutional ownership (404), no recommendation trends, no social sentiment, no SEC filings, no bulk/batch downloads for full-market crawls.

---

## PHASE 0: FMP STABLE API CAPABILITY INVENTORY

### Plan & Rate Limits

| Item | Result |
|------|--------|
| Plan tier | Cannot confirm name directly (`/stable/user` → 404). Endpoint access pattern consistent with **FMP Ultimate**. |
| Legacy API | ALL `/api/v3/*` and `/api/v4/*` → **403** ("Legacy Endpoint: no longer supported") |
| Stable API | ALL tested `/stable/*` endpoints → **200 OK** |
| Rate limit (measured) | 77 sequential requests in 65.2s → **0 rate-limited (no 429s)** |
| Sequential throughput | ~1.2 req/s (~71 req/min) — bottlenecked by network latency (~850ms RTT) |
| Async potential | With concurrency: **~240-300 req/min** (configured limit: 300/min) |

### Available Endpoints (200 OK)

| Endpoint | Path | Response | Key Fields |
|----------|------|----------|------------|
| **Batch Quote** | `/stable/batch-quote?symbols=CSV` | array[N] | price, volume, change%, marketCap, avgVol, yearHigh/Low, priceAvg50/200 |
| **Income Statement** | `/stable/income-statement?symbol=X&period=annual` | array[5yr] | 30+ line items (revenue → netIncome), CIK, filingDate |
| **Balance Sheet** | `/stable/balance-sheet-statement?symbol=X&period=annual` | array[5yr] | 40+ items (assets, liabilities, equity) |
| **Cash Flow** | `/stable/cash-flow-statement?symbol=X&period=annual` | array[5yr] | 25+ items (OCF, ICF, FCF, capex, SBC, dividends paid) |
| **Company Profile** | `/stable/profile?symbol=X` | array[1] | price, marketCap, beta, sector, industry, description, CEO, employees, country, exchange, ipoDate, website |
| **Key Metrics TTM** | `/stable/key-metrics-ttm?symbol=X` | array[1] | marketCap, EV, EV/Sales, EV/OCF, EV/FCF, P/E, P/B, P/S, ROIC, ROE, ROA, etc. |
| **Ratios TTM** | `/stable/ratios-ttm?symbol=X` | array[1] | All margin/efficiency/leverage/turnover/per-share ratios |
| **Ratios (Historical)** | `/stable/ratios?symbol=X&period=annual` | array[5yr] | Same as TTM but per-fiscal-year with date |
| **Stock Peers** | `/stable/stock-peers?symbol=X` | array[9] | symbol, companyName, price, mktCap |
| **Analyst Estimates** | `/stable/analyst-estimates?symbol=X&period=annual` | array[10] | revenueLow/High/Avg, ebitdaLow/High/Avg, epsAvg/High/Low, numAnalysts |
| **Price Target Consensus** | `/stable/price-target-consensus?symbol=X` | array[1] | targetHigh, targetLow, targetConsensus, targetMedian |
| **Price Target Summary** | `/stable/price-target-summary?symbol=X` | array[1] | lastMonthCount/Avg, lastQuarterCount/Avg, lastYearCount/Avg |
| **Earnings History** | `/stable/earnings?symbol=X` | array[163] | date, epsActual, epsEstimated, revenueActual, revenueEstimated |
| **Insider Trading** | `/stable/insider-trading/search?symbol=X` | array[N] | filingDate, transactionDate, transactionType, securitiesOwned/Transacted, price |
| **Historical Price EOD** | `/stable/historical-price-eod/full?symbol=X&from=&to=` | array[N] | date, open, high, low, close, volume, change |
| **Company Screener** | `/stable/company-screener?exchange=...` | array[N] | symbol, companyName, marketCap, sector, industry, beta, price |
| **Earning Call Transcript** | `/stable/earning-call-transcript?symbol=X&year=&quarter=` | array[1] | Full 48KB+ transcript text |
| **Historical Market Cap** | `/stable/historical-market-capitalization?symbol=X` | array[N] | date, marketCap |
| **Financial Growth** | `/stable/financial-growth?symbol=X&period=annual` | array[N] | revenueGrowth, grossProfitGrowth, ebitGrowth, netIncomeGrowth, epsGrowth, fcfGrowth |
| **Enterprise Values** | `/stable/enterprise-values?symbol=X&period=annual` | array[N] | stockPrice, numberOfShares, marketCap, cashAndEquiv, totalDebt, enterpriseValue |
| **Key Executives** | `/stable/key-executives?symbol=X` | array[10] | title, name, pay, gender, yearBorn |
| **Owner Earnings** | `/stable/owner-earnings?symbol=X` | array[5yr] | averagePPE, maintenanceCapex, ownersEarnings |
| **DCF** | `/stable/discounted-cash-flow?symbol=X` | array[1] | dcf, stockPrice |
| **Revenue Product Segmentation** | `/stable/revenue-product-segmentation?symbol=X` | array[16yr] | Product-line revenue breakdown |
| **Revenue Geo Segmentation** | `/stable/revenue-geographic-segmentation?symbol=X` | array[13yr] | Geographic revenue breakdown |
| **Dividends** | `/stable/dividends?symbol=X` | array[N] | date, recordDate, paymentDate, adjDividend |
| **Shares Float** | `/stable/shares-float?symbol=X` | array[1] | freeFloat, floatShares, outstandingShares |

### Not Available (404 in Stable API)

| Endpoint | Impact |
|----------|--------|
| **Institutional Ownership** | Cannot replace Finnhub/FMP smart_money for institutional data |
| Upgrades/Downgrades | No analyst revision tracking |
| Company Outlook (aggregated) | No single-call company overview |
| Sector P/E Ratios | Must compute from screener data |
| Social Sentiment | Not available |
| SEC Filings | Not available |
| Stock News | Not available |
| Rating/Score | Not available |
| ESG Data | Not available |
| Senate Trading | Not available |
| Advanced DCF | Only simple DCF available |
| Batch EOD (bulk market-wide) | Must fetch per-symbol |

---

## PHASE 1: CURRENT DATA PROVIDER USAGE AUDIT

### Provider Responsibility Map

| Data Category | Primary Provider | Secondary/Fallback | FMP Role Today |
|---------------|-----------------|-------------------|----------------|
| Financial Statements (IS, BS, CF) | Polygon (SEC XBRL) | FMP (fallback) | Fallback via fmp_normalizer.py |
| Pre-computed Ratios (117 metrics) | Finnhub | — | Cross-validation (key-metrics-ttm, ratios-ttm) |
| Company Name | Polygon | Finnhub → FMP | Tertiary fallback |
| Sector / Industry | FMP | Finnhub → Polygon SIC | **Primary** (overrides Polygon SIC codes) |
| Market Cap | Polygon | Finnhub → FMP | Tertiary fallback |
| Shares Outstanding | Finnhub | — | Not used |
| Price History (OHLCV) | Polygon | FMP (shadow via router) | Shadow comparison |
| Technical Indicators (RSI/SMA/MACD) | Polygon | FMP (shadow via router) | Shadow comparison |
| Real-time Quote | Polygon snapshot | FMP (shadow) | Shadow comparison |
| Stock Peers | Finnhub (symbols only) | — | Not used (but available with names!) |
| Analyst EPS Estimates | Finnhub | — | Not used (richer version available!) |
| Price Targets | Finnhub | — | Not used (consensus+summary available!) |
| Recommendations (buy/hold/sell) | Finnhub | — | No FMP equivalent |
| Earnings Calendar | Finnhub | — | Not used |
| Insider Transactions | FMP smart_money | Finnhub fallback | **Primary** |
| Institutional Ownership | FMP smart_money | — | **Primary** (but uses legacy endpoint) |
| Earning Call Transcripts | FMP | — | **Primary** |
| Universe Discovery | FMP screener | — | **Primary** |

### API Calls Per Symbol

**Standard Evaluation (crawler):** ~14 calls  
| Provider | Calls | Purpose |
|----------|-------|---------|
| Polygon | 4 | financials_quarterly, financials_annual, price_history, company_details |
| Finnhub | 4 | basic_financials, company_profile, insider_transactions, recommendations |
| FMP | 5 | key-metrics-ttm, ratios-ttm, insider_trading, insider_stats, institutional_ownership |
| LM Studio | 1 | analyze_company |

**On-Demand Research:** ~30 calls + 3 LLM  
Standard 14 + DCF model (2-3) + Comps model (6+) + Entry point indicators (4) + Price targets (1) + Transcript (1) + 3 LLM calls

### Major Reconciliation / Merge Points

1. **Company Profile Merge** (company_data_service.py) — 3 providers merged: Polygon name/desc → FMP sector/industry → Finnhub shares. Risk: sector inconsistency if FMP unavailable.

2. **Cross-Validation** (cross_validator.py) — Finnhub 117 metrics compared against FMP TTM ratios. Adjusts outliers >5% divergence. Risk: different computation methodologies.

3. **Financial Statement Normalization** (fmp_normalizer.py) — Converts FMP camelCase shapes to Polygon-format when Polygon fails. Risk: incomplete field mapping.

4. **Smart Money vs Finnhub Insider** (company_data_service.py) — FMP smart money score primary, Finnhub fallback. Risk: different coverage windows.

---

## CONSOLIDATION OPPORTUNITY ASSESSMENT

### EASY: Safe to consolidate (better FMP endpoint exists)

| Category | Current → Proposed | Benefit | Risk |
|----------|-------------------|---------|------|
| **Stock Peers** | Finnhub symbols-only + LLM names → FMP `/stable/stock-peers` | Eliminates LLM hallucination of competitor names; adds mcap for peer sizing | Low |
| **Analyst Estimates** | Finnhub EPS-only → FMP `/stable/analyst-estimates` | Revenue+EBITDA+EBIT consensus for DCF; analyst count | Low-Medium |
| **Price Targets** | Finnhub → FMP `/stable/price-target-consensus` + `summary` | Adds coverage depth metrics | Low |
| **Pre-computed Growth** | Manual computation → FMP `/stable/financial-growth` cross-check | Catches computation bugs | Low |
| **Enterprise Values** | Manual computation → FMP `/stable/enterprise-values` | Direct EV, avoids debt/cash calculation | Low |

### NEW: Capabilities not currently used

| Endpoint | Potential Use | Pillar Impact |
|----------|-------------|---------------|
| **Earnings History** (163 quarters) | Earnings surprise consistency score | Pillar 1 (quality) or Pillar 5 (expectations) |
| **Owner Earnings** | Buffett-style owner earnings metric | Pillar 1 (business quality) |
| **Revenue Segmentation** (product + geo) | Diversification + moat analysis for LLM | Pillar 1 (moat), Pillar 3 (capital allocation) |
| **Key Executives** | Management context for LLM analysis | LLM quality |
| **Historical Market Cap** | Market cap CAGR for growth scoring | Pillar 4 (growth quality) |
| **Dividends** | Dividend consistency + growth | Pillar 3 (capital allocation) |
| **Shares Float** | Float analysis, short interest context | Risk assessment |
| **FMP DCF** | Third-party DCF sanity check against our model | Cross-validation |

### RISKY: Could consolidate but needs careful testing

| Category | Current → Proposed | Risk |
|----------|-------------------|------|
| **Financials** (Polygon → FMP primary) | Polygon SEC XBRL is gold standard. FMP may have different filing lag. | Medium — fmp_normalizer.py exists but Polygon is more trustworthy. |
| **Profile** (3-source → FMP only) | FMP profile has all fields. But loses Polygon SIC description + Finnhub precision. | Medium — need universe-wide coverage test. |
| **Ratios** (Finnhub → FMP only) | Finnhub has 117 unique metrics; some may not exist in FMP. | Medium — need metric overlap audit. |

### KEEP MULTI-SOURCE: FMP cannot replace

| Category | Why |
|----------|-----|
| **Institutional Ownership** | FMP `/stable/institutional-*` returns **404** — not available |
| **Recommendation Trends** | Finnhub provides buy/hold/sell breakdown; no FMP equivalent |
| **Earnings Calendar** | Finnhub's format suits entry point timing logic |
| **Technical Indicators** | Both work; already shadowed via DataSourceRouter |

---

## ESTIMATED IMPACT

### Per-Symbol Speed
| Metric | Current | After Phase 2 | Change |
|--------|---------|--------------|--------|
| API calls (standard eval) | ~14 | ~11 | -3 calls |
| API calls (on-demand) | ~30 | ~24 | -6 calls |
| Time per symbol | ~60-86s | ~50-55s | -15% |
| Merge/reconciliation points | 4 major | 2 major | -50% bug surface |

### Full Universe Crawl
| Metric | Current | After Phase 2 | Change |
|--------|---------|--------------|--------|
| Universe size | 2,941 symbols | 2,941 symbols | Same |
| Estimated wall time | ~49 hours | ~42 hours | -7 hours |
| Note | Bulk endpoints (403) would be transformative but unavailable on current plan | | |

### Code Complexity
- Remove 3-provider profile merge → single FMP profile + Polygon fallback
- Simplify cross-validator to compare computed vs FMP (not Finnhub vs FMP)
- Add new data paths for earnings history, owner earnings, segmentation

---

## RECOMMENDED PHASE 2 ROADMAP

### Priority 1: Stock Peers Upgrade
Replace Finnhub `get_peers` (symbols only) with FMP `/stable/stock-peers` (symbols + names + mcap). Eliminates LLM hallucination of competitor names in `business_profile.py` and `comps_model.py`.  
**Risk:** Low | **Effort:** ~2 hours

### Priority 2: Earnings History Integration
Add FMP `/stable/earnings` — 163 quarters of EPS actual vs estimated. Create earnings surprise consistency score. Can feed into Pillar 1 (quality) or Pillar 5 (expectations).  
**Risk:** Medium (new data path) | **Effort:** ~4 hours

### Priority 3: Analyst Estimates Upgrade
Replace Finnhub EPS-only estimates with FMP `/stable/analyst-estimates` (revenue + EBITDA + EPS consensus with analyst count). Feeds richer data into DCF model.  
**Risk:** Medium (DCF model change) | **Effort:** ~4 hours

### Priority 4: Profile Merge Simplification
Make FMP `/stable/profile` the primary profile source. Keep Polygon as fallback. Removes the 3-source merge reconciliation point.  
**Risk:** Medium (need universe-wide coverage verification) | **Effort:** ~3 hours

### Priority 5: New Capabilities
Incrementally add: owner earnings → Pillar 1, revenue segmentation → LLM moat analysis, key executives → LLM context, financial growth → cross-validation, enterprise values → Pillar 5.  
**Risk:** Low per addition | **Effort:** ~2 hours each

### NOT Recommended for Phase 2
- **Dropping Polygon financials** — SEC XBRL is gold standard, keep as primary
- **Dropping Finnhub entirely** — Still provides unique data (recommendations, earnings calendar, IPO calendar)
- **Bulk endpoints** — All 403, not available on current plan

---

*Report generated from live endpoint testing + comprehensive codebase audit. All endpoint status codes verified against production FMP API.*

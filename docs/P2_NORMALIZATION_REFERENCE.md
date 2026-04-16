# P2 — Operational & Financial Health: Normalization Reference

Pillar weight: **15%** of composite.
Inner-pillar weights:

| Sub-metric | Weight | Direction |
|---|---:|---|
| `sga_efficiency` | 20% | lower-better |
| `debt_to_ebitda` | 20% | lower-better |
| `interest_coverage` | 20% | higher-better |
| `current_ratio` | 15% | higher-better |
| `cash_conversion` | 15% | higher-better |
| `altman_z` | 10% | higher-better |

All six sub-metrics are scored 0–100 via linear clipping between a `low` and `high` bound (`metrics.helpers.score`). Inner weights are averaged with `metrics.helpers.weighted_avg`, which penalizes missing values (they contribute 0 to the numerator but still count against the denominator). Low data completeness then triggers a completeness cap in `metrics.helpers.apply_completeness_cap` (<70% → cap 80; <50% → cap 60; <30% → cap 40).

The pillar-level validator is `metrics.validation.METRIC_BOUNDS`: values outside the validator bounds become `None` before scoring (this is distinct from the scoring bounds, which are the clipping range for the linear score).

---

## `sga_efficiency`
- **Measures:** SG&A expense as a share of revenue.
- **Raw inputs:**
  - `ttm_sum(selling_general_administrative) / ttm_sum(revenue)` from Polygon quarterly statements (`financials_quarterly.statements`).
  - Fallback: Finnhub `basic_financials.metrics.sgaToSaleTTM` (divided by 100 if expressed as a percent).
- **Sentinel handling:** None — genuine missing data stays missing.
- **Validator bounds:** `(0.0, 1.0)`.
- **Normalization (clipped linear, inverted):**

| Input (SG&A / Revenue) | Score |
|---|---:|
| ≤ 0.05 (≤ 5%) | 100 |
| 0.10 | ~86 |
| 0.20 | ~57 |
| 0.30 | ~29 |
| ≥ 0.40 (≥ 40%) | 0 |

- **Direction:** lower is better.
- **Known limitations:** ~44% coverage; the remainder are `None`. Financials, REITs, and utilities typically do not disclose SG&A on a separate line, so neither Polygon nor Finnhub surfaces a value. Not addressed in this cleanup.

---

## `debt_to_ebitda`
- **Measures:** Long-term debt divided by operating income (EBIT proxy; we lack a clean D&A line).
- **Raw inputs:**
  - `latest(long_term_debt)` from Polygon quarterly statements.
  - `ttm_sum(operating_income)` from Polygon.
  - Fallback: Finnhub `evEbitdaTTM` × `profile.market_cap` to derive an EBITDA approximation, then `total_debt / EBITDA_approx`.
- **Sentinel handling:** None.
  - A parallel `"no_debt"` sentinel was evaluated during the p2-cleanup and **rejected**. 10/10 spot-check candidates with `long_term_debt: None` in Polygon showed substantial debt per Finnhub's `longTermDebt/equityQuarterly` (e.g., AMT=9.26, MCD=3.40, NEE=1.64). The `None` is a data-provider gap, not genuine debt-freeness, so emitting `"no_debt"` would conflate the two.
- **Validator bounds:** `(-50.0, 50.0)`.
- **Normalization (clipped linear, inverted):**

| Input (Debt / EBIT) | Score |
|---|---:|
| 0.0× | 100 |
| 1.0× | 80 |
| 2.0× | 60 |
| 3.0× | 40 |
| 4.0× | 20 |
| ≥ 5.0× | 0 |

- **Direction:** lower is better.
- **Known limitations:** ~45% coverage. The EBIT-as-EBITDA proxy biases this metric toward worse readings than a true EV/EBITDA would show (no D&A add-back). Not addressed in this cleanup.

---

## `interest_coverage`
- **Measures:** Operating income divided by interest expense — how many times over the company earns its coupon.
- **Raw inputs:**
  - `ttm_sum(operating_income) / abs(ttm_sum(interest_expense))` from Polygon quarterly.
  - Fallback: Finnhub `netInterestCoverageTTM` when either Polygon TTM is missing OR operating income is non-positive.
- **Sentinel handling:** `"no_debt"` when interest_expense is **reported** by Polygon and its magnitude is below `NO_DEBT_THRESHOLD = $1,000,000`. Maps to score 100 in `rescore_from_metrics`.
  - **Post-p2-cleanup:** the prior code collapsed `None` into `0` via `abs(ttm_sum(...) or 0)`, which falsely tripped the no-debt sentinel on every company where Polygon simply hadn't populated `interest_expense`. The cleanup disambiguates:
    - `interest_expense_ttm is None` → try Finnhub fallback, do **not** emit sentinel.
    - `interest_expense_ttm < $1M` (reported) → emit `"no_debt"` → score 100.
    - `interest_expense_ttm ≥ $1M` with positive op_inc → compute numeric ratio.
  - Numeric values are capped at 100× before scoring to prevent outliers from skewing downstream math.
- **Validator bounds:** `(-100.0, 1000.0)`; the sentinel `"no_debt"` bypasses validation.
- **Normalization (clipped linear):**

| Input (Op Inc / Interest) | Score |
|---|---:|
| ≤ 2.0× | 0 |
| 5.0× | ~17 |
| 10.0× | ~44 |
| 15.0× | ~72 |
| ≥ 20.0× | 100 |
| `"no_debt"` | 100 |

- **Direction:** higher is better.
- **Known limitations:** `NO_DEBT_THRESHOLD = $1M` is an arbitrary-but-stable choice flagged for future review. Values straddling the threshold across snapshots will flip the sentinel on/off, contributing to the per-symbol snapshot noise Phase A flagged for P2. Not changed in this cleanup.

---

## `current_ratio`
- **Measures:** Current assets / current liabilities — short-term liquidity.
- **Raw inputs:**
  - `latest(current_assets) / latest(current_liabilities)` from Polygon quarterly.
  - Fallback: Finnhub `currentRatioQuarterly`.
- **Sentinel handling:** None.
- **Validator bounds:** `(0.0, 50.0)`.
- **Normalization (clipped linear):**

| Input | Score |
|---|---:|
| ≤ 0.8 | 0 |
| 1.0 | ~12 |
| 1.5 | ~41 |
| 2.0 | ~71 |
| ≥ 2.5 | 100 |

- **Direction:** higher is better.
- **Known limitations:** High coverage (~97%). No material issues identified.

---

## `cash_conversion`
- **Measures:** Operating cash flow divided by net income — how much of reported earnings shows up as cash.
- **Raw inputs:** `ttm_sum(operating_cash_flow) / ttm_sum(net_income)` from Polygon quarterly, gated by `net_income > 0`.
- **Sentinel handling:** None — when net income is zero or negative the metric is `None` (avoids divide-by-negative producing misleading ratios).
- **Validator bounds:** `(-10.0, 10.0)`.
- **Normalization (clipped linear):**

| Input (OCF / NI) | Score |
|---|---:|
| ≤ 0.5 | 0 |
| 0.8 | 30 |
| 1.0 | 50 |
| 1.2 | 70 |
| ≥ 1.5 | 100 |

- **Direction:** higher is better.
- **Known limitations:** Structurally penalizes banks because net income is accrual-based and operating cash flow includes loan-book cash movements that swamp NI (JPM scored 0 with OCF/NI=-6.0 in the spot-check). Not addressed in this cleanup.

---

## `altman_z`
- **Measures:** Simplified five-factor Altman Z-score composite distress indicator; higher = further from bankruptcy.
- **Raw inputs:**
  - `latest(current_assets)`, `latest(current_liabilities)`, `latest(total_assets)`, `latest(total_liabilities)` from Polygon.
  - `ttm_sum(operating_income)`, `ttm_sum(revenue)` from Polygon.
  - `profile.market_cap` from Polygon/Finnhub profile.
  - Retained earnings is approximated by `total_assets - total_liabilities` because Polygon does not break it out.
  - Formula: `Z = 1.2·WC/TA + 1.4·RE/TA + 3.3·EBIT/TA + 0.6·MktCap/TL + 1.0·Rev/TA`.
- **Sentinel handling:** None.
- **Validator bounds:** `(-10.0, 100.0)`.
  - **Post-p2-cleanup:** raised from `(-10.0, 20.0)`. The prior upper bound of 20 clipped cash-rich companies (NVDA Z≈72 was observed) to `None`, propagating as missing data. The scoring ceiling of Z≥4.0→score=100 is unchanged; the widened validator bound simply lets realistic high-Z values survive into scoring instead of being nulled.
- **Normalization (clipped linear):**

| Input (Z) | Score |
|---|---:|
| ≤ 1.8 | 0 (distress) |
| 2.5 | ~32 |
| 3.0 | ~55 |
| 3.5 | ~77 |
| ≥ 4.0 | 100 |

- **Direction:** higher is better.
- **Known limitations:** The manufacturing-era Altman coefficients are a weak fit for financial-sector balance sheets (BAC Z=0.37, JPM Z=0.41 in spot-check), and large-cap tech reliably saturates at 100. The metric captures distress risk well at the bottom end but is not discriminating at the top. Not addressed in this cleanup.

---

## Known Limitations (summary)

- **`NO_DEBT_THRESHOLD = $1M`** is an arbitrary-but-stable choice, flagged for future review. Values straddling the threshold across snapshots will flip the sentinel on/off, contributing to P2's higher snapshot-over-snapshot noise (Phase A Analysis 12: P2 mean |Δ| = 4.22, 22.96% of pairs ≥5 points). **Not changed in this cleanup.**
- **`sga_efficiency` missing ~56%** of the universe, driven by financials/REITs/utilities not disclosing SG&A as a separate line in Polygon, with no Finnhub fallback available for most. **Not changed in this cleanup.**
- **`debt_to_ebitda` missing ~55%** of the universe, driven by Polygon not populating `long_term_debt` for many sectors. Finnhub `longTermDebt/equity` has broader coverage but represents a different ratio; using it would require a schema change to also pull equity and derive EBITDA, out of cleanup scope. **Not changed in this cleanup.**
- **`cash_conversion` structural penalty on banks** — accrual-based net income mismatches operating cash flow that includes loan-book flows. **Not changed in this cleanup.**
- **`debt_to_ebitda` parallel `"no_debt"` sentinel was rejected** during the cleanup spot-check: 10/10 candidate symbols with `long_term_debt: None` in Polygon had material debt per Finnhub (longTermDebt/equity ≥ 0.11, several >1.5). Emitting `"no_debt"` would conflate "genuinely debt-free" with "long-term debt not captured by data provider". If later reinstated, this caveat must be called out prominently in the sentinel logic.

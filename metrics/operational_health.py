"""Pillar 2 — Operational & Financial Health (weight 15 %).

Measures efficiency, leverage, liquidity, and distress risk.

Metrics and inner-pillar weights:
  sga_efficiency     20 %   SG&A / Revenue (lower is better)
  debt_to_ebitda     20 %   Total debt / EBITDA proxy (lower is better)
  interest_coverage  20 %   From Finnhub netInterestCoverageTTM
  current_ratio      15 %   Current assets / current liabilities
  cash_conversion    15 %   Operating CF / Net income (higher is better)
  altman_z           10 %   Simplified Altman Z-score
"""

from __future__ import annotations

import logging

from metrics.helpers import (
    safe_div,
    score,
    weighted_avg,
    apply_completeness_cap,
    get_statements,
    get_finnhub_metrics,
    ttm_sum,
    latest,
)
from metrics.validation import validate_pillar_metrics

_log = logging.getLogger(__name__)

_BOUNDS = {
    "sga_efficiency":    (0.05, 0.40),    # 5 % (best) → 40 % (worst)
    "debt_to_ebitda":    (0.0, 5.0),      # 0× (best) → 5× (worst)
    "interest_coverage": (2.0, 20.0),     # 2× → 20×
    "current_ratio":     (0.8, 2.5),      # 0.8 → 2.5
    "cash_conversion":   (0.5, 1.5),      # 0.5× → 1.5×
    "altman_z":          (1.8, 4.0),      # 1.8 (distress edge) → 4.0 (safe)
}

_WEIGHTS = {
    "sga_efficiency":    0.20,
    "debt_to_ebitda":    0.20,
    "interest_coverage": 0.20,
    "current_ratio":     0.15,
    "cash_conversion":   0.15,
    "altman_z":          0.10,
}

# Metrics where lower raw value is better
_INVERTED = {"sga_efficiency", "debt_to_ebitda"}


def compute(data: dict) -> dict:
    quarterly = get_statements(data, "quarterly")
    fh = get_finnhub_metrics(data)
    price = data.get("price_history") or {}
    profile = data.get("profile") or {}

    # --- SG&A Efficiency ---
    # Measures:     Selling, general, and administrative expense as a share
    #               of revenue (lower = more operationally efficient).
    # Inputs:       TTM sum of selling_general_administrative (Polygon
    #               quarterly); TTM revenue (Polygon); Finnhub sgaToSaleTTM
    #               as fallback (divided by 100 when expressed as a percent).
    # Sentinel:     None — genuine missing data stays missing.
    # Normalization: 0.05 (5%) → 100, 0.40 (40%) → 0 (linear, inverted).
    # Direction:    lower is better.
    # Limitation:   Coverage drops to ~44% because financials, REITs, and
    #               utilities typically do not disclose SG&A as a separate
    #               line. Not addressed in this cleanup.
    sga_ttm = ttm_sum(quarterly, "selling_general_administrative")
    rev_ttm = ttm_sum(quarterly, "revenue")
    sga_eff = safe_div(sga_ttm, rev_ttm)
    if sga_eff is None:
        v = fh.get("sgaToSaleTTM")
        sga_eff = v / 100 if (v is not None and v > 1) else v  # Finnhub stores as ratio < 1

    # --- Debt / EBITDA ---
    # Measures:     Total long-term debt divided by operating income (used as
    #               EBIT proxy; we lack a clean D&A line). Lower = less levered.
    # Inputs:       latest(long_term_debt) (Polygon); operating_income TTM
    #               (Polygon). Fallback derives EBITDA from Finnhub
    #               evEbitdaTTM × market_cap.
    # Sentinel:     None — see limitation. A parallel "no_debt" sentinel was
    #               evaluated and rejected during the p2-cleanup because
    #               Polygon's long_term_debt=None frequently coincides with
    #               real debt reported by Finnhub (10/10 spot-check failed).
    # Normalization: 0.0 → 100, 5.0× → 0 (linear, inverted).
    # Direction:    lower is better.
    # Limitation:   Coverage ~45%; the long_term_debt field is sparsely
    #               populated by Polygon for financials, REITs, utilities.
    # Approximate EBITDA ≈ operating_income (we lack D&A separately)
    # Use Finnhub's totalDebt/totalEquity and ebitdPerShareTTM for a better proxy
    op_inc_ttm = ttm_sum(quarterly, "operating_income")
    total_debt = latest(quarterly, "long_term_debt")
    # Add current portion rough proxy: total_liabilities - noncurrent_liabilities ≈ current debt
    # But simpler: use Finnhub ratio directly
    debt_ebitda = None
    if total_debt is not None and op_inc_ttm is not None and op_inc_ttm > 0:
        debt_ebitda = total_debt / op_inc_ttm  # EBIT proxy
    # Finnhub fallback
    if debt_ebitda is None:
        # Derive from Finnhub: totalDebt/totalEquityQuarterly * equity / EBITDA
        # Simpler: use net debt / EBITDA from EV math
        ev_ebitda = fh.get("evEbitdaTTM")
        mkt = profile.get("market_cap")
        if ev_ebitda is not None and mkt and total_debt is not None:
            ebitda_approx = safe_div(mkt, ev_ebitda)  # rough
            debt_ebitda = safe_div(total_debt, ebitda_approx)

    # --- Interest Coverage ---
    # Measures:     Operating income / interest expense (higher = safer).
    # Inputs:       operating_income TTM (Polygon quarterly statements);
    #               interest_expense TTM (Polygon); Finnhub
    #               netInterestCoverageTTM as fallback.
    # Sentinel:     "no_debt" when interest_expense is *reported* and its
    #               magnitude is below NO_DEBT_THRESHOLD ($1M). Distinct from
    #               interest_expense being *missing* (None), which now falls
    #               through to the Finnhub fallback without triggering the
    #               sentinel. Sentinel maps to score 100 in rescore.
    # Normalization: 2× → 0, 20× → 100 (linear; see _BOUNDS).
    # Direction:    higher is better.
    #
    # Post-p2-cleanup: the prior code did `abs(ttm_sum(...) or 0)` which
    # collapsed "None" into "0", falsely tripping the no_debt sentinel on any
    # company where Polygon simply hadn't populated interest_expense. We now
    # distinguish missing vs. reported-but-small so Finnhub gets a chance.
    interest_exp_ttm = ttm_sum(quarterly, "interest_expense")
    interest_exp = abs(interest_exp_ttm) if interest_exp_ttm is not None else None
    NO_DEBT_THRESHOLD = 1_000_000  # $1M — below this (when reported), treat as essentially no debt
    if interest_exp is None:
        # interest_expense not reported by Polygon — fall back to Finnhub.
        interest_cov = fh.get("netInterestCoverageTTM")
        # Finnhub returns 0 for companies with effectively no interest expense.
        # Without a Polygon number to corroborate we cannot confidently emit
        # the "no_debt" sentinel; leave as numeric 0 → scores low. This is
        # conservative but honest.
    elif op_inc_ttm is not None and interest_exp >= NO_DEBT_THRESHOLD and op_inc_ttm > 0:
        interest_cov = op_inc_ttm / interest_exp
    elif op_inc_ttm is not None and interest_exp < NO_DEBT_THRESHOLD:
        interest_cov = "no_debt"
    else:
        # op_inc_ttm missing or non-positive — Finnhub fallback.
        interest_cov = fh.get("netInterestCoverageTTM")
        if interest_cov is not None and interest_cov == 0 and interest_exp < NO_DEBT_THRESHOLD:
            interest_cov = "no_debt"
    # Cap extreme numeric values for scoring stability
    if isinstance(interest_cov, (int, float)) and interest_cov is not None:
        interest_cov = min(interest_cov, 100.0)

    # --- Current Ratio ---
    # Measures:     Current assets / current liabilities (short-term liquidity).
    # Inputs:       latest(current_assets), latest(current_liabilities)
    #               (Polygon); Finnhub currentRatioQuarterly fallback.
    # Sentinel:     None.
    # Normalization: 0.8 → 0, 2.5 → 100 (linear).
    # Direction:    higher is better.
    ca = latest(quarterly, "current_assets")
    cl = latest(quarterly, "current_liabilities")
    current_ratio = safe_div(ca, cl)
    if current_ratio is None:
        current_ratio = fh.get("currentRatioQuarterly")

    # --- Cash Conversion (OCF / Net Income) ---
    # Measures:     How much of reported earnings shows up as cash flow.
    # Inputs:       TTM operating_cash_flow, TTM net_income (Polygon).
    # Sentinel:     None — gated by net_income > 0 to avoid divide-by-negative
    #               producing misleading ratios.
    # Normalization: 0.5 → 0, 1.5 → 100 (linear).
    # Direction:    higher is better.
    # Limitation:   Structurally penalizes banks because net income is
    #               accrual-based and operating cash flow for banks includes
    #               loan book flows that swamp NI. Not addressed in cleanup.
    ocf_ttm = ttm_sum(quarterly, "operating_cash_flow")
    ni_ttm = ttm_sum(quarterly, "net_income")
    cash_conv = safe_div(ocf_ttm, ni_ttm) if (ni_ttm and ni_ttm > 0) else None

    # --- Altman Z (simplified manufacturing formula adapted) ---
    # Measures:     Composite distress indicator; higher = further from
    #               bankruptcy. Uses the classic 5-factor Altman coefficients.
    # Inputs:       current_assets, current_liabilities, total_assets,
    #               total_liabilities, operating_income TTM, revenue TTM,
    #               market_cap (from profile). "Retained earnings" is
    #               approximated by (total_assets - total_liabilities) since
    #               Polygon doesn't break RE out separately.
    # Sentinel:     None.
    # Normalization: 1.8 → 0 (distress edge), 4.0 → 100 (safe); cash-rich
    #               outliers (e.g. NVDA Z≈72) saturate at 100.
    # Direction:    higher is better.
    # Post-p2-cleanup: validation upper bound in metrics/validation.py was
    # raised from 20 → 100 so that realistic high-Z values are not clipped
    # to None by the validator. Scoring ceiling at Z=4.0 is unchanged.
    # Z = 1.2*WC/TA + 1.4*RE/TA + 3.3*EBIT/TA + 0.6*MktCap/TL + 1.0*Rev/TA
    ta = latest(quarterly, "total_assets")
    tl = latest(quarterly, "total_liabilities")
    mkt_cap = profile.get("market_cap")
    altman_z = None
    if ta and ta > 0 and tl and rev_ttm and op_inc_ttm:
        wc = (ca or 0) - (cl or 0)
        retained = (ta - tl) if tl else None  # proxy for retained earnings
        a = 1.2 * safe_div(wc, ta, 0)
        b = 1.4 * safe_div(retained, ta, 0)
        c = 3.3 * safe_div(op_inc_ttm, ta, 0)
        d = 0.6 * safe_div(mkt_cap, tl, 0) if mkt_cap else 0
        e = 1.0 * safe_div(rev_ttm, ta, 0)
        altman_z = a + b + c + d + e

    # --- Assemble ---
    metrics = {
        "sga_efficiency":    _r(sga_eff),
        "debt_to_ebitda":    _r(debt_ebitda),
        "interest_coverage": _r(interest_cov),
        "current_ratio":     _r(current_ratio),
        "cash_conversion":   _r(cash_conv),
        "altman_z":          _r(altman_z),
    }

    return rescore_from_metrics(metrics, raw_metrics=metrics)


def rescore_from_metrics(metrics: dict, raw_metrics: dict | None = None) -> dict:
    """Re-score Operational Health from persisted metric values."""
    raw_metrics = dict(raw_metrics or metrics)
    validated_metrics, flags = validate_pillar_metrics(metrics)
    for key in _BOUNDS:
        validated_metrics.setdefault(key, None)

    scores = {}
    for k in _BOUNDS:
        v = validated_metrics.get(k)
        # "no_debt" sentinel for interest_coverage → perfect score
        if k == "interest_coverage" and v == "no_debt":
            scores[k] = 100.0
        else:
            scores[k] = score(v, *_BOUNDS[k], invert=(k in _INVERTED))

    raw_score, completeness_pct = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])
    pillar_score = apply_completeness_cap(raw_score, completeness_pct)

    _log.info("    [OH] Final: sga=%s d/ebitda=%s int_cov=%s cur_r=%s cash_c=%s alt_z=%s -> raw=%.1f pillar=%.1f completeness=%.1f%%",
              validated_metrics["sga_efficiency"], validated_metrics["debt_to_ebitda"], validated_metrics["interest_coverage"],
              validated_metrics["current_ratio"], validated_metrics["cash_conversion"], validated_metrics["altman_z"],
              raw_score, pillar_score, completeness_pct)

    return {
        "pillar_score": pillar_score,
        "raw_score": raw_score,
        "completeness_pct": completeness_pct,
        "raw_metrics": raw_metrics,
        "metrics": validated_metrics,
        "scores": scores,
        "data_quality_flags": flags,
        "cap_applied": pillar_score != raw_score,
    }


def _r(v, decimals=4):
    if v is None:
        return None
    if isinstance(v, str):
        return v  # pass through sentinel values like "no_debt"
    return round(v, decimals)

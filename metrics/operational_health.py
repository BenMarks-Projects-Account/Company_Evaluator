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
    get_statements,
    get_finnhub_metrics,
    ttm_sum,
    latest,
)

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
    sga_ttm = ttm_sum(quarterly, "selling_general_administrative")
    rev_ttm = ttm_sum(quarterly, "revenue")
    sga_eff = safe_div(sga_ttm, rev_ttm)
    if sga_eff is None:
        v = fh.get("sgaToSaleTTM")
        sga_eff = v / 100 if (v is not None and v > 1) else v  # Finnhub stores as ratio < 1

    # --- Debt / EBITDA ---
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
    interest_cov = fh.get("netInterestCoverageTTM")
    # Cap extreme values for scoring stability
    if interest_cov is not None:
        interest_cov = min(interest_cov, 100.0)

    # --- Current Ratio ---
    ca = latest(quarterly, "current_assets")
    cl = latest(quarterly, "current_liabilities")
    current_ratio = safe_div(ca, cl)
    if current_ratio is None:
        current_ratio = fh.get("currentRatioQuarterly")

    # --- Cash Conversion (OCF / Net Income) ---
    ocf_ttm = ttm_sum(quarterly, "operating_cash_flow")
    ni_ttm = ttm_sum(quarterly, "net_income")
    cash_conv = safe_div(ocf_ttm, ni_ttm) if (ni_ttm and ni_ttm > 0) else None

    # --- Altman Z (simplified manufacturing formula adapted) ---
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

    scores = {}
    for k in _BOUNDS:
        scores[k] = score(metrics[k], *_BOUNDS[k], invert=(k in _INVERTED))

    pillar = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])

    _log.info("    [OH] Final: sga=%s d/ebitda=%s int_cov=%s cur_r=%s cash_c=%s alt_z=%s -> pillar=%.1f",
              metrics["sga_efficiency"], metrics["debt_to_ebitda"], metrics["interest_coverage"],
              metrics["current_ratio"], metrics["cash_conversion"], metrics["altman_z"],
              pillar or 0)

    return {"pillar_score": pillar, "metrics": metrics, "scores": scores}


def _r(v, decimals=4):
    return round(v, decimals) if v is not None else None

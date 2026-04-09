"""Pillar 1 — Business Quality (weight 30 %).

Measures profitability, cash generation, and competitive moat using data
computed directly from Polygon financial statements, supplemented by
Finnhub pre-computed ratios where raw data is unavailable.

Metrics and inner-pillar weights:
  roic          25 %   Return on invested capital (TTM, computed)
  gross_margin  15 %   Gross profit / Revenue (TTM)
  op_margin     20 %   Operating income / Revenue (TTM)
  net_margin    15 %   Net income / Revenue (TTM)
  fcf_yield     15 %   Free cash flow / Market cap
  rev_stability 10 %   1 − coefficient-of-variation of quarterly revenue
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
    coeff_of_variation,
)
from metrics.validation import validate_pillar_metrics

_log = logging.getLogger(__name__)

# Scoring bounds  (value_at_0, value_at_100)
_BOUNDS = {
    "roic":          (0.03, 0.30),   # 3 % → 30 %
    "gross_margin":  (0.15, 0.65),   # 15 % → 65 %
    "op_margin":     (0.03, 0.35),   # 3 % → 35 %
    "net_margin":    (0.02, 0.25),   # 2 % → 25 %
    "fcf_yield":     (0.01, 0.08),   # 1 % → 8 %
    "rev_stability": (0.70, 1.00),   # 0.70 → 1.00 (1−CoV, higher is more stable)
}

_WEIGHTS = {
    "roic":          0.25,
    "gross_margin":  0.15,
    "op_margin":     0.20,
    "net_margin":    0.15,
    "fcf_yield":     0.15,
    "rev_stability": 0.10,
}


def compute(data: dict) -> dict:
    """Return Business Quality scores from raw company data."""
    quarterly = get_statements(data, "quarterly")
    fh = get_finnhub_metrics(data)
    _log.info("    [BQ] Computing with %d quarterly statements, %d finnhub metrics",
              len(quarterly), len(fh))

    # --- ROIC (TTM) ---
    # NOPAT = operating_income * (1 − effective_tax_rate)
    # Invested Capital = total_assets − current_liabilities (simplified)
    op_income_ttm = ttm_sum(quarterly, "operating_income")
    tax_ttm = ttm_sum(quarterly, "income_tax")
    pretax_ttm = ttm_sum(quarterly, "income_before_tax")
    tax_rate = safe_div(tax_ttm, pretax_ttm, default=0.21)
    if tax_rate is not None:
        tax_rate = max(0.0, min(tax_rate, 0.50))  # sanity clamp
    nopat = op_income_ttm * (1 - (tax_rate or 0.21)) if op_income_ttm is not None else None

    # Average invested capital over last 4 quarters
    ic_vals = []
    for s in quarterly[:4]:
        ta = s.get("total_assets")
        cl = s.get("current_liabilities")
        if ta is not None and cl is not None:
            ic_vals.append(ta - cl)
    avg_ic = (sum(ic_vals) / len(ic_vals)) if ic_vals else None
    roic = safe_div(nopat, avg_ic)

    # Fallback to Finnhub if we couldn't compute
    if roic is None:
        fh_roic = fh.get("roicTTM")
        roic = fh_roic / 100 if fh_roic is not None else None
        if roic is not None:
            _log.info("    [BQ] ROIC: using Finnhub fallback = %.4f", roic)
    else:
        _log.info("    [BQ] ROIC: computed from statements = %.4f", roic)

    # --- Margins (TTM) ---
    rev_ttm = ttm_sum(quarterly, "revenue")
    gp_ttm = ttm_sum(quarterly, "gross_profit")
    ni_ttm = ttm_sum(quarterly, "net_income")

    gross_margin = safe_div(gp_ttm, rev_ttm)
    op_margin = safe_div(op_income_ttm, rev_ttm)
    net_margin = safe_div(ni_ttm, rev_ttm)

    # Finnhub fallbacks (stored as percentages, e.g. 47.33 → 0.4733)
    if gross_margin is None:
        v = fh.get("grossMarginTTM")
        gross_margin = v / 100 if v is not None else None
    if op_margin is None:
        v = fh.get("operatingMarginTTM")
        op_margin = v / 100 if v is not None else None
    if net_margin is None:
        v = fh.get("netProfitMarginTTM")
        net_margin = v / 100 if v is not None else None

    # --- FCF Yield ---
    fcf_ttm = ttm_sum(quarterly, "free_cash_flow")
    market_cap = (data.get("profile") or {}).get("market_cap")
    fcf_yield = safe_div(fcf_ttm, market_cap)

    # Finnhub fallback  (pfcfShareTTM = Price / FCF per share → yield = 1/pfcf)
    if fcf_yield is None:
        pfcf = fh.get("pfcfShareTTM")
        fcf_yield = safe_div(1, pfcf) if pfcf else None

    # --- Revenue Stability ---
    rev_vals = [s.get("revenue") for s in quarterly[:12]]
    cv = coeff_of_variation(rev_vals)
    rev_stability = (1 - cv) if cv is not None else None

    # --- Assemble ---
    metrics = {
        "roic": _r(roic),
        "gross_margin": _r(gross_margin),
        "op_margin": _r(op_margin),
        "net_margin": _r(net_margin),
        "fcf_yield": _r(fcf_yield),
        "rev_stability": _r(rev_stability),
    }

    return rescore_from_metrics(metrics, raw_metrics=metrics)


def rescore_from_metrics(metrics: dict, raw_metrics: dict | None = None) -> dict:
    """Re-score Business Quality from persisted metric values."""
    raw_metrics = dict(raw_metrics or metrics)
    validated_metrics, flags = validate_pillar_metrics(metrics)
    for key in _BOUNDS:
        validated_metrics.setdefault(key, None)

    scores = {k: score(validated_metrics.get(k), *_BOUNDS[k]) for k in _BOUNDS}

    raw_score, completeness_pct = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])
    pillar_score = apply_completeness_cap(raw_score, completeness_pct)

    _log.info("    [BQ] Final: roic=%s gm=%s om=%s nm=%s fcf_y=%s rev_s=%s -> raw=%.1f pillar=%.1f completeness=%.1f%%",
              validated_metrics["roic"], validated_metrics["gross_margin"], validated_metrics["op_margin"],
              validated_metrics["net_margin"], validated_metrics["fcf_yield"], validated_metrics["rev_stability"],
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
    return round(v, decimals) if v is not None else None

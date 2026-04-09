"""Pillar 4 — Growth Quality (weight 20 %).

Measures the pace, consistency, and quality of growth.

Metrics and inner-pillar weights:
  revenue_cagr_3y   25 %   3-year revenue CAGR from annual statements
  revenue_cagr_5y   20 %   5-year revenue CAGR
  fcf_growth        20 %   FCF CAGR (Finnhub focfCagr5Y or computed)
  eps_growth         20 %   EPS growth YoY (Finnhub epsGrowthTTMYoy)
  margin_trend      15 %   Operating margin direction over 3 years
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
    cagr,
)
from metrics.validation import validate_pillar_metrics

_log = logging.getLogger(__name__)

_BOUNDS = {
    "revenue_cagr_3y": (-0.02, 0.20),  # −2 % → +20 %
    "revenue_cagr_5y": (-0.02, 0.15),  # −2 % → +15 %
    "fcf_growth":      (-0.05, 0.15),  # −5 % → +15 %
    "eps_growth":      (-5.0, 25.0),   # −5 % → +25 % (Finnhub in pct)
    "margin_trend":    (-0.05, 0.05),  # −5 pp → +5 pp change
}

_WEIGHTS = {
    "revenue_cagr_3y": 0.25,
    "revenue_cagr_5y": 0.20,
    "fcf_growth":      0.20,
    "eps_growth":      0.20,
    "margin_trend":    0.15,
}


def compute(data: dict) -> dict:
    annual = get_statements(data, "annual")
    fh = get_finnhub_metrics(data)

    # --- Revenue CAGR ---
    rev_3y = _annual_cagr(annual, "revenue", 3)
    rev_5y = _annual_cagr(annual, "revenue", 5)

    # Finnhub fallbacks (stored as %, e.g. 8.68 → 0.0868)
    if rev_3y is None:
        v = fh.get("revenueGrowth3Y")
        rev_3y = v / 100 if v is not None else None
    if rev_5y is None:
        v = fh.get("revenueGrowth5Y")
        rev_5y = v / 100 if v is not None else None

    # --- FCF Growth ---
    fcf_cagr = _annual_cagr(annual, "free_cash_flow", 5)
    if fcf_cagr is None:
        v = fh.get("focfCagr5Y")
        fcf_cagr = v / 100 if v is not None else None

    # --- EPS Growth ---
    eps_growth = fh.get("epsGrowthTTMYoy")  # already in pct

    # --- Operating Margin Trend ---
    # Change in trailing operating margin over ~3 years of annual data
    margin_trend = None
    if len(annual) >= 3:
        rev_new = annual[0].get("revenue")
        oi_new = annual[0].get("operating_income")
        rev_old = annual[2].get("revenue")
        oi_old = annual[2].get("operating_income")
        m_new = safe_div(oi_new, rev_new)
        m_old = safe_div(oi_old, rev_old)
        if m_new is not None and m_old is not None:
            margin_trend = m_new - m_old

    # --- Assemble ---
    metrics = {
        "revenue_cagr_3y": _r(rev_3y),
        "revenue_cagr_5y": _r(rev_5y),
        "fcf_growth":      _r(fcf_cagr),
        "eps_growth_yoy":  _r(eps_growth),
        "margin_trend":    _r(margin_trend),
    }

    return rescore_from_metrics(metrics, raw_metrics=metrics)


def rescore_from_metrics(metrics: dict, raw_metrics: dict | None = None) -> dict:
    """Re-score Growth Quality from persisted metric values."""
    raw_metrics = dict(raw_metrics or metrics)
    validated_metrics, flags = validate_pillar_metrics(metrics)
    for key in ("revenue_cagr_3y", "revenue_cagr_5y", "fcf_growth", "eps_growth_yoy", "margin_trend"):
        validated_metrics.setdefault(key, None)

    scores_dict = {
        "revenue_cagr_3y": score(validated_metrics.get("revenue_cagr_3y"), *_BOUNDS["revenue_cagr_3y"]),
        "revenue_cagr_5y": score(validated_metrics.get("revenue_cagr_5y"), *_BOUNDS["revenue_cagr_5y"]),
        "fcf_growth":      score(validated_metrics.get("fcf_growth"), *_BOUNDS["fcf_growth"]),
        "eps_growth":      score(validated_metrics.get("eps_growth_yoy"), *_BOUNDS["eps_growth"]),
        "margin_trend":    score(validated_metrics.get("margin_trend"), *_BOUNDS["margin_trend"]),
    }

    raw_score, completeness_pct = weighted_avg([(scores_dict[k], _WEIGHTS[k]) for k in _WEIGHTS])
    pillar_score = apply_completeness_cap(raw_score, completeness_pct)

    _log.info("    [GQ] Final: rev3y=%s rev5y=%s fcf=%s eps=%s margin=%s -> raw=%.1f pillar=%.1f completeness=%.1f%%",
              validated_metrics["revenue_cagr_3y"], validated_metrics["revenue_cagr_5y"],
              validated_metrics["fcf_growth"], validated_metrics["eps_growth_yoy"],
              validated_metrics["margin_trend"], raw_score, pillar_score, completeness_pct)

    return {
        "pillar_score": pillar_score,
        "raw_score": raw_score,
        "completeness_pct": completeness_pct,
        "raw_metrics": raw_metrics,
        "metrics": validated_metrics,
        "scores": scores_dict,
        "data_quality_flags": flags,
        "cap_applied": pillar_score != raw_score,
    }


def _annual_cagr(annual: list[dict], field: str, years: int) -> float | None:
    """Compute CAGR from annual statement list (newest-first)."""
    if len(annual) < years + 1:
        # Try with whatever we have
        if len(annual) < 2:
            return None
        years = len(annual) - 1

    new_val = annual[0].get(field)
    old_val = annual[years].get(field)
    return cagr(old_val, new_val, years)


def _r(v, decimals=4):
    return round(v, decimals) if v is not None else None

"""Pillar 5 — Valuation & Expectations (weight 15 %).

Judges whether the market is over- or under-pricing the company's
fundamentals.  Lower valuations score higher (contrarian value tilt).

Metrics and inner-pillar weights:
  ev_ebitda          25 %   EV / EBITDA (lower is cheaper)
  pe_ratio           20 %   P/E ratio (lower is cheaper)
  pfcf               20 %   Price / FCF (lower is cheaper)
  earnings_quality   20 %   Accruals ratio — (NI − OCF) / TA (lower is better)
  analyst_consensus  15 %   Weighted analyst recommendation score
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
    "ev_ebitda":         (5.0, 30.0),     # 5× (cheap) → 30× (expensive)
    "pe_ratio":          (8.0, 40.0),     # 8× → 40×
    "pfcf":              (8.0, 40.0),     # 8× → 40×
    "earnings_quality":  (-0.10, 0.10),   # −10 % (cash > earnings, great) → +10 %
    "analyst_consensus": (0.0, 100.0),    # pre-scored
}

_WEIGHTS = {
    "ev_ebitda":         0.25,
    "pe_ratio":          0.20,
    "pfcf":              0.20,
    "earnings_quality":  0.20,
    "analyst_consensus": 0.15,
}

# Valuation ratios: lower is better (cheaper)
_INVERTED = {"ev_ebitda", "pe_ratio", "pfcf", "earnings_quality"}


def compute(data: dict) -> dict:
    quarterly = get_statements(data, "quarterly")
    fh = get_finnhub_metrics(data)

    # --- EV/EBITDA ---
    ev_ebitda = fh.get("evEbitdaTTM")

    # --- P/E ---
    pe = fh.get("peBasicExclExtraTTM") or fh.get("peTTM")

    # --- Price / FCF ---
    pfcf = fh.get("pfcfShareTTM")

    # --- Earnings Quality (Accruals Ratio) ---
    # Accruals = (Net Income - Operating Cash Flow) / Total Assets
    # Lower (more negative) = higher quality (cash-backed earnings)
    ni_ttm = ttm_sum(quarterly, "net_income")
    ocf_ttm = ttm_sum(quarterly, "operating_cash_flow")
    ta = latest(quarterly, "total_assets")
    accruals = None
    if ni_ttm is not None and ocf_ttm is not None and ta and ta > 0:
        accruals = (ni_ttm - ocf_ttm) / ta

    # --- Analyst Consensus ---
    recs = data.get("analyst_recommendations") or {}
    analyst_score = _score_recommendations(recs)

    # --- Assemble ---
    metrics = {
        "ev_ebitda":         _r(ev_ebitda),
        "pe_ratio":          _r(pe),
        "pfcf":              _r(pfcf),
        "accruals_ratio":    _r(accruals),
        "analyst_strong_buy": recs.get("strong_buy"),
        "analyst_buy":       recs.get("buy"),
        "analyst_hold":      recs.get("hold"),
        "analyst_sell":      recs.get("sell"),
        "analyst_strong_sell": recs.get("strong_sell"),
    }

    scores = {
        "ev_ebitda":         score(ev_ebitda, *_BOUNDS["ev_ebitda"], invert=True),
        "pe_ratio":          score(pe, *_BOUNDS["pe_ratio"], invert=True),
        "pfcf":              score(pfcf, *_BOUNDS["pfcf"], invert=True),
        "earnings_quality":  score(accruals, *_BOUNDS["earnings_quality"], invert=True),
        "analyst_consensus": analyst_score,
    }

    pillar = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])

    _log.info("    [VE] Final: ev/ebitda=%s pe=%s pfcf=%s accruals=%s analyst=%s -> pillar=%.1f",
              metrics["ev_ebitda"], metrics["pe_ratio"], metrics["pfcf"],
              metrics["accruals_ratio"],
              scores.get("analyst_consensus"), pillar or 0)

    return {"pillar_score": pillar, "metrics": metrics, "scores": scores}


def _score_recommendations(recs: dict) -> float | None:
    """Convert analyst recommendation counts to a 0–100 score.

    strong_buy=100, buy=80, hold=50, sell=20, strong_sell=0
    """
    if recs.get("error") or not any(recs.get(k) for k in ("strong_buy", "buy", "hold", "sell", "strong_sell")):
        return None

    sb = recs.get("strong_buy", 0)
    b = recs.get("buy", 0)
    h = recs.get("hold", 0)
    s = recs.get("sell", 0)
    ss = recs.get("strong_sell", 0)

    total = sb + b + h + s + ss
    if total == 0:
        return None

    weighted = sb * 100 + b * 80 + h * 50 + s * 20 + ss * 0
    return round(weighted / total, 1)


def _r(v, decimals=4):
    return round(v, decimals) if v is not None else None

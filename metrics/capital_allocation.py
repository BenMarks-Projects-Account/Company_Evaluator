"""Pillar 3 — Capital Allocation (weight 20 %).

Measures how well management deploys capital for shareholder value.

Metrics and inner-pillar weights:
  roic_wacc_spread  30 %   ROIC − estimated WACC (wider is better)
  share_trend       20 %   % change in diluted shares (negative = buybacks ✓)
  dividend_sustain  15 %   Payout ratio health (not too high, not zero)
  insider_activity  15 %   Net insider buy/sell signal
  rd_intensity      20 %   R&D / Revenue (higher = more investment in future)
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
    "roic_wacc_spread": (-0.02, 0.20),   # −2 % → +20 %
    "share_trend":      (-0.05, 0.05),    # −5 % (buybacks, good) → +5 % (dilution, bad)
    "dividend_sustain": (0.0, 100.0),     # custom V-shaped, see below
    "insider_activity": (0.0, 100.0),     # already pre-scored
    "rd_intensity":     (0.0, 0.15),      # 0 % → 15 %
}

_WEIGHTS = {
    "roic_wacc_spread": 0.30,
    "share_trend":      0.20,
    "dividend_sustain": 0.15,
    "insider_activity": 0.15,
    "rd_intensity":     0.20,
}


def compute(data: dict) -> dict:
    quarterly = get_statements(data, "quarterly")
    fh = get_finnhub_metrics(data)
    insiders = data.get("insider_transactions") or {}
    smart_money = data.get("smart_money") or {}

    # --- ROIC-WACC Spread ---
    # Re-compute ROIC from statements
    op_inc_ttm = ttm_sum(quarterly, "operating_income")
    tax_ttm = ttm_sum(quarterly, "income_tax")
    pretax_ttm = ttm_sum(quarterly, "income_before_tax")
    tax_rate = safe_div(tax_ttm, pretax_ttm, default=0.21)
    if tax_rate is not None:
        tax_rate = max(0.0, min(tax_rate, 0.50))
    nopat = op_inc_ttm * (1 - (tax_rate or 0.21)) if op_inc_ttm is not None else None

    ic_vals = []
    for s in quarterly[:4]:
        ta = s.get("total_assets")
        cl = s.get("current_liabilities")
        if ta is not None and cl is not None:
            ic_vals.append(ta - cl)
    avg_ic = (sum(ic_vals) / len(ic_vals)) if ic_vals else None
    roic = safe_div(nopat, avg_ic)

    if roic is None:
        v = fh.get("roicTTM")
        roic = v / 100 if v is not None else None

    # Estimate WACC ≈ risk-free + beta * equity premium + debt cost weight
    beta = fh.get("beta") or 1.0
    risk_free = 0.043    # ~4.3 % US 10Y
    equity_premium = 0.05
    cost_of_equity = risk_free + beta * equity_premium
    # Simple blended: assume 70 % equity funded
    wacc_est = cost_of_equity * 0.70 + 0.05 * 0.30  # 5 % pre-tax debt cost
    spread = (roic - wacc_est) if roic is not None else None

    # --- Share Trend (diluted shares change over ~3 years) ---
    shares_recent = None
    shares_old = None
    if len(quarterly) >= 4:
        shares_recent = quarterly[0].get("diluted_avg_shares")
    if len(quarterly) >= 12:
        shares_old = quarterly[11].get("diluted_avg_shares")
    elif len(quarterly) >= 8:
        shares_old = quarterly[7].get("diluted_avg_shares")
    elif len(quarterly) >= 4:
        shares_old = quarterly[3].get("diluted_avg_shares")

    share_trend = safe_div(
        (shares_recent - shares_old) if (shares_recent and shares_old) else None,
        shares_old,
    )

    # --- Dividend Sustainability ---
    # Ideal payout: 20–60 %. 0 % or > 90 % is less ideal.
    payout = fh.get("payoutRatioTTM")
    div_score = _score_payout(payout)

    # --- Insider Activity ---
    # Prefer FMP smart money score (richer data), fall back to Finnhub signal
    insider_activity = smart_money.get("insider_activity") or {}
    if insider_activity.get("score") is not None:
        insider_score = insider_activity["score"]
        insider_metric_value = insider_activity["signal"]
    else:
        insider_score = _score_insiders(insiders)
        insider_metric_value = insiders.get("net_activity", "unknown")

    # --- R&D Intensity ---
    rd_ttm = ttm_sum(quarterly, "research_and_development")
    rev_ttm = ttm_sum(quarterly, "revenue")
    rd_intensity = safe_div(rd_ttm, rev_ttm)

    # --- Assemble ---
    metrics = {
        "roic_wacc_spread": _r(spread),
        "share_trend":      _r(share_trend),
        "payout_ratio":     _r(payout / 100 if payout is not None else None),
        "rd_intensity":     _r(rd_intensity),
        "insider_net":      insider_metric_value,
        "insider_score":    _r(insider_score),
        "wacc_est":         _r(wacc_est),
        "roic":             _r(roic),
    }

    return rescore_from_metrics(metrics, raw_metrics=metrics)


def rescore_from_metrics(metrics: dict, raw_metrics: dict | None = None) -> dict:
    """Re-score Capital Allocation from persisted metric values."""
    raw_metrics = dict(raw_metrics or metrics)
    validated_metrics, flags = validate_pillar_metrics(metrics)
    for key in ("roic_wacc_spread", "share_trend", "payout_ratio", "rd_intensity", "insider_net", "insider_score", "wacc_est", "roic"):
        validated_metrics.setdefault(key, None)
    payout_ratio = validated_metrics.get("payout_ratio")
    payout_pct = payout_ratio * 100 if payout_ratio is not None else None

    # Use pre-computed insider_score if available (from FMP smart money),
    # otherwise fall back to legacy signal-based scoring.
    insider_sub = validated_metrics.get("insider_score")
    if insider_sub is None:
        insider_sub = _score_insiders({"net_activity": validated_metrics.get("insider_net", "unknown")})

    scores = {
        "roic_wacc_spread": score(validated_metrics.get("roic_wacc_spread"), -0.02, 0.20),
        "share_trend":      score(validated_metrics.get("share_trend"), -0.05, 0.05, invert=True),
        "dividend_sustain": _score_payout(payout_pct),
        "insider_activity": insider_sub,
        "rd_intensity":     score(validated_metrics.get("rd_intensity"), 0.0, 0.15),
    }

    raw_score, completeness_pct = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])
    pillar_score = apply_completeness_cap(raw_score, completeness_pct)

    _log.info("    [CA] Final: spread=%s shares=%s payout=%s insider=%s rd=%s -> raw=%.1f pillar=%.1f completeness=%.1f%%",
              validated_metrics["roic_wacc_spread"], validated_metrics["share_trend"],
              validated_metrics.get("payout_ratio"), validated_metrics.get("insider_net"),
              validated_metrics["rd_intensity"], raw_score, pillar_score, completeness_pct)

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


def _score_payout(payout_pct: float | None) -> float | None:
    """V-shaped score: ideal payout 20–60 %, penalise 0 % and >90 %."""
    if payout_pct is None:
        return None
    p = payout_pct  # already in % (e.g. 13.15)
    if p < 0:
        return 30.0  # negative payout = losses
    if p <= 10:
        return 40.0 + p * 2  # low but existent
    if 10 < p <= 20:
        return 60.0 + (p - 10) * 2
    if 20 < p <= 60:
        return 80.0 + (40 - abs(p - 40)) * 0.5  # peak near 40 %
    if 60 < p <= 90:
        return 80.0 - (p - 60) * 1.5  # declining
    return max(0, 80 - (p - 60) * 1.5)


def _score_insiders(ins: dict) -> float | None:
    """Score insider activity.  Net buying → high, net selling → low."""
    activity = ins.get("net_activity", "unknown")
    if activity == "net_buying":
        return 85.0
    if activity == "neutral":
        return 55.0
    if activity == "net_selling":
        return 25.0
    return None  # unknown / no data


def _r(v, decimals=4):
    return round(v, decimals) if v is not None else None

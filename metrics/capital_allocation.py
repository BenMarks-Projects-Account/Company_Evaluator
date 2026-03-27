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

from metrics.helpers import (
    safe_div,
    score,
    weighted_avg,
    get_statements,
    get_finnhub_metrics,
    ttm_sum,
    latest,
)

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
    insider_score = _score_insiders(insiders)

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
        "insider_net":      insiders.get("net_activity", "unknown"),
        "wacc_est":         _r(wacc_est),
        "roic":             _r(roic),
    }

    scores = {
        "roic_wacc_spread": score(spread, -0.02, 0.20),
        "share_trend":      score(share_trend, -0.05, 0.05, invert=True),
        "dividend_sustain": div_score,
        "insider_activity": insider_score,
        "rd_intensity":     score(rd_intensity, 0.0, 0.15),
    }

    pillar = weighted_avg([(scores[k], _WEIGHTS[k]) for k in _WEIGHTS])

    return {"pillar_score": pillar, "metrics": metrics, "scores": scores}


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

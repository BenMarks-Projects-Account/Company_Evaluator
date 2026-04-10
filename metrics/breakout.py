"""Breakout Potential Score — parallel to composite, not blended.

Identifies companies at inflection points showing patterns that historically
precede major moves higher (mid-cap → mega-cap transitions).

Hard Filters (must pass or score = 0):
  - Market cap $500M–$50B
  - Revenue > $100M TTM
  - Excludes SPACs, blank-check, non-operating structures

Components and weights:
  growth_acceleration      30 %   Revenue/earnings growth ACCELERATING (not just high)
  operating_leverage       25 %   Margins expanding as revenue scales
  reinvestment_quality     15 %   R&D / capex intensity vs peers
  capital_efficiency       15 %   ROIC trending up, positive EVA spread
  smart_money_confirmation 15 %   Insider buying + institutional accumulation
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
    cagr,
)

_log = logging.getLogger(__name__)

_COMPONENT_WEIGHTS = {
    "growth_acceleration":      0.30,
    "operating_leverage":       0.25,
    "reinvestment_quality":     0.15,
    "capital_efficiency":       0.15,
    "smart_money_confirmation": 0.15,
}


def compute_breakout_score(data: dict) -> dict:
    """Compute the breakout potential score for a company.

    Returns dict with:
        score           float | None   0-100 breakout score (None if filtered out)
        filtered_out    bool           True if hard filters exclude this company
        filter_reason   str | None     Why filtered, if applicable
        components      dict           Per-component scores and metrics
    """
    profile = data.get("profile") or {}
    market_cap = profile.get("market_cap")
    quarterly = get_statements(data, "quarterly")
    annual = get_statements(data, "annual")

    # --- Hard Filters ---
    filter_reason = _apply_hard_filters(market_cap, quarterly, profile)
    if filter_reason:
        return {
            "score": None,
            "filtered_out": True,
            "filter_reason": filter_reason,
            "components": {},
        }

    fh = get_finnhub_metrics(data)
    smart_money = data.get("smart_money") or {}
    insiders = data.get("insider_transactions") or {}

    # --- Component 1: Growth Acceleration (30%) ---
    ga_score, ga_metrics = _growth_acceleration(annual, quarterly, fh)

    # --- Component 2: Operating Leverage (25%) ---
    ol_score, ol_metrics = _operating_leverage(annual, quarterly)

    # --- Component 3: Reinvestment Quality (15%) ---
    rq_score, rq_metrics = _reinvestment_quality(quarterly, fh)

    # --- Component 4: Capital Efficiency (15%) ---
    ce_score, ce_metrics = _capital_efficiency(quarterly, annual, fh)

    # --- Component 5: Smart Money Confirmation (15%) ---
    sm_score, sm_metrics = _smart_money_confirmation(smart_money, insiders, fh)

    # --- Weighted composite ---
    items = [
        (ga_score, _COMPONENT_WEIGHTS["growth_acceleration"]),
        (ol_score, _COMPONENT_WEIGHTS["operating_leverage"]),
        (rq_score, _COMPONENT_WEIGHTS["reinvestment_quality"]),
        (ce_score, _COMPONENT_WEIGHTS["capital_efficiency"]),
        (sm_score, _COMPONENT_WEIGHTS["smart_money_confirmation"]),
    ]
    final_score, completeness = weighted_avg(items)

    components = {
        "growth_acceleration":      {"score": ga_score, "weight": 0.30, "metrics": ga_metrics},
        "operating_leverage":       {"score": ol_score, "weight": 0.25, "metrics": ol_metrics},
        "reinvestment_quality":     {"score": rq_score, "weight": 0.15, "metrics": rq_metrics},
        "capital_efficiency":       {"score": ce_score, "weight": 0.15, "metrics": ce_metrics},
        "smart_money_confirmation": {"score": sm_score, "weight": 0.15, "metrics": sm_metrics},
    }

    return {
        "score": round(final_score, 1) if completeness > 0 else None,
        "filtered_out": False,
        "filter_reason": None,
        "completeness_pct": completeness,
        "components": components,
    }


# ---------------------------------------------------------------------------
# Hard filters
# ---------------------------------------------------------------------------

def _apply_hard_filters(market_cap, quarterly, profile) -> str | None:
    """Return a reason string if the company should be excluded, else None."""
    # Market cap filter: $500M–$50B
    if market_cap is None:
        return "missing_market_cap"
    if market_cap < 500_000_000:
        return f"market_cap_too_small ({market_cap/1e6:.0f}M < 500M)"
    if market_cap > 50_000_000_000:
        return f"market_cap_too_large ({market_cap/1e9:.1f}B > 50B)"

    # Revenue filter: >$100M TTM
    rev_ttm = ttm_sum(quarterly, "revenue")
    if rev_ttm is not None and rev_ttm < 100_000_000:
        return f"revenue_too_low ({rev_ttm/1e6:.0f}M < 100M)"

    # SPAC / blank-check exclusion
    name = (profile.get("company_name") or "").upper()
    industry = (profile.get("industry") or "").upper()
    if any(tag in name for tag in ("SPAC", "BLANK CHECK", "ACQUISITION CORP")):
        return "spac_or_blank_check"
    if "BLANK CHECK" in industry or "SHELL COMPANIES" in industry:
        return "non_operating_structure"

    return None


# ---------------------------------------------------------------------------
# Component 1: Growth Acceleration (30%)
# ---------------------------------------------------------------------------

def _growth_acceleration(annual, quarterly, fh) -> tuple[float | None, dict]:
    """Measures whether growth is ACCELERATING — getting better matters more
    than already being good.

    Sub-metrics:
      rev_accel       40%  Recent revenue growth > older revenue growth
      earnings_accel  30%  EPS/operating income growth accelerating
      beat_momentum   30%  Finnhub estimates beat trend
    """
    metrics = {}

    # Revenue acceleration: compare recent 1Y growth to prior 1Y growth
    rev_recent_growth = None
    rev_older_growth = None
    if len(annual) >= 3:
        rev_recent_growth = _yoy_growth(annual, "revenue", 0, 1)
        rev_older_growth = _yoy_growth(annual, "revenue", 1, 2)
    metrics["rev_growth_recent"] = _r(rev_recent_growth)
    metrics["rev_growth_older"] = _r(rev_older_growth)

    rev_accel = None
    if rev_recent_growth is not None and rev_older_growth is not None:
        # Acceleration = recent - older. Positive means accelerating.
        accel_diff = rev_recent_growth - rev_older_growth
        # Also reward absolute recent growth
        # Score: acceleration diff from -10pp to +10pp → 0-100
        accel_score = score(accel_diff, -0.10, 0.10)
        # Absolute recent growth bonus: 0-20% → 0-100
        abs_score = score(rev_recent_growth, 0.0, 0.25)
        if accel_score is not None and abs_score is not None:
            rev_accel = accel_score * 0.6 + abs_score * 0.4
        elif accel_score is not None:
            rev_accel = accel_score
    metrics["rev_accel_score"] = _r(rev_accel)

    # Earnings acceleration: operating income growth
    oi_recent = _yoy_growth(annual, "operating_income", 0, 1) if len(annual) >= 2 else None
    oi_older = _yoy_growth(annual, "operating_income", 1, 2) if len(annual) >= 3 else None
    metrics["oi_growth_recent"] = _r(oi_recent)
    metrics["oi_growth_older"] = _r(oi_older)

    earnings_accel = None
    if oi_recent is not None and oi_older is not None:
        accel_diff = oi_recent - oi_older
        accel_s = score(accel_diff, -0.15, 0.15)
        abs_s = score(oi_recent, 0.0, 0.30)
        if accel_s is not None and abs_s is not None:
            earnings_accel = accel_s * 0.6 + abs_s * 0.4
        elif accel_s is not None:
            earnings_accel = accel_s
    elif oi_recent is not None:
        # No older data — just score absolute growth
        earnings_accel = score(oi_recent, -0.05, 0.30)
    metrics["earnings_accel_score"] = _r(earnings_accel)

    # Beat momentum: use Finnhub eps surprise or growth indicators
    eps_surprise = fh.get("epsNormalizedAnnual")
    eps_growth_ttm = fh.get("epsGrowthTTMYoy")
    eps_growth_3y = fh.get("epsGrowth3Y")
    beat_score = None
    if eps_growth_ttm is not None and eps_growth_3y is not None:
        # If TTM growth > 3Y growth, earnings are accelerating
        accel = eps_growth_ttm - eps_growth_3y
        beat_score = score(accel, -10.0, 15.0)
    elif eps_growth_ttm is not None:
        beat_score = score(eps_growth_ttm, -5.0, 25.0)
    metrics["beat_momentum_score"] = _r(beat_score)

    items = [
        (rev_accel, 0.40),
        (earnings_accel, 0.30),
        (beat_score, 0.30),
    ]
    component_score, _ = weighted_avg(items)
    return (_r(component_score), metrics) if any(s is not None for s, _ in items) else (None, metrics)


# ---------------------------------------------------------------------------
# Component 2: Operating Leverage (25%)
# ---------------------------------------------------------------------------

def _operating_leverage(annual, quarterly) -> tuple[float | None, dict]:
    """Measures margin expansion as revenue scales.

    Sub-metrics:
      gross_margin_trend   35%  Gross margin improving over 3 years
      op_margin_trend      35%  Operating margin improving over 3 years
      incremental_margin   30%  Incremental operating margin (margin on new revenue)
    """
    metrics = {}

    # Gross margin trend: most recent vs 2-3 years ago
    gm_new = _margin(annual, 0, "gross_profit", "revenue")
    gm_old = _margin(annual, 2, "gross_profit", "revenue") if len(annual) >= 3 else None
    gm_trend = (gm_new - gm_old) if (gm_new is not None and gm_old is not None) else None
    metrics["gross_margin_current"] = _r(gm_new)
    metrics["gross_margin_3y_ago"] = _r(gm_old)
    metrics["gross_margin_trend"] = _r(gm_trend)
    gm_score = score(gm_trend, -0.05, 0.08)  # -5pp to +8pp

    # Operating margin trend
    om_new = _margin(annual, 0, "operating_income", "revenue")
    om_old = _margin(annual, 2, "operating_income", "revenue") if len(annual) >= 3 else None
    om_trend = (om_new - om_old) if (om_new is not None and om_old is not None) else None
    metrics["op_margin_current"] = _r(om_new)
    metrics["op_margin_3y_ago"] = _r(om_old)
    metrics["op_margin_trend"] = _r(om_trend)
    om_score = score(om_trend, -0.05, 0.08)

    # Incremental operating margin: ΔOI / ΔRevenue over most recent year
    inc_margin = None
    if len(annual) >= 2:
        rev_new = annual[0].get("revenue")
        rev_old = annual[1].get("revenue")
        oi_new = annual[0].get("operating_income")
        oi_old = annual[1].get("operating_income")
        d_rev = (rev_new - rev_old) if (rev_new is not None and rev_old is not None) else None
        d_oi = (oi_new - oi_old) if (oi_new is not None and oi_old is not None) else None
        if d_rev is not None and d_oi is not None and d_rev > 0:
            inc_margin = d_oi / d_rev
    metrics["incremental_margin"] = _r(inc_margin)
    inc_score = score(inc_margin, -0.10, 0.40)  # -10% to 40%

    items = [
        (gm_score, 0.35),
        (om_score, 0.35),
        (inc_score, 0.30),
    ]
    component_score, _ = weighted_avg(items)
    return (_r(component_score), metrics) if any(s is not None for s, _ in items) else (None, metrics)


# ---------------------------------------------------------------------------
# Component 3: Reinvestment Quality (15%)
# ---------------------------------------------------------------------------

def _reinvestment_quality(quarterly, fh) -> tuple[float | None, dict]:
    """Measures quality of reinvestment into future growth.

    Sub-metrics:
      rd_intensity      40%  R&D / Revenue (higher = investing in moat)
      capex_intensity   30%  Capex / Revenue (building scale)
      rd_growth         30%  R&D spending growth (accelerating investment)
    """
    metrics = {}

    rev_ttm = ttm_sum(quarterly, "revenue")
    rd_ttm = ttm_sum(quarterly, "research_and_development")
    capex_ttm = ttm_sum(quarterly, "capital_expenditure")

    rd_intensity = safe_div(rd_ttm, rev_ttm)
    capex_intensity = safe_div(abs(capex_ttm) if capex_ttm else None, rev_ttm)

    metrics["rd_intensity"] = _r(rd_intensity)
    metrics["capex_intensity"] = _r(capex_intensity)

    # R&D intensity: 0-15% → 0-100 (higher is more future-invested)
    rd_score = score(rd_intensity, 0.0, 0.15)

    # Capex intensity: 3-15% is the sweet spot for growth companies
    capex_score = score(capex_intensity, 0.01, 0.12)

    # R&D growth: compare TTM R&D to older quarters
    rd_growth = None
    if len(quarterly) >= 8 and rd_ttm is not None:
        rd_older = sum(
            s.get("research_and_development", 0) or 0
            for s in quarterly[4:8]
        )
        if rd_older > 0:
            rd_growth = (rd_ttm - rd_older) / rd_older
    metrics["rd_growth"] = _r(rd_growth)
    rd_growth_score = score(rd_growth, -0.05, 0.25)

    items = [
        (rd_score, 0.40),
        (capex_score, 0.30),
        (rd_growth_score, 0.30),
    ]
    component_score, _ = weighted_avg(items)
    return (_r(component_score), metrics) if any(s is not None for s, _ in items) else (None, metrics)


# ---------------------------------------------------------------------------
# Component 4: Capital Efficiency (15%)
# ---------------------------------------------------------------------------

def _capital_efficiency(quarterly, annual, fh) -> tuple[float | None, dict]:
    """Measures improving capital efficiency — ROIC trending up.

    Sub-metrics:
      roic_level        40%  Current ROIC level
      roic_trend        35%  ROIC improving over time
      asset_turnover    25%  Revenue / Total Assets trending up
    """
    metrics = {}

    # Current ROIC (TTM)
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
    roic_current = safe_div(nopat, avg_ic)

    # Fallback to Finnhub
    if roic_current is None:
        v = fh.get("roicTTM")
        roic_current = v / 100 if v is not None else None
    metrics["roic_current"] = _r(roic_current)

    # ROIC from ~2 years ago (using annual statements)
    roic_old = None
    if len(annual) >= 3:
        oi_old = annual[2].get("operating_income")
        ta_old = annual[2].get("total_assets")
        cl_old = annual[2].get("current_liabilities")
        if oi_old is not None and ta_old is not None and cl_old is not None:
            ic_old = ta_old - cl_old
            if ic_old > 0:
                roic_old = (oi_old * (1 - (tax_rate or 0.21))) / ic_old
    metrics["roic_2y_ago"] = _r(roic_old)

    roic_trend = None
    if roic_current is not None and roic_old is not None:
        roic_trend = roic_current - roic_old
    metrics["roic_trend"] = _r(roic_trend)

    roic_score = score(roic_current, 0.0, 0.25)  # 0-25% ROIC → 0-100
    roic_trend_score = score(roic_trend, -0.05, 0.10)  # -5pp to +10pp improvement

    # Asset turnover trend
    at_new = None
    at_old = None
    if len(annual) >= 1:
        at_new = safe_div(annual[0].get("revenue"), annual[0].get("total_assets"))
    if len(annual) >= 3:
        at_old = safe_div(annual[2].get("revenue"), annual[2].get("total_assets"))
    at_trend = (at_new - at_old) if (at_new is not None and at_old is not None) else None
    metrics["asset_turnover_current"] = _r(at_new)
    metrics["asset_turnover_trend"] = _r(at_trend)
    at_score = score(at_trend, -0.10, 0.10)

    items = [
        (roic_score, 0.40),
        (roic_trend_score, 0.35),
        (at_score, 0.25),
    ]
    component_score, _ = weighted_avg(items)
    return (_r(component_score), metrics) if any(s is not None for s, _ in items) else (None, metrics)


# ---------------------------------------------------------------------------
# Component 5: Smart Money Confirmation (15%)
# ---------------------------------------------------------------------------

def _smart_money_confirmation(smart_money, insiders, fh) -> tuple[float | None, dict]:
    """Insider buying + analyst sentiment as confirmation signals.

    Sub-metrics:
      insider_signal    50%  Net insider buying activity
      analyst_revisions 50%  Positive estimate revisions / recommendations
    """
    metrics = {}

    # Insider signal from FMP smart money (preferred) or Finnhub
    insider_activity = smart_money.get("insider_activity") or {}
    insider_score_val = None
    if insider_activity.get("score") is not None:
        insider_score_val = insider_activity["score"]
        metrics["insider_signal"] = insider_activity.get("signal", "unknown")
        metrics["insider_source"] = "fmp"
    else:
        # Basic Finnhub insider signal
        insider_score_val = _score_finnhub_insiders(insiders)
        metrics["insider_signal"] = insiders.get("net_activity", "unknown")
        metrics["insider_source"] = "finnhub"
    metrics["insider_score"] = _r(insider_score_val)

    # Analyst estimate revisions as a proxy for momentum
    # Use Finnhub recommendation trends
    rec_buy = fh.get("recommendationBuy") or 0
    rec_strong_buy = fh.get("recommendationStrongBuy") or 0
    rec_hold = fh.get("recommendationHold") or 0
    rec_sell = fh.get("recommendationSell") or 0
    rec_strong_sell = fh.get("recommendationStrongSell") or 0
    total_recs = rec_buy + rec_strong_buy + rec_hold + rec_sell + rec_strong_sell

    analyst_score = None
    if total_recs > 0:
        # % bullish (buy + strong_buy) out of total
        pct_bullish = (rec_buy + rec_strong_buy) / total_recs
        analyst_score = score(pct_bullish, 0.30, 0.80)
        metrics["pct_bullish_analysts"] = _r(pct_bullish)
        metrics["total_analyst_ratings"] = total_recs
    else:
        metrics["pct_bullish_analysts"] = None
        metrics["total_analyst_ratings"] = 0

    items = [
        (insider_score_val, 0.50),
        (analyst_score, 0.50),
    ]
    component_score, _ = weighted_avg(items)
    return (_r(component_score), metrics) if any(s is not None for s, _ in items) else (None, metrics)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _yoy_growth(statements, field, idx_new, idx_old) -> float | None:
    """Year-over-year growth between two annual statement indices."""
    if idx_new >= len(statements) or idx_old >= len(statements):
        return None
    new = statements[idx_new].get(field)
    old = statements[idx_old].get(field)
    if new is None or old is None or old == 0:
        return None
    return (new - old) / abs(old)


def _margin(statements, idx, numerator_field, denominator_field) -> float | None:
    """Compute margin ratio at a given statement index."""
    if idx >= len(statements):
        return None
    num = statements[idx].get(numerator_field)
    den = statements[idx].get(denominator_field)
    return safe_div(num, den)


def _score_finnhub_insiders(insiders: dict) -> float | None:
    """Simple scoring for Finnhub insider transaction data."""
    signal = (insiders.get("net_activity") or "").lower()
    signal_map = {
        "strong_buying": 90.0,
        "buying": 75.0,
        "weak_buying": 60.0,
        "neutral": 50.0,
        "routine_selling": 45.0,
        "weak_selling": 35.0,
        "selling": 20.0,
        "strong_selling": 10.0,
    }
    return signal_map.get(signal)


def _r(val) -> float | None:
    """Round a value to 4 decimal places, or return None."""
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None

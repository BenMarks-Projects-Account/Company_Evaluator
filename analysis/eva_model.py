"""Economic Value Added (EVA) / ROIC analysis — on-demand valuation.

Measures whether a company creates or destroys shareholder value by
comparing Return on Invested Capital (ROIC) to Weighted Average Cost
of Capital (WACC).  If ROIC > WACC the company creates value.  If
ROIC < WACC it destroys value regardless of reported profitability.

NOT integrated into the crawler or pillar scoring — runs on demand only.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone

from config import get_settings
from data.finnhub_client import FinnhubClient
from analysis.llm_client import call_llm

_log = logging.getLogger(__name__)

# ── Defaults ─────────────────────────────────────────────────

DEFAULTS = {
    "beta": 1.0,
    "effective_tax_rate": 0.21,
    "risk_free_rate": 0.04,
    "equity_risk_premium": 0.055,
    "cost_of_debt": 0.05,
    "operating_margin": 0.15,
}

COURTESY_DELAY = 0.05


# ═════════════════════════════════════════════════════════════
#  Public entry point
# ═════════════════════════════════════════════════════════════

async def analyze_eva(symbol: str, *, skip_llm: bool = False) -> dict:
    """Run EVA/ROIC analysis for *symbol*.

    Returns dict with keys: ok, symbol, current_price, roic_analysis,
    wacc, eva, quality, implied_valuation, comparison, verdict,
    confidence, llm_available, llm_analysis, analyzed_at.
    """
    symbol = symbol.upper().strip()
    settings = get_settings()
    fh = FinnhubClient(api_key=settings.finnhub_api_key, rate_limit=settings.finnhub_rate_limit)

    # ── 1. Fetch data ────────────────────────────────────────
    profile_task = asyncio.create_task(fh.get_company_profile(symbol))
    metrics_task = asyncio.create_task(fh.get_basic_financials(symbol))

    profile = await profile_task
    if profile.get("error") or not profile.get("market_cap"):
        return {"ok": False, "symbol": symbol, "error": "Could not fetch company profile"}

    fin_data = await metrics_task
    metrics = fin_data.get("metrics", {})
    if not metrics:
        return {"ok": False, "symbol": symbol, "error": "No financial metrics available"}

    # ── 2. Extract base data ─────────────────────────────────
    base, defaults_used = _extract_base_data(profile, metrics)

    # ── 3. Compute ROIC ──────────────────────────────────────
    roic_data = _compute_roic(base, defaults_used)
    if roic_data["roic"] is None:
        return _insufficient_data_result(
            symbol, base.get("current_price"),
            "Cannot compute ROIC — missing operating income or invested capital data",
        )

    # Guard: negative invested capital
    if roic_data["invested_capital"] is not None and roic_data["invested_capital"] <= 0:
        return _insufficient_data_result(
            symbol, base.get("current_price"),
            "Negative or zero invested capital — EVA model not applicable",
        )

    # ── 4. Compute WACC ──────────────────────────────────────
    wacc_data = _compute_wacc(base, defaults_used)

    # ── 5. Compute EVA ───────────────────────────────────────
    eva_data = _compute_eva(roic_data, wacc_data["wacc"], base)

    # ── 6. Quality assessment ────────────────────────────────
    quality = _assess_quality(roic_data["roic"], wacc_data["wacc"], base, roic_data)

    # ── 7. EVA-implied valuation ─────────────────────────────
    implied = _eva_implied_value(eva_data, wacc_data["wacc"], base)

    # ── 8. Comparison metrics ────────────────────────────────
    comparison = {
        "roic": round(roic_data["roic"], 4),
        "roic_pct": f"{roic_data['roic']:.1%}",
        "roe": round(base["roe"], 4) if base["roe"] else None,
        "roe_pct": f"{base['roe']:.1%}" if base["roe"] else None,
        "roa": round(base["roa"], 4) if base["roa"] else None,
        "roa_pct": f"{base['roa']:.1%}" if base["roa"] else None,
        "wacc": round(wacc_data["wacc"], 4),
        "wacc_pct": f"{wacc_data['wacc']:.1%}",
        "spread": round(roic_data["roic"] - wacc_data["wacc"], 4),
        "spread_pct": f"{roic_data['roic'] - wacc_data['wacc']:.1%}",
    }

    # ── 9. Verdict ───────────────────────────────────────────
    spread = roic_data["roic"] - wacc_data["wacc"]
    current_price = base["current_price"]

    valuation_verdict = "INSUFFICIENT_DATA"
    if implied.get("per_share") and current_price and current_price > 0:
        upside = (implied["per_share"] / current_price - 1) * 100
        if upside > 25:
            valuation_verdict = "UNDERVALUED"
        elif upside > 5:
            valuation_verdict = "SLIGHTLY_UNDERVALUED"
        elif upside >= -10:
            valuation_verdict = "FAIRLY_VALUED"
        elif upside >= -25:
            valuation_verdict = "SLIGHTLY_OVERVALUED"
        else:
            valuation_verdict = "OVERVALUED"
    else:
        upside = None

    verdict = {
        "status": quality["grade"] + "_VALUE_CREATOR" if spread > 0 else "VALUE_DESTROYER",
        "value_creation": "positive" if spread > 0 else "negative",
        "valuation": valuation_verdict,
        "summary": (
            f"{symbol} {'creates' if spread > 0 else 'destroys'} value with "
            f"ROIC of {roic_data['roic']:.1%} vs WACC of {wacc_data['wacc']:.1%} "
            f"({spread:+.1%} spread). "
            + (f"EVA-implied value ${implied['per_share']:.2f} vs "
               f"current ${current_price:.2f} ({upside:+.1f}% upside)."
               if implied.get("per_share") and current_price else
               "Insufficient data for implied valuation.")
        ),
    }

    # ── 10. Confidence ───────────────────────────────────────
    confidence = _assess_confidence(defaults_used, roic_data, base)

    # ── 11. LLM narrative ────────────────────────────────────
    llm_analysis = None
    llm_recommendation = None
    if not skip_llm:
        llm_result = await _llm_eva_narrative(
            symbol, profile.get("company_name", symbol),
            current_price, roic_data, wacc_data, eva_data,
            quality, implied, base, confidence,
        )
        if llm_result:
            llm_analysis = llm_result.get("analysis")
            llm_recommendation = llm_result.get("recommendation")

    # ── 12. Build response ───────────────────────────────────
    shares_m = base.get("shares_outstanding_m")
    return {
        "ok": True,
        "symbol": symbol,
        "current_price": round(current_price, 2) if current_price else None,
        "grade": quality["grade"],
        "capital_structure": {
            "invested_capital": _fmt(roic_data["invested_capital"]),
            "total_debt": _fmt(roic_data["total_debt"]),
            "total_equity": _fmt(roic_data["total_equity"]),
            "cash_and_equivalents": _fmt(roic_data["cash"]),
            "net_debt": _fmt(roic_data["total_debt"] - roic_data["cash"])
                        if roic_data["total_debt"] is not None and roic_data["cash"] is not None else None,
            "nopat": _fmt(roic_data["nopat"]),
            "operating_income": _fmt(roic_data["operating_income"]),
            "tax_rate": round(roic_data["tax_rate"], 4),
            "shares_outstanding": round(shares_m * 1_000_000) if shares_m else None,
        },
        "roic_analysis": {
            "roic": round(roic_data["roic"], 4),
            "roic_pct": f"{roic_data['roic']:.1%}",
            "nopat": _fmt(roic_data["nopat"]),
            "invested_capital": _fmt(roic_data["invested_capital"]),
            "operating_income": _fmt(roic_data["operating_income"]),
            "tax_rate": round(roic_data["tax_rate"], 4),
            "total_debt": _fmt(roic_data["total_debt"]),
            "total_equity": _fmt(roic_data["total_equity"]),
            "cash": _fmt(roic_data["cash"]),
            "roic_source": roic_data["roic_source"],
        },
        "wacc": {
            "wacc": round(wacc_data["wacc"], 4),
            "wacc_pct": f"{wacc_data['wacc']:.1%}",
            "cost_of_equity": round(wacc_data["cost_of_equity"], 4),
            "cost_of_debt": round(wacc_data["cost_of_debt"], 4),
            "beta": round(base["beta"], 4),
            "equity_weight": round(wacc_data["equity_weight"], 4),
            "debt_weight": round(wacc_data["debt_weight"], 4),
        },
        "eva": {
            "value_spread": round(eva_data["value_spread"], 4),
            "value_spread_pct": f"{eva_data['value_spread']:.1%}",
            "eva_annual": _fmt(eva_data["eva"]),
            "eva_per_share": round(eva_data["eva_per_share"], 2) if eva_data["eva_per_share"] else None,
            "creates_value": eva_data["creates_value"],
        },
        "quality": quality,
        "implied_valuation": {
            "ev_implied": _fmt(implied.get("ev_implied")),
            "equity_value": _fmt(implied.get("equity_value")),
            "per_share": round(implied["per_share"], 2) if implied.get("per_share") else None,
            "current_price": round(current_price, 2) if current_price else None,
            "upside_pct": round(upside, 1) if upside is not None else None,
            "note": "EVA perpetuity valuation — assumes current EVA is sustainable but does not grow",
        },
        "comparison": comparison,
        "verdict": verdict,
        "confidence": confidence,
        "defaults_used": defaults_used,
        "llm_available": llm_analysis is not None,
        "llm_analysis": llm_analysis,
        "llm_recommendation": llm_recommendation,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


# ═════════════════════════════════════════════════════════════
#  Stage 1: Extract base data from Finnhub
# ═════════════════════════════════════════════════════════════

def _extract_base_data(profile: dict, metrics: dict) -> tuple[dict, list[str]]:
    """Pull EVA inputs from Finnhub profile + metrics.

    Returns (base_dict, defaults_used_list).
    """
    defaults_used: list[str] = []

    # Market cap + shares from profile (both in millions)
    market_cap_m = metrics.get("marketCapitalization") or profile.get("market_cap") or 0
    market_cap = market_cap_m * 1_000_000  # absolute USD

    shares_m = profile.get("shares_outstanding") or 0  # millions
    shares = shares_m * 1_000_000  # absolute shares

    # Current price derived from market_cap / shares (both millions → $/share)
    current_price = None
    if market_cap_m and shares_m and shares_m > 0:
        current_price = market_cap_m / shares_m

    # Revenue (absolute)
    rev_per_share = metrics.get("revenuePerShareTTM") or metrics.get("revenuePerShareAnnual") or 0
    revenue = rev_per_share * shares if shares > 0 else 0

    # Operating margin → operating income
    op_margin = (
        metrics.get("operatingMarginTTM")
        or metrics.get("operatingMarginAnnual")
    )
    if op_margin is not None:
        op_margin = op_margin / 100  # Finnhub returns as percentage
    else:
        op_margin = DEFAULTS["operating_margin"]
        defaults_used.append("operating_margin")

    operating_income = revenue * op_margin if revenue else 0

    # EBITDA (absolute)
    ebitda_per_share = metrics.get("ebitdPerShareTTM") or metrics.get("ebitdPerShareAnnual") or 0
    ebitda = ebitda_per_share * shares if shares > 0 else 0

    # EPS → Net Income
    eps = metrics.get("epsTTM") or metrics.get("epsAnnual") or 0
    net_income = eps * shares if shares > 0 else 0

    # FCF (absolute)
    fcf_per_share = metrics.get("cashFlowPerShareTTM") or metrics.get("cashFlowPerShareAnnual") or 0
    fcf = fcf_per_share * shares if shares > 0 else 0

    # Book value → total equity (absolute)
    bv_per_share = metrics.get("bookValuePerShareQuarterly") or metrics.get("bookValuePerShareAnnual") or 0
    total_equity = bv_per_share * shares if shares > 0 else 0

    # Cash (absolute)
    cash_per_share = metrics.get("cashPerSharePerShareQuarterly") or metrics.get("cashPerSharePerShareAnnual") or 0
    cash = cash_per_share * shares if shares > 0 else 0

    # Debt: derive from D/E ratio × equity
    de_ratio = metrics.get("totalDebt/totalEquityQuarterly") or metrics.get("totalDebt/totalEquityAnnual")
    if de_ratio is not None and total_equity > 0:
        total_debt = (de_ratio / 100) * total_equity if de_ratio > 5 else de_ratio * total_equity
        # Finnhub D/E ratios > 5 are likely already in percentage form
    else:
        # Fallback: EV - market_cap as rough debt estimate
        ev_m = metrics.get("enterpriseValue") or 0
        ev = ev_m * 1_000_000
        total_debt = max(0, ev - market_cap) if ev > 0 else 0

    # Beta
    beta = metrics.get("beta")
    if not beta or beta <= 0:
        beta = DEFAULTS["beta"]
        defaults_used.append("beta")

    # Tax rate proxy: pretax margin vs net margin
    pretax_margin = metrics.get("pretaxMarginTTM") or metrics.get("pretaxMarginAnnual")
    net_margin = metrics.get("netProfitMarginTTM") or metrics.get("netProfitMarginAnnual")
    if pretax_margin and net_margin and pretax_margin > 0:
        effective_tax_rate = 1 - (net_margin / pretax_margin)
        effective_tax_rate = max(0.0, min(0.50, effective_tax_rate))
    else:
        effective_tax_rate = DEFAULTS["effective_tax_rate"]
        defaults_used.append("effective_tax_rate")

    # ROIC pre-computed (Finnhub calls it ROI)
    roi_ttm = metrics.get("roiTTM")
    roi_annual = metrics.get("roiAnnual")
    roi_5y = metrics.get("roi5Y")
    finnhub_roic = None
    if roi_ttm is not None:
        finnhub_roic = roi_ttm / 100
    elif roi_annual is not None:
        finnhub_roic = roi_annual / 100

    # ROE, ROA (as decimals)
    roe_raw = metrics.get("roeTTM") or metrics.get("roeRfy")
    roe = roe_raw / 100 if roe_raw is not None else None

    roa_raw = metrics.get("roaTTM") or metrics.get("roaRfy")
    roa = roa_raw / 100 if roa_raw is not None else None

    # Margins (as decimals)
    gross_margin = metrics.get("grossMarginTTM") or metrics.get("grossMarginAnnual")
    if gross_margin is not None:
        gross_margin = gross_margin / 100

    net_margin_dec = net_margin / 100 if net_margin else None

    # Enterprise value
    ev_m = metrics.get("enterpriseValue") or 0
    enterprise_value = ev_m * 1_000_000

    # Asset turnover
    asset_turnover = metrics.get("assetTurnoverTTM") or metrics.get("assetTurnoverAnnual")

    # CapEx CAGR
    capex_cagr_5y = metrics.get("capexCagr5Y")

    # Interest coverage
    interest_coverage = metrics.get("netInterestCoverageTTM") or metrics.get("netInterestCoverageAnnual")

    return {
        "current_price": current_price,
        "market_cap": market_cap,
        "shares_outstanding": shares,
        "shares_outstanding_m": shares_m,
        "revenue": revenue,
        "operating_income": operating_income,
        "operating_margin": op_margin,
        "ebitda": ebitda,
        "net_income": net_income,
        "fcf": fcf,
        "eps": eps,
        "total_equity": total_equity,
        "total_debt": total_debt,
        "cash": cash,
        "beta": beta,
        "effective_tax_rate": effective_tax_rate,
        "finnhub_roic": finnhub_roic,
        "finnhub_roic_5y": roi_5y / 100 if roi_5y is not None else None,
        "roe": roe,
        "roa": roa,
        "gross_margin": gross_margin,
        "net_margin": net_margin_dec,
        "enterprise_value": enterprise_value,
        "asset_turnover": asset_turnover,
        "capex_cagr_5y": capex_cagr_5y,
        "interest_coverage": interest_coverage,
    }, defaults_used


# ═════════════════════════════════════════════════════════════
#  Stage 2: Compute ROIC
# ═════════════════════════════════════════════════════════════

def _compute_roic(base: dict, defaults_used: list[str]) -> dict:
    """ROIC = NOPAT / Invested Capital.

    NOPAT = Operating Income × (1 - Tax Rate)
    Invested Capital = Total Debt + Total Equity - Cash
    """
    operating_income = base["operating_income"]
    tax_rate = base["effective_tax_rate"]
    total_debt = base["total_debt"]
    total_equity = base["total_equity"]
    cash = base["cash"]

    nopat = operating_income * (1 - tax_rate) if operating_income else None
    invested_capital = total_debt + total_equity - cash

    # Compute ROIC from raw data
    computed_roic = None
    roic_source = "computed"
    if nopat and invested_capital and invested_capital > 0:
        computed_roic = nopat / invested_capital

    # Use Finnhub pre-computed ROIC as primary (it's from actual statements)
    # Fall back to computed if not available
    finnhub_roic = base["finnhub_roic"]
    if finnhub_roic is not None:
        roic = finnhub_roic
        roic_source = "finnhub_roiTTM"
    elif computed_roic is not None:
        roic = computed_roic
        roic_source = "computed_nopat_over_ic"
    else:
        roic = None
        roic_source = "unavailable"

    return {
        "roic": roic,
        "roic_computed": computed_roic,
        "roic_finnhub": finnhub_roic,
        "roic_source": roic_source,
        "nopat": nopat,
        "invested_capital": invested_capital,
        "operating_income": operating_income,
        "tax_rate": tax_rate,
        "total_debt": total_debt,
        "total_equity": total_equity,
        "cash": cash,
    }


# ═════════════════════════════════════════════════════════════
#  Stage 3: Compute WACC (same methodology as DCF model)
# ═════════════════════════════════════════════════════════════

def _compute_wacc(base: dict, defaults_used: list[str]) -> dict:
    """CAPM-based WACC: Re = Rf + beta * ERP, weighted with after-tax Rd."""
    risk_free = DEFAULTS["risk_free_rate"]
    erp = DEFAULTS["equity_risk_premium"]
    beta = base["beta"]
    tax_rate = base["effective_tax_rate"]
    market_cap = base["market_cap"]
    total_debt = base["total_debt"]

    cost_of_equity = risk_free + beta * erp

    # Cost of debt: estimate from interest coverage if available
    coverage = base.get("interest_coverage")
    if coverage and coverage > 0:
        if coverage > 20:
            cost_of_debt = 0.035
        elif coverage > 10:
            cost_of_debt = 0.04
        elif coverage > 5:
            cost_of_debt = 0.05
        elif coverage > 2:
            cost_of_debt = 0.065
        else:
            cost_of_debt = 0.08
    else:
        cost_of_debt = DEFAULTS["cost_of_debt"]
        if "cost_of_debt" not in defaults_used:
            defaults_used.append("cost_of_debt")

    total_value = market_cap + total_debt
    if total_value <= 0:
        equity_weight = 1.0
        debt_weight = 0.0
    else:
        equity_weight = market_cap / total_value
        debt_weight = total_debt / total_value

    wacc = (equity_weight * cost_of_equity) + (debt_weight * cost_of_debt * (1 - tax_rate))
    wacc = max(0.04, min(0.20, wacc))

    return {
        "wacc": wacc,
        "cost_of_equity": cost_of_equity,
        "cost_of_debt": cost_of_debt,
        "equity_weight": equity_weight,
        "debt_weight": debt_weight,
    }


# ═════════════════════════════════════════════════════════════
#  Stage 4: Compute EVA
# ═════════════════════════════════════════════════════════════

def _compute_eva(roic_data: dict, wacc: float, base: dict) -> dict:
    """EVA = Invested Capital * (ROIC - WACC)."""
    roic = roic_data["roic"]
    ic = roic_data["invested_capital"]
    shares = base["shares_outstanding"]

    value_spread = roic - wacc
    eva = ic * value_spread if ic and ic > 0 else 0

    eva_per_share = None
    if shares and shares > 0 and eva:
        eva_per_share = eva / shares

    return {
        "eva": eva,
        "value_spread": value_spread,
        "creates_value": value_spread > 0,
        "eva_per_share": eva_per_share,
        "roic": roic,
        "wacc": wacc,
        "invested_capital": ic,
    }


# ═════════════════════════════════════════════════════════════
#  Stage 5: Quality assessment
# ═════════════════════════════════════════════════════════════

def _assess_quality(roic: float, wacc: float, base: dict, roic_data: dict) -> dict:
    """Grade value creation quality based on ROIC-WACC spread."""
    spread = roic - wacc

    if spread > 0.15:
        grade, description = "ELITE", "Exceptional value creator — earns far above cost of capital"
    elif spread > 0.08:
        grade, description = "STRONG", "Strong value creator — significant excess returns"
    elif spread > 0.03:
        grade, description = "GOOD", "Solid value creator — meaningful excess returns"
    elif spread > 0:
        grade, description = "MARGINAL", "Creates value but barely — limited competitive moat"
    else:
        grade, description = "DESTROYING", "Destroys shareholder value — earns below cost of capital"

    # Quality signals
    signals = []

    roe = base.get("roe")
    if roe is not None:
        if roe > 0.20:
            signals.append({"signal": f"ROE {roe:.0%} — strong equity returns", "direction": "positive"})
        elif roe < 0.08:
            signals.append({"signal": f"ROE {roe:.0%} — weak equity returns", "direction": "negative"})

    roa = base.get("roa")
    if roa is not None:
        if roa > 0.10:
            signals.append({"signal": f"ROA {roa:.0%} — efficient asset utilization", "direction": "positive"})
        elif roa < 0.03:
            signals.append({"signal": f"ROA {roa:.0%} — poor asset utilization", "direction": "negative"})

    # Asset turnover
    at = base.get("asset_turnover")
    if at is not None:
        if at > 1.0:
            signals.append({"signal": f"Asset turnover {at:.2f}x — efficient capital use", "direction": "positive"})
        elif at < 0.3:
            signals.append({"signal": f"Asset turnover {at:.2f}x — capital intensive", "direction": "negative"})

    # Operating margin
    op_margin = base.get("operating_margin")
    if op_margin is not None:
        if op_margin > 0.25:
            signals.append({"signal": f"Operating margin {op_margin:.0%} — strong pricing power", "direction": "positive"})
        elif op_margin < 0.10:
            signals.append({"signal": f"Operating margin {op_margin:.0%} — thin margins", "direction": "negative"})

    # Gross margin
    gm = base.get("gross_margin")
    if gm is not None:
        if gm > 0.60:
            signals.append({"signal": f"Gross margin {gm:.0%} — high value-add", "direction": "positive"})
        elif gm < 0.25:
            signals.append({"signal": f"Gross margin {gm:.0%} — commodity-like", "direction": "negative"})

    # Finnhub ROIC consistency (TTM vs 5Y)
    roic_5y = base.get("finnhub_roic_5y")
    if roic_5y is not None and roic is not None:
        if roic > roic_5y * 1.2:
            signals.append({"signal": f"ROIC trending up ({roic_5y:.0%} 5Y avg -> {roic:.0%} TTM)", "direction": "positive"})
        elif roic < roic_5y * 0.8:
            signals.append({"signal": f"ROIC declining ({roic_5y:.0%} 5Y avg -> {roic:.0%} TTM)", "direction": "warning"})
        else:
            signals.append({"signal": f"ROIC stable ({roic_5y:.0%} 5Y avg vs {roic:.0%} TTM)", "direction": "positive"})

    # Interest coverage
    ic = base.get("interest_coverage")
    if ic is not None:
        if ic > 20:
            signals.append({"signal": f"Interest coverage {ic:.0f}x — minimal debt burden", "direction": "positive"})
        elif ic < 3:
            signals.append({"signal": f"Interest coverage {ic:.1f}x — high debt burden", "direction": "negative"})

    return {
        "grade": grade,
        "description": description,
        "value_spread": round(spread, 4),
        "value_spread_pct": f"{spread:.1%}",
        "signals": signals,
        "score": _spread_to_score(spread),
    }


def _spread_to_score(spread: float) -> int:
    """Map ROIC-WACC spread to a 0-100 score."""
    if spread >= 0.20:
        return 98
    if spread >= 0.15:
        return 92
    if spread >= 0.10:
        return 85
    if spread >= 0.05:
        return 75
    if spread >= 0.02:
        return 65
    if spread >= 0:
        return 55
    if spread >= -0.05:
        return 35
    if spread >= -0.10:
        return 20
    return 10


# ═════════════════════════════════════════════════════════════
#  Stage 6: EVA-implied valuation
# ═════════════════════════════════════════════════════════════

def _eva_implied_value(eva_data: dict, wacc: float, base: dict) -> dict:
    """Market Value = Invested Capital + PV of future EVAs (perpetuity).

    Enterprise Value = IC + EVA / WACC
    Equity Value = EV - Net Debt
    Per Share = Equity Value / Shares
    """
    eva = eva_data["eva"]
    ic = eva_data["invested_capital"]
    total_debt = base["total_debt"]
    cash = base["cash"]
    shares = base["shares_outstanding"]
    net_debt = total_debt - cash

    if wacc <= 0 or ic is None:
        return {}

    ev_implied = ic + (eva / wacc) if wacc > 0 else ic
    equity_value = ev_implied - net_debt

    per_share = None
    if shares and shares > 0:
        per_share = equity_value / shares

    return {
        "ev_implied": ev_implied,
        "equity_value": equity_value,
        "per_share": per_share,
        "net_debt": net_debt,
    }


# ═════════════════════════════════════════════════════════════
#  Confidence assessment
# ═════════════════════════════════════════════════════════════

def _assess_confidence(defaults_used: list[str], roic_data: dict, base: dict) -> str:
    """Rate confidence as HIGH / MEDIUM / LOW."""
    issues = len(defaults_used)

    # Penalise if ROIC came from computation only (no Finnhub validation)
    if roic_data["roic_source"] == "computed_nopat_over_ic" and roic_data["roic_finnhub"] is None:
        issues += 1

    # Penalise if invested capital is very small relative to market cap
    ic = roic_data["invested_capital"] or 0
    mc = base.get("market_cap", 0)
    if mc > 0 and ic > 0 and ic < mc * 0.05:
        issues += 1  # asset-light company — ROIC can be misleadingly high

    if issues <= 1:
        return "HIGH"
    if issues <= 3:
        return "MEDIUM"
    return "LOW"


# ═════════════════════════════════════════════════════════════
#  LLM narrative
# ═════════════════════════════════════════════════════════════

async def _llm_eva_narrative(
    symbol, name, current_price, roic_data, wacc_data, eva_data,
    quality, implied, base, confidence,
) -> dict | None:
    """Ask LLM for EVA/ROIC analysis commentary."""
    import json as _json

    system = (
        "You are a capital allocation analyst reviewing an EVA/ROIC analysis. "
        "You MUST respond with a single JSON object only. No markdown, no explanation outside JSON."
    )

    roic = roic_data["roic"]
    wacc = wacc_data["wacc"]
    spread = roic - wacc

    user = (
        f"EVA Results for {name} ({symbol}):\n"
        f"- ROIC: {roic:.1%} vs WACC: {wacc:.1%}\n"
        f"- Value spread: {spread:+.1%}\n"
        f"- Annual EVA: ${eva_data['eva']/1e9:.1f}B\n"
        f"- Quality grade: {quality['grade']}\n"
        f"- ROE: {base['roe']:.0%}, ROA: {base['roa']:.0%}\n"
        f"- Operating margin: {base['operating_margin']:.0%}\n"
        + (f"- EVA-implied value: ${implied['per_share']:.2f} vs current ${current_price:.2f}\n"
           if implied.get("per_share") and current_price else "")
        + f"- Confidence: {confidence}\n\n"
        f'Respond in this JSON format only:\n'
        f'{{"analysis":"2-3 paragraphs assessing ROIC sustainability, moat quality, '
        f'and whether current price reflects value creation quality",'
        f'"moat_assessment":"wide/narrow/none",'
        f'"roic_sustainability":"high/medium/low",'
        f'"recommendation":"STRONG_VALUE_CREATOR/VALUE_CREATOR/VALUE_NEUTRAL/VALUE_DESTROYER"}}'
    )

    try:
        raw = await call_llm(system, user, max_tokens=1000)
        if not raw:
            return None

        text = raw.strip()
        # Strip markdown code fences
        if text.startswith("```"):
            lines = text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines)

        # Try to extract JSON object if surrounded by other text
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            text = json_match.group(0)

        parsed = _json.loads(text)
        return {
            "analysis": str(parsed.get("analysis", "")),
            "moat_assessment": str(parsed.get("moat_assessment", "")),
            "roic_sustainability": str(parsed.get("roic_sustainability", "")),
            "recommendation": str(parsed.get("recommendation", "VALUE_NEUTRAL")),
        }
    except _json.JSONDecodeError:
        _log.warning("EVA LLM JSON parse failed for %s — using raw text", symbol)
        if raw:
            return {
                "analysis": raw.strip()[:2000],
                "recommendation": quality["grade"] + "_VALUE_CREATOR" if spread > 0 else "VALUE_DESTROYER",
                "moat_assessment": "",
                "roic_sustainability": "",
            }
        return None
    except Exception as exc:
        _log.warning("EVA LLM narrative failed for %s: %s", symbol, exc)
        return None


# ── Helpers ──────────────────────────────────────────────────

def _fmt(val: float | None) -> float | None:
    """Round large numbers for JSON output."""
    if val is None:
        return None
    if abs(val) >= 1e6:
        return round(val, 0)
    return round(val, 2)


def _insufficient_data_result(symbol: str, current_price: float | None, reason: str) -> dict:
    """Return a partial result when EVA cannot be fully computed."""
    return {
        "ok": True,
        "symbol": symbol,
        "current_price": round(current_price, 2) if current_price else None,
        "grade": "INSUFFICIENT_DATA",
        "capital_structure": None,
        "roic_analysis": None,
        "wacc": None,
        "eva": None,
        "quality": {"grade": "INSUFFICIENT_DATA", "description": reason, "signals": [], "score": None,
                     "value_spread": None, "value_spread_pct": None},
        "implied_valuation": None,
        "comparison": None,
        "verdict": {
            "status": "INSUFFICIENT_DATA",
            "value_creation": "unknown",
            "valuation": "INSUFFICIENT_DATA",
            "summary": reason,
        },
        "confidence": "LOW",
        "defaults_used": [],
        "llm_available": False,
        "llm_analysis": None,
        "llm_recommendation": None,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }

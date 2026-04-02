"""Discounted Cash Flow (DCF) valuation model — on-demand analysis.

Projects 5 years of free cash flows, computes terminal value via
Gordon Growth, discounts to present at WACC, and derives intrinsic
value per share.  Includes sensitivity analysis across WACC and
terminal-growth assumptions.

NOT integrated into the crawler or pillar scoring — runs on demand only.
"""

import asyncio
import logging
from datetime import datetime, timezone

from config import get_settings
from data.finnhub_client import FinnhubClient
from analysis.llm_client import call_llm

_log = logging.getLogger(__name__)

# ── Defaults for missing data ────────────────────────────────

DEFAULTS = {
    "beta": 1.0,
    "effective_tax_rate": 0.21,
    "terminal_growth": 0.03,
    "risk_free_rate": 0.04,
    "equity_risk_premium": 0.055,
    "cost_of_debt": 0.05,
    "fcf_margin": 0.10,
    "revenue_growth": 0.05,
}

PROJECTION_YEARS = 5
COURTESY_DELAY = 0.05


# ═════════════════════════════════════════════════════════════
#  Public entry point
# ═════════════════════════════════════════════════════════════

async def analyze_dcf(symbol: str, *, skip_llm: bool = False) -> dict:
    """Run DCF analysis for *symbol*.

    Returns dict with keys: ok, symbol, inputs, projections,
    terminal_value, valuation, sensitivity, assumptions, confidence,
    caveats, llm_available, llm_analysis, analyzed_at.
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

    # ── 2. Extract base financials ───────────────────────────
    base, defaults_used = _extract_base_financials(profile, metrics)

    if base["revenue"] is None or base["revenue"] <= 0:
        return {"ok": False, "symbol": symbol, "error": "Revenue data not available"}

    # ── 3. Compute WACC ──────────────────────────────────────
    wacc = _compute_wacc(base)

    # ── 4. Project growth rates ──────────────────────────────
    growth_rates = _project_growth(base)

    # ── 5. Project FCF ───────────────────────────────────────
    projections = _project_fcf(base, growth_rates)

    # ── 6. Terminal value ────────────────────────────────────
    terminal = _compute_terminal_value(projections[-1]["fcf"], wacc, base["terminal_growth"])

    # ── 7. Intrinsic value ───────────────────────────────────
    valuation = _compute_intrinsic_value(
        projections, terminal["terminal_value"], wacc,
        base["net_debt"], base["shares_outstanding"],
    )

    # ── 8. Upside / verdict ──────────────────────────────────
    current_price = base["current_price"]
    iv = valuation["intrinsic_value_per_share"]
    upside_pct = None
    verdict = "INSUFFICIENT_DATA"
    if iv and current_price and current_price > 0:
        upside_pct = round((iv / current_price - 1) * 100, 1)
        if upside_pct > 25:
            verdict = "UNDERVALUED"
        elif upside_pct > 5:
            verdict = "SLIGHTLY_UNDERVALUED"
        elif upside_pct >= -10:
            verdict = "FAIRLY_VALUED"
        elif upside_pct >= -25:
            verdict = "SLIGHTLY_OVERVALUED"
        else:
            verdict = "OVERVALUED"

    valuation["current_price"] = current_price
    valuation["upside_pct"] = upside_pct
    valuation["verdict"] = verdict

    # ── 9. Sensitivity ───────────────────────────────────────
    sensitivity = _sensitivity_analysis(
        projections, wacc, base["terminal_growth"],
        base["net_debt"], base["shares_outstanding"],
    )

    # ── 10. Confidence + caveats ─────────────────────────────
    tv_pct = (terminal["pv_of_terminal"] / valuation["enterprise_value"] * 100
              if valuation["enterprise_value"] and valuation["enterprise_value"] > 0 else 0)
    terminal["pct_of_total"] = round(tv_pct, 1)

    confidence, caveats = _assess_confidence(defaults_used, tv_pct, base)

    # ── 11. Build assumptions dict ───────────────────────────
    assumptions = {
        "risk_free_rate": base["risk_free_rate"],
        "equity_risk_premium": base["equity_risk_premium"],
        "projection_years": PROJECTION_YEARS,
        "terminal_growth": base["terminal_growth"],
        "growth_fade": "linear from current to terminal",
        "fcf_method": base["fcf_method"],
        "defaults_used": defaults_used,
    }

    # ── 12. Data sources tracking ────────────────────────────
    data_sources = {}
    for key in ["revenue", "fcf", "beta", "market_cap", "shares_outstanding"]:
        data_sources[key] = "default" if key in defaults_used else "finnhub_metric"

    inputs = {
        "base_revenue": _fmt(base["revenue"]),
        "base_fcf": _fmt(base["fcf"]),
        "fcf_margin": round(base["fcf_margin"], 4),
        "revenue_growth_used": round(base["revenue_growth"], 4),
        "shares_outstanding_m": round(base["shares_outstanding"], 2),
        "total_debt": _fmt(base["total_debt"]),
        "cash": _fmt(base["cash"]),
        "net_debt": _fmt(base["net_debt"]),
        "beta": round(base["beta"], 2),
        "effective_tax_rate": round(base["effective_tax_rate"], 3),
        "wacc": round(wacc, 4),
        "terminal_growth": base["terminal_growth"],
        "data_sources": data_sources,
    }

    # ── 13. Optional LLM narrative ───────────────────────────
    llm_analysis = None
    llm_recommendation = None
    if not skip_llm:
        llm_result = await _llm_dcf_narrative(
            symbol, profile.get("company_name", symbol),
            current_price, iv, upside_pct, verdict, wacc,
            base["terminal_growth"], base["fcf_margin"],
            base["revenue_growth"], tv_pct, projections, confidence,
        )
        if llm_result:
            llm_analysis = llm_result.get("analysis")
            llm_recommendation = llm_result.get("recommendation")

    return {
        "ok": True,
        "symbol": symbol,
        "current_price": round(current_price, 2) if current_price else None,
        "inputs": inputs,
        "projections": [
            {
                "year": p["year"],
                "revenue": _fmt(p["revenue"]),
                "fcf": _fmt(p["fcf"]),
                "growth": round(p["growth_rate"], 4),
                "pv": _fmt(p["present_value"]),
            }
            for p in projections
        ],
        "terminal_value": {
            "terminal_fcf": _fmt(projections[-1]["fcf"]),
            "terminal_value": _fmt(terminal["terminal_value"]),
            "pv_of_terminal": _fmt(terminal["pv_of_terminal"]),
            "pct_of_total": terminal["pct_of_total"],
        },
        "valuation": {
            "pv_of_fcfs": _fmt(valuation["pv_of_fcfs"]),
            "pv_of_terminal": _fmt(terminal["pv_of_terminal"]),
            "enterprise_value": _fmt(valuation["enterprise_value"]),
            "net_debt": _fmt(base["net_debt"]),
            "equity_value": _fmt(valuation["equity_value"]),
            "intrinsic_value_per_share": round(iv, 2) if iv else None,
            "current_price": round(current_price, 2) if current_price else None,
            "upside_pct": upside_pct,
            "verdict": verdict,
        },
        "sensitivity": sensitivity,
        "assumptions": assumptions,
        "confidence": confidence,
        "caveats": caveats,
        "llm_available": llm_analysis is not None,
        "llm_analysis": llm_analysis,
        "llm_recommendation": llm_recommendation,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


# ═════════════════════════════════════════════════════════════
#  Stage 1: Extract base financials from Finnhub metrics
# ═════════════════════════════════════════════════════════════

def _extract_base_financials(profile: dict, metrics: dict) -> tuple[dict, list[str]]:
    """Pull DCF inputs from Finnhub metrics dict.

    Returns (base_dict, list_of_defaults_used).
    """
    defaults_used: list[str] = []

    market_cap_m = profile.get("market_cap") or 0  # millions
    shares_m = profile.get("shares_outstanding") or 0  # millions

    # Current price from market_cap / shares
    current_price = None
    if market_cap_m and shares_m and shares_m > 0:
        current_price = market_cap_m / shares_m

    # Revenue TTM: revenuePerShareTTM × shares (both in millions)
    rev_per_share = metrics.get("revenuePerShareTTM")
    revenue = None
    if rev_per_share and shares_m and shares_m > 0:
        revenue = rev_per_share * shares_m * 1_000_000  # convert to absolute $

    # FCF TTM: cashFlowPerShareTTM × shares
    fcf_per_share = metrics.get("cashFlowPerShareTTM")
    fcf = None
    if fcf_per_share and shares_m and shares_m > 0:
        fcf = fcf_per_share * shares_m * 1_000_000

    # FCF margin
    fcf_margin = DEFAULTS["fcf_margin"]
    fcf_method = "revenue × default_fcf_margin"
    if fcf and revenue and revenue > 0:
        fcf_margin = fcf / revenue
        fcf_method = "revenue × actual_fcf_margin"
    else:
        defaults_used.append("fcf_margin")

    # If FCF not available, derive from revenue × margin
    if not fcf and revenue:
        fcf = revenue * fcf_margin

    # Revenue growth
    revenue_growth = metrics.get("revenueGrowth5Y")
    if revenue_growth is not None:
        revenue_growth = revenue_growth / 100.0  # Finnhub returns percentage
    else:
        revenue_growth = metrics.get("revenueGrowth3Y")
        if revenue_growth is not None:
            revenue_growth = revenue_growth / 100.0
        else:
            revenue_growth = DEFAULTS["revenue_growth"]
            defaults_used.append("revenue_growth")

    # Clamp growth to reasonable range
    revenue_growth = max(-0.10, min(0.50, revenue_growth))

    # Beta
    beta = metrics.get("beta")
    if beta is None or beta <= 0:
        beta = DEFAULTS["beta"]
        defaults_used.append("beta")

    # Total debt: approximate from EV/EBITDA and market cap
    # totalDebt = EV - marketCap, EV can be estimated from evEbitdaTTM × EBITDA
    total_debt_m = 0
    ev_ebitda = metrics.get("evEbitdaTTM")
    ebitda_per_share = None
    # Try using known EV: EV = EV/Revenue × Revenue
    ev_revenue = metrics.get("evRevenueTTM")
    if ev_revenue and rev_per_share and shares_m:
        ev_m = ev_revenue * rev_per_share * shares_m
        total_debt_m = max(0, ev_m - market_cap_m)
    total_debt = total_debt_m * 1_000_000

    # Cash estimate: net debt = total_debt - cash → cash = debt - net_debt
    # Without direct data, estimate cash as fraction of revenue
    cash = 0
    net_interest_coverage = metrics.get("netInterestCoverageTTM")
    # Rough: if coverage is high, company likely has cash
    if revenue:
        cash = revenue * 0.05  # conservative 5% of revenue as cash proxy
    net_debt = total_debt - cash

    # Effective tax rate
    effective_tax_rate = DEFAULTS["effective_tax_rate"]
    defaults_used.append("effective_tax_rate")

    # Cost of debt
    cost_of_debt = DEFAULTS["cost_of_debt"]
    # If interest coverage is available, estimate cost of debt
    if net_interest_coverage and net_interest_coverage > 0 and total_debt > 0:
        # interest_expense ≈ operating_income / coverage
        # cost_of_debt ≈ interest_expense / total_debt
        # This is very rough — keep the default if coverage is extreme
        if 1 < net_interest_coverage < 50:
            cost_of_debt = min(0.12, max(0.02, 1.0 / net_interest_coverage * 0.5))

    base = {
        "market_cap": market_cap_m * 1_000_000,
        "shares_outstanding": shares_m,
        "current_price": current_price,
        "revenue": revenue,
        "fcf": fcf,
        "fcf_margin": fcf_margin,
        "fcf_method": fcf_method,
        "revenue_growth": revenue_growth,
        "beta": beta,
        "total_debt": total_debt,
        "cash": cash,
        "net_debt": net_debt,
        "effective_tax_rate": effective_tax_rate,
        "cost_of_debt": cost_of_debt,
        "risk_free_rate": DEFAULTS["risk_free_rate"],
        "equity_risk_premium": DEFAULTS["equity_risk_premium"],
        "terminal_growth": DEFAULTS["terminal_growth"],
    }

    return base, defaults_used


# ═════════════════════════════════════════════════════════════
#  Stage 2: Growth projection (5-year fade)
# ═════════════════════════════════════════════════════════════

def _project_growth(base: dict) -> list[float]:
    """Linear fade from current growth rate to terminal growth over 5 years."""
    current = base["revenue_growth"]
    terminal = base["terminal_growth"]

    rates = []
    for year in range(1, PROJECTION_YEARS + 1):
        fade = year / PROJECTION_YEARS
        rate = current * (1 - fade) + terminal * fade
        rates.append(max(terminal, rate))

    return rates


# ═════════════════════════════════════════════════════════════
#  Stage 3: FCF projection
# ═════════════════════════════════════════════════════════════

def _project_fcf(base: dict, growth_rates: list[float]) -> list[dict]:
    """Project revenue and FCF for each year."""
    revenue = base["revenue"]
    margin = base["fcf_margin"]
    projections = []

    for i, rate in enumerate(growth_rates):
        revenue = revenue * (1 + rate)
        fcf = revenue * margin
        projections.append({
            "year": i + 1,
            "revenue": revenue,
            "fcf": fcf,
            "growth_rate": rate,
            "present_value": 0,  # filled in stage 7
        })

    return projections


# ═════════════════════════════════════════════════════════════
#  Stage 4: WACC
# ═════════════════════════════════════════════════════════════

def _compute_wacc(base: dict) -> float:
    """Weighted Average Cost of Capital via CAPM."""
    market_cap = base["market_cap"]
    total_debt = base["total_debt"]
    beta = base["beta"]
    tax_rate = base["effective_tax_rate"]
    risk_free = base["risk_free_rate"]
    erp = base["equity_risk_premium"]
    cost_of_debt = base["cost_of_debt"]

    cost_of_equity = risk_free + beta * erp

    total_value = market_cap + total_debt
    if total_value <= 0:
        return max(0.06, min(0.20, cost_of_equity))

    eq_weight = market_cap / total_value
    debt_weight = total_debt / total_value

    wacc = (eq_weight * cost_of_equity) + (debt_weight * cost_of_debt * (1 - tax_rate))
    return max(0.06, min(0.20, wacc))


# ═════════════════════════════════════════════════════════════
#  Stage 5: Terminal value (Gordon Growth)
# ═════════════════════════════════════════════════════════════

def _compute_terminal_value(final_fcf: float, wacc: float, terminal_growth: float = 0.03) -> dict:
    """Terminal value = FCF × (1+g) / (WACC - g)."""
    if wacc <= terminal_growth:
        terminal_growth = wacc - 0.01

    tv = final_fcf * (1 + terminal_growth) / (wacc - terminal_growth)
    pv_tv = tv / (1 + wacc) ** PROJECTION_YEARS

    return {
        "terminal_value": tv,
        "pv_of_terminal": pv_tv,
    }


# ═════════════════════════════════════════════════════════════
#  Stage 6: Intrinsic value
# ═════════════════════════════════════════════════════════════

def _compute_intrinsic_value(
    projections: list[dict], terminal_value: float,
    wacc: float, net_debt: float, shares_m: float,
) -> dict:
    """Discount projected FCFs + terminal to present value."""
    pv_fcfs = 0
    for p in projections:
        pv = p["fcf"] / (1 + wacc) ** p["year"]
        p["present_value"] = pv
        pv_fcfs += pv

    pv_terminal = terminal_value / (1 + wacc) ** PROJECTION_YEARS
    enterprise_value = pv_fcfs + pv_terminal
    equity_value = enterprise_value - net_debt

    iv_per_share = None
    if shares_m and shares_m > 0:
        iv_per_share = equity_value / (shares_m * 1_000_000)

    return {
        "pv_of_fcfs": pv_fcfs,
        "enterprise_value": enterprise_value,
        "equity_value": equity_value,
        "intrinsic_value_per_share": iv_per_share,
    }


# ═════════════════════════════════════════════════════════════
#  Stage 7: Sensitivity analysis
# ═════════════════════════════════════════════════════════════

def _sensitivity_analysis(
    projections: list[dict], base_wacc: float, base_tg: float,
    net_debt: float, shares_m: float,
) -> list[dict]:
    """Vary WACC and terminal growth to show value sensitivity."""
    wacc_range = [
        round(base_wacc - 0.02, 4),
        round(base_wacc - 0.01, 4),
        round(base_wacc, 4),
        round(base_wacc + 0.01, 4),
        round(base_wacc + 0.02, 4),
    ]
    growth_range = [0.02, 0.025, 0.03, 0.035, 0.04]

    table = []
    for w in wacc_range:
        w_clamped = max(0.04, w)  # avoid nonsensical WACC
        row = {"wacc": round(w_clamped, 4), "values": {}}
        for g in growth_range:
            if w_clamped <= g:
                row["values"][f"{g:.1%}"] = None
                continue
            tv = projections[-1]["fcf"] * (1 + g) / (w_clamped - g)
            result = _compute_intrinsic_value(projections, tv, w_clamped, net_debt, shares_m)
            iv = result["intrinsic_value_per_share"]
            row["values"][f"{g:.1%}"] = round(iv, 2) if iv else None
        table.append(row)

    return table


# ═════════════════════════════════════════════════════════════
#  Confidence & caveats
# ═════════════════════════════════════════════════════════════

def _assess_confidence(defaults_used: list[str], tv_pct: float, base: dict) -> tuple[str, list[str]]:
    """Determine confidence level and list caveats."""
    caveats = []

    if tv_pct > 80:
        caveats.append(
            f"Terminal value represents {tv_pct:.0f}% of total — highly sensitive to assumptions"
        )
    if "fcf_margin" in defaults_used:
        caveats.append("FCF margin defaulted to 10% — actual margin unknown")
    if "revenue_growth" in defaults_used:
        caveats.append("Revenue growth defaulted to 5% — no historical data available")
    if "beta" in defaults_used:
        caveats.append("Beta defaulted to 1.0 — market beta assumed")
    if base["fcf_margin"] > 0.40:
        caveats.append(
            f"FCF margin of {base['fcf_margin']:.0%} is unusually high — may not be sustainable"
        )
    if base["revenue_growth"] > 0.25:
        caveats.append(
            f"Projected initial growth of {base['revenue_growth']:.0%} is aggressive — fade may be too slow"
        )

    critical_defaults = {"fcf_margin", "revenue_growth"}
    n_critical = len(critical_defaults & set(defaults_used))

    if n_critical >= 2:
        confidence = "LOW"
    elif len(defaults_used) >= 3 or n_critical >= 1:
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"

    return confidence, caveats


# ═════════════════════════════════════════════════════════════
#  LLM narrative
# ═════════════════════════════════════════════════════════════

async def _llm_dcf_narrative(
    symbol, name, current_price, iv, upside_pct, verdict,
    wacc, terminal_growth, fcf_margin, revenue_growth,
    tv_pct, projections, confidence,
) -> dict | None:
    """Ask LLM for DCF analysis commentary."""
    import json as _json

    system = (
        "You are a senior equity research analyst reviewing a DCF valuation model. "
        "You MUST respond with a single JSON object only. No markdown, no explanation outside JSON."
    )

    proj_summary = "\n".join(
        f"  Year {p['year']}: Revenue ${p['revenue']/1e9:.1f}B, FCF ${p['fcf']/1e9:.1f}B, Growth {p['growth_rate']:.1%}"
        for p in projections
    )

    user = (
        f"DCF Results for {name} ({symbol}):\n"
        f"- Intrinsic value: ${iv:.2f} vs current ${current_price:.2f}\n"
        f"- Verdict: {verdict} ({upside_pct:+.1f}% upside)\n"
        f"- WACC: {wacc:.1%}, Terminal growth: {terminal_growth:.1%}\n"
        f"- Terminal value is {tv_pct:.0f}% of total enterprise value\n"
        f"- FCF margin: {fcf_margin:.1%}\n"
        f"- Revenue growth: {revenue_growth:.1%} (base, fading to {terminal_growth:.1%})\n\n"
        f"Projections:\n{proj_summary}\n\n"
        f"Confidence: {confidence}\n\n"
        f'Respond in this JSON format only:\n'
        f'{{"analysis":"2-3 paragraphs assessing growth assumptions, FCF margin sustainability, '
        f'sensitivity to WACC/terminal value, and whether market price is justified",'
        f'"key_risk":"biggest risk to this valuation",'
        f'"recommendation":"UNDERVALUED/FAIRLY_VALUED/OVERVALUED",'
        f'"confidence_note":"what would change your view"}}'
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
        import re
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            text = json_match.group(0)

        parsed = _json.loads(text)
        return {
            "analysis": str(parsed.get("analysis", "")),
            "key_risk": str(parsed.get("key_risk", "")),
            "recommendation": str(parsed.get("recommendation", "FAIRLY_VALUED")),
            "confidence_note": str(parsed.get("confidence_note", "")),
        }
    except _json.JSONDecodeError as exc:
        _log.warning("DCF LLM JSON parse failed: %s", exc)
        if raw:
            return {"analysis": raw.strip()[:2000], "recommendation": verdict, "key_risk": "", "confidence_note": ""}
        return None
    except Exception as exc:
        _log.warning("DCF LLM narrative failed: %s", exc)
        return None


# ── Helpers ──────────────────────────────────────────────────

def _fmt(val: float | None) -> float | None:
    """Round large numbers for JSON output."""
    if val is None:
        return None
    if abs(val) >= 1e6:
        return round(val, 0)
    return round(val, 2)

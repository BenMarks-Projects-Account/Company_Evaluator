"""
Piotroski F-Score — 9-point quality screen.

Reference: Piotroski, Joseph D. (2000). "Value Investing: The Use of
Historical Financial Statement Information to Separate Winners from
Losers." Journal of Accounting Research, Vol. 38.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)


def compute_piotroski_score(annual: list[dict], quarterly: Optional[list[dict]] = None) -> dict:
    """
    Compute the Piotroski F-Score from annual financial statements.

    Args:
        annual: List of annual records sorted newest-first. Index 0 is
                the most recent year, index 1 is the prior year.
        quarterly: Optional quarterly records (not currently used but
                   reserved for future enhancements)

    Returns:
        dict with score, max_score, label, checks, interpretation,
        and computed_at. Returns ok=False if insufficient data.
    """
    now = datetime.now(timezone.utc).isoformat()

    if not annual or len(annual) < 2:
        return {
            "ok": False,
            "error": "Insufficient historical data — need 2+ years of annual financials",
            "computed_at": now,
        }

    current = annual[0]
    prior = annual[1]

    checks = {}
    score = 0

    # === PROFITABILITY (4 checks) ===

    # Check 1: Net Income > 0
    ni = _get(current, "net_income")
    check = _check_positive("Net Income Positive", ni, "Net income")
    checks["net_income_positive"] = check
    if check["passed"]:
        score += 1

    # Check 2: Operating Cash Flow > 0
    ocf = _get(current, "operating_cash_flow")
    check = _check_positive("Operating Cash Flow Positive", ocf, "OCF")
    checks["ocf_positive"] = check
    if check["passed"]:
        score += 1

    # Check 3: ROA increasing YoY
    roa_curr = _safe_divide(ni, _get(current, "total_assets"))
    roa_prior = _safe_divide(_get(prior, "net_income"), _get(prior, "total_assets"))
    check = _check_improving("ROA Improving YoY", roa_curr, roa_prior, "ROA", as_pct=True)
    checks["roa_improving"] = check
    if check["passed"]:
        score += 1

    # Check 4: OCF > Net Income (earnings quality)
    if ocf is not None and ni is not None:
        passed = ocf > ni
        checks["ocf_greater_than_ni"] = {
            "passed": passed,
            "label": "OCF > Net Income",
            "details": f"OCF ${_fmt_money(ocf)} {'>' if passed else '<='} NI ${_fmt_money(ni)} ({'cash-backed earnings' if passed else 'accrual-heavy earnings'})",
        }
        if passed:
            score += 1
    else:
        checks["ocf_greater_than_ni"] = {
            "passed": False,
            "label": "OCF > Net Income",
            "details": "Insufficient data",
        }

    # === LEVERAGE / LIQUIDITY (3 checks) ===

    # Check 5: Long-term debt ratio decreasing YoY
    ltd_curr = _safe_divide(_get(current, "long_term_debt"), _get(current, "total_assets"))
    ltd_prior = _safe_divide(_get(prior, "long_term_debt"), _get(prior, "total_assets"))
    if ltd_curr is not None and ltd_prior is not None:
        passed = ltd_curr <= ltd_prior
        delta = (ltd_curr - ltd_prior) * 100
        checks["lt_debt_decreasing"] = {
            "passed": passed,
            "label": "Long-Term Debt Decreasing",
            "current_ratio": round(ltd_curr, 4),
            "prior_ratio": round(ltd_prior, 4),
            "details": f"Debt/assets {ltd_curr*100:.1f}% vs prior {ltd_prior*100:.1f}% ({'+' if delta >= 0 else ''}{delta:.1f}pts, {'increasing' if delta > 0 else 'decreasing' if delta < 0 else 'flat'})",
        }
        if passed:
            score += 1
    else:
        checks["lt_debt_decreasing"] = {
            "passed": False,
            "label": "Long-Term Debt Decreasing",
            "details": "Insufficient debt or assets data",
        }

    # Check 6: Current ratio increasing YoY
    cr_curr = _safe_divide(_get(current, "current_assets"), _get(current, "current_liabilities"))
    cr_prior = _safe_divide(_get(prior, "current_assets"), _get(prior, "current_liabilities"))
    check = _check_improving("Current Ratio Improving", cr_curr, cr_prior, "Current ratio", as_ratio=True)
    checks["current_ratio_improving"] = check
    if check["passed"]:
        score += 1

    # Check 7: No share dilution (shares decreased or stayed flat)
    shares_curr = _get(current, "diluted_avg_shares") or _get(current, "basic_avg_shares")
    shares_prior = _get(prior, "diluted_avg_shares") or _get(prior, "basic_avg_shares")
    if shares_curr is not None and shares_prior is not None and shares_prior > 0:
        passed = shares_curr <= shares_prior
        delta_pct = ((shares_curr - shares_prior) / shares_prior) * 100
        if passed:
            details_label = "buyback / no dilution" if delta_pct < 0 else "flat (no dilution)"
        else:
            details_label = "DILUTIVE"
        checks["no_share_dilution"] = {
            "passed": passed,
            "label": "No Share Dilution",
            "current_shares": int(shares_curr),
            "prior_shares": int(shares_prior),
            "details": f"Shares {'+' if delta_pct >= 0 else ''}{delta_pct:.1f}% ({details_label})",
        }
        if passed:
            score += 1
    else:
        checks["no_share_dilution"] = {
            "passed": False,
            "label": "No Share Dilution",
            "details": "Insufficient shares data",
        }

    # === OPERATING EFFICIENCY (2 checks) ===

    # Check 8: Gross margin increasing YoY
    gm_curr = _safe_divide(_get(current, "gross_profit"), _get(current, "revenue"))
    gm_prior = _safe_divide(_get(prior, "gross_profit"), _get(prior, "revenue"))
    check = _check_improving("Gross Margin Improving", gm_curr, gm_prior, "Gross margin", as_pct=True)
    checks["gross_margin_improving"] = check
    if check["passed"]:
        score += 1

    # Check 9: Asset turnover increasing YoY (revenue / total_assets)
    at_curr = _safe_divide(_get(current, "revenue"), _get(current, "total_assets"))
    at_prior = _safe_divide(_get(prior, "revenue"), _get(prior, "total_assets"))
    check = _check_improving("Asset Turnover Improving", at_curr, at_prior, "Asset turnover", as_ratio=True)
    checks["asset_turnover_improving"] = check
    if check["passed"]:
        score += 1

    # Determine label
    if score >= 8:
        label = "STRONG"
    elif score >= 5:
        label = "AVERAGE"
    else:
        label = "WEAK"

    interpretation = _build_interpretation(score, checks)

    return {
        "ok": True,
        "score": score,
        "max_score": 9,
        "label": label,
        "checks": checks,
        "interpretation": interpretation,
        "computed_at": now,
    }


# === Helper functions ===

def _get(record: dict, key: str) -> Optional[float]:
    """Safely extract a numeric value from a financial record."""
    if not record:
        return None
    val = record.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_divide(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    """Divide two values, returning None on missing or zero denominator."""
    if num is None or denom is None:
        return None
    if denom == 0:
        return None
    return num / denom


def _check_positive(label: str, value: Optional[float], display_name: str) -> dict:
    """Generic positive-value check."""
    if value is None:
        return {
            "passed": False,
            "label": label,
            "details": f"{display_name} data not available",
        }

    passed = value > 0
    return {
        "passed": passed,
        "label": label,
        "value": value,
        "details": f"{display_name} ${_fmt_money(value)} ({'positive' if passed else 'negative'})",
    }


def _check_improving(label: str, current: Optional[float], prior: Optional[float],
                     display_name: str, as_pct: bool = False, as_ratio: bool = False) -> dict:
    """Generic year-over-year improvement check."""
    if current is None or prior is None:
        return {
            "passed": False,
            "label": label,
            "details": f"{display_name} historical data not available",
        }

    passed = current > prior

    if as_pct:
        curr_str = f"{current*100:.1f}%"
        prior_str = f"{prior*100:.1f}%"
        delta = (current - prior) * 100
        delta_str = f"{'+' if delta >= 0 else ''}{delta:.1f}pts"
    elif as_ratio:
        curr_str = f"{current:.2f}"
        prior_str = f"{prior:.2f}"
        delta = current - prior
        delta_str = f"{'+' if delta >= 0 else ''}{delta:.2f}"
    else:
        curr_str = str(round(current, 2))
        prior_str = str(round(prior, 2))
        delta_str = ""

    direction = "improving" if passed else "declining"

    return {
        "passed": passed,
        "label": label,
        "current": round(current, 4),
        "prior": round(prior, 4),
        "details": f"{display_name} {curr_str} vs prior {prior_str} ({delta_str}, {direction})",
    }


def _fmt_money(v: float) -> str:
    """Format a dollar value with magnitude suffix."""
    if v is None:
        return "—"
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    if abs_v >= 1e12:
        return f"{sign}{abs_v/1e12:.2f}T"
    if abs_v >= 1e9:
        return f"{sign}{abs_v/1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{sign}{abs_v/1e6:.2f}M"
    return f"{sign}{abs_v:,.0f}"


def _build_interpretation(score: int, checks: dict) -> str:
    """Generate a one-sentence interpretation of the F-Score result."""
    failed = [c["label"] for c in checks.values() if not c["passed"]]

    if score >= 8:
        if not failed:
            return "Excellent fundamentals across all 9 Piotroski checks."
        return f"Strong fundamentals with minor concerns on: {', '.join(failed)}."
    elif score >= 5:
        return f"Average fundamentals. Concerns on {len(failed)} of 9 checks: {', '.join(failed[:3])}{'...' if len(failed) > 3 else ''}."
    else:
        return f"Weak fundamentals. Failed {len(failed)} of 9 checks. Possible value trap — verify with deeper analysis."

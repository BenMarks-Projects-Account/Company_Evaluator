"""
Earnings Power Value (EPV) — Dual trailing/normalized with emergence signal.

Computes two EPV views side-by-side:
  - Trailing EPV: most recent year operating income (best for growing companies)
  - Normalized EPV: 5-year average with 3-year fallback (best for mature/cyclical)

The gap between the two drives the "emergence signal" — a categorization
of whether a company's earning power is expanding, stable, or declining.

Reference: Greenwald, Bruce C. N. (2001). "Value Investing: From
Graham to Buffett and Beyond." John Wiley & Sons.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

PRIMARY_NORMALIZATION_YEARS = 5
FALLBACK_NORMALIZATION_YEARS = 3
MIN_REQUIRED_YEARS = 3

DEFAULT_TAX_RATE = 0.21
MIN_VALID_TAX_RATE = 0.0
MAX_VALID_TAX_RATE = 0.50

# Growth premium label thresholds (decimal, e.g. 0.50 = 50%)
PREMIUM_THRESHOLDS = [
    (-0.25, "DEEP_DISCOUNT"),          # < -25%
    (0.25,  "DISCOUNTED"),             # -25% to 25%
    (1.00,  "MODEST_GROWTH"),          # 25% to 100%
    (2.00,  "SIGNIFICANT_GROWTH"),     # 100% to 200%
    (3.50,  "HIGH_GROWTH"),            # 200% to 350%
    (5.00,  "VERY_HIGH_GROWTH"),       # 350% to 500%
    (float("inf"), "SPECULATIVE"),     # > 500%
]

# CAPM fallback defaults
_RISK_FREE_RATE = 0.045
_EQUITY_RISK_PREMIUM = 0.055
_DEFAULT_BETA = 1.0


# ── Public API ───────────────────────────────────────────────────

def compute_epv(
    annual: list[dict],
    wacc: Optional[float],
    market_cap: Optional[float],
    current_price: Optional[float],
    diluted_shares: Optional[float] = None,
) -> dict:
    """
    Compute dual Earnings Power Value (trailing + normalized) with
    emergence signal detection for identifying growth transitions.

    Args:
        annual: Annual financial records sorted newest-first.
        wacc: Weighted average cost of capital (decimal). CAPM fallback if None.
        market_cap: Current market capitalization in dollars.
        current_price: Current stock price.
        diluted_shares: Diluted shares outstanding. Extracted from annual if None.

    Returns:
        dict with trailing/normalized EPV results and emergence signal,
        or ok=False on insufficient data.
    """
    now = datetime.now(timezone.utc).isoformat()

    # === Input validation ===
    if not annual or len(annual) < MIN_REQUIRED_YEARS:
        return _error(f"Insufficient annual history — need {MIN_REQUIRED_YEARS}+ years", now)

    if not market_cap or market_cap <= 0:
        return _error("Market cap not available", now)

    # === Extract shares with fallback ===
    shares_source = "explicit_parameter"
    if diluted_shares is None or diluted_shares <= 0:
        diluted_shares, shares_source = _extract_shares(annual)

    if not diluted_shares or diluted_shares <= 0:
        return _error("Diluted shares not available", now)

    # === WACC validation with CAPM fallback ===
    wacc, wacc_source = _validate_wacc(wacc, annual)

    # === Collect operating income history (newest-first) ===
    ebit_history = []
    for record in annual[:5]:
        oi = _get(record, "operating_income")
        if oi is not None:
            ebit_history.append(oi)

    if len(ebit_history) < MIN_REQUIRED_YEARS:
        return _error(
            f"Insufficient operating income history — need {MIN_REQUIRED_YEARS}+ years, "
            f"have {len(ebit_history)}",
            now,
        )

    # Oldest-first for trend analysis
    ebit_oldest_first = list(reversed(ebit_history))

    # === Compute shared tax rate ===
    tax_rate, tax_source = _compute_tax_rate(annual)

    # === Trailing EPV (most recent year) ===
    trailing_ebit = ebit_history[0]
    trailing = _compute_single_epv(
        ebit=trailing_ebit,
        tax_rate=tax_rate,
        wacc=wacc,
        market_cap=market_cap,
        diluted_shares=diluted_shares,
        period_years=1,
    )

    # === Normalized EPV (5-year preferred, 3-year fallback) ===
    if len(ebit_history) >= PRIMARY_NORMALIZATION_YEARS:
        norm_window = ebit_history[:PRIMARY_NORMALIZATION_YEARS]
        norm_period = PRIMARY_NORMALIZATION_YEARS
    else:
        norm_window = ebit_history[:FALLBACK_NORMALIZATION_YEARS]
        norm_period = FALLBACK_NORMALIZATION_YEARS

    normalized_ebit = sum(norm_window) / len(norm_window)
    normalized = _compute_single_epv(
        ebit=normalized_ebit,
        tax_rate=tax_rate,
        wacc=wacc,
        market_cap=market_cap,
        diluted_shares=diluted_shares,
        period_years=norm_period,
    )

    # === Emergence signal ===
    emergence = _compute_emergence_signal(
        ebit_oldest_first=ebit_oldest_first,
        trailing_ebit=trailing_ebit,
        normalized_ebit=normalized_ebit,
        trailing_epv_total=trailing.get("epv_total"),
        market_cap=market_cap,
    )

    return {
        "ok": True,
        "trailing": trailing,
        "normalized": normalized,
        "emergence": emergence,
        "shared_inputs": {
            "tax_rate": round(tax_rate, 4),
            "tax_rate_source": tax_source,
            "wacc": round(wacc, 4),
            "wacc_source": wacc_source,
            "market_cap": round(market_cap, 2),
            "current_price": round(current_price, 2) if current_price else None,
            "diluted_shares": int(diluted_shares),
            "shares_source": shares_source,
        },
        "computed_at": now,
    }


# ── Single EPV calculation ───────────────────────────────────────

def _compute_single_epv(
    ebit: float,
    tax_rate: float,
    wacc: float,
    market_cap: float,
    diluted_shares: float,
    period_years: int,
) -> dict:
    """Compute a single EPV calculation for a given EBIT value."""
    nopat = ebit * (1 - tax_rate)
    epv_total = nopat / wacc if wacc > 0 else None

    if epv_total is None or epv_total <= 0:
        return {
            "fair_value_per_share": round(epv_total / diluted_shares, 2) if epv_total else None,
            "epv_total": round(epv_total, 2) if epv_total else None,
            "growth_premium_pct": None,
            "growth_premium_label": "NEGATIVE_EPV",
            "ebit": round(ebit, 2),
            "period_years": period_years,
        }

    fair_value_per_share = epv_total / diluted_shares
    growth_premium = (market_cap - epv_total) / epv_total

    return {
        "fair_value_per_share": round(fair_value_per_share, 2),
        "epv_total": round(epv_total, 2),
        "growth_premium_pct": round(growth_premium * 100, 2),
        "growth_premium_label": _label_growth_premium(growth_premium),
        "ebit": round(ebit, 2),
        "period_years": period_years,
    }


# ── Emergence signal ─────────────────────────────────────────────

def _compute_emergence_signal(
    ebit_oldest_first: list[float],
    trailing_ebit: float,
    normalized_ebit: float,
    trailing_epv_total: Optional[float],
    market_cap: float,
) -> dict:
    """
    Analyze the gap between trailing and normalized EBIT to detect
    growth transitions (small/mid-cap emerging into quality status).
    """
    # Ratio of trailing to normalized
    if normalized_ebit <= 0:
        ratio = float("inf") if trailing_ebit > 0 else 1.0
    else:
        ratio = trailing_ebit / normalized_ebit

    # Years of growth: count upward transitions
    years_of_growth = 0
    for i in range(1, len(ebit_oldest_first)):
        if ebit_oldest_first[i] > ebit_oldest_first[i - 1]:
            years_of_growth += 1

    # Trailing EPV as fraction of market cap
    if trailing_epv_total and trailing_epv_total > 0 and market_cap > 0:
        trailing_to_mc = trailing_epv_total / market_cap
    else:
        trailing_to_mc = 0.0

    # One-time gain detection
    one_time_flag = _detect_one_time_jump(ebit_oldest_first)

    # Classify
    signal = _classify_emergence(
        ratio=ratio,
        years_of_growth=years_of_growth,
        trailing_to_mc=trailing_to_mc,
        one_time_flag=one_time_flag,
        history_length=len(ebit_oldest_first),
        ebit_history_oldest_first=ebit_oldest_first,
    )

    interpretation = _build_emergence_interpretation(
        signal=signal,
        ratio=ratio,
        years_of_growth=years_of_growth,
        trailing_to_mc=trailing_to_mc,
    )

    return {
        "signal": signal,
        "ebit_history": [round(x, 2) for x in ebit_oldest_first],
        "trailing_to_normalized_ratio": round(ratio, 3) if ratio != float("inf") else None,
        "trailing_to_market_cap_ratio": round(trailing_to_mc, 3),
        "years_of_growth": years_of_growth,
        "one_time_flag": one_time_flag,
        "interpretation": interpretation,
    }


def _detect_one_time_jump(history: list[float]) -> bool:
    """
    Detect if the most recent year is a likely one-time jump rather
    than a ramp. Requires at least 3 years of history.

    Flags True if:
    - Most recent >= 2 × prior year
    - AND prior year is within 30% of the year before that (flat baseline)
    """
    if len(history) < 3:
        return False

    recent = history[-1]
    prior = history[-2]
    prior_prior = history[-3]

    if prior <= 0 or recent < (prior * 2):
        return False

    if prior_prior <= 0:
        return False

    baseline_ratio = prior / prior_prior
    return 0.70 <= baseline_ratio <= 1.30


def _classify_emergence(
    ratio: float,
    years_of_growth: int,
    trailing_to_mc: float,
    one_time_flag: bool,
    history_length: int,
    ebit_history_oldest_first: list[float],
) -> str:
    """
    Classify the emergence signal based on ratio, growth pattern, and
    market cap context. Distinguishes GROWING (steady compounders)
    from RECOVERING (post-trough recoveries) using actual decline detection.
    """
    if history_length < MIN_REQUIRED_YEARS:
        return "INSUFFICIENT_DATA"

    if one_time_flag:
        return "POSSIBLE_ONE_TIME"

    # Declining: trailing significantly below normalized
    if ratio < 0.80:
        return "DECLINING"

    # Stable: within ±20% of normalized
    if 0.80 <= ratio <= 1.20:
        return "STABLE"

    # Strong emergence pattern
    if ratio >= 2.00 and years_of_growth >= 3 and trailing_to_mc >= 0.50:
        return "EMERGING"

    # Expansion in progress
    if ratio >= 1.50 and years_of_growth >= 2 and trailing_to_mc >= 0.30:
        return "EXPANDING"

    # Modest above-normalized: distinguish GROWING vs RECOVERING by post-trough check
    if ratio >= 1.20 and years_of_growth >= 2:
        if _has_recent_trough(ebit_history_oldest_first):
            return "RECOVERING"
        return "GROWING"

    # ratio > 1.20 but fewer than 2 growth years
    return "STABLE"


def _has_recent_trough(history: list[float]) -> bool:
    """
    Detect whether the EBIT history shows a genuine post-trough recovery.

    Returns True if:
    1. There is at least one year-over-year DECLINE in the history, AND
    2. The most recent value is HIGHER than the value just before the decline
       (meaning the company actually climbed back above its prior level)

    This distinguishes companies that genuinely dipped and recovered from
    companies that simply grew consistently.

    Examples:
        [50, 70, 60, 75, 95] → True  (decline 70→60, most recent 95 > 70)
        [50, 70, 60, 65, 68] → False (decline 70→60, most recent 68 < 70)
        [70, 80, 90, 100, 110] → False (no decline at all)
        [69.92, 83.38, 88.52, 109.43, 128.53] → False (MSFT — no decline)
    """
    if not history or len(history) < 3:
        return False

    most_recent = history[-1]

    # Walk through each transition looking for a decline
    for i in range(1, len(history)):
        if history[i] < history[i - 1]:
            # Found a decline at index i (history[i-1] → history[i])
            # Check if the most recent value exceeds the pre-decline level
            if most_recent > history[i - 1]:
                return True

    return False


def _build_emergence_interpretation(
    signal: str,
    ratio: float,
    years_of_growth: int,
    trailing_to_mc: float,
) -> str:
    """Generate a one-sentence interpretation of the emergence signal."""

    if signal == "INSUFFICIENT_DATA":
        return "Insufficient history to assess emergence pattern."

    if signal == "POSSIBLE_ONE_TIME":
        return (
            f"Most recent year's operating income is {ratio:.1f}x the average of prior years. "
            f"This may be a one-time gain (asset sale, tax windfall, accounting change) rather "
            f"than sustained earning power growth. Verify before using trailing EPV."
        )

    if signal == "EMERGING":
        mc_pct = trailing_to_mc * 100
        return (
            f"Strong emergence signal — trailing earning power is {(ratio - 1) * 100:.0f}% "
            f"above the normalized average, driven by consistent growth over {years_of_growth} "
            f"of the last 4 years. Trailing EPV represents {mc_pct:.0f}% of market cap, "
            f"suggesting recent earning power is not yet fully priced in."
        )

    if signal == "EXPANDING":
        return (
            f"Expansion in progress — trailing earning power is {(ratio - 1) * 100:.0f}% above "
            f"normalized, with {years_of_growth} growth years. Mid-stage transition; monitor "
            f"for continued acceleration."
        )

    if signal == "GROWING":
        return (
            f"Steady compounder — trailing earning power is {(ratio - 1) * 100:.0f}% above the "
            f"normalized average, with {years_of_growth} consecutive years of operating income "
            f"growth and no decline years. Consistent expansion without a trough."
        )

    if signal == "RECOVERING":
        return (
            f"Post-trough recovery — trailing earning power is {(ratio - 1) * 100:.0f}% above "
            f"the normalized average, climbing back above pre-decline levels. The history shows "
            f"an actual dip followed by recovery, distinguishing this from steady growth."
        )

    if signal == "STABLE":
        return (
            "Stable earning power — trailing and normalized values are within 20%. "
            "Use normalized EPV for conservative valuation."
        )

    if signal == "DECLINING":
        return (
            f"Earning power is declining — trailing is {(1 - ratio) * 100:.0f}% BELOW the "
            f"normalized average. Red flag for a company in secular decline."
        )

    return ""


# ── Shared helpers ───────────────────────────────────────────────

def _compute_tax_rate(annual: list[dict]) -> tuple[float, str]:
    """
    Compute effective tax rate from trailing annual data.
    Tries: 5-year avg → 3-year avg → most recent → default 21%.
    """
    rates = []
    for record in annual[:5]:
        ibt = _get(record, "income_before_tax")
        tax = _get(record, "income_tax")
        if ibt is not None and ibt > 0 and tax is not None:
            rate = tax / ibt
            if MIN_VALID_TAX_RATE <= rate <= MAX_VALID_TAX_RATE:
                rates.append(rate)

    if len(rates) >= 5:
        return sum(rates[:5]) / 5, "trailing_5y_avg"
    if len(rates) >= 3:
        return sum(rates[:3]) / 3, "trailing_3y_avg"
    if len(rates) >= 1:
        return rates[0], "most_recent_year"
    return DEFAULT_TAX_RATE, "default_21pct"


def _validate_wacc(wacc: Optional[float], annual: list[dict]) -> tuple[float, str]:
    """Validate WACC; fall back to CAPM estimate if invalid."""
    if wacc is not None and 0 < wacc <= 0.30:
        return wacc, "provided"

    beta = _extract_beta(annual) or _DEFAULT_BETA
    computed = _RISK_FREE_RATE + beta * _EQUITY_RISK_PREMIUM
    source = f"capm_fallback_beta_{beta:.2f}"
    _log.info(f"[epv] WACC fallback triggered: computed {computed:.4f} from CAPM (beta={beta:.2f})")
    return computed, source


def _extract_beta(annual: list[dict]) -> Optional[float]:
    """Try to get beta from any available field in the annual records."""
    if not annual:
        return None
    for record in annual[:3]:
        beta = record.get("beta")
        if beta is not None:
            try:
                b = float(beta)
                if 0.1 <= b <= 3.0:
                    return b
            except (TypeError, ValueError):
                pass
    return None


def _extract_shares(annual: list[dict]) -> tuple[Optional[float], str]:
    """Extract diluted or basic shares from the most recent annual record."""
    if not annual:
        return None, "none_available"
    latest = annual[0]
    diluted = latest.get("diluted_avg_shares")
    if diluted is not None and diluted > 0:
        return float(diluted), "diluted_avg_shares"
    basic = latest.get("basic_avg_shares")
    if basic is not None and basic > 0:
        return float(basic), "basic_avg_shares"
    return None, "none_available"


def _label_growth_premium(premium: float) -> str:
    """Map a growth premium decimal to a categorical label."""
    for threshold, label in PREMIUM_THRESHOLDS:
        if premium < threshold:
            return label
    return "SPECULATIVE"


def _get(record: dict, key: str) -> Optional[float]:
    """Safely extract a numeric value from a record."""
    if not record:
        return None
    val = record.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _fmt_money(v: float) -> str:
    """Format a dollar value with magnitude suffix."""
    if v is None:
        return "—"
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    if abs_v >= 1e12:
        return f"{sign}${abs_v/1e12:.2f}T"
    if abs_v >= 1e9:
        return f"{sign}${abs_v/1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v/1e6:.2f}M"
    return f"{sign}${abs_v:,.0f}"


def _error(msg: str, now: str) -> dict:
    return {
        "ok": False,
        "error": msg,
        "computed_at": now,
    }

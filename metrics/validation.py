"""Metric validation bounds for scoring inputs."""

from __future__ import annotations

from numbers import Real


METRIC_BOUNDS = {
    # Business Quality
    "roic": (-2.0, 1.0, "ROIC: -200% to 100%"),
    "gross_margin": (-1.0, 1.0, "Gross margin"),
    "op_margin": (-2.0, 1.0, "Operating margin"),
    "net_margin": (-2.0, 1.0, "Net margin"),
    "fcf_yield": (-1.0, 0.5, "FCF yield: max 50%"),
    "rev_stability": (0.0, 1.0, "Revenue stability"),

    # Operational Health
    "sga_efficiency": (0.0, 1.0, "SGA / revenue ratio"),
    "debt_to_ebitda": (-50.0, 50.0, "Debt to EBITDA"),
    "interest_coverage": (-100.0, 1000.0, "Interest coverage"),
    "current_ratio": (0.0, 50.0, "Current ratio"),
    "cash_conversion": (-10.0, 10.0, "OCF / Net Income"),
    "altman_z": (-10.0, 20.0, "Altman Z score"),

    # Capital Allocation
    "roic_wacc_spread": (-1.0, 1.0, "ROIC - WACC spread"),
    "share_trend": (-1.0, 1.0, "Share count trend"),
    "payout_ratio": (0.0, 5.0, "Dividend payout ratio"),
    "rd_intensity": (0.0, 1.0, "R&D / revenue"),

    # Growth Quality
    "revenue_cagr_3y": (-1.0, 2.0, "Revenue CAGR 3Y"),
    "revenue_cagr_5y": (-1.0, 2.0, "Revenue CAGR 5Y"),
    "fcf_growth": (-1.0, 2.0, "FCF growth"),
    "eps_growth_yoy": (-5.0, 5.0, "EPS growth YoY"),
    "margin_trend": (-1.0, 1.0, "Margin trend"),

    # Valuation
    "ev_ebitda": (0.5, 200.0, "EV/EBITDA"),
    "pe_ratio": (1.0, 500.0, "P/E ratio"),
    "pfcf": (1.0, 500.0, "P/FCF"),
    "accruals_ratio": (-2.0, 2.0, "Accruals ratio"),
}


def validate_metric(name: str, value):
    """Return ``(validated_value, is_valid, reason)`` for a single metric."""
    if value is None:
        return None, True, "missing"

    if name not in METRIC_BOUNDS:
        return value, True, "no_bounds_defined"

    if not isinstance(value, Real) or isinstance(value, bool):
        return value, True, "non_numeric"

    min_val, max_val, _ = METRIC_BOUNDS[name]
    if value < min_val or value > max_val:
        return None, False, f"out_of_range ({value} not in [{min_val}, {max_val}])"

    return value, True, "ok"


def validate_pillar_metrics(metrics: dict) -> tuple[dict, list[dict]]:
    """Validate metrics for a pillar and return cleaned values plus flags."""
    validated = {}
    flags = []

    for name, value in metrics.items():
        validated_value, is_valid, reason = validate_metric(name, value)
        validated[name] = validated_value
        if not is_valid:
            flags.append({
                "metric": name,
                "raw_value": value,
                "reason": reason,
            })

    return validated, flags
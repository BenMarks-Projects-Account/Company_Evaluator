"""Cross-validate Finnhub ratio metrics against FMP data.

When both sources report a metric, flag large disagreements and optionally
pick the more conservative (lower-for-good, higher-for-bad) value.

This module does NOT replace Finnhub — it annotates and optionally adjusts.
"""

from __future__ import annotations

import logging

_log = logging.getLogger(__name__)


# ── Field mapping: Finnhub metric name → (FMP source dict, FMP field name) ──
# source dict is "metrics" (key-metrics-ttm) or "ratios" (ratios-ttm)
FIELD_MAP: dict[str, tuple[str, str]] = {
    # Business Quality
    "roicTTM":              ("metrics", "returnOnInvestedCapitalTTM"),
    "grossMarginTTM":       ("ratios",  "grossProfitMarginTTM"),
    "operatingMarginTTM":   ("ratios",  "operatingProfitMarginTTM"),
    "netProfitMarginTTM":   ("ratios",  "netProfitMarginTTM"),
    "pfcfShareTTM":         ("ratios",  "priceToFreeCashFlowRatioTTM"),

    # Operational Health
    "netInterestCoverageTTM": ("ratios",  "interestCoverageRatioTTM"),
    "currentRatioQuarterly":  ("ratios",  "currentRatioTTM"),
    "evEbitdaTTM":            ("metrics", "evToEBITDATTM"),

    # Capital Allocation
    "payoutRatioTTM":       ("ratios",  "dividendPayoutRatioTTM"),

    # Valuation
    "peBasicExclExtraTTM":  ("ratios",  "priceToEarningsRatioTTM"),
    "peTTM":                ("ratios",  "priceToEarningsRatioTTM"),
}


# ── Disagreement thresholds ─────────────────────────────────────────────────
# For ratio-type metrics (margins, ROIC), disagreement = absolute difference
# For multiple-type metrics (EV/EBITDA, PE, P/FCF), disagreement = relative %
#
# (threshold_type, threshold_value, "higher_is_better" flag for conservative pick)
THRESHOLDS: dict[str, tuple[str, float, bool]] = {
    "roicTTM":                ("abs", 0.05, True),     # 5pp
    "grossMarginTTM":         ("abs", 0.05, True),     # 5pp
    "operatingMarginTTM":     ("abs", 0.05, True),     # 5pp
    "netProfitMarginTTM":     ("abs", 0.05, True),     # 5pp
    "pfcfShareTTM":           ("rel", 0.30, False),    # 30% — lower P/FCF is "better"(cheaper)
    "netInterestCoverageTTM": ("rel", 0.30, True),     # 30%
    "currentRatioQuarterly":  ("rel", 0.25, True),     # 25%
    "evEbitdaTTM":            ("rel", 0.25, False),    # 25% — lower EV/EBITDA is cheaper
    "payoutRatioTTM":         ("abs", 0.10, False),    # 10pp — lower payout is safer
    "peBasicExclExtraTTM":    ("rel", 0.25, False),    # 25%
    "peTTM":                  ("rel", 0.25, False),    # 25%
}


def cross_validate_finnhub_metrics(
    finnhub_metrics: dict,
    fmp_data: dict,
) -> tuple[dict, list[dict]]:
    """Compare Finnhub metrics against FMP and return adjusted metrics + flags.

    Parameters
    ----------
    finnhub_metrics : dict
        The ``data["basic_financials"]["metrics"]`` dict (mutated in-place).
    fmp_data : dict
        Output of ``FMPClient.get_all_cross_validation_data()``.

    Returns
    -------
    adjusted_metrics : dict
        The (possibly adjusted) Finnhub metrics dict.
    flags : list[dict]
        Cross-validation flags for logging/storage.
    """
    flags: list[dict] = []

    if not fmp_data or not fmp_data.get("fetched"):
        return finnhub_metrics, flags

    fmp_metrics = fmp_data.get("metrics") or {}
    fmp_ratios = fmp_data.get("ratios") or {}
    fmp_sources = {"metrics": fmp_metrics, "ratios": fmp_ratios}

    for finnhub_field, (fmp_source_key, fmp_field) in FIELD_MAP.items():
        finnhub_val = finnhub_metrics.get(finnhub_field)
        fmp_val = fmp_sources.get(fmp_source_key, {}).get(fmp_field)

        if finnhub_val is None or fmp_val is None:
            continue

        if not isinstance(finnhub_val, (int, float)) or not isinstance(fmp_val, (int, float)):
            continue

        disagreement = _compute_disagreement(finnhub_field, finnhub_val, fmp_val)
        if disagreement is None:
            continue

        thresh_type, thresh_val, higher_is_better = THRESHOLDS.get(
            finnhub_field, ("rel", 0.30, True)
        )

        if disagreement <= thresh_val:
            # Within tolerance — no flag
            continue

        # Disagreement exceeds threshold → flag it
        conservative_val = _pick_conservative(finnhub_val, fmp_val, higher_is_better)
        original_val = finnhub_val

        flag = {
            "metric": finnhub_field,
            "finnhub_value": finnhub_val,
            "fmp_value": fmp_val,
            "disagreement": round(disagreement, 4),
            "threshold": thresh_val,
            "threshold_type": thresh_type,
            "action": "adjusted_to_conservative",
            "original": original_val,
            "adjusted": conservative_val,
        }

        # Apply conservative adjustment
        finnhub_metrics[finnhub_field] = conservative_val
        flags.append(flag)

        _log.warning(
            "[CrossVal] %s: Finnhub=%.4f FMP=%.4f disagree=%.2f%% → adjusted to %.4f",
            finnhub_field, original_val, fmp_val,
            disagreement * 100 if thresh_type == "rel" else disagreement,
            conservative_val,
        )

    if flags:
        _log.info("[CrossVal] %d metric(s) adjusted via FMP cross-validation", len(flags))
    else:
        _log.info("[CrossVal] All mapped metrics within tolerance — no adjustments")

    return finnhub_metrics, flags


def _compute_disagreement(field: str, val_a: float, val_b: float) -> float | None:
    """Compute disagreement between two values based on the field's threshold type."""
    thresh_info = THRESHOLDS.get(field)
    if not thresh_info:
        return None

    thresh_type = thresh_info[0]

    if thresh_type == "abs":
        return abs(val_a - val_b)
    elif thresh_type == "rel":
        avg = (abs(val_a) + abs(val_b)) / 2
        if avg == 0:
            return 0.0
        return abs(val_a - val_b) / avg
    return None


def _pick_conservative(finnhub_val: float, fmp_val: float, higher_is_better: bool) -> float:
    """Pick the more conservative (pessimistic) value.

    For "higher is better" metrics (ROIC, margins, coverage):
        conservative = min(finnhub, fmp)
    For "lower is better" metrics (EV/EBITDA, PE, payout):
        conservative = max(finnhub, fmp)  (higher = worse = more conservative)
    """
    if higher_is_better:
        return min(finnhub_val, fmp_val)
    else:
        return max(finnhub_val, fmp_val)

"""Shared helpers for all pillar metric modules."""

from __future__ import annotations


def safe_div(numerator, denominator, default=None):
    """Safe division returning default when denominator is zero/None."""
    if numerator is None or denominator is None or denominator == 0:
        return default
    return numerator / denominator


def score(value, low, high, invert=False):
    """Score a metric 0–100 linearly between *low* (→0) and *high* (→100).

    If *invert* is True, low → 100 and high → 0 (for metrics where lower is better).
    Returns None when *value* is None.
    """
    if value is None:
        return None
    if high == low:
        return 50.0
    raw = (value - low) / (high - low)
    raw = max(0.0, min(1.0, raw))
    if invert:
        raw = 1.0 - raw
    return round(raw * 100, 1)


def weighted_avg(items: list[tuple[float | None, float]]) -> tuple[float, float]:
    """Weighted average that penalizes missing values.

    Missing scores contribute 0 to the numerator while still counting in the
    denominator, so sparse data lowers the final score instead of being
    re-normalized away.
    """
    if not items:
        return 0.0, 0.0

    total_weight = sum(weight for _, weight in items)
    if total_weight == 0:
        return 0.0, 0.0

    populated_weight = sum(weight for score_value, weight in items if score_value is not None)
    populated_score = sum(score_value * weight for score_value, weight in items if score_value is not None)

    final_score = populated_score / total_weight
    completeness_pct = (populated_weight / total_weight) * 100
    return round(final_score, 1), round(completeness_pct, 1)


def apply_completeness_cap(score_value: float, completeness_pct: float) -> float:
    """Cap pillar scores when data completeness is low."""
    if completeness_pct < 30:
        return min(score_value, 40.0)
    if completeness_pct < 50:
        return min(score_value, 60.0)
    if completeness_pct < 70:
        return min(score_value, 80.0)
    return score_value


def get_statements(data: dict, timeframe: str = "quarterly") -> list[dict]:
    """Extract the list of statement dicts from company_data."""
    key = f"financials_{timeframe}"
    blob = data.get(key)
    if not blob or blob.get("error"):
        return []
    return blob.get("statements", [])


def get_finnhub_metrics(data: dict) -> dict:
    """Extract the flat Finnhub metrics dict."""
    bf = data.get("basic_financials")
    if not bf or bf.get("error"):
        return {}
    return bf.get("metrics", {})


def get_finnhub_series(data: dict) -> dict:
    """Extract the Finnhub historical series dict."""
    bf = data.get("basic_financials")
    if not bf or bf.get("error"):
        return {}
    return bf.get("series", {})


def ttm_sum(statements: list[dict], field: str) -> float | None:
    """Sum the most recent 4 quarters for a field (trailing-twelve-month)."""
    vals = [s.get(field) for s in statements[:4] if s.get(field) is not None]
    if len(vals) < 4:
        return None
    return sum(vals)


def latest(statements: list[dict], field: str) -> float | None:
    """Get the most recent non-None value for a field."""
    for s in statements:
        v = s.get(field)
        if v is not None:
            return v
    return None


def cagr(old_val, new_val, years: int) -> float | None:
    """Compound annual growth rate.  Returns as a ratio (0.10 = 10 %)."""
    if old_val is None or new_val is None or old_val <= 0 or years <= 0:
        return None
    return (new_val / old_val) ** (1 / years) - 1


def coeff_of_variation(values: list[float]) -> float | None:
    """Coefficient of variation (std / mean). Lower is more stable."""
    values = [v for v in values if v is not None]
    if len(values) < 3:
        return None
    mean = sum(values) / len(values)
    if mean == 0:
        return None
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return (variance ** 0.5) / abs(mean)

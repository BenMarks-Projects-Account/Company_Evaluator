"""Composite scoring — combines all 5 pillar scores into a single 0–100 rank.

Pillar weights (from framework spec):
  1. Business Quality          30 %
  2. Operational & Fin. Health 15 %
  3. Capital Allocation        20 %
  4. Growth Quality            20 %
  5. Valuation & Expectations  15 %
"""

from __future__ import annotations

import logging
import time

from metrics import (
    business_quality,
    operational_health,
    capital_allocation,
    growth_quality,
    valuation_expectations,
)
from metrics.helpers import weighted_avg

_log = logging.getLogger(__name__)

PILLAR_WEIGHTS = {
    "business_quality":     0.30,
    "operational_health":   0.15,
    "capital_allocation":   0.20,
    "growth_quality":       0.20,
    "valuation":            0.15,
}

_PILLAR_FUNCS = {
    "business_quality":   business_quality.compute,
    "operational_health": operational_health.compute,
    "capital_allocation": capital_allocation.compute,
    "growth_quality":     growth_quality.compute,
    "valuation":          valuation_expectations.compute,
}

_PILLAR_RESCORE_FUNCS = {
    "business_quality":   business_quality.rescore_from_metrics,
    "operational_health": operational_health.rescore_from_metrics,
    "capital_allocation": capital_allocation.rescore_from_metrics,
    "growth_quality":     growth_quality.rescore_from_metrics,
    "valuation":          valuation_expectations.rescore_from_metrics,
}


def compute_composite_score(data: dict) -> dict:
    """Run all 5 pillars and produce a composite evaluation dict.

    Returns::

        {
            "composite_score": float 0–100,
            "pillar_scores": { name: float, ... },
            "pillar_details": { name: { metrics, scores }, ... },
            "data_quality": str,
        }
    """
    pillars: dict[str, dict] = {}

    for name, func in _PILLAR_FUNCS.items():
        t = time.time()
        try:
            pillars[name] = func(data)
            ps = pillars[name].get("pillar_score")
            _log.info("  Pillar %-25s = %s/100  (%.2fs)", name, ps, time.time() - t)
        except Exception as exc:
            _log.error("  Pillar %-25s FAILED: %s  (%.2fs)", name, exc, time.time() - t, exc_info=True)
            pillars[name] = {
                "pillar_score": None,
                "raw_score": 0.0,
                "completeness_pct": 0.0,
                "raw_metrics": {},
                "metrics": {},
                "scores": {},
                "data_quality_flags": [],
                "cap_applied": False,
            }

    return _assemble_composite_result(pillars, data.get("data_quality", "unknown"))


def recompute_composite_from_metrics(pillar_metrics: dict[str, dict], data_quality: str = "unknown") -> dict:
    """Re-score a company from stored per-pillar metrics without refetching data."""
    pillars: dict[str, dict] = {}
    for name, func in _PILLAR_RESCORE_FUNCS.items():
        metrics = pillar_metrics.get(name) or {}
        try:
            pillars[name] = func(metrics)
        except Exception as exc:
            _log.error("  Stored pillar %-18s FAILED: %s", name, exc, exc_info=True)
            pillars[name] = {
                "pillar_score": None,
                "raw_score": 0.0,
                "completeness_pct": 0.0,
                "raw_metrics": metrics,
                "metrics": metrics,
                "scores": {},
                "data_quality_flags": [{"metric": name, "raw_value": None, "reason": "rescoring_failed"}],
                "cap_applied": False,
            }

    return _assemble_composite_result(pillars, data_quality)


def _assemble_composite_result(pillars: dict[str, dict], data_quality: str) -> dict:
    composite_score, pillar_completeness_pct = weighted_avg([
        ((pillars[name]["pillar_score"] if pillars[name].get("completeness_pct", 0) > 0 else None), PILLAR_WEIGHTS[name])
        for name in PILLAR_WEIGHTS
    ])

    overall_completeness_pct = round(
        sum(p.get("completeness_pct", 0.0) for p in pillars.values()) / len(PILLAR_WEIGHTS),
        1,
    )

    pillar_scores = {name: p["pillar_score"] for name, p in pillars.items()}
    pillar_details = {
        name: {
            "raw_score": p.get("raw_score"),
            "completeness_pct": p.get("completeness_pct", 0.0),
            "raw_metrics": p.get("raw_metrics", {}),
            "metrics": p.get("metrics", {}),
            "scores": p.get("scores", {}),
            "data_quality_flags": p.get("data_quality_flags", []),
            "cap_applied": p.get("cap_applied", False),
        }
        for name, p in pillars.items()
    }

    data_quality_flags = []
    for pillar_name, pillar in pillars.items():
        for flag in pillar.get("data_quality_flags", []):
            data_quality_flags.append({"pillar": pillar_name, **flag})

    return {
        "composite_score": composite_score,
        "pillar_completeness_pct": pillar_completeness_pct,
        "overall_completeness_pct": overall_completeness_pct,
        "missing_pillar_count": sum(1 for p in pillars.values() if p.get("completeness_pct", 0) == 0),
        "pillar_scores": pillar_scores,
        "pillar_details": pillar_details,
        "data_quality_flags": data_quality_flags,
        "data_quality": data_quality,
    }


# Backward-compat alias
compute_composite = compute_composite_score

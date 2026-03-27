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
        try:
            pillars[name] = func(data)
        except Exception as exc:
            _log.error("event=pillar_error pillar=%s error=%s", name, exc)
            pillars[name] = {"pillar_score": None, "metrics": {}, "scores": {}}

    composite = weighted_avg([
        (pillars[name]["pillar_score"], PILLAR_WEIGHTS[name])
        for name in PILLAR_WEIGHTS
    ])

    pillar_scores = {name: p["pillar_score"] for name, p in pillars.items()}
    pillar_details = {
        name: {"metrics": p.get("metrics", {}), "scores": p.get("scores", {})}
        for name, p in pillars.items()
    }

    return {
        "composite_score": composite,
        "pillar_scores": pillar_scores,
        "pillar_details": pillar_details,
        "data_quality": data.get("data_quality", "unknown"),
    }


# Backward-compat alias
compute_composite = compute_composite_score

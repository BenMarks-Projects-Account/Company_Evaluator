"""Analysis functions for the Phase A audit.

Each ``run_analysis_N`` function returns an :class:`AnalysisResult` with
its tables, chart paths, stats, interpretation and notes. The functions
are pure given their inputs — I/O lives in ``run_audit.py``.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as sci_stats

from analysis.backtest.audit import plots

_log = logging.getLogger(__name__)


PILLAR_COLS = [
    "pillar_1_business_quality",
    "pillar_2_operational_health",
    "pillar_3_capital_allocation",
    "pillar_4_growth_quality",
    "pillar_5_valuation",
]
PILLAR_SHORT = {
    "pillar_1_business_quality":    "P1 BizQual",
    "pillar_2_operational_health":  "P2 OpsHealth",
    "pillar_3_capital_allocation":  "P3 CapAlloc",
    "pillar_4_growth_quality":      "P4 Growth",
    "pillar_5_valuation":           "P5 Val",
}
PILLAR_WEIGHTS = {
    "pillar_1_business_quality":    0.30,
    "pillar_2_operational_health":  0.15,
    "pillar_3_capital_allocation":  0.20,
    "pillar_4_growth_quality":      0.20,
    "pillar_5_valuation":           0.15,
}


@dataclass
class AnalysisResult:
    name: str
    stats_dict: dict[str, Any] = field(default_factory=dict)
    chart_paths: list[str] = field(default_factory=list)
    interpretation: str = ""
    notes: list[str] = field(default_factory=list)
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)


# ── helpers ─────────────────────────────────────────────────────────
def _safe_json(blob) -> dict | None:
    if blob is None:
        return None
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, (bytes, bytearray)):
        try:
            blob = blob.decode("utf-8")
        except Exception:
            return None
    if isinstance(blob, str):
        try:
            v = json.loads(blob)
        except Exception:
            return None
        return v if isinstance(v, dict) else None
    return None


def _coerce_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return float(v)
    if isinstance(v, str):
        try:
            x = float(v)
            if math.isnan(x) or math.isinf(x):
                return None
            return x
        except Exception:
            return None
    return None


def _slug(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in s).strip("_").lower()


# ── Analysis 1: Pillar correlation matrix ───────────────────────────
def run_analysis_1_pillar_correlations(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df[PILLAR_COLS].dropna()
    short_sub = sub.rename(columns=PILLAR_SHORT)
    pearson = short_sub.corr(method="pearson")
    spearman = short_sub.corr(method="spearman")

    p_path = plots.heatmap(
        pearson, "Pillar score Pearson correlation",
        os.path.join(charts_dir, "01_pillar_corr_pearson.png"),
    )
    s_path = plots.heatmap(
        spearman, "Pillar score Spearman correlation",
        os.path.join(charts_dir, "01_pillar_corr_spearman.png"),
    )

    # Off-diagonal summary
    vals = pearson.values.copy()
    np.fill_diagonal(vals, np.nan)
    off = vals[~np.isnan(vals)]
    max_abs = float(np.max(np.abs(off)))
    mean_abs = float(np.mean(np.abs(off)))

    interp = (
        f"Pairwise Pearson correlations between the 5 pillar scores run at "
        f"mean |r|={mean_abs:.2f} with max |r|={max_abs:.2f}. "
        f"Low-to-moderate correlation is healthy: it means pillars capture "
        f"different dimensions of quality. Values above 0.7 would indicate "
        f"redundancy; values near 0 mean independence."
    )

    return AnalysisResult(
        name="1. Pillar correlation matrix",
        stats_dict={
            "n_symbols":           int(len(sub)),
            "pearson_mean_abs":    round(mean_abs, 4),
            "pearson_max_abs":     round(max_abs, 4),
        },
        chart_paths=[p_path, s_path],
        interpretation=interp,
        tables={"pearson": pearson, "spearman": spearman},
    )


# ── Analysis 2: Within-pillar metric correlation ────────────────────
def _extract_subscores(df: pd.DataFrame, pillar_n: int) -> tuple[pd.DataFrame, list[str]]:
    col = f"pillar_{pillar_n}_detail"
    rows: list[dict] = []
    notes: list[str] = []
    malformed = 0
    missing_scores = 0
    for blob in df[col]:
        d = _safe_json(blob)
        if d is None:
            malformed += 1
            continue
        scores = d.get("scores")
        if not isinstance(scores, dict):
            missing_scores += 1
            continue
        rows.append({k: _coerce_float(v) for k, v in scores.items()})
    if malformed:
        notes.append(f"pillar_{pillar_n}: {malformed} row(s) with malformed JSON")
    if missing_scores:
        notes.append(f"pillar_{pillar_n}: {missing_scores} row(s) missing 'scores' dict")
    return pd.DataFrame(rows), notes


def run_analysis_2_within_pillar_correlations(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    all_notes: list[str] = []
    chart_paths: list[str] = []
    tables: dict[str, pd.DataFrame] = {}
    stats: dict[str, Any] = {}

    for n in range(1, 6):
        sub, notes = _extract_subscores(df, n)
        all_notes.extend(notes)
        if sub.empty or sub.shape[1] < 2:
            all_notes.append(f"pillar_{n}: skipped — not enough sub-metrics ({sub.shape[1]} cols, {len(sub)} rows)")
            continue
        sub = sub.dropna(how="all", axis=1)
        # Keep only columns with >=50% coverage
        min_cov = int(0.5 * len(sub))
        keep = [c for c in sub.columns if sub[c].notna().sum() >= min_cov]
        dropped = set(sub.columns) - set(keep)
        if dropped:
            all_notes.append(f"pillar_{n}: dropped low-coverage sub-metrics {sorted(dropped)}")
        sub = sub[keep]
        if sub.shape[1] < 2:
            all_notes.append(f"pillar_{n}: skipped after coverage filter")
            continue
        corr = sub.corr(method="pearson")
        tables[f"pillar_{n}"] = corr
        path = plots.heatmap(
            corr, f"Pillar {n} sub-score correlations (Pearson)",
            os.path.join(charts_dir, f"02_within_pillar_{n}_corr.png"),
        )
        chart_paths.append(path)
        vals = corr.values.copy()
        np.fill_diagonal(vals, np.nan)
        off = vals[~np.isnan(vals)]
        stats[f"pillar_{n}_mean_abs_r"] = round(float(np.mean(np.abs(off))), 4) if off.size else None
        stats[f"pillar_{n}_max_abs_r"]  = round(float(np.max(np.abs(off))), 4) if off.size else None
        stats[f"pillar_{n}_n"]          = int(len(sub))

    interp = (
        "Within each pillar, sub-metrics are expected to be moderately correlated "
        "(they measure related aspects of the same dimension) but not redundant. "
        "Look for pairs with |r|>0.85 — those are candidates for consolidation. "
        "Pairs near r=0 suggest the sub-metric is measuring something orthogonal "
        "to the rest of its pillar, which may be a design feature or a mismatch."
    )

    return AnalysisResult(
        name="2. Within-pillar metric correlation",
        stats_dict=stats,
        chart_paths=chart_paths,
        interpretation=interp,
        notes=all_notes,
        tables=tables,
    )


# ── Analysis 3: PCA on pillar scores ────────────────────────────────
def run_analysis_3_pca_on_pillars(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df[PILLAR_COLS].dropna()
    if len(sub) < 10:
        return AnalysisResult(
            name="3. PCA on pillar scores",
            notes=[f"insufficient data (n={len(sub)})"],
            interpretation="Skipped — not enough rows for PCA.",
        )
    X = sub.values.astype(float)
    mu = X.mean(axis=0)
    sd = X.std(axis=0, ddof=0)
    sd[sd == 0] = 1.0
    Z = (X - mu) / sd
    cov = np.cov(Z.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]
    evr = eigvals / eigvals.sum()

    short_labels = [PILLAR_SHORT[c] for c in PILLAR_COLS]
    loadings = pd.DataFrame(
        eigvecs,
        index=short_labels,
        columns=[f"PC{i+1}" for i in range(len(eigvals))],
    )

    evr_df = pd.DataFrame({
        "component":            [f"PC{i+1}" for i in range(len(evr))],
        "eigenvalue":           eigvals,
        "explained_variance":   evr,
        "cumulative_variance":  np.cumsum(evr),
    })

    scree_path = plots.scree_plot(
        evr, "PCA scree — pillar scores",
        os.path.join(charts_dir, "03_pca_scree.png"),
    )

    pc1_loadings = loadings["PC1"].abs().sort_values(ascending=False)
    pc1_top = pc1_loadings.index[0]
    interp = (
        f"PC1 explains {evr[0]*100:.1f}% of variance across the 5 pillars "
        f"(PC1+PC2 = {(evr[0]+evr[1])*100:.1f}%). PC1's largest absolute "
        f"loading is on **{pc1_top}** ({loadings.loc[pc1_top,'PC1']:+.2f}), "
        f"suggesting this pillar dominates the first axis. If PC1>70% the "
        f"framework collapses toward a single dimension; if PC1<40% the "
        f"pillars are closer to genuinely orthogonal."
    )

    return AnalysisResult(
        name="3. PCA on pillar scores",
        stats_dict={
            "n":                         int(len(sub)),
            "pc1_explained":             round(float(evr[0]), 4),
            "pc1_pc2_cumulative":        round(float(evr[0] + evr[1]), 4),
            "all_explained_variance":    [round(float(x), 4) for x in evr],
        },
        chart_paths=[scree_path],
        interpretation=interp,
        tables={"explained_variance": evr_df, "loadings": loadings},
    )


# ── Analysis 4: Pillar score distributions ──────────────────────────
def _describe_dist(ser: pd.Series) -> dict[str, float]:
    s = ser.dropna()
    if len(s) == 0:
        return {}
    q25, q50, q75 = np.percentile(s, [25, 50, 75])
    return {
        "n":        int(len(s)),
        "mean":     float(s.mean()),
        "stdev":    float(s.std(ddof=1)) if len(s) > 1 else 0.0,
        "min":      float(s.min()),
        "p25":      float(q25),
        "median":   float(q50),
        "p75":      float(q75),
        "max":      float(s.max()),
        "iqr":      float(q75 - q25),
        "skew":     float(sci_stats.skew(s, bias=False)) if len(s) > 2 else 0.0,
        "kurtosis": float(sci_stats.kurtosis(s, bias=False, fisher=True)) if len(s) > 3 else 0.0,
    }


def run_analysis_4_pillar_distributions(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    series_map = {PILLAR_SHORT[c]: df[c] for c in PILLAR_COLS}
    stats_rows = []
    for col in PILLAR_COLS:
        d = _describe_dist(df[col])
        d["pillar"] = PILLAR_SHORT[col]
        stats_rows.append(d)
    stats_df = pd.DataFrame(stats_rows).set_index("pillar")[
        ["n", "mean", "stdev", "min", "p25", "median", "p75", "max", "iqr", "skew", "kurtosis"]
    ]

    path = plots.histogram_grid(
        series_map, "Pillar score distributions",
        os.path.join(charts_dir, "04_pillar_distributions.png"),
        ncols=3,
    )

    means = stats_df["mean"]
    stdevs = stats_df["stdev"]
    interp = (
        f"Pillar means range from {means.min():.1f} ({means.idxmin()}) to "
        f"{means.max():.1f} ({means.idxmax()}); stdevs range from "
        f"{stdevs.min():.1f} to {stdevs.max():.1f}. A pillar with very low "
        f"stdev discriminates poorly across companies; a pillar with a mean "
        f"far from 50 may be too easy or too hard to score."
    )

    return AnalysisResult(
        name="4. Pillar score distributions",
        stats_dict={"pillar_means": {k: round(float(v), 2) for k, v in means.items()},
                    "pillar_stdevs": {k: round(float(v), 2) for k, v in stdevs.items()}},
        chart_paths=[path],
        interpretation=interp,
        tables={"stats": stats_df.round(3)},
    )


# ── Analysis 5: Composite score distribution ────────────────────────
def run_analysis_5_composite_distribution(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    ser = df["composite_score"].dropna()
    dist_stats = _describe_dist(ser)

    # Normality — Anderson-Darling is more discriminating than SW at n≈2000
    # and Shapiro-Wilk's upper-n bound is 5000 but its p-value becomes
    # uninformative at large n (tiny deviations trip it). We report BOTH.
    ad = sci_stats.anderson(ser.values, dist="norm")
    try:
        sw_stat, sw_p = sci_stats.shapiro(ser.values)
    except Exception:
        sw_stat, sw_p = float("nan"), float("nan")

    path = plots.single_histogram(
        ser, "Composite score distribution",
        os.path.join(charts_dir, "05_composite_distribution.png"),
        bins=40, xlabel="composite_score",
    )

    interp = (
        f"Composite scores span [{dist_stats['min']:.1f}, {dist_stats['max']:.1f}] "
        f"with mean={dist_stats['mean']:.2f}, median={dist_stats['median']:.2f}, "
        f"stdev={dist_stats['stdev']:.2f}, skew={dist_stats['skew']:.2f}, "
        f"excess kurtosis={dist_stats['kurtosis']:.2f}. At n={dist_stats['n']} "
        f"any formal normality test will almost certainly reject — trivially "
        f"small deviations are statistically significant at this sample size. "
        f"The practical questions are: does the shape discriminate (IQR "
        f"{dist_stats['iqr']:.1f}, stdev {dist_stats['stdev']:.2f}) and is it "
        f"grossly bimodal/spiky. Anderson-Darling statistic reported for "
        f"completeness; treat skew and kurtosis as the operative summaries."
    )

    return AnalysisResult(
        name="5. Composite score distribution",
        stats_dict={
            **dist_stats,
            "anderson_darling_stat":   round(float(ad.statistic), 4),
            "anderson_darling_crit_5pct": round(float(ad.critical_values[2]), 4),
            "shapiro_wilk_stat":       round(float(sw_stat), 4) if not math.isnan(sw_stat) else None,
            "shapiro_wilk_p":          round(float(sw_p), 6)    if not math.isnan(sw_p)    else None,
        },
        chart_paths=[path],
        interpretation=interp,
        notes=["Normality test reported but not weighted heavily at n=2000+; "
               "use skew/kurtosis for shape assessment."],
    )


# ── Analysis 6: Pillar contribution analysis ────────────────────────
def run_analysis_6_pillar_contributions(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df[PILLAR_COLS].dropna().copy()
    weights = np.array([PILLAR_WEIGHTS[c] for c in PILLAR_COLS])
    weighted = sub.values * weights[None, :]
    contrib_df = pd.DataFrame(weighted, index=sub.index,
                              columns=[PILLAR_SHORT[c] for c in PILLAR_COLS])
    top_idx = contrib_df.values.argmax(axis=1)
    top_labels = [contrib_df.columns[i] for i in top_idx]
    counts = pd.Series(top_labels).value_counts()
    pct = (counts / counts.sum() * 100).round(2)

    summary = pd.DataFrame({
        "top_count": counts,
        "top_pct":   pct,
        "weight":    [PILLAR_WEIGHTS[c] * 100 for c in PILLAR_COLS[:len(counts)]]
                      if len(counts) == len(PILLAR_COLS) else None,
    })
    # Align weight by label lookup
    weight_lookup = {PILLAR_SHORT[c]: PILLAR_WEIGHTS[c] * 100 for c in PILLAR_COLS}
    summary["weight_pct"] = summary.index.map(weight_lookup)
    summary = summary[["top_count", "top_pct", "weight_pct"]]
    summary.index.name = "pillar"

    mean_contrib = contrib_df.mean().round(3)

    path = plots.bar_chart(
        list(pct.index), list(pct.values),
        "Top contributing pillar per symbol (% of universe)",
        os.path.join(charts_dir, "06_pillar_contributions.png"),
        ylabel="% of symbols",
    )

    top_label, top_pct_v = pct.idxmax(), pct.max()
    interp = (
        f"**{top_label}** is the largest weighted contributor for "
        f"{top_pct_v:.1f}% of symbols. Because weights are fixed, a pillar "
        f"with a larger weight AND higher typical score will dominate — "
        f"this is by design for P1 (weight=30%). If a pillar with a small "
        f"weight dominates, its score distribution is running hot relative "
        f"to the others."
    )

    return AnalysisResult(
        name="6. Pillar contribution analysis",
        stats_dict={
            "n":                   int(len(sub)),
            "top_contributor":     top_label,
            "top_contributor_pct": round(float(top_pct_v), 2),
            "mean_contribution":   {k: float(v) for k, v in mean_contrib.items()},
        },
        chart_paths=[path],
        interpretation=interp,
        tables={"summary": summary, "mean_contribution": mean_contrib.to_frame("mean")},
    )


# ══ helpers for phase 2 ═════════════════════════════════════════════
def _deciles(scores: pd.Series, n: int = 10) -> pd.Series:
    """Returns decile labels D1..Dn with D1 = top."""
    return pd.qcut(scores.rank(method="first", ascending=False),
                   n, labels=[f"D{i+1}" for i in range(n)], duplicates="drop")


def _extract_computed_input(df: pd.DataFrame, bucket: str, key: str) -> pd.Series:
    """Pulls raw_financials.computed_inputs[bucket][key] as a coerced float series."""
    out = []
    for blob in df["raw_financials"]:
        d = _safe_json(blob)
        if d is None:
            out.append(np.nan)
            continue
        v = d.get("computed_inputs", {}).get(bucket, {}).get(key)
        out.append(_coerce_float(v) if v is not None else np.nan)
    return pd.Series(out, index=df.index, dtype=float)


# ── Analysis 7: Sector by composite decile + pillar-mean-by-sector ──
def run_analysis_7_sector_by_decile(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df.dropna(subset=["composite_score", "sector"]).copy()
    sub["sector"] = sub["sector"].fillna("Unknown").replace("", "Unknown")
    sub["decile"] = _deciles(sub["composite_score"])

    # Composition table: sector counts per decile, pct within decile
    counts = sub.groupby(["decile", "sector"]).size().unstack(fill_value=0)
    pct = counts.div(counts.sum(axis=1), axis=0) * 100.0
    pct = pct.round(2)

    comp_path = plots.stacked_bar(
        pct, "Sector composition by composite decile (% within decile)",
        os.path.join(charts_dir, "07a_sector_by_decile.png"),
        xlabel="decile (D1 = top)",
    )

    # Mean pillar score per sector: sectors × 5 pillars
    pillar_by_sector = sub.groupby("sector")[PILLAR_COLS].mean()
    pillar_by_sector = pillar_by_sector.rename(columns=PILLAR_SHORT).round(2)
    # Sort sectors by row mean descending for a readable heatmap
    pillar_by_sector = pillar_by_sector.loc[
        pillar_by_sector.mean(axis=1).sort_values(ascending=False).index
    ]

    heat_path = plots.heatmap(
        pillar_by_sector, "Mean pillar score per sector",
        os.path.join(charts_dir, "07b_pillar_mean_by_sector.png"),
        fmt=".1f", cmap="RdYlGn", vmin=0.0, vmax=70.0,
    )

    # Top-decile sector concentration
    top_decile_mix = pct.loc["D1"].sort_values(ascending=False).head(5) if "D1" in pct.index else pd.Series()

    # Which pillar is worst for which sector
    worst_pillar = pillar_by_sector.idxmin(axis=1)
    worst_score  = pillar_by_sector.min(axis=1)
    worst_df = pd.DataFrame({
        "worst_pillar": worst_pillar,
        "score":        worst_score.round(2),
    })
    worst_df.index.name = "sector"

    interp = (
        f"Top decile (D1) sector concentration: "
        + ", ".join(f"{s} {v:.1f}%" for s, v in top_decile_mix.items()) + ". "
        "The pillar-by-sector heatmap reveals whether a specific pillar "
        "systematically penalizes a specific sector — cells well below the "
        "row/column mean indicate structural mis-calibration (e.g., P3 "
        "punishing sectors with low dividend cultures)."
    )

    return AnalysisResult(
        name="7. Sector by composite decile + pillar-mean-by-sector",
        stats_dict={
            "n":                   int(len(sub)),
            "top_decile_top_sectors": {k: round(float(v), 2) for k, v in top_decile_mix.items()},
        },
        chart_paths=[comp_path, heat_path],
        interpretation=interp,
        tables={
            "sector_pct_by_decile": pct,
            "pillar_mean_by_sector": pillar_by_sector,
            "worst_pillar_per_sector": worst_df,
        },
    )


# ── Analysis 8: Market-cap tier by composite decile ─────────────────
def _tier_from_mcap(x: float) -> str:
    if not np.isfinite(x) or x <= 0:
        return "Unknown"
    if x >= 200e9:  return "Mega (>$200B)"
    if x >=  10e9:  return "Large ($10B-$200B)"
    if x >=   2e9:  return "Mid ($2B-$10B)"
    return "Small (<$2B)"


def run_analysis_8_market_cap_tier_by_decile(
    df: pd.DataFrame, universe_df: pd.DataFrame, charts_dir: str
) -> AnalysisResult:
    sub = df.dropna(subset=["composite_score", "market_cap"]).copy()
    sub["tier"] = sub["market_cap"].apply(_tier_from_mcap)
    sub["decile"] = _deciles(sub["composite_score"])

    tier_order = ["Mega (>$200B)", "Large ($10B-$200B)", "Mid ($2B-$10B)", "Small (<$2B)", "Unknown"]
    counts = sub.groupby(["decile", "tier"]).size().unstack(fill_value=0)
    counts = counts.reindex(columns=[t for t in tier_order if t in counts.columns], fill_value=0)
    pct = (counts.div(counts.sum(axis=1), axis=0) * 100.0).round(2)

    path = plots.stacked_bar(
        pct, "Market-cap tier composition by composite decile",
        os.path.join(charts_dir, "08_mcap_tier_by_decile.png"),
        xlabel="decile (D1 = top)",
    )

    # Tier-level mean composite
    tier_stats = sub.groupby("tier")["composite_score"].agg(["count", "mean", "std"]).round(2)
    tier_stats.index.name = "tier"

    top1 = pct.loc["D1"].sort_values(ascending=False) if "D1" in pct.index else pd.Series()
    bot1 = pct.loc[pct.index[-1]].sort_values(ascending=False) if len(pct.index) else pd.Series()

    interp = (
        f"D1 (top) tier mix: " + ", ".join(f"{k} {v:.1f}%" for k, v in top1.head(3).items()) + ". "
        f"Bottom decile tier mix: " + ", ".join(f"{k} {v:.1f}%" for k, v in bot1.head(3).items()) + ". "
        "If large/mega caps over-index in D1 relative to the universe, the "
        "framework favors size; if small caps over-index, the framework "
        "rewards scrappy scorers (and may be noisier)."
    )

    return AnalysisResult(
        name="8. Market-cap tier by composite decile",
        stats_dict={
            "n": int(len(sub)),
            "tier_distribution": {k: int(v) for k, v in sub["tier"].value_counts().items()},
        },
        chart_paths=[path],
        interpretation=interp,
        tables={"tier_pct_by_decile": pct, "tier_stats": tier_stats},
    )


# ── Analysis 9: Valuation traps + rare P3 winners ───────────────────
def run_analysis_9_valuation_traps(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df.dropna(subset=["composite_score"]).copy()

    # (a) High composite + low P5 — "expensive-but-loved" (high score despite poor valuation)
    a = sub[(sub["composite_score"] >= 70) & (sub["pillar_5_valuation"] < 30)].copy()
    a = a.sort_values("composite_score", ascending=False).head(15)

    # (b) High composite + high P5 — "reasonably priced winners"
    b = sub[(sub["composite_score"] >= 70) & (sub["pillar_5_valuation"] >= 70)].copy()
    b = b.sort_values("composite_score", ascending=False).head(15)

    # (c) Rare P3 winners — P3 >= 80
    c = sub[sub["pillar_3_capital_allocation"] >= 80].copy()
    # Pull cap_allocation fundamentals for context
    c["share_trend"]      = _extract_computed_input(c, "cap_allocation", "share_trend")
    c["payout_ratio"]     = _extract_computed_input(c, "cap_allocation", "payout_ratio")
    c["roic_wacc_spread"] = _extract_computed_input(c, "cap_allocation", "roic_wacc_spread")
    c = c.sort_values("pillar_3_capital_allocation", ascending=False).head(15)

    cols = ["symbol", "sector", "composite_score",
            "pillar_1_business_quality", "pillar_3_capital_allocation", "pillar_5_valuation"]

    def _tidy(xdf):
        if xdf.empty:
            return xdf
        x = xdf[cols].copy().rename(columns={
            "pillar_1_business_quality":   "P1",
            "pillar_3_capital_allocation": "P3",
            "pillar_5_valuation":          "P5",
            "composite_score":             "composite",
        })
        x.set_index("symbol", inplace=True)
        return x.round(2)

    cols_c = cols + ["share_trend", "payout_ratio", "roic_wacc_spread"]
    c_tidy = c[cols_c].rename(columns={
        "pillar_1_business_quality":   "P1",
        "pillar_3_capital_allocation": "P3",
        "pillar_5_valuation":          "P5",
        "composite_score":             "composite",
    }).set_index("symbol").round(3) if not c.empty else c

    counts = {
        "expensive_but_loved": int(len(sub[(sub["composite_score"] >= 70) &
                                           (sub["pillar_5_valuation"] < 30)])),
        "priced_winners":      int(len(sub[(sub["composite_score"] >= 70) &
                                           (sub["pillar_5_valuation"] >= 70)])),
        "rare_p3_winners":     int(len(sub[sub["pillar_3_capital_allocation"] >= 80])),
    }

    interp = (
        f"Expensive-but-loved (composite≥70 AND P5<30): **{counts['expensive_but_loved']}** symbols — these pass the "
        "quality bar but the valuation pillar flags them as rich. "
        f"Reasonably-priced winners (composite≥70 AND P5≥70): **{counts['priced_winners']}** — should be prioritized "
        "for entry point analysis. "
        f"Rare P3 winners (P3≥80): **{counts['rare_p3_winners']}** out of {len(sub)} — inspect the table "
        "to see whether high P3 is actually rewarding strong buyback/dividend/ROIC-WACC fundamentals "
        "(share_trend<0, payout_ratio in a reasonable band, roic_wacc_spread>0) or something spurious."
    )

    return AnalysisResult(
        name="9. Valuation traps + rare P3 winners",
        stats_dict=counts,
        interpretation=interp,
        tables={
            "expensive_but_loved": _tidy(a),
            "priced_winners":      _tidy(b),
            "rare_p3_winners":     c_tidy,
        },
    )


# ── Analysis 10: Composite vs fundamentals + per-pillar diagnostic ──
def run_analysis_10_composite_vs_fundamentals(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    # Core 4 fundamentals
    fundamentals = {
        "ROIC":            _extract_computed_input(df, "biz_quality",    "roic"),
        "debt_to_ebitda":  _extract_computed_input(df, "ops_health",     "debt_to_ebitda"),
        "EV_EBITDA":       _extract_computed_input(df, "valuation",      "ev_ebitda"),
        "revenue_cagr_3y": _extract_computed_input(df, "growth",         "revenue_cagr_3y"),
    }
    # P3-specific diagnostics
    p3_fund = {
        "share_trend":      _extract_computed_input(df, "cap_allocation", "share_trend"),
        "payout_ratio":     _extract_computed_input(df, "cap_allocation", "payout_ratio"),
        "roic_wacc_spread": _extract_computed_input(df, "cap_allocation", "roic_wacc_spread"),
    }

    # Clip extreme EV/EBITDA outliers for plotting only (keeps rank-corr intact)
    fundamentals["EV_EBITDA_clip"] = fundamentals["EV_EBITDA"].clip(lower=-200, upper=200)

    # ── Composite vs core fundamentals (Spearman rank correlation) ──
    comp_rows = []
    scatter_pairs = []
    for name, ser in list(fundamentals.items()):
        if name.endswith("_clip"):
            continue
        pair = pd.concat([df["composite_score"], ser], axis=1).dropna()
        if len(pair) < 30:
            comp_rows.append({"fundamental": name, "n": len(pair), "spearman_r": None, "pearson_r": None})
            continue
        r_sp, p_sp = sci_stats.spearmanr(pair.iloc[:, 0], pair.iloc[:, 1])
        r_pe, p_pe = sci_stats.pearsonr (pair.iloc[:, 0], pair.iloc[:, 1])
        comp_rows.append({
            "fundamental": name, "n": len(pair),
            "spearman_r": round(float(r_sp), 4), "spearman_p": round(float(p_sp), 6),
            "pearson_r":  round(float(r_pe), 4), "pearson_p":  round(float(p_pe), 6),
        })
        # Use clipped EV/EBITDA for plotting
        xser = fundamentals["EV_EBITDA_clip"] if name == "EV_EBITDA" else ser
        scatter_pairs.append((name, xser.loc[pair.index], pair.iloc[:, 0], name))
    comp_df = pd.DataFrame(comp_rows).set_index("fundamental")

    comp_path = plots.scatter_grid(
        scatter_pairs, "Composite score vs core fundamentals",
        os.path.join(charts_dir, "10a_composite_vs_fundamentals.png"),
        ncols=2,
    )

    # ── Per-pillar vs corresponding fundamental (Spearman) ──
    pillar_pairs = [
        ("P1 BizQual",   "pillar_1_business_quality",   fundamentals["ROIC"],            True,  "higher-better"),
        ("P2 OpsHealth", "pillar_2_operational_health", fundamentals["debt_to_ebitda"],  False, "lower-better"),
        ("P3 CapAlloc(share_trend)",   "pillar_3_capital_allocation", p3_fund["share_trend"],
             False, "more-negative = more buybacks"),
        ("P3 CapAlloc(payout_ratio)",  "pillar_3_capital_allocation", p3_fund["payout_ratio"],
             None,  "bell-shaped ideal"),
        ("P3 CapAlloc(roic_wacc)",     "pillar_3_capital_allocation", p3_fund["roic_wacc_spread"],
             True,  "higher-better"),
        ("P4 Growth",    "pillar_4_growth_quality",     fundamentals["revenue_cagr_3y"], True,  "higher-better"),
        ("P5 Valuation", "pillar_5_valuation",          fundamentals["EV_EBITDA"],       False, "lower-better"),
    ]
    pillar_rows = []
    flags: list[str] = []
    for label, pcol, fser, expect_positive, expected_note in pillar_pairs:
        pair = pd.concat([df[pcol], fser], axis=1).dropna()
        if len(pair) < 30:
            pillar_rows.append({"pillar_vs_fundamental": label, "n": len(pair),
                                "spearman_r": None, "expected": expected_note, "verdict": "N/A (low coverage)"})
            continue
        r, p = sci_stats.spearmanr(pair.iloc[:, 0], pair.iloc[:, 1])
        # verdict
        if expect_positive is True:
            ok = r > 0.3
            verdict = "OK" if ok else ("WEAK" if r > 0.1 else "BROKEN")
        elif expect_positive is False:
            ok = r < -0.3
            verdict = "OK" if ok else ("WEAK" if r < -0.1 else "BROKEN")
        else:
            verdict = "non-monotonic expected"
        if verdict == "BROKEN":
            flags.append(f"{label}: Spearman r={r:+.3f} (expected {expected_note})")
        pillar_rows.append({
            "pillar_vs_fundamental": label, "n": len(pair),
            "spearman_r": round(float(r), 4), "spearman_p": round(float(p), 6),
            "expected":   expected_note, "verdict": verdict,
        })
    pillar_df = pd.DataFrame(pillar_rows).set_index("pillar_vs_fundamental")

    # P3 diagnostic heatmap (P3 score vs all 3 cap_alloc fundamentals)
    p3_corr_rows = []
    for name, ser in p3_fund.items():
        pair = pd.concat([df["pillar_3_capital_allocation"], ser], axis=1).dropna()
        r = sci_stats.spearmanr(pair.iloc[:, 0], pair.iloc[:, 1])[0] if len(pair) >= 30 else np.nan
        p3_corr_rows.append({"fundamental": name, "n": len(pair), "spearman_r": None if np.isnan(r) else round(float(r), 4)})
    p3_corr_df = pd.DataFrame(p3_corr_rows).set_index("fundamental")

    interp = (
        "**Composite vs core fundamentals** (Spearman): ROIC "
        f"{comp_df.loc['ROIC','spearman_r'] if 'ROIC' in comp_df.index else 'N/A'}, "
        f"debt/EBITDA {comp_df.loc['debt_to_ebitda','spearman_r'] if 'debt_to_ebitda' in comp_df.index else 'N/A'}, "
        f"EV/EBITDA {comp_df.loc['EV_EBITDA','spearman_r'] if 'EV_EBITDA' in comp_df.index else 'N/A'}, "
        f"rev CAGR 3y {comp_df.loc['revenue_cagr_3y','spearman_r'] if 'revenue_cagr_3y' in comp_df.index else 'N/A'}. "
        "**Per-pillar vs should-be-strong fundamentals** — any row with "
        "verdict=BROKEN indicates scoring-logic failure for that pillar. "
        + (f"Flagged: {'; '.join(flags)}" if flags else "No pillar-fundamental links are BROKEN.")
    )

    return AnalysisResult(
        name="10. Composite vs fundamentals + per-pillar diagnostic",
        stats_dict={
            "core_n":   int(comp_df["n"].max()) if not comp_df.empty else 0,
            "broken_pillars": flags,
        },
        chart_paths=[comp_path],
        interpretation=interp,
        tables={
            "composite_vs_fundamentals":  comp_df,
            "pillar_vs_fundamentals":     pillar_df,
            "p3_diagnostic_correlations": p3_corr_df,
        },
    )


# ── Analysis 11: LLM vs quant alignment ─────────────────────────────
REC_ORDER = ["STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"]


def run_analysis_11_llm_quant_alignment(df: pd.DataFrame, charts_dir: str) -> AnalysisResult:
    sub = df.dropna(subset=["composite_score"]).copy()

    # (a) composite vs llm_conviction (Spearman)
    pair = sub[["composite_score", "llm_conviction"]].dropna()
    if len(pair) >= 30:
        r_sp, p_sp = sci_stats.spearmanr(pair["composite_score"], pair["llm_conviction"])
        r_pe, p_pe = sci_stats.pearsonr (pair["composite_score"], pair["llm_conviction"])
    else:
        r_sp = r_pe = p_sp = p_pe = float("nan")

    # (b) composite distribution by recommendation
    rec_sub = sub.dropna(subset=["llm_recommendation"]).copy()
    rec_sub["llm_recommendation"] = rec_sub["llm_recommendation"].astype(str)
    by_rec = rec_sub.groupby("llm_recommendation")["composite_score"].agg(
        ["count", "mean", "std", "median"]
    ).round(2)
    # Order by the canonical recommendation ladder where present
    order = [r for r in REC_ORDER if r in by_rec.index] + [r for r in by_rec.index if r not in REC_ORDER]
    by_rec = by_rec.reindex(order)

    # Monotonicity check: expected mean composite should descend from STRONG_BUY→STRONG_SELL
    means_ordered = [by_rec.loc[r, "mean"] for r in REC_ORDER if r in by_rec.index]
    monotonic = all(means_ordered[i] >= means_ordered[i+1] for i in range(len(means_ordered)-1))

    # Chart: histogram per recommendation
    hist_path = plots.histogram_grid(
        {r: rec_sub[rec_sub["llm_recommendation"] == r]["composite_score"]
         for r in order if (rec_sub["llm_recommendation"] == r).any()},
        "Composite score distribution by LLM recommendation",
        os.path.join(charts_dir, "11_llm_composite_by_rec.png"),
        ncols=3,
    )

    # Chart: bar of mean composite per recommendation
    bar_path = plots.bar_chart(
        list(by_rec.index), [float(x) for x in by_rec["mean"]],
        "Mean composite by LLM recommendation",
        os.path.join(charts_dir, "11_llm_mean_composite.png"),
        ylabel="mean composite",
    )

    rec_counts = rec_sub["llm_recommendation"].value_counts().reindex(order).fillna(0).astype(int)
    dominant = rec_counts.idxmax() if not rec_counts.empty else None
    dom_pct = (rec_counts.max() / rec_counts.sum() * 100) if rec_counts.sum() else 0.0

    interp = (
        f"Composite ↔ llm_conviction Spearman r={r_sp:+.3f} (Pearson {r_pe:+.3f}, n={len(pair)}). "
        f"LLM recommendation is dominated by **{dominant}** ({dom_pct:.1f}% of rated symbols) — "
        "if one bucket holds the majority the LLM is either calibrated very conservatively or "
        "being handed raw scores that push it there. "
        f"Mean composite across {len(by_rec)} recommendation buckets is "
        f"{'monotonic' if monotonic else '**NOT monotonic**'} in the STRONG_BUY→STRONG_SELL ladder; "
        "non-monotonicity indicates the LLM disagrees with the quant in a structured way."
    )

    return AnalysisResult(
        name="11. LLM vs quant alignment",
        stats_dict={
            "n_composite_vs_conviction":   int(len(pair)),
            "spearman_r":                  round(float(r_sp), 4) if not math.isnan(r_sp) else None,
            "pearson_r":                   round(float(r_pe), 4) if not math.isnan(r_pe) else None,
            "dominant_recommendation":     str(dominant) if dominant else None,
            "dominant_pct":                round(float(dom_pct), 2),
            "ladder_monotonic":            bool(monotonic),
        },
        chart_paths=[hist_path, bar_path],
        interpretation=interp,
        tables={"composite_by_recommendation": by_rec,
                "recommendation_counts":       rec_counts.to_frame("count")},
    )


# ── Analysis 12: Score stability from history ───────────────────────
def run_analysis_12_score_stability(
    history_df: pd.DataFrame, universe_size: int, charts_dir: str
) -> AnalysisResult:
    if history_df.empty:
        return AnalysisResult(
            name="12. Score stability from history",
            notes=["No history rows returned — table may be empty."],
            interpretation="Skipped — no history data available.",
        )

    df = history_df.copy()
    df["evaluated_at"] = pd.to_datetime(df["evaluated_at"], errors="coerce")
    df = df.dropna(subset=["evaluated_at", "composite_score"]).sort_values(
        ["symbol", "evaluated_at"]
    )

    # Parse snapshot → pillar_scores dict per row. Snapshot uses short keys
    # (business_quality, operational_health, capital_allocation, growth_quality,
    # valuation) rather than the company_evaluations column names.
    SNAPSHOT_TO_COL = {
        "business_quality":    "pillar_1_business_quality",
        "operational_health":  "pillar_2_operational_health",
        "capital_allocation":  "pillar_3_capital_allocation",
        "growth_quality":      "pillar_4_growth_quality",
        "valuation":           "pillar_5_valuation",
    }

    def _extract_pillars(blob) -> dict[str, float] | None:
        d = _safe_json(blob)
        if d is None:
            return None
        ps = d.get("pillar_scores")
        if not isinstance(ps, dict):
            return None
        out = {}
        for short, col in SNAPSHOT_TO_COL.items():
            v = ps.get(short)
            if v is not None:
                out[col] = _coerce_float(v)
        return out

    pscores = df["snapshot"].apply(_extract_pillars) if "snapshot" in df.columns else pd.Series([None] * len(df))
    pscores_df = pd.DataFrame(list(pscores.fillna({}).values), index=df.index)
    df = pd.concat([df, pscores_df], axis=1)

    # Compute consecutive deltas per symbol
    deltas: list[dict] = []
    for symbol, g in df.groupby("symbol"):
        g = g.reset_index(drop=True)
        if len(g) < 2:
            continue
        for i in range(1, len(g)):
            prev, cur = g.iloc[i - 1], g.iloc[i]
            dt_days = (cur["evaluated_at"] - prev["evaluated_at"]).total_seconds() / 86400.0
            if dt_days <= 0:
                continue
            row = {
                "symbol":          symbol,
                "from":            prev["evaluated_at"],
                "to":              cur["evaluated_at"],
                "days":            dt_days,
                "delta_composite": float(cur["composite_score"] - prev["composite_score"]),
            }
            for pcol in ["pillar_1_business_quality", "pillar_2_operational_health",
                         "pillar_3_capital_allocation", "pillar_4_growth_quality",
                         "pillar_5_valuation"]:
                a, b = prev.get(pcol), cur.get(pcol)
                if pd.notna(a) and pd.notna(b):
                    row[f"delta_{pcol}"] = float(b - a)
            deltas.append(row)

    if not deltas:
        return AnalysisResult(
            name="12. Score stability from history",
            notes=["No symbols have >=2 usable consecutive snapshots."],
            interpretation="Skipped — insufficient paired snapshots.",
        )

    ddf = pd.DataFrame(deltas)
    ddf["abs_delta"]         = ddf["delta_composite"].abs()
    ddf["per_day_abs_delta"] = ddf["abs_delta"] / ddf["days"].clip(lower=1.0)
    ddf["unstable_flag"]     = (ddf["abs_delta"] >= 10) & (ddf["days"] <= 14)

    n_pairs = len(ddf)
    thresh_stats = {
        "pct_abs_delta_ge_5":  round(float((ddf["abs_delta"] >= 5).mean()  * 100), 2),
        "pct_abs_delta_ge_10": round(float((ddf["abs_delta"] >= 10).mean() * 100), 2),
        "pct_abs_delta_ge_20": round(float((ddf["abs_delta"] >= 20).mean() * 100), 2),
        "pct_unstable_flag":   round(float(ddf["unstable_flag"].mean()     * 100), 2),
    }

    summary = pd.DataFrame([{
        "n_pairs":              n_pairs,
        "n_symbols":            int(ddf["symbol"].nunique()),
        "mean_days_between":    round(float(ddf["days"].mean()),          2),
        "median_days_between":  round(float(ddf["days"].median()),        2),
        "mean_abs_delta":       round(float(ddf["abs_delta"].mean()),     2),
        "median_abs_delta":     round(float(ddf["abs_delta"].median()),   2),
        "mean_per_day_abs":     round(float(ddf["per_day_abs_delta"].mean()), 4),
        **thresh_stats,
    }]).T.rename(columns={0: "value"})
    summary.index.name = "metric"

    # Per-pillar stability
    per_pillar_rows = []
    for pcol in ["pillar_1_business_quality", "pillar_2_operational_health",
                 "pillar_3_capital_allocation", "pillar_4_growth_quality",
                 "pillar_5_valuation"]:
        col = f"delta_{pcol}"
        if col not in ddf.columns:
            continue
        s = ddf[col].dropna().abs()
        if s.empty:
            continue
        per_pillar_rows.append({
            "pillar":             PILLAR_SHORT[pcol],
            "n":                  int(len(s)),
            "mean_abs_delta":     round(float(s.mean()), 3),
            "median_abs_delta":   round(float(s.median()), 3),
            "pct_ge_5":           round(float((s >= 5).mean()  * 100), 2),
            "pct_ge_10":          round(float((s >= 10).mean() * 100), 2),
        })
    per_pillar_df = pd.DataFrame(per_pillar_rows).set_index("pillar") if per_pillar_rows else pd.DataFrame()

    # Histograms
    hist_path = plots.single_histogram(
        ddf["delta_composite"], "Composite-score delta between consecutive snapshots",
        os.path.join(charts_dir, "12a_composite_delta_hist.png"),
        bins=50, xlabel="Δ composite_score",
    )

    pillar_hist_path = None
    if per_pillar_rows:
        series_map = {}
        for pcol in ["pillar_1_business_quality", "pillar_2_operational_health",
                     "pillar_3_capital_allocation", "pillar_4_growth_quality",
                     "pillar_5_valuation"]:
            col = f"delta_{pcol}"
            if col in ddf.columns:
                series_map[PILLAR_SHORT[pcol]] = ddf[col].dropna()
        pillar_hist_path = plots.histogram_grid(
            series_map, "Per-pillar score delta between consecutive snapshots",
            os.path.join(charts_dir, "12b_pillar_delta_hist.png"),
            ncols=3,
        )

    noisy_pillar = None
    if not per_pillar_df.empty:
        noisy_pillar = per_pillar_df["mean_abs_delta"].idxmax()

    interp = (
        f"{n_pairs} consecutive snapshot pairs across {ddf['symbol'].nunique()} symbols, "
        f"mean gap {ddf['days'].mean():.1f} days. Composite |Δ| distribution: "
        f"{thresh_stats['pct_abs_delta_ge_5']}% ≥5, {thresh_stats['pct_abs_delta_ge_10']}% ≥10, "
        f"{thresh_stats['pct_abs_delta_ge_20']}% ≥20. "
        f"**{thresh_stats['pct_unstable_flag']}% flagged unstable** (|Δ|≥10 AND gap ≤14 days). "
        + (f"Noisiest pillar by mean |Δ|: **{noisy_pillar}** "
           f"({per_pillar_df.loc[noisy_pillar, 'mean_abs_delta']:.2f} avg absolute change). "
           if noisy_pillar else "")
        + "Rank is NULL throughout history; composite-score delta is the stability proxy."
    )

    charts = [hist_path]
    if pillar_hist_path:
        charts.append(pillar_hist_path)

    return AnalysisResult(
        name="12. Score stability from history",
        stats_dict={
            "n_pairs":            n_pairs,
            "n_symbols":          int(ddf["symbol"].nunique()),
            "mean_abs_delta":     round(float(ddf["abs_delta"].mean()), 3),
            **thresh_stats,
            "noisiest_pillar":    noisy_pillar,
        },
        chart_paths=charts,
        interpretation=interp,
        tables={"summary": summary, "per_pillar": per_pillar_df},
        notes=["Rank is NULL in evaluation_history for every row; composite-score delta used as stability proxy."],
    )


# ── Analysis 13: Evaluation version audit ───────────────────────────
def run_analysis_13_evaluation_version_audit(
    version_df: pd.DataFrame, selected: str | None
) -> AnalysisResult:
    if version_df.empty:
        return AnalysisResult(
            name="13. Evaluation version audit",
            interpretation="No rows in company_evaluations to version.",
        )
    tbl = version_df.copy()
    total = int(tbl["n"].sum())
    tbl["pct_of_total"] = (tbl["n"] / total * 100).round(2)
    tbl = tbl.set_index("evaluation_version")

    dominant = tbl["n"].idxmax()
    dom_pct  = float(tbl.loc[dominant, "pct_of_total"])
    n_versions = len(tbl)

    if n_versions == 1:
        interp = (
            f"Single evaluation_version `{dominant}` covers all {total} rows. "
            "No cross-version contamination risk; version filter in the audit "
            "is effectively a no-op."
        )
    else:
        interp = (
            f"{n_versions} distinct evaluation_version values. Dominant: "
            f"`{dominant}` ({dom_pct:.1f}%). Audit filtered to `{selected}`. "
            "Rows at other versions are excluded from all cross-sectional "
            "analyses to avoid scoring-logic drift contamination."
        )

    return AnalysisResult(
        name="13. Evaluation version audit",
        stats_dict={
            "n_versions":        n_versions,
            "dominant_version":  dominant,
            "dominant_pct":      round(dom_pct, 2),
            "selected_filter":   selected,
        },
        interpretation=interp,
        tables={"version_distribution": tbl},
    )

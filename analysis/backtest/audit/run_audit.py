"""CLI entrypoint for the Phase A audit.

Usage:
    python -m analysis.backtest.audit.run_audit              # full audit
    python -m analysis.backtest.audit.run_audit --partial    # analyses 1-6 only

Reads from the main DB via the existing async session factory. Write path
is ``docs/BACKTEST_PHASE_A_AUDIT.md`` plus PNGs under
``docs/backtest_audit_charts/``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from config import get_settings
from db.database import init_db, get_session
from analysis.backtest.audit import analyses as A
from analysis.backtest.audit import queries as Q
from analysis.backtest.audit.report_builder import build_report


_log = logging.getLogger("audit")


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPORT_PATH = PROJECT_ROOT / "docs" / "BACKTEST_PHASE_A_AUDIT.md"
CHARTS_DIR = PROJECT_ROOT / "docs" / "backtest_audit_charts"


async def _load_data(session, version_filter: str | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    params = {"version": version_filter}

    ver_rows = (await session.execute(text(Q.Q_EVALUATION_VERSION_DISTRIBUTION))).fetchall()
    version_df = pd.DataFrame(ver_rows, columns=["evaluation_version", "n"])

    eval_rows = (await session.execute(text(Q.Q_COMPANY_EVALUATIONS), params)).mappings().all()
    eval_df = pd.DataFrame(eval_rows)

    univ_rows = (await session.execute(text(Q.Q_UNIVERSE_SYMBOLS))).mappings().all()
    universe_df = pd.DataFrame(univ_rows)

    hist_rows = (await session.execute(text(Q.Q_EVALUATION_HISTORY))).mappings().all()
    history_df = pd.DataFrame(hist_rows)

    size_row = (await session.execute(text(Q.Q_UNIVERSE_SIZE), params)).fetchone()
    universe_size = int(size_row[0]) if size_row else 0

    return eval_df, universe_df, history_df, version_df, universe_size


def _git_describe() -> str:
    try:
        out = subprocess.check_output(
            ["git", "describe", "--always", "--tags", "--dirty"],
            cwd=str(PROJECT_ROOT), stderr=subprocess.DEVNULL,
        )
        return out.decode().strip()
    except Exception:
        return "unknown"


def _pick_version(version_df: pd.DataFrame) -> str | None:
    if version_df.empty:
        return None
    # Most common version; ignore '<null>' unless it's the only option
    non_null = version_df[version_df["evaluation_version"] != "<null>"]
    if not non_null.empty:
        return str(non_null.sort_values("n", ascending=False).iloc[0]["evaluation_version"])
    return None


async def _amain(partial: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    charts_dir = str(CHARTS_DIR)
    os.makedirs(charts_dir, exist_ok=True)

    settings = get_settings()
    await init_db(settings.database_url)

    async with get_session() as session:
        # Preflight: pick version
        ver_rows = (await session.execute(text(Q.Q_EVALUATION_VERSION_DISTRIBUTION))).fetchall()
        version_df = pd.DataFrame(ver_rows, columns=["evaluation_version", "n"])
        selected_version = _pick_version(version_df)
        _log.info("evaluation_version filter = %s (distribution: %s)",
                  selected_version, dict(zip(version_df["evaluation_version"], version_df["n"])))

        eval_df, universe_df, history_df, version_df, universe_size = await _load_data(session, selected_version)

    _log.info("Loaded eval_df=%d rows, universe_df=%d, history_df=%d, universe_size=%d",
              len(eval_df), len(universe_df), len(history_df), universe_size)

    results: list[A.AnalysisResult] = []

    funcs_phase1 = [
        ("1. Pillar correlation matrix",             lambda: A.run_analysis_1_pillar_correlations(eval_df, charts_dir)),
        ("2. Within-pillar metric correlation",      lambda: A.run_analysis_2_within_pillar_correlations(eval_df, charts_dir)),
        ("3. PCA on pillar scores",                  lambda: A.run_analysis_3_pca_on_pillars(eval_df, charts_dir)),
        ("4. Pillar score distributions",            lambda: A.run_analysis_4_pillar_distributions(eval_df, charts_dir)),
        ("5. Composite score distribution",          lambda: A.run_analysis_5_composite_distribution(eval_df, charts_dir)),
        ("6. Pillar contribution analysis",          lambda: A.run_analysis_6_pillar_contributions(eval_df, charts_dir)),
    ]
    funcs_phase2 = []
    if not partial:
        funcs_phase2 = [
            ("7. Sector by decile",                  lambda: A.run_analysis_7_sector_by_decile(eval_df, charts_dir)),
            ("8. Market cap tier by decile",         lambda: A.run_analysis_8_market_cap_tier_by_decile(eval_df, universe_df, charts_dir)),
            ("9. Valuation traps",                   lambda: A.run_analysis_9_valuation_traps(eval_df, charts_dir)),
            ("10. Composite vs fundamentals",        lambda: A.run_analysis_10_composite_vs_fundamentals(eval_df, charts_dir)),
            ("11. LLM-quant alignment",              lambda: A.run_analysis_11_llm_quant_alignment(eval_df, charts_dir)),
            ("12. Score stability from history",     lambda: A.run_analysis_12_score_stability(history_df, universe_size, charts_dir)),
            ("13. Evaluation version audit",         lambda: A.run_analysis_13_evaluation_version_audit(version_df, selected_version)),
        ]

    all_funcs = funcs_phase1 + funcs_phase2
    for label, fn in tqdm(all_funcs, desc="analyses"):
        try:
            res = fn()
            results.append(res)
            _log.info("OK   %s", label)
        except NotImplementedError:
            _log.warning("SKIP %s (not implemented yet)", label)
        except Exception as e:
            _log.exception("FAIL %s: %s", label, e)
            results.append(A.AnalysisResult(
                name=label, notes=[f"FAILED: {e}"],
                interpretation="Analysis failed — see notes.",
            ))

    methodology_notes = [
        "NULL composite_score rows were excluded at the SQL level.",
        f"Filtered to evaluation_version = `{selected_version}`; other versions excluded.",
        "Deciles use `pd.qcut(composite_score, 10, labels=D1..D10, duplicates='drop')` with D1 = top.",
        "Within-pillar sub-metric extraction tolerates missing or malformed JSON (row skipped, "
         "noted in Analysis 2). Sub-metrics with <50% coverage across the universe are dropped.",
        "Analysis 12 measures **composite-score delta** between consecutive history snapshots per "
         "symbol, not rank delta, because `evaluation_history.rank` is NULL for all rows — rank "
         "is computed and persisted only to `company_evaluations`. Thresholds: |Δ|≥5, ≥10, ≥20; "
         "flagged-as-unstable = |Δ|≥10 AND gap ≤ 14 days.",
        "Normality test (Analysis 5): Anderson-Darling + Shapiro-Wilk reported, but at n≈2000 both "
         "trivially reject for minor deviations. Skew and kurtosis are the operative shape summaries.",
        "Sector (Analysis 7) taken from `company_evaluations.sector` (evaluation-time snapshot, "
         "authoritative for that run).",
        "Analyses may reference slightly different N values due to ongoing crawler activity during "
         "audit execution (observed drift ~1% between STOP 1 and STOP 3); this does not change "
         "any structural conclusions.",
    ]

    # ── Auto-populate Framework Concerns / Strengths from results ────
    concerns: list[str] = []
    strengths: list[str] = []

    # Standing concerns from the audit design
    concerns.append(
        "**P2 sub-metric coverage (data-provider gap).** `debt_to_ebitda` and `sga_efficiency` "
        "fall below 50% coverage primarily because Polygon does not populate `long_term_debt` "
        "and `selling_general_administrative` for many financials, REITs, and utilities — not "
        "because of sentinel coercion. The only string sentinel in P2 is `no_debt` on "
        "`interest_coverage`, which is already mapped to score=100 in the scoring layer. "
        "(The `routine_selling` sentinel lives in P3's smart-money analyzer and the breakout "
        "logic; it does not touch P2.) Full normalization details are in "
        "`docs/P2_NORMALIZATION_REFERENCE.md`."
    )

    # Pull per-analysis flags
    for res in results:
        # Analysis 10 pillar-fundamental BROKEN flags
        broken = res.stats_dict.get("broken_pillars") if isinstance(res.stats_dict, dict) else None
        if broken:
            for b in broken:
                concerns.append(f"**Pillar-fundamental link BROKEN** ({res.name}): {b}")
        # Analysis 12 flagged instability rate
        pct_unstable = res.stats_dict.get("pct_unstable_flag") if isinstance(res.stats_dict, dict) else None
        if isinstance(pct_unstable, (int, float)) and pct_unstable >= 10.0:
            concerns.append(
                f"**Score instability (Analysis 12):** {pct_unstable}% of consecutive snapshot "
                f"pairs are flagged unstable (|Δ composite|≥10 AND gap ≤14 days). Noisiest "
                f"pillar: {res.stats_dict.get('noisiest_pillar')}."
            )
        # Analysis 11 non-monotonic ladder
        if res.name.startswith("11.") and res.stats_dict.get("ladder_monotonic") is False:
            concerns.append(
                "**LLM ladder is NOT monotonic in composite score** (Analysis 11) — "
                "the LLM recommendation ordering disagrees with the quant composite in a structured way."
            )

    # Strengths — also auto-flagged where obvious
    for res in results:
        if res.name.startswith("3.") and isinstance(res.stats_dict.get("pc1_explained"), (int, float)):
            if res.stats_dict["pc1_explained"] < 0.5:
                strengths.append(
                    f"**Multi-factor framework retained** (Analysis 3): PC1 = "
                    f"{res.stats_dict['pc1_explained']*100:.1f}% < 50% — pillars genuinely capture "
                    f"distinct dimensions rather than collapsing to a single axis."
                )
        if res.name.startswith("5.") and isinstance(res.stats_dict.get("skew"), (int, float)):
            if abs(res.stats_dict["skew"]) < 0.2 and abs(res.stats_dict.get("kurtosis") or 0) < 1.0:
                strengths.append(
                    f"**Composite distribution is well-shaped** (Analysis 5): skew="
                    f"{res.stats_dict['skew']:.2f}, excess kurtosis="
                    f"{res.stats_dict['kurtosis']:.2f} — symmetric and platykurtic, clean "
                    f"discrimination across the universe."
                )
        if res.name.startswith("1.") and isinstance(res.stats_dict.get("pearson_max_abs"), (int, float)):
            if res.stats_dict["pearson_max_abs"] < 0.7:
                strengths.append(
                    f"**No redundant pillar pair** (Analysis 1): max |Pearson r| = "
                    f"{res.stats_dict['pearson_max_abs']:.2f} < 0.7 between any two pillar scores."
                )

    report_path = str(REPORT_PATH)
    build_report(
        results,
        report_path=report_path,
        universe_size=universe_size,
        version_filter=selected_version,
        code_tag=_git_describe(),
        concerns=concerns,
        strengths=strengths,
        queries=Q.ALL_QUERIES,
        methodology_notes=methodology_notes,
        partial=partial,
    )
    _log.info("Report written: %s", report_path)
    _log.info("Charts dir: %s", charts_dir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partial", action="store_true",
                        help="Run analyses 1-6 only (STOP 2 checkpoint)")
    args = parser.parse_args()
    asyncio.run(_amain(partial=args.partial))


if __name__ == "__main__":
    main()

"""Markdown report assembler.

No interpretation logic here — interpretations come from the analyses.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd

from analysis.backtest.audit.analyses import AnalysisResult


def _df_to_md(df: pd.DataFrame, floatfmt: str = "{:.3f}") -> str:
    if df is None or df.empty:
        return "_(empty)_"
    # Normalize numeric values
    def fmt(v):
        if isinstance(v, float):
            if pd.isna(v):
                return ""
            return floatfmt.format(v)
        return str(v)
    cols = list(df.columns)
    lines = ["| " + (df.index.name or "") + " | " + " | ".join(str(c) for c in cols) + " |",
             "| " + "---" + " | " + " | ".join(["---"] * len(cols)) + " |"]
    for idx, row in df.iterrows():
        lines.append("| " + str(idx) + " | " + " | ".join(fmt(row[c]) for c in cols) + " |")
    return "\n".join(lines)


def _chart_md(paths: Iterable[str], report_dir: str) -> str:
    out = []
    for p in paths:
        rel = os.path.relpath(p, report_dir).replace("\\", "/")
        name = os.path.splitext(os.path.basename(p))[0]
        out.append(f"![{name}]({rel})")
    return "\n\n".join(out)


def _notes_md(notes: list[str]) -> str:
    if not notes:
        return ""
    return "\n".join(f"- {n}" for n in notes)


def build_report(
    results: list[AnalysisResult],
    *,
    report_path: str,
    universe_size: int,
    version_filter: str | None,
    code_tag: str,
    concerns: list[str],
    strengths: list[str],
    queries: dict[str, str],
    methodology_notes: list[str],
    partial: bool = False,
) -> str:
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report_dir = os.path.dirname(report_path)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    parts: list[str] = []
    parts.append("# Company Evaluator — Phase A Cross-Sectional Audit")
    if partial:
        parts.append("> **Partial report — STOP 2 checkpoint.** Covers analyses 1–6 only.")
    parts.append("")
    parts.append(f"Generated: {now_iso}")
    parts.append(f"Universe size (evaluated, filtered): {universe_size}")
    parts.append(f"Evaluation version filter: `{version_filter or '<none — all versions>'}`")
    parts.append(f"Code tag: `{code_tag}`")
    parts.append("")

    # Executive summary
    parts.append("## Executive Summary")
    if partial:
        parts.append("_Deferred to full report._")
    else:
        if not concerns and not strengths:
            parts.append("_(populated manually from analysis findings; "
                         "otherwise left for the human reviewer)_")
        for s in strengths[:5]:
            parts.append(f"- ✅ {s}")
        for c in concerns[:5]:
            parts.append(f"- ⚠️ {c}")
    parts.append("")

    # Per-analysis sections
    for i, res in enumerate(results, start=1):
        parts.append(f"## {res.name}")
        if res.interpretation:
            parts.append(f"**Interpretation:** {res.interpretation}")
            parts.append("")
        if res.stats_dict:
            parts.append("**Stats:**")
            for k, v in res.stats_dict.items():
                if isinstance(v, dict):
                    parts.append(f"- `{k}`:")
                    for k2, v2 in v.items():
                        parts.append(f"    - `{k2}` = {v2}")
                elif isinstance(v, list):
                    parts.append(f"- `{k}` = {v}")
                else:
                    parts.append(f"- `{k}` = {v}")
            parts.append("")
        if res.tables:
            for name, df in res.tables.items():
                parts.append(f"**Table — {name}:**")
                parts.append("")
                parts.append(_df_to_md(df))
                parts.append("")
        if res.chart_paths:
            parts.append(_chart_md(res.chart_paths, report_dir))
            parts.append("")
        if res.notes:
            parts.append("**Notes:**")
            parts.append(_notes_md(res.notes))
            parts.append("")

    # Trailers
    if not partial:
        parts.append("## Framework Concerns")
        if concerns:
            for c in concerns:
                parts.append(f"- {c}")
        else:
            parts.append("_(to be populated from analyses above)_")
        parts.append("")
        parts.append("## Framework Strengths")
        if strengths:
            for s in strengths:
                parts.append(f"- {s}")
        else:
            parts.append("_(to be populated from analyses above)_")
        parts.append("")

    parts.append("## Appendix A: SQL Queries")
    parts.append("")
    for name, sql in queries.items():
        parts.append(f"### `{name}`")
        parts.append("```sql")
        parts.append(sql.strip())
        parts.append("```")
        parts.append("")

    parts.append("## Appendix B: Methodology Notes")
    for note in methodology_notes:
        parts.append(f"- {note}")
    parts.append("")

    content = "\n".join(parts)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)
    return report_path

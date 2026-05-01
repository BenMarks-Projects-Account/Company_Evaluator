"""Verification for growth_quality CAGR fix.

Covers:
  1. Synthetic test cases for `metrics.helpers.cagr` that exercise the
     negative/zero-start, negative-end, sign-flip, identity and numerical
     stability paths.
  2. Real symbols currently persisted with pillar_4_growth_quality IS NULL.
     Re-runs P4 on their persisted raw_financials.company_data snapshot
     (read-only, nothing is written back to the DB).

Run:
    python _verify_growth_quality_fix.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
import traceback
from typing import Any

from metrics.helpers import cagr
from metrics import growth_quality

DB_PATH = r"\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db"


# ---------------------------------------------------------------------------
# 1. Synthetic CAGR cases
# ---------------------------------------------------------------------------

SYNTHETIC_CASES: list[tuple[str, float | None, float | None, int, Any]] = [
    # (label, old, new, years, expected)
    ("negative start, positive end",   -100,   200,   3,  None),
    ("zero start",                        0,   100,   3,  None),
    ("positive start, negative end",    100,  -200,   3,  None),
    ("normal growth 100 -> 150 over 3y", 100,   150,   3,  "positive_float"),
    ("normal decline 100 -> 50 over 3y", 100,    50,   3,  "negative_float"),
    ("identity 100 -> 100 over 3y",      100,   100,   3,  0.0),
    ("tiny growth 100 -> 100.000001",    100,   100.000001, 3, "tiny_positive"),
    # Extra edge cases surfaced in investigation:
    ("None start",                       None,  100,   3,  None),
    ("None end",                         100,  None,   3,  None),
    ("zero years",                       100,   200,   0,  None),
    ("negative years",                   100,   200,  -1,  None),
]


def _classify(v) -> str:
    if v is None:
        return "None"
    if isinstance(v, complex):
        return f"complex({v})"
    if isinstance(v, float):
        return f"float({v!r})"
    return f"{type(v).__name__}({v!r})"


def _check_synthetic(label, old, new, years, expected) -> tuple[str, bool]:
    try:
        got = cagr(old, new, years)
    except Exception as e:
        return (f"raised {type(e).__name__}: {e}", False)

    got_repr = _classify(got)

    if expected is None:
        ok = got is None
    elif expected == "positive_float":
        ok = isinstance(got, float) and got > 0 and not isinstance(got, complex)
    elif expected == "negative_float":
        ok = isinstance(got, float) and got < 0 and not isinstance(got, complex)
    elif expected == "tiny_positive":
        ok = isinstance(got, float) and 0 < got < 1e-6
    elif isinstance(expected, float):
        ok = isinstance(got, float) and abs(got - expected) < 1e-12
    else:
        ok = False
    return (got_repr, ok)


def run_synthetic() -> list[dict]:
    print("=" * 70)
    print("SYNTHETIC CAGR CASES")
    print("=" * 70)
    print(f"{'Case':<42} {'Result':<28} {'Pass'}")
    print("-" * 78)
    rows = []
    for label, old, new, years, expected in SYNTHETIC_CASES:
        got_repr, ok = _check_synthetic(label, old, new, years, expected)
        rows.append({"case": label, "result": got_repr, "pass": ok})
        print(f"{label:<42} {got_repr:<28} {'PASS' if ok else 'FAIL'}")
    passed = sum(1 for r in rows if r["pass"])
    print(f"\nSynthetic: {passed}/{len(rows)} passed")
    return rows


# ---------------------------------------------------------------------------
# 2. Real-symbol rerun
# ---------------------------------------------------------------------------

def _load_null_p4_symbols(limit: int = 10) -> list[tuple[str, dict]]:
    """Return [(symbol, company_data), ...] for symbols with pillar_4 NULL.

    Only returns rows whose raw_financials JSON can be parsed and contains
    a company_data payload.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, raw_financials, evaluated_at
            FROM company_evaluations
            WHERE pillar_4_growth_quality IS NULL
              AND raw_financials IS NOT NULL
            ORDER BY evaluated_at DESC
        """)
        out: list[tuple[str, dict]] = []
        for symbol, raw_json, evaluated_at in cur.fetchall():
            if raw_json is None:
                continue
            try:
                raw = json.loads(raw_json)
            except Exception:
                continue
            # raw_financials schema: {"company_data": {...}, ...}
            company_data = raw.get("company_data") if isinstance(raw, dict) else None
            if not isinstance(company_data, dict):
                continue
            out.append((symbol, company_data))
            if len(out) >= limit:
                break
        return out
    finally:
        conn.close()


def rerun_real() -> list[dict]:
    print()
    print("=" * 70)
    print("REAL SYMBOLS (pillar_4 currently NULL) — re-run P4 with fix")
    print("=" * 70)
    samples = _load_null_p4_symbols(limit=10)
    if not samples:
        print("No symbols with NULL pillar_4 AND usable raw_financials found.")
        return []

    print(f"{'Symbol':<8} {'Pre-fix':<10} {'Post-fix pillar':<18} "
          f"{'rev3y':<10} {'rev5y':<10} {'fcf':<10} {'eps':<10} {'margin':<10}")
    print("-" * 96)
    rows = []
    for symbol, company_data in samples:
        try:
            result = growth_quality.compute(company_data)
            post = result.get("pillar_score")
            m = result.get("metrics", {})
            row = {
                "symbol": symbol,
                "pre_fix": None,
                "post_fix": post,
                "metrics": m,
                "error": None,
            }
        except Exception as e:
            row = {
                "symbol": symbol,
                "pre_fix": None,
                "post_fix": f"ERROR {type(e).__name__}",
                "metrics": {},
                "error": f"{e}\n{traceback.format_exc()}",
            }
        rows.append(row)
        m = row["metrics"]

        def _fmt(v):
            if v is None:
                return "None"
            if isinstance(v, (int, float)):
                return f"{v:.4f}"
            return str(v)

        print(f"{row['symbol']:<8} {'NULL':<10} {str(row['post_fix']):<18} "
              f"{_fmt(m.get('revenue_cagr_3y')):<10} {_fmt(m.get('revenue_cagr_5y')):<10} "
              f"{_fmt(m.get('fcf_growth')):<10} {_fmt(m.get('eps_growth_yoy')):<10} "
              f"{_fmt(m.get('margin_trend')):<10}")
    # Error detail footer
    for r in rows:
        if r["error"]:
            print(f"\n[ERROR] {r['symbol']}:\n{r['error']}")
    return rows


# ---------------------------------------------------------------------------
# 3. Summary table
# ---------------------------------------------------------------------------

def print_summary(synthetic_rows: list[dict], real_rows: list[dict]) -> int:
    print()
    print("=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    print(f"| {'Test case':<40} | {'Pre-fix':<20} | {'Post-fix':<20} | {'Pass?':<5} |")
    print(f"|{'-'*42}|{'-'*22}|{'-'*22}|{'-'*7}|")

    pre_expectations = {
        "negative start, positive end":   "complex",
        "zero start":                     "ZeroDivisionError/None",
        "positive start, negative end":   "complex",
        "normal growth 100 -> 150 over 3y":"~0.1447",
        "normal decline 100 -> 50 over 3y":"~-0.2063",
        "identity 100 -> 100 over 3y":    "0.0",
        "tiny growth 100 -> 100.000001":  "~3.3e-9",
        "None start":                     "None",
        "None end":                       "None",
        "zero years":                     "ZeroDivisionError/None",
        "negative years":                 "complex/float",
    }
    failed = 0
    for r in synthetic_rows:
        pre = pre_expectations.get(r["case"], "-")
        ok = "PASS" if r["pass"] else "FAIL"
        if not r["pass"]:
            failed += 1
        print(f"| {r['case']:<40} | {pre:<20} | {r['result']:<20} | {ok:<5} |")

    for r in real_rows:
        post = str(r["post_fix"])
        ok = "PASS" if not r["error"] else "FAIL"
        if r["error"]:
            failed += 1
        label = f"real: {r['symbol']}"
        print(f"| {label:<40} | {'error (complex)':<20} | {post:<20} | {ok:<5} |")

    print()
    print(f"Overall: {failed} failure(s)")
    return failed


if __name__ == "__main__":
    syn = run_synthetic()
    real = rerun_real()
    failures = print_summary(syn, real)
    sys.exit(1 if failures else 0)

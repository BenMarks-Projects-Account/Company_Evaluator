"""Integrity check: compare post-Tier-1 evaluation scores against the
audit snapshot taken before the optimizations.

Scores should match closely (identical composite/pillars for the same
input data; LLM text may differ since LLM is non-deterministic).
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import get_settings

SYMBOLS = ["MSFT", "AAPL", "WMS", "EXEL", "PLTR", "KO", "JPM", "XOM", "AMT", "DDOG"]

# Pre-Tier-1 scores (from PERF_AUDIT_REPORT.md / perf_audit.log composites).
# These were captured after the pre-tier1-optimizations tag.
PRE_TIER1 = {
    "MSFT": 69.9,
    "AAPL": None,  # not logged in audit summary
    "WMS": None,
    "EXEL": None,
    "PLTR": None,
    "KO": None,
    "JPM": None,
    "XOM": None,
    "AMT": 51.6,
    "DDOG": 39.3,
}


def main():
    settings = get_settings()
    conn = sqlite3.connect(settings.database_path)

    print(f"{'symbol':<6} {'composite':>9} {'P1':>6} {'P2':>6} {'P3':>6} "
          f"{'P4':>6} {'P5':>6} {'rank':>5}  pre_comp   delta")
    for sym in SYMBOLS:
        row = conn.execute(
            """SELECT symbol, composite_score,
                      pillar_1_business_quality, pillar_2_operational_health,
                      pillar_3_capital_allocation, pillar_4_growth_quality,
                      pillar_5_valuation, rank
               FROM company_evaluations WHERE symbol = ?""",
            (sym,),
        ).fetchone()
        if not row:
            print(f"{sym}: no row")
            continue
        comp = row[1] or 0
        pre = PRE_TIER1.get(sym)
        delta_s = f"{comp - pre:+.1f}" if pre is not None else "n/a"
        pre_s = f"{pre:.1f}" if pre is not None else "n/a"
        print(f"{row[0]:<6} {comp:>9.1f} "
              f"{(row[2] or 0):>6.1f} {(row[3] or 0):>6.1f} "
              f"{(row[4] or 0):>6.1f} {(row[5] or 0):>6.1f} "
              f"{(row[6] or 0):>6.1f} {(row[7] or 0):>5}  "
              f"{pre_s:>8}  {delta_s:>6}")


if __name__ == "__main__":
    main()

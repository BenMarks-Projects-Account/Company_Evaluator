"""P2 cleanup verification probe — STOP 2.

Compares persisted P2 scores (produced by the PRE-FIX code and stored in
`company_evaluations.pillar_2_detail.scores`) against scores recomputed
right now from the same persisted `raw_financials.company_data` using the
POST-FIX code. Reports per-symbol deltas and a per-metric summary.

Throwaway probe, kept as `_verify_*.py` per repo convention.
Run: `python _verify_p2_cleanup.py`
"""
from __future__ import annotations

import asyncio
import json
import statistics
from collections import defaultdict

from sqlalchemy import text

from config import get_settings
from db.database import get_session, init_db
from metrics import operational_health as oh

# 30 symbols covering the mix requested in STOP 2 plan.
SYMBOLS = [
    # Tech / Semi (mega & large)
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "ORCL", "CRM", "ADBE", "NOW", "INTC",
    # Financials (mega & large)
    "JPM", "BAC", "V", "MA", "BRK.B", "PYPL",
    # Utilities / Real Estate
    "NEE", "DUK", "AMT", "PLD",
    # Energy
    "XOM", "CVX",
    # Healthcare / Pharma
    "JNJ", "PFE", "LLY",
    # Industrials / Consumer
    "CAT", "DE", "WMT", "TGT", "MCD",
]

METRICS = ["sga_efficiency", "debt_to_ebitda", "interest_coverage",
           "current_ratio", "cash_conversion", "altman_z"]


async def main() -> None:
    s_cfg = get_settings()
    await init_db(s_cfg.database_url)

    placeholders = ",".join(f"'{s}'" for s in SYMBOLS)
    async with get_session() as s:
        r = await s.execute(text(f"""
            SELECT symbol, sector, market_cap, pillar_2_detail, raw_financials
            FROM company_evaluations
            WHERE symbol IN ({placeholders})
        """))
        rows = r.fetchall()

    rows_by_sym = {r[0]: r for r in rows}

    print(f"{'sym':8} {'sector':22} {'metric':20} {'pre_raw':>12} {'pre':>6} "
          f"{'post_raw':>12} {'post':>6} {'Δ':>7}")
    print("-" * 108)

    deltas_by_metric: dict[str, list[float]] = defaultdict(list)
    changed_by_metric: dict[str, int] = defaultdict(int)

    for sym in SYMBOLS:
        if sym not in rows_by_sym:
            print(f"{sym:8} -- MISSING FROM DB --")
            continue
        _, sector, _mcap, p2_json, rf_json = rows_by_sym[sym]
        p2 = p2_json if isinstance(p2_json, dict) else json.loads(p2_json)
        rf = json.loads(rf_json) if isinstance(rf_json, str) else rf_json
        company_data = rf.get("company_data") or {}

        pre_scores = p2.get("scores") or {}
        pre_raw = p2.get("raw_metrics") or {}

        post = oh.compute(company_data)
        post_scores = post.get("scores") or {}
        post_raw = post.get("raw_metrics") or {}

        for m in METRICS:
            pre_s = pre_scores.get(m)
            post_s = post_scores.get(m)
            pre_r = pre_raw.get(m)
            post_r = post_raw.get(m)

            # Delta only makes sense when both are numeric.
            if isinstance(pre_s, (int, float)) and isinstance(post_s, (int, float)):
                d = round(post_s - pre_s, 2)
            else:
                d = None

            changed = (pre_s != post_s)
            if changed:
                changed_by_metric[m] += 1
                if d is not None:
                    deltas_by_metric[m].append(abs(d))

            def _fmt(v):
                if v is None:
                    return "None"
                if isinstance(v, (int, float)):
                    return f"{v:.4g}"
                return str(v)

            if changed:
                print(f"{sym:8} {sector[:22]:22} {m:20} "
                      f"{_fmt(pre_r):>12} {_fmt(pre_s):>6} "
                      f"{_fmt(post_r):>12} {_fmt(post_s):>6} "
                      f"{(str(d) if d is not None else 'n/a'):>7}")

    print()
    print("=" * 72)
    print("Per-metric summary")
    print("=" * 72)
    print(f"{'metric':20} {'changed':>8} {'mean |Δ|':>10} {'max |Δ|':>10}")
    print("-" * 50)
    for m in METRICS:
        n = changed_by_metric[m]
        ds = deltas_by_metric[m]
        mean_d = statistics.mean(ds) if ds else 0.0
        max_d = max(ds) if ds else 0.0
        print(f"{m:20} {n:>8} {mean_d:>10.2f} {max_d:>10.2f}")

    print()
    print(f"Total symbols tested: {sum(1 for s in SYMBOLS if s in rows_by_sym)} / {len(SYMBOLS)}")


if __name__ == "__main__":
    asyncio.run(main())

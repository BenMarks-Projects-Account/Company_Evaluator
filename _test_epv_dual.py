"""Test dual EPV (trailing + normalized) with emergence signal across symbols."""
import sys, os, json, asyncio

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("DATABASE_URL", "sqlite:////192.168.1.149/CompanyEvaluatorData/company_evaluator/db/company_eval.db")

from db.database import init_db, get_session, CompanyEvaluation, DcfAnalysis, EvaAnalysis
from sqlalchemy import select
from metrics.helpers import get_statements
from analysis.epv_model import compute_epv


async def test_symbol(symbol: str):
    async with get_session() as session:
        row = (await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
        )).scalar_one_or_none()
        if not row:
            return f"{symbol}: NOT IN DB"

        wacc = None
        dcf_row = (await session.execute(
            select(DcfAnalysis).where(DcfAnalysis.symbol == symbol)
        )).scalar_one_or_none()
        if dcf_row and dcf_row.wacc:
            wacc = dcf_row.wacc
        if wacc is None:
            eva_row = (await session.execute(
                select(EvaAnalysis).where(EvaAnalysis.symbol == symbol)
            )).scalar_one_or_none()
            if eva_row and eva_row.wacc:
                wacc = eva_row.wacc

    raw = row.raw_financials if isinstance(row.raw_financials, dict) else (json.loads(row.raw_financials) if row.raw_financials else {})
    cd = raw.get("company_data", raw)
    annual = get_statements(cd, "annual")
    market_cap = row.market_cap

    epv = compute_epv(
        annual=annual,
        wacc=wacc,
        market_cap=market_cap,
        current_price=None,
        diluted_shares=None,
    )

    if not epv.get("ok"):
        return f"{symbol}: FAILED - {epv.get('error')}"

    t = epv["trailing"]
    n = epv["normalized"]
    e = epv["emergence"]
    inp = epv["shared_inputs"]

    def _fv(d):
        v = d.get("fair_value_per_share")
        return f"${v:>10}" if v is not None else "       N/A"
    def _pr(d):
        v = d.get("growth_premium_pct")
        return f"{v:>7.1f}%" if v is not None else "    N/A" 

    lines = [
        f"{symbol}:",
        f"  Trailing:    EPV/sh={_fv(t)}  premium={_pr(t)} [{t['growth_premium_label']}]",
        f"  Normalized:  EPV/sh={_fv(n)}  premium={_pr(n)} [{n['growth_premium_label']}]",
        f"  Emergence:   {e['signal']}  (ratio={e['trailing_to_normalized_ratio']}, growth_yrs={e['years_of_growth']}, one_time={e['one_time_flag']})",
        f"  WACC: {inp['wacc']:.4f} ({inp['wacc_source']})  |  Shares: {inp['diluted_shares']:,} ({inp['shares_source']})",
        f"  Interp: {e['interpretation']}",
    ]
    return "\n".join(lines)


async def main():
    await init_db(os.environ["DATABASE_URL"])

    print("=" * 80)
    print("QUALITY MEGA-CAPS")
    print("=" * 80)
    for sym in ["MSFT", "AAPL", "KO", "WMT", "SBUX"]:
        print(await test_symbol(sym))
        print()

    print("=" * 80)
    print("DEEP VALUE / CYCLICAL")
    print("=" * 80)
    for sym in ["F", "T", "VZ", "INTC", "CVS", "PFE"]:
        print(await test_symbol(sym))
        print()

    print("=" * 80)
    print("GROWTH / EMERGING")
    print("=" * 80)
    for sym in ["APP", "HOOD", "PLTR"]:
        print(await test_symbol(sym))
        print()

    print("=" * 80)
    print("EDGE CASES")
    print("=" * 80)
    for sym in ["SNOW", "ERIE", "CRWV"]:
        print(await test_symbol(sym))
        print()

    print("=" * 80)
    print("RECOVERING VALIDATION — checking for actual declines")
    print("=" * 80)
    # Re-run all and flag any RECOVERING without a decline
    all_syms = ["MSFT","AAPL","KO","WMT","SBUX","F","T","VZ","INTC","CVS","PFE","PLTR","SNOW"]
    for sym in all_syms:
        r = await test_symbol(sym)
        if "RECOVERING" in r:
            # Extract ebit history line
            for line in r.split("\n"):
                if "ebit_history" not in line and "Emergence:" in line:
                    print(f"  ** {sym}: {line.strip()}")
    print()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())

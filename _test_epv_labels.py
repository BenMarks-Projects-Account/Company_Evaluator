"""Test EPV label retuning — all 11 symbols via DB data."""
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

    if epv.get("ok"):
        inp = epv["inputs"]
        return (f"{symbol}: premium={epv['growth_premium_pct']}% [{epv['growth_premium_label']}] "
                f"| EPV/sh=${epv['fair_value_per_share']} | wacc_src={inp.get('wacc_source')} "
                f"| shares_src={inp.get('shares_source')}\n"
                f"    Interpretation: {epv['interpretation']}")
    else:
        return f"{symbol}: FAILED - {epv.get('error')}"


async def main():
    await init_db(os.environ["DATABASE_URL"])

    print("=" * 70)
    print("HEADLINE 5 (quality mega-caps)")
    print("=" * 70)
    for sym in ["MSFT", "AAPL", "SBUX", "KO", "WMT"]:
        print(await test_symbol(sym))

    print(f"\n{'=' * 70}")
    print("DEEP VALUE 6")
    print("=" * 70)
    for sym in ["F", "T", "VZ", "INTC", "CVS", "PFE"]:
        print(await test_symbol(sym))

    print(f"\n{'=' * 70}")
    print("EDGE CASES")
    print("=" * 70)
    for sym in ["SNOW", "ERIE", "CRWV"]:
        print(await test_symbol(sym))

    print(f"\n{'=' * 70}")
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())

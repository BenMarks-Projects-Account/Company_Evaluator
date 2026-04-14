"""Unit test: compute_epv() against saved DB data — no server needed."""
import sys, os, json, asyncio

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("DATABASE_URL", "sqlite:////192.168.1.149/CompanyEvaluatorData/company_evaluator/db/company_eval.db")

from db.database import init_db, get_session, CompanyEvaluation, DcfAnalysis, EvaAnalysis
from sqlalchemy import select
from metrics.helpers import get_statements
from analysis.epv_model import compute_epv


async def test_symbol(symbol: str):
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
        )
        row = result.scalar_one_or_none()
        if not row:
            print(f"\n{symbol}: NOT IN DB")
            return

        # Get WACC from DCF or EVA tables
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

    # Get market cap and shares
    market_cap = row.market_cap
    diluted_shares = annual[0].get("diluted_avg_shares") if annual else None

    # Derive price
    price = None
    if market_cap and diluted_shares:
        price = market_cap / diluted_shares

    epv = compute_epv(
        annual=annual,
        wacc=wacc,
        market_cap=market_cap,
        current_price=price,
        diluted_shares=diluted_shares,
    )

    print(f"\n{'='*60}")
    print(f"{symbol}:")
    if epv.get("ok"):
        print(f"  EPV/share: ${epv['fair_value_per_share']}")
        print(f"  Price:     ${epv.get('current_price')}")
        print(f"  EPV total: ${epv['epv_total']:,.0f}")
        print(f"  Mkt cap:   ${epv['market_cap']:,.0f}")
        print(f"  Premium:   {epv['growth_premium_pct']}% [{epv['growth_premium_label']}]")
        print(f"  WACC:      {epv['inputs']['wacc']}")
        print(f"  Tax rate:  {epv['inputs']['tax_rate']} ({epv['inputs']['tax_rate_source']})")
        print(f"  Norm EBIT: ${epv['inputs']['normalized_ebit']:,.0f} ({epv['inputs']['normalization_period_years']}y avg)")
        print(f"  NOPAT:     ${epv['inputs']['nopat']:,.0f}")
        print(f"  Shares:    {epv['inputs']['diluted_shares']:,}")
        print(f"  {epv['interpretation']}")
    else:
        print(f"  ok=false: {epv.get('error')}")


async def main():
    db_url = os.environ["DATABASE_URL"]
    await init_db(db_url)
    symbols = sys.argv[1:] or ["MSFT", "AAPL", "ERIE", "SBUX", "SNOW", "KO", "WMT", "CRWV"]
    for sym in symbols:
        await test_symbol(sym)
    print(f"\n{'='*60}\nDone.")


if __name__ == "__main__":
    asyncio.run(main())

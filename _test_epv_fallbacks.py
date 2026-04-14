"""B3: Re-test EPV with fallback fixes — unit test against DB data."""
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
            print(f"\n{symbol}: NOT IN DB")
            return

        # Get WACC from DCF or EVA tables (simulates full pipeline)
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
    # NOTE: we pass diluted_shares=None to exercise the in-function fallback
    # for ALL symbols, to test both diluted and basic fallback paths
    price = None
    diluted_via_annual = None
    if annual:
        diluted_via_annual = annual[0].get("diluted_avg_shares") or annual[0].get("basic_avg_shares")
    if market_cap and diluted_via_annual:
        price = market_cap / diluted_via_annual

    epv = compute_epv(
        annual=annual,
        wacc=wacc,            # None for SBUX/KO/WMT → triggers CAPM fallback
        market_cap=market_cap,
        current_price=price,
        diluted_shares=None,  # Force in-function extraction
    )

    if epv.get("ok"):
        inp = epv["inputs"]
        print(f"\n{symbol}: EPV/share=${epv['fair_value_per_share']}, "
              f"premium={epv['growth_premium_pct']}% [{epv['growth_premium_label']}], "
              f"shares_src={inp.get('shares_source')}, wacc_src={inp.get('wacc_source')}")
    else:
        print(f"\n{symbol}: FAILED - {epv.get('error')}")


async def test_wacc_fallback_directly():
    """Directly verify CAPM fallback triggers when wacc=None."""
    print("\n" + "=" * 60)
    print("DIRECT WACC FALLBACK TEST (wacc=None, synthetic data)")
    print("=" * 60)

    fake_annual = [
        {"operating_income": 1_000_000, "income_before_tax": 900_000, "income_tax": 180_000,
         "diluted_avg_shares": 1_000_000},
        {"operating_income": 900_000, "income_before_tax": 800_000, "income_tax": 160_000,
         "diluted_avg_shares": 1_000_000},
        {"operating_income": 800_000, "income_before_tax": 700_000, "income_tax": 140_000,
         "diluted_avg_shares": 1_000_000},
    ]
    result = compute_epv(
        annual=fake_annual,
        wacc=None,
        market_cap=10_000_000,
        current_price=10.0,
        diluted_shares=1_000_000,
    )
    if result.get("ok"):
        print(f"  WACC used: {result['inputs']['wacc']} (source: {result['inputs']['wacc_source']})")
        print(f"  EPV/share: ${result['fair_value_per_share']}")
        print(f"  Expected wacc_source to start with 'capm_fallback': "
              f"{'PASS' if result['inputs']['wacc_source'].startswith('capm_fallback') else 'FAIL'}")
    else:
        print(f"  UNEXPECTED FAILURE: {result.get('error')}")


async def test_shares_fallback_directly():
    """Verify basic_avg_shares fallback when diluted is None."""
    print("\n" + "=" * 60)
    print("DIRECT SHARES FALLBACK TEST (diluted=None, basic available)")
    print("=" * 60)

    fake_annual = [
        {"operating_income": 1_000_000, "income_before_tax": 900_000, "income_tax": 180_000,
         "basic_avg_shares": 500_000},  # NO diluted_avg_shares
        {"operating_income": 900_000, "income_before_tax": 800_000, "income_tax": 160_000,
         "basic_avg_shares": 500_000},
        {"operating_income": 800_000, "income_before_tax": 700_000, "income_tax": 140_000,
         "basic_avg_shares": 500_000},
    ]
    result = compute_epv(
        annual=fake_annual,
        wacc=0.10,
        market_cap=10_000_000,
        current_price=20.0,
        diluted_shares=None,  # Force fallback
    )
    if result.get("ok"):
        print(f"  Shares used: {result['inputs']['diluted_shares']} (source: {result['inputs']['shares_source']})")
        print(f"  Expected shares_source='basic_avg_shares': "
              f"{'PASS' if result['inputs']['shares_source'] == 'basic_avg_shares' else 'FAIL'}")
    else:
        print(f"  UNEXPECTED FAILURE: {result.get('error')}")


async def main():
    await init_db(os.environ["DATABASE_URL"])

    print("=" * 60)
    print("B3: EPV Fallback Verification")
    print("=" * 60)

    # Test real symbols from DB
    for sym in ["MSFT", "ERIE", "SBUX", "SNOW", "KO", "WMT", "CRWV"]:
        await test_symbol(sym)

    # Direct fallback tests
    await test_wacc_fallback_directly()
    await test_shares_fallback_directly()

    print(f"\n{'='*60}\nDone.")


if __name__ == "__main__":
    asyncio.run(main())

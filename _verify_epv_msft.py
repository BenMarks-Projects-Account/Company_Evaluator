"""Part A: Verify MSFT EPV math — dump the full input chain and hand-verify."""
import sys, os, json, asyncio

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("DATABASE_URL", "sqlite:////192.168.1.149/CompanyEvaluatorData/company_evaluator/db/company_eval.db")

from db.database import init_db, get_session, CompanyEvaluation, DcfAnalysis, EvaAnalysis
from sqlalchemy import select
from metrics.helpers import get_statements
from analysis.epv_model import compute_epv, _compute_tax_rate, _get


async def main():
    await init_db(os.environ["DATABASE_URL"])

    async with get_session() as session:
        row = (await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == "MSFT")
        )).scalar_one_or_none()
        dcf_row = (await session.execute(
            select(DcfAnalysis).where(DcfAnalysis.symbol == "MSFT")
        )).scalar_one_or_none()
        eva_row = (await session.execute(
            select(EvaAnalysis).where(EvaAnalysis.symbol == "MSFT")
        )).scalar_one_or_none()

    if not row:
        print("MSFT not in DB"); return

    # --- Extract raw data ---
    raw = row.raw_financials if isinstance(row.raw_financials, dict) else json.loads(row.raw_financials)
    cd = raw.get("company_data", raw)
    annual = get_statements(cd, "annual")

    # --- WACC ---
    wacc = None
    wacc_provenance = None
    if dcf_row and dcf_row.wacc:
        wacc = dcf_row.wacc
        wacc_provenance = f"DcfAnalysis table (wacc column) = {wacc}"
    if wacc is None and eva_row and eva_row.wacc:
        wacc = eva_row.wacc
        wacc_provenance = f"EvaAnalysis table (wacc column) = {wacc}"

    # --- market_cap and shares ---
    market_cap = row.market_cap
    diluted_shares = annual[0].get("diluted_avg_shares") if annual else None
    price = market_cap / diluted_shares if market_cap and diluted_shares else None

    print("=" * 70)
    print("PART A — MSFT EPV Verification Dump")
    print("=" * 70)

    # --- A1: Dump raw operating income history ---
    print("\n--- A1a: Raw annual operating income values (newest first) ---")
    print(f"{'Year':<12} {'Operating Income':>20} {'IBT':>20} {'Income Tax':>16} {'Eff Tax Rate':>14} {'Diluted Shares':>16}")
    print("-" * 100)
    for i, rec in enumerate(annual[:8]):
        oi = _get(rec, "operating_income")
        ibt = _get(rec, "income_before_tax")
        tax = _get(rec, "income_tax")
        ds = _get(rec, "diluted_avg_shares")
        period = rec.get("period_of_report_date") or rec.get("fiscal_year") or f"[idx {i}]"
        eff_tax = f"{tax/ibt:.4f}" if ibt and ibt > 0 and tax is not None else "n/a"
        print(f"{str(period):<12} {oi or 'None':>20} {ibt or 'None':>20} {tax or 'None':>16} {eff_tax:>14} {ds or 'None':>16}")

    # --- A1b: What the 5-year normalization picks up ---
    operating_incomes_5y = []
    for rec in annual:
        oi = _get(rec, "operating_income")
        if oi is not None:
            operating_incomes_5y.append(oi)
        if len(operating_incomes_5y) >= 5:
            break

    print(f"\n--- A1b: 5-year operating incomes used for normalization ---")
    for i, oi in enumerate(operating_incomes_5y):
        print(f"  Year {i+1}: ${oi:,.0f}")
    if len(operating_incomes_5y) == 5:
        avg = sum(operating_incomes_5y) / 5
        print(f"  Average (normalized EBIT): ${avg:,.0f}")

    # --- A1c: Tax rate computation ---
    tax_rate, tax_source = _compute_tax_rate(annual)
    print(f"\n--- A1c: Tax rate ---")
    print(f"  Computed tax rate: {tax_rate:.4f} ({tax_source})")
    # Show the individual rates that fed into it
    rates = []
    for rec in annual[:5]:
        ibt = _get(rec, "income_before_tax")
        tax = _get(rec, "income_tax")
        if ibt and ibt > 0 and tax is not None:
            r = tax / ibt
            if 0.0 <= r <= 0.50:
                rates.append(r)
                period = rec.get("period_of_report_date") or "?"
                print(f"    {period}: tax/ibt = {tax:,.0f} / {ibt:,.0f} = {r:.4f}")

    # --- A1d: WACC provenance ---
    print(f"\n--- A1d: WACC ---")
    print(f"  {wacc_provenance}")

    # --- A1e: Shares and price ---
    print(f"\n--- A1e: Shares, price, market cap ---")
    print(f"  diluted_avg_shares: {diluted_shares:,.0f}" if diluted_shares else "  diluted_avg_shares: None")
    print(f"  market_cap: ${market_cap:,.0f}" if market_cap else "  market_cap: None")
    print(f"  derived price (mktcap/shares): ${price:,.2f}" if price else "  derived price: None")

    # --- Now run compute_epv and show full result ---
    print(f"\n{'=' * 70}")
    print("RUNNING compute_epv()...")
    print("=" * 70)
    epv = compute_epv(
        annual=annual,
        wacc=wacc,
        market_cap=market_cap,
        current_price=price,
        diluted_shares=diluted_shares,
    )
    print(json.dumps(epv, indent=2))

    # --- A2: Hand-verify table ---
    print(f"\n{'=' * 70}")
    print("A2: VERIFICATION TABLE")
    print("=" * 70)
    if epv.get("ok"):
        inp = epv["inputs"]
        rows = [
            ("Normalized EBIT (5y avg)", "~$80B-$100B", f"${inp['normalized_ebit']:,.0f}"),
            ("Tax rate", "0.13-0.18", f"{inp['tax_rate']:.4f}"),
            ("Tax rate source", "trailing_5y_avg", inp['tax_rate_source']),
            ("NOPAT (EBIT × (1-tax))", "~$68B-$85B", f"${inp['nopat']:,.0f}"),
            ("WACC", "0.08-0.10", f"{inp['wacc']:.4f}"),
            ("EPV total (NOPAT / WACC)", "~$700B-$900B", f"${epv['epv_total']:,.0f}"),
            ("Diluted shares", "~7.4B", f"{inp['diluted_shares']:,}"),
            ("EPV per share", "~$95-$120", f"${epv['fair_value_per_share']:.2f}"),
            ("Current price", "~$350-$420", f"${epv['current_price']:.2f}" if epv['current_price'] else "None"),
            ("Market cap", "~$2.6T-$3.1T", f"${epv['market_cap']:,.0f}"),
            ("Growth premium %", "200%-350%", f"{epv['growth_premium_pct']:.2f}%"),
            ("Growth premium label", "HIGH/EXTREME_GROWTH", epv['growth_premium_label']),
        ]
        print(f"{'Field':<35} {'Expected Range':<20} {'Actual':<30}")
        print("-" * 85)
        for field, expected, actual in rows:
            print(f"{field:<35} {expected:<20} {actual:<30}")

    # --- A3: Independent verification ---
    print(f"\n{'=' * 70}")
    print("A3: INDEPENDENT VERIFICATION")
    print("=" * 70)
    if epv.get("ok"):
        inp = epv["inputs"]
        # Recompute step by step
        nopat_check = inp["normalized_ebit"] * (1 - inp["tax_rate"])
        epv_total_check = nopat_check / inp["wacc"]
        per_share_check = epv_total_check / inp["diluted_shares"]
        premium_check = (epv["market_cap"] - epv_total_check) / epv_total_check * 100

        print(f"  NOPAT = {inp['normalized_ebit']:,.0f} × (1 - {inp['tax_rate']:.4f}) = {nopat_check:,.0f}")
        print(f"    Matches reported NOPAT ({inp['nopat']:,.0f})? {'YES' if abs(nopat_check - inp['nopat']) < 1 else 'NO'}")
        print(f"  EPV total = {nopat_check:,.0f} / {inp['wacc']:.4f} = {epv_total_check:,.0f}")
        print(f"    Matches reported EPV ({epv['epv_total']:,.0f})? {'YES' if abs(epv_total_check - epv['epv_total']) < 1 else 'NO'}")
        print(f"  EPV/share = {epv_total_check:,.0f} / {inp['diluted_shares']:,} = {per_share_check:.2f}")
        print(f"    Matches reported per-share ({epv['fair_value_per_share']:.2f})? {'YES' if abs(per_share_check - epv['fair_value_per_share']) < 0.01 else 'NO'}")
        print(f"  Growth premium = ({epv['market_cap']:,.0f} - {epv_total_check:,.0f}) / {epv_total_check:,.0f} × 100 = {premium_check:.2f}%")
        print(f"    Matches reported premium ({epv['growth_premium_pct']:.2f}%)? {'YES' if abs(premium_check - epv['growth_premium_pct']) < 0.01 else 'NO'}")

        # Compare against public approximate values
        print(f"\n  --- Public reference comparison ---")
        print(f"  Our normalized EBIT: ${inp['normalized_ebit']/1e9:.2f}B")
        print(f"  Rough public 5y avg EBIT: ~$80.6B (FY2020-FY2024)")
        print(f"  Difference: {abs(inp['normalized_ebit']/1e9 - 80.6):.2f}B")
        print(f"  Note: Our data may include FY2025 and differ from public estimates")

    print(f"\n{'=' * 70}")
    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())

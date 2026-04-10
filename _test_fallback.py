"""Temporary test script for data service verification."""
import asyncio
import json
import sys
sys.stdout.reconfigure(line_buffering=True)

from data.company_data_service import CompanyDataService


async def test_one(svc, sym):
    print(f"\n=== {sym} ===", flush=True)
    try:
        data = await svc.get_company_data(sym)
        src = data.get("sources_used", {})
        fq = data.get("financials_quarterly") or {}
        fa = data.get("financials_annual") or {}
        stmts_q = fq.get("statements", [])
        stmts_a = fa.get("statements", [])
        print(f"  Yahoo fallback: {src.get('yahoo_fallback', False)}", flush=True)
        print(f"  FQ source: {src.get('financials_quarterly', 'polygon (default)')}", flush=True)
        print(f"  FA source: {src.get('financials_annual', 'polygon (default)')}", flush=True)
        print(f"  Quarterly: {len(stmts_q)} stmts, Annual: {len(stmts_a)} stmts", flush=True)

        # Check removed fields are absent
        for removed in ["eps_estimates", "price_target", "peers"]:
            present = removed in data
            print(f"  {removed} in result: {present}", flush=True)

        # Profile fields
        p = data.get("profile", {})
        print(f"  Profile:", flush=True)
        for f in ["company_name", "sector", "industry", "market_cap", "exchange",
                   "shares_outstanding", "institutional_ownership_pct", "insider_ownership_pct",
                   "country", "employees"]:
            print(f"    {f}: {p.get(f)}", flush=True)
    except Exception as e:
        import traceback
        print(f"  ERROR: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()


async def main(symbols):
    svc = CompanyDataService()
    for sym in symbols:
        await test_one(svc, sym)


if __name__ == "__main__":
    test_symbols = sys.argv[1:] if len(sys.argv) > 1 else ["ADBE", "NVDA", "MSFT"]
    asyncio.run(main(test_symbols))

"""Quick inspection of AAPL data structure for metrics development."""
import asyncio, json
from data.company_data_service import CompanyDataService

async def main():
    svc = CompanyDataService()
    data = await svc.get_company_data("AAPL")
    
    fq = data.get("financials_quarterly", {})
    stmts = fq.get("statements", [])
    if stmts:
        q0 = stmts[0]
        print("=== Latest quarter fields ===")
        for k, v in sorted(q0.items()):
            print(f"  {k}: {v}")
        print(f"\nTotal quarters: {len(stmts)}")
    
    fm = (data.get("basic_financials") or {}).get("metrics", {})
    print(f"\n=== Finnhub metrics ({len(fm)} keys) ===")
    for k, v in sorted(fm.items()):
        print(f"  {k}: {v}")
    
    fs = (data.get("basic_financials") or {}).get("series", {})
    print(f"\n=== Finnhub series keys ===")
    for k in sorted(fs.keys()):
        sub = fs[k]
        if isinstance(sub, dict):
            for sk in sorted(sub.keys()):
                arr = sub[sk]
                print(f"  {k}.{sk}: {len(arr)} points, latest={arr[0] if arr else 'empty'}")
    
    ins = data.get("insider_transactions", {})
    print(f"\n=== Insiders ===\n  {ins}")
    
    pt = data.get("price_target", {})
    print(f"\n=== Price target ===\n  {pt}")
    
    ph = data.get("price_history", {})
    print(f"\n=== Price history ===")
    for k, v in ph.items():
        print(f"  {k}: {v}")
    
    # Check annual statements
    fa = data.get("financials_annual", {})
    astmts = fa.get("statements", [])
    print(f"\n=== Annual statements: {len(astmts)} years ===")
    if astmts:
        a0 = astmts[0]
        print(f"  Latest: period={a0.get('period')} revenue={a0.get('revenue')}")

asyncio.run(main())

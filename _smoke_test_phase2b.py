"""Quick smoke test for Phase 2b CachedFMPClient integration."""
import asyncio
import sys
import os
import json

sys.path.insert(0, os.path.dirname(__file__))

async def smoke():
    from config import Settings
    settings = Settings()
    print(f"enable_bulk_cache: {settings.enable_bulk_cache}")
    print(f"database_url: {settings.database_url}")

    from pipeline.evaluator import _get_fmp_client
    fmp = _get_fmp_client()
    print(f"FMP client type: {type(fmp).__name__}")

    # Test cached methods
    profile = await fmp.get_company_profile("MSFT")
    ptype = type(profile).__name__
    pkeys = len(profile) if isinstance(profile, dict) else f"list:{len(profile)}"
    print(f"MSFT profile: {ptype}, keys={pkeys}")

    metrics = await fmp.get_key_metrics_ttm("MSFT")
    print(f"MSFT key_metrics_ttm: {len(metrics)} keys")

    ratios = await fmp.get_ratios_ttm("MSFT")
    print(f"MSFT ratios_ttm: {len(ratios)} keys")

    growth = await fmp.get_financial_growth("MSFT")
    print(f"MSFT financial_growth: {len(growth)} keys")

    income = await fmp.get_income_statement("MSFT", period="annual", limit=5)
    print(f"MSFT income_statement: {len(income)} years")

    balance = await fmp.get_balance_sheet("MSFT", period="annual", limit=5)
    print(f"MSFT balance_sheet: {len(balance)} years")

    cashflow = await fmp.get_cash_flow_statement("MSFT", period="annual", limit=5)
    print(f"MSFT cash_flow: {len(cashflow)} years")

    # Quarterly should fall through to API
    q_income = await fmp.get_income_statement("MSFT", period="quarter", limit=4)
    print(f"MSFT income_statement(quarter): {len(q_income)} quarters")

    # Cache stats
    if hasattr(fmp, "get_cache_stats"):
        stats = fmp.get_cache_stats()
        print(f"\nCache stats: {json.dumps(stats, indent=2)}")

    print("\nSmoke test PASSED")

if __name__ == "__main__":
    asyncio.run(smoke())

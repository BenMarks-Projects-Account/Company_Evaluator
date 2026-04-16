"""
Validation test for CachedFMPClient.

Verifies cache and API produce equivalent results for the same
inputs.  Tests shape compatibility, not exact value matching
(bulk data may lag per-symbol API by hours).

Run from project root:
    python scripts/test_cached_fmp_client.py
"""
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_settings
from data.fmp_client import FMPClient
from bulk.bulk_cache import BulkCache
from bulk.cache_lookup import BulkCacheLookup
from bulk.cached_fmp_client import CachedFMPClient

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def compare_shapes(name: str, api_result, cache_result) -> tuple[bool, str]:
    """Compare API and cache results for shape equivalence."""
    if api_result is None and cache_result is None:
        return True, "Both None"
    if api_result is None:
        return False, "API=None, cache has data"
    if cache_result is None:
        return False, "Cache=None, API has data (expected miss)"

    if type(api_result) != type(cache_result):
        return False, f"Type mismatch: api={type(api_result).__name__} cache={type(cache_result).__name__}"

    if isinstance(api_result, list):
        if not api_result:
            return True, f"Both empty lists" if not cache_result else (False, "API empty, cache not")
        if not cache_result:
            return False, "Cache empty list, API has data"

        # Compare first element keys
        if isinstance(api_result[0], dict) and isinstance(cache_result[0], dict):
            api_keys = set(api_result[0].keys())
            cache_keys = set(cache_result[0].keys())
            missing = api_keys - cache_keys
            if missing and len(missing) > 5:
                return False, f"Cache missing {len(missing)} keys: {sorted(missing)[:5]}..."
        return True, f"list[dict] len api={len(api_result)} cache={len(cache_result)}"

    if isinstance(api_result, dict):
        api_keys = set(api_result.keys())
        cache_keys = set(cache_result.keys())
        missing = api_keys - cache_keys
        if missing and len(missing) > len(api_keys) * 0.5:
            return False, f"Cache missing >50% keys: {sorted(missing)[:5]}..."
        # Check some values are present (not all None)
        cache_non_none = sum(1 for v in cache_result.values() if v is not None)
        if cache_non_none == 0 and any(v is not None for v in api_result.values()):
            return False, "Cache has all None values"
        return True, f"dict with {len(cache_keys)} keys ({cache_non_none} non-null)"

    return True, "Same type, not dict/list"


async def run_validation():
    settings = get_settings()

    if not settings.fmp_api_key:
        print("ERROR: No FMP_API_KEY in .env")
        sys.exit(1)

    # Set up API client
    raw_client = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )

    # Set up cached client
    cache_db = Path(settings.database_path).parent / "company_eval_bulk.db"
    if not cache_db.exists():
        print(f"ERROR: Bulk cache not found at {cache_db}")
        print("Run: python scripts/refresh_bulk_cache.py")
        sys.exit(1)

    cache = BulkCache(str(cache_db))
    lookup = BulkCacheLookup(cache)
    cached_client = CachedFMPClient(raw_client, lookup)

    print(f"Cache DB: {cache_db}")
    summary = cache.get_refresh_summary()
    print(f"Cache: {summary.get('table_count', 0)} tables, "
          f"{summary.get('total_rows', 0)} rows")
    print()

    # Test symbols: common ones (should hit cache) + dot-tickers (should miss)
    test_symbols = ["MSFT", "AAPL", "NVDA"]
    dot_tickers = ["BRK-A", "BF-B"]  # FMP uses dash not dot

    methods = [
        ("get_company_profile", {}),
        ("get_key_metrics_ttm", {}),
        ("get_ratios_ttm", {}),
        ("get_financial_growth", {}),
        ("get_income_statement", {"period": "annual", "limit": 5}),
        ("get_balance_sheet", {"period": "annual", "limit": 5}),
        ("get_cash_flow_statement", {"period": "annual", "limit": 5}),
    ]

    results = []

    # Test common symbols (expect cache hits)
    print("=" * 60)
    print("CACHE-HIT TESTS (common symbols, annual periods)")
    print("=" * 60)

    for symbol in test_symbols:
        for method_name, kwargs in methods:
            try:
                cached_method = getattr(cached_client, method_name)
                cache_result = await cached_method(symbol, **kwargs)

                api_method = getattr(raw_client, method_name)
                api_result = await api_method(symbol, **kwargs)

                match, detail = compare_shapes(method_name, api_result, cache_result)
                results.append({
                    "symbol": symbol,
                    "method": method_name,
                    "match": match,
                    "detail": detail,
                    "source": "cache" if cache_result is not None else "miss",
                })
                status = "OK" if match else "FAIL"
                print(f"  {status} {symbol} {method_name}: {detail}")
            except Exception as e:
                results.append({
                    "symbol": symbol,
                    "method": method_name,
                    "match": False,
                    "detail": f"Exception: {e}",
                    "source": "error",
                })
                print(f"  FAIL {symbol} {method_name}: EXCEPTION {e}")

    # Test quarterly (should fall through to API)
    print()
    print("=" * 60)
    print("FALLTHROUGH TESTS (quarterly periods → API)")
    print("=" * 60)

    for method_name in ["get_income_statement", "get_balance_sheet", "get_cash_flow_statement"]:
        cached_method = getattr(cached_client, method_name)
        result = await cached_method("MSFT", period="quarter", limit=4)
        has_data = result is not None and len(result) > 0
        status = "OK" if has_data else "FAIL"
        print(f"  {status} MSFT {method_name}(quarter): {'data returned' if has_data else 'None'}")

    # Test composite methods
    print()
    print("=" * 60)
    print("COMPOSITE METHOD TESTS")
    print("=" * 60)

    cross_val = await cached_client.get_all_cross_validation_data("MSFT")
    has_metrics = bool(cross_val.get("metrics"))
    has_ratios = bool(cross_val.get("ratios"))
    print(f"  {'OK' if has_metrics else 'FAIL'} get_all_cross_validation_data: "
          f"fetched={cross_val['fetched']} metrics={has_metrics} ratios={has_ratios}")

    full_fin = await cached_client.get_full_financials("MSFT", period="annual", limit=5)
    if full_fin:
        print(f"  OK get_full_financials(annual): "
              f"income={len(full_fin.get('income_statement', []))} "
              f"balance={len(full_fin.get('balance_sheet', []))} "
              f"cashflow={len(full_fin.get('cash_flow_statement', []))}")
    else:
        print(f"  FAIL get_full_financials(annual): None")

    # Summary
    matches = sum(1 for r in results if r["match"])
    total = len(results)
    print()
    print("=" * 60)
    print(f"Shape match rate: {matches}/{total} ({matches/total*100:.1f}%)" if total else "No tests run")
    print(f"Cache stats: {cached_client.get_cache_stats()}")
    print("=" * 60)

    if any(not r["match"] for r in results):
        print("\nFAILED CASES:")
        for r in results:
            if not r["match"]:
                print(f"  {r['symbol']} {r['method']}: {r['detail']}")


def main():
    asyncio.run(run_validation())


if __name__ == "__main__":
    main()

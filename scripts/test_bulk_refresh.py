"""
Standalone test of bulk refresh pipeline.

Does NOT modify production code. Creates a SEPARATE test cache DB
that can be inspected and deleted afterward.

Run from project root:
    python scripts/test_bulk_refresh.py
"""
import logging
import sqlite3
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from bulk.bulk_refresh import BulkRefreshOrchestrator
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    settings = get_settings()

    # Load universe symbols from the production DB (READ ONLY)
    prod_db_path = settings.database_path
    logger.info(f"Reading universe from: {prod_db_path}")
    with sqlite3.connect(prod_db_path) as conn:
        cursor = conn.execute(
            "SELECT symbol FROM universe_symbols WHERE active = 1"
        )
        universe = set(row[0].upper() for row in cursor.fetchall())

    logger.info(f"Loaded {len(universe)} universe symbols")

    # Create a SEPARATE test cache DB (next to prod DB, with _bulk_test suffix)
    prod_db_parent = Path(prod_db_path).parent
    test_cache_path = str(prod_db_parent / "company_eval_bulk_test.db")
    logger.info(f"Test cache at: {test_cache_path}")

    # Get FMP API key
    fmp_key = settings.fmp_api_key
    if not fmp_key:
        logger.error("FMP API key not found in settings — set FMP_API_KEY in .env")
        sys.exit(1)

    # Run the refresh
    orchestrator = BulkRefreshOrchestrator(
        api_key=fmp_key,
        cache_db_path=test_cache_path,
        universe_symbols=universe,
    )

    summary = orchestrator.refresh_all()

    print("\n" + "=" * 60)
    print("BULK REFRESH SUMMARY")
    print("=" * 60)
    for key, value in summary.items():
        print(f"  {key}: {value}")

    # Spot check: look up MSFT across all cached tables
    print("\n" + "=" * 60)
    print("SPOT CHECK: MSFT lookup across all cached tables")
    print("=" * 60)
    msft_data = orchestrator.cache.get_all_rows_for_symbol("MSFT")
    for key, row in sorted(msft_data.items()):
        print(f"  {key}: {len(row)} fields")
    print(f"  Total tables with MSFT: {len(msft_data)}")

    # Cache state
    print("\n" + "=" * 60)
    print("CACHE STATE")
    print("=" * 60)
    cache_summary = orchestrator.cache.get_refresh_summary()
    for key, value in cache_summary.items():
        print(f"  {key}: {value}")

    print(f"\nTest cache DB at: {test_cache_path}")


if __name__ == "__main__":
    main()

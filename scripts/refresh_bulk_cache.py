"""
Production bulk cache refresh.

Populates the production bulk cache DB (next to main company_eval.db)
with data from all FMP bulk endpoints.

Run from project root:
    python scripts/refresh_bulk_cache.py
"""
import logging
import sqlite3
import sys
from pathlib import Path

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

    # Load universe
    prod_db_path = settings.database_path
    logger.info(f"Reading universe from: {prod_db_path}")
    with sqlite3.connect(prod_db_path) as conn:
        cursor = conn.execute(
            "SELECT symbol FROM universe_symbols WHERE active = 1"
        )
        universe = set(row[0].upper() for row in cursor.fetchall())

    logger.info(f"Loaded {len(universe)} universe symbols")

    # Production cache path (same parent dir as main DB)
    cache_path = str(Path(prod_db_path).parent / "company_eval_bulk.db")
    logger.info(f"Production cache at: {cache_path}")

    fmp_key = settings.fmp_api_key
    if not fmp_key:
        logger.error("FMP API key not found — set FMP_API_KEY in .env")
        sys.exit(1)

    orchestrator = BulkRefreshOrchestrator(
        api_key=fmp_key,
        cache_db_path=cache_path,
        universe_symbols=universe,
    )

    summary = orchestrator.refresh_all()

    print("\n" + "=" * 60)
    print("PRODUCTION BULK CACHE REFRESH COMPLETE")
    print("=" * 60)
    for key, value in summary.items():
        print(f"  {key}: {value}")

    # Verify
    cache_state = orchestrator.cache.get_refresh_summary()
    print("\nCache state:")
    for key, value in cache_state.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()

"""
Orchestration entry point for bulk data refresh.

Combines BulkFetcher + BulkParser + BulkCache into a single
refresh operation. This is what the crawler will call on its
first cycle of each day.
"""
import logging
from datetime import datetime
from typing import Set

from .bulk_endpoints import BULK_ENDPOINTS
from .bulk_fetcher import BulkFetcher
from .bulk_parser import BulkParser
from .bulk_cache import BulkCache

logger = logging.getLogger(__name__)


class BulkRefreshOrchestrator:
    """Coordinates bulk data download, parsing, and caching."""

    def __init__(
        self,
        api_key: str,
        cache_db_path: str,
        universe_symbols: Set[str],
    ):
        self.fetcher = BulkFetcher(api_key)
        self.parser = BulkParser(universe_symbols)
        self.cache = BulkCache(cache_db_path)

    def refresh_all(self) -> dict:
        """
        Perform a full bulk refresh cycle.

        Returns a summary dict with counts and timing.
        """
        start = datetime.now()
        logger.info(f"Starting bulk refresh at {start.isoformat()}")

        # Fetch all endpoints
        fetch_results = self.fetcher.fetch_all()

        # Parse and cache each successful fetch
        parse_successes = 0
        parse_failures = 0
        total_rows_cached = 0

        for fetch_result in fetch_results:
            endpoint = next(
                (e for e in BULK_ENDPOINTS if e.name == fetch_result.endpoint_name),
                None,
            )
            if not endpoint:
                logger.error(f"Unknown endpoint: {fetch_result.endpoint_name}")
                continue

            parse_result = self.parser.parse(fetch_result, endpoint)

            if not parse_result.is_success:
                parse_failures += 1
                logger.error(
                    f"Parse failed for {fetch_result.endpoint_name}: "
                    f"{parse_result.error}"
                )
                continue

            self.cache.store(
                endpoint_name=parse_result.endpoint_name,
                dataframe=parse_result.dataframe,
                part=parse_result.part,
                year=parse_result.year,
            )
            parse_successes += 1
            total_rows_cached += parse_result.filtered_row_count

        end = datetime.now()
        elapsed = (end - start).total_seconds()

        summary = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "elapsed_minutes": round(elapsed / 60, 1),
            "fetch_attempts": len(fetch_results),
            "fetch_successes": sum(1 for r in fetch_results if r.is_success),
            "fetch_failures": sum(1 for r in fetch_results if not r.is_success),
            "parse_successes": parse_successes,
            "parse_failures": parse_failures,
            "total_rows_cached": total_rows_cached,
            "total_mb_downloaded": round(
                sum(r.size_mb for r in fetch_results if r.is_success), 1
            ),
        }

        logger.info(
            f"Bulk refresh complete in {elapsed / 60:.1f} min: "
            f"{parse_successes} tables cached, {total_rows_cached} rows"
        )

        return summary

"""
Cycle-level orchestration for cache-aware crawler operation.

Handles:
- Stale cache detection at cycle start
- Bulk refresh trigger if needed
- Cache statistics logging at cycle end
- Timing metrics for observability
"""
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .bulk_cache import BulkCache
from .bulk_refresh import BulkRefreshOrchestrator

logger = logging.getLogger(__name__)


class CycleOrchestrator:
    """Manages bulk cache lifecycle around a crawler cycle."""

    # Default staleness threshold (overridable via settings)
    DEFAULT_STALE_THRESHOLD_HOURS = 24

    def __init__(
        self,
        settings,
        cached_fmp_client,
        bulk_cache_path: Optional[str] = None,
    ):
        self.settings = settings
        self.cached_client = cached_fmp_client

        # Determine cache path
        if bulk_cache_path:
            self.cache_path = bulk_cache_path
        else:
            configured = getattr(settings, "bulk_cache_path", None)
            if configured:
                self.cache_path = configured
            else:
                db_parent = Path(settings.database_path).parent
                self.cache_path = str(db_parent / "company_eval_bulk.db")

        # Configurable thresholds / toggles
        self.stale_threshold_hours = int(
            getattr(settings, "bulk_cache_stale_hours", self.DEFAULT_STALE_THRESHOLD_HOURS)
        )
        self.auto_refresh_enabled = bool(
            getattr(settings, "bulk_auto_refresh", True)
        )

        # Lazy-create the cache handle (file may not exist yet)
        try:
            self.cache = BulkCache(self.cache_path)
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Could not open bulk cache at %s: %s", self.cache_path, exc)
            self.cache = None

        self._cycle_start_time: Optional[float] = None
        self._symbols_processed = 0
        self._refresh_log: Dict[str, Any] = {
            "last_refresh_attempted_at": None,
            "last_refresh_succeeded_at": None,
            "last_refresh_duration_seconds": None,
            "last_refresh_error": None,
        }

    # ── Startup diagnostics ─────────────────────────────────

    def log_startup_state(self):
        """Log current cache state at process startup."""
        cache_file = Path(self.cache_path)
        if not cache_file.exists():
            logger.warning(
                "Bulk cache DB not found at %s — will refresh on first cycle",
                self.cache_path,
            )
            return

        if self.cache is None:
            logger.warning("Bulk cache handle unavailable for %s", self.cache_path)
            return

        summary = self.cache.get_refresh_summary() or {}
        table_count = summary.get("table_count") or 0
        if table_count == 0:
            logger.warning(
                "Bulk cache exists but is empty — will refresh on first cycle",
            )
            return

        oldest = summary.get("oldest_refresh")
        age_hours = 0.0
        if oldest:
            try:
                age = datetime.now() - datetime.fromisoformat(oldest)
                age_hours = age.total_seconds() / 3600
            except Exception:
                age_hours = 0.0

        logger.info(
            "Bulk cache: %s tables, %s rows, age: %.1fh (oldest refresh: %s)",
            table_count,
            summary.get("total_rows", 0) or 0,
            age_hours,
            oldest,
        )

    # ── Cycle hooks ─────────────────────────────────────────

    def before_cycle(self) -> Dict[str, Any]:
        """Run before each crawler cycle.

        Checks cache staleness, triggers refresh if needed, resets
        the CachedFMPClient's hit/miss counters.
        """
        self._cycle_start_time = time.time()
        self._symbols_processed = 0

        summary: Dict[str, Any] = {
            "cycle_started_at": datetime.now().isoformat(),
            "cache_was_stale": False,
            "refresh_attempted": False,
            "refresh_succeeded": False,
            "refresh_duration_seconds": None,
            "refresh_error": None,
        }

        # Check staleness
        is_stale = False
        if self.cache is not None:
            try:
                is_stale = self.cache.is_stale(hours=self.stale_threshold_hours)
            except Exception as exc:
                logger.warning("Could not check cache staleness: %s", exc)
                is_stale = False
        summary["cache_was_stale"] = is_stale

        if is_stale and self.auto_refresh_enabled:
            logger.info(
                "Bulk cache is stale (>%sh old). Triggering refresh before "
                "symbol processing...",
                self.stale_threshold_hours,
            )
            summary["refresh_attempted"] = True
            summary.update(self._run_refresh())
        elif is_stale and not self.auto_refresh_enabled:
            logger.warning(
                "Bulk cache is stale (>%sh) but bulk_auto_refresh is disabled "
                "— proceeding with stale cache",
                self.stale_threshold_hours,
            )
        else:
            logger.info("Bulk cache is fresh — proceeding without refresh")

        # Reset cache stats for this cycle
        if self.cached_client is not None and hasattr(
            self.cached_client, "reset_cache_stats"
        ):
            try:
                self.cached_client.reset_cache_stats()
            except Exception as exc:
                logger.debug("reset_cache_stats failed: %s", exc)

        return summary

    def _load_universe(self) -> set:
        with sqlite3.connect(self.settings.database_path, timeout=30) as conn:
            cursor = conn.execute(
                "SELECT symbol FROM universe_symbols WHERE active = 1"
            )
            return {row[0].upper() for row in cursor.fetchall() if row and row[0]}

    def _run_refresh(self) -> Dict[str, Any]:
        """Execute a bulk refresh. Returns timing/error info.

        On failure, logs the error and returns. Does NOT raise —
        cycle continues with stale cache.
        """
        result: Dict[str, Any] = {
            "refresh_succeeded": False,
            "refresh_duration_seconds": None,
            "refresh_error": None,
        }

        refresh_start = time.time()
        self._refresh_log["last_refresh_attempted_at"] = datetime.now().isoformat()

        try:
            api_key = getattr(self.settings, "fmp_api_key", "") or ""
            if not api_key:
                raise RuntimeError("fmp_api_key not configured — cannot refresh")

            universe = self._load_universe()
            logger.info(
                "Refreshing bulk cache for %d universe symbols", len(universe)
            )

            orchestrator = BulkRefreshOrchestrator(
                api_key=api_key,
                cache_db_path=self.cache_path,
                universe_symbols=universe,
            )
            refresh_summary = orchestrator.refresh_all() or {}

            duration = time.time() - refresh_start
            result["refresh_duration_seconds"] = duration

            fetch_successes = int(refresh_summary.get("fetch_successes", 0) or 0)
            fetch_attempts = int(refresh_summary.get("fetch_attempts", 0) or 0)

            if fetch_attempts > 0 and fetch_successes == fetch_attempts:
                result["refresh_succeeded"] = True
                self._refresh_log["last_refresh_succeeded_at"] = (
                    datetime.now().isoformat()
                )
                self._refresh_log["last_refresh_duration_seconds"] = duration
                logger.info(
                    "Bulk refresh complete in %.1f min: %s endpoints, "
                    "%s rows, %.1f MB",
                    duration / 60,
                    fetch_successes,
                    refresh_summary.get("total_rows_cached", 0),
                    refresh_summary.get("total_mb_downloaded", 0) or 0,
                )
            elif fetch_attempts > 0 and fetch_successes >= fetch_attempts * 0.8:
                result["refresh_succeeded"] = True
                self._refresh_log["last_refresh_succeeded_at"] = (
                    datetime.now().isoformat()
                )
                self._refresh_log["last_refresh_duration_seconds"] = duration
                logger.warning(
                    "Bulk refresh partially complete in %.1f min: %s/%s "
                    "endpoints succeeded. Continuing with partial cache update.",
                    duration / 60,
                    fetch_successes,
                    fetch_attempts,
                )
            else:
                result["refresh_succeeded"] = False
                err = (
                    f"Only {fetch_successes}/{fetch_attempts} endpoints succeeded"
                )
                result["refresh_error"] = err
                self._refresh_log["last_refresh_error"] = err
                logger.error(err)

        except Exception as exc:
            duration = time.time() - refresh_start
            result["refresh_duration_seconds"] = duration
            result["refresh_succeeded"] = False
            result["refresh_error"] = str(exc)
            self._refresh_log["last_refresh_error"] = str(exc)
            logger.error(
                "Bulk refresh failed after %.1fs: %s. Cycle continuing with "
                "stale cache.",
                duration,
                exc,
            )

        return result

    def record_symbol_processed(self):
        """Call after each symbol evaluation completes."""
        self._symbols_processed += 1

    def after_cycle(self) -> Dict[str, Any]:
        """Run after the crawler cycle completes. Logs stats."""
        if self._cycle_start_time is None:
            logger.warning("after_cycle called without before_cycle")
            return {}

        cycle_duration = time.time() - self._cycle_start_time

        cache_stats: Dict[str, Any] = {}
        if self.cached_client is not None and hasattr(
            self.cached_client, "get_cache_stats"
        ):
            try:
                cache_stats = self.cached_client.get_cache_stats() or {}
            except Exception as exc:
                logger.debug("get_cache_stats failed: %s", exc)

        avg = (
            cycle_duration / self._symbols_processed
            if self._symbols_processed > 0
            else None
        )

        summary = {
            "cycle_duration_seconds": cycle_duration,
            "cycle_duration_minutes": cycle_duration / 60,
            "symbols_processed": self._symbols_processed,
            "avg_seconds_per_symbol": avg,
            "cache_stats": cache_stats,
        }

        hit_rate_pct = (cache_stats.get("hit_rate") or 0) * 100
        avg_display = f"{avg:.1f}s/symbol" if avg is not None else "n/a"
        logger.info(
            "Cycle complete: %s symbols in %.1f min (%s) | "
            "Cache: %s hits, %s misses (%.1f%% hit rate)",
            self._symbols_processed,
            cycle_duration / 60,
            avg_display,
            cache_stats.get("cache_hits", 0),
            cache_stats.get("cache_misses", 0),
            hit_rate_pct,
        )

        return summary

    def get_refresh_log(self) -> Dict[str, Any]:
        """Return the refresh history log."""
        return dict(self._refresh_log)

    # ── Persistence ─────────────────────────────────────────

    def persist_cycle_summary(
        self, before_summary: Dict[str, Any], after_summary: Dict[str, Any]
    ):
        """Insert a row into crawler_cycle_metrics."""
        try:
            cache_stats = (after_summary or {}).get("cache_stats") or {}
            with sqlite3.connect(self.settings.database_path, timeout=30) as conn:
                conn.execute(
                    """
                    INSERT INTO crawler_cycle_metrics (
                        cycle_started_at, cycle_duration_seconds,
                        symbols_processed, avg_seconds_per_symbol,
                        cache_hits, cache_misses, hit_rate,
                        cache_was_stale, refresh_attempted, refresh_succeeded,
                        refresh_duration_seconds, refresh_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (before_summary or {}).get("cycle_started_at"),
                        (after_summary or {}).get("cycle_duration_seconds"),
                        (after_summary or {}).get("symbols_processed"),
                        (after_summary or {}).get("avg_seconds_per_symbol"),
                        cache_stats.get("cache_hits"),
                        cache_stats.get("cache_misses"),
                        cache_stats.get("hit_rate"),
                        1 if (before_summary or {}).get("cache_was_stale") else 0,
                        1 if (before_summary or {}).get("refresh_attempted") else 0,
                        1 if (before_summary or {}).get("refresh_succeeded") else 0,
                        (before_summary or {}).get("refresh_duration_seconds"),
                        (before_summary or {}).get("refresh_error"),
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Could not persist cycle summary: %s", exc)

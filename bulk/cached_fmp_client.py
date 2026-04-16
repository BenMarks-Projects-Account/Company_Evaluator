"""
FMP client with transparent bulk cache fallback.

Wraps the standard FMPClient with cache-first lookups for
methods that have bulk equivalents.  Falls through to the
underlying API client for:
  - Methods without bulk equivalents (insider, institutional, etc.)
  - Symbols missing from cache (dot-tickers like BRK.A)
  - Quarterly periods (only annual is cached)
  - When cache is unavailable
"""
import logging
from typing import Dict, Any

from .cache_lookup import BulkCacheLookup

logger = logging.getLogger(__name__)


class CachedFMPClient:
    """FMPClient wrapper that prefers bulk cache for supported methods.

    All methods are async to match the underlying FMPClient contract.
    Non-cached methods pass through via __getattr__.

    Usage::

        from data.fmp_client import FMPClient
        from bulk.bulk_cache import BulkCache
        from bulk.cache_lookup import BulkCacheLookup
        from bulk.cached_fmp_client import CachedFMPClient

        api = FMPClient(api_key=...)
        cache = BulkCache(db_path=...)
        lookup = BulkCacheLookup(cache)
        client = CachedFMPClient(api, lookup)

        profile = await client.get_company_profile("MSFT")  # cache-first
    """

    def __init__(self, api_client, cache_lookup: BulkCacheLookup):
        self._api = api_client
        self._lookup = cache_lookup
        self._stats: Dict[str, int] = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_fallthroughs": 0,
        }

    # ── Cached methods ───────────────────────────────────────

    async def get_company_profile(self, symbol: str) -> dict | None:
        cached = self._lookup.get_profile(symbol)
        if cached is not None:
            self._stats["cache_hits"] += 1
            logger.debug("Cache HIT: profile %s", symbol)
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        logger.debug("Cache MISS: profile %s — API fallthrough", symbol)
        return await self._api.get_company_profile(symbol)

    async def get_key_metrics_ttm(self, symbol: str) -> dict | None:
        cached = self._lookup.get_key_metrics_ttm(symbol)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_key_metrics_ttm(symbol)

    async def get_ratios_ttm(self, symbol: str) -> dict | None:
        cached = self._lookup.get_ratios_ttm(symbol)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_ratios_ttm(symbol)

    async def get_financial_growth(self, symbol: str) -> dict | None:
        cached = self._lookup.get_financial_growth(symbol)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_financial_growth(symbol)

    async def get_income_statement(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> list[dict] | None:
        cached = self._lookup.get_income_statement(symbol, period, limit)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_income_statement(symbol, period, limit)

    async def get_balance_sheet(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> list[dict] | None:
        cached = self._lookup.get_balance_sheet(symbol, period, limit)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_balance_sheet(symbol, period, limit)

    async def get_cash_flow_statement(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> list[dict] | None:
        cached = self._lookup.get_cash_flow_statement(symbol, period, limit)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached
        self._stats["cache_misses"] += 1
        self._stats["api_fallthroughs"] += 1
        return await self._api.get_cash_flow_statement(symbol, period, limit)

    # ── Composite methods (must override to use cached sub-methods) ──

    async def get_all_cross_validation_data(self, symbol: str) -> dict:
        """Matches FMPClient.get_all_cross_validation_data() shape exactly."""
        result = {"symbol": symbol, "fetched": False, "metrics": {}, "ratios": {}}

        metrics = await self.get_key_metrics_ttm(symbol)
        ratios = await self.get_ratios_ttm(symbol)

        if metrics:
            result["metrics"] = metrics
        if ratios:
            result["ratios"] = ratios

        result["fetched"] = bool(metrics or ratios)
        return result

    async def get_full_financials(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> dict | None:
        """Matches FMPClient.get_full_financials() shape exactly."""
        income = await self.get_income_statement(symbol, period, limit)
        balance = await self.get_balance_sheet(symbol, period, limit)
        cash_flow = await self.get_cash_flow_statement(symbol, period, limit)

        if not income and not balance and not cash_flow:
            return None

        return {
            "income_statement": income or [],
            "balance_sheet": balance or [],
            "cash_flow_statement": cash_flow or [],
        }

    # ── Observability ────────────────────────────────────────

    def get_cache_stats(self) -> Dict[str, Any]:
        """Return cache hit/miss counters."""
        total = self._stats["cache_hits"] + self._stats["cache_misses"]
        hit_rate = self._stats["cache_hits"] / total if total > 0 else 0.0
        return {
            **self._stats,
            "total_cacheable_calls": total,
            "hit_rate": round(hit_rate, 3),
        }

    def reset_cache_stats(self):
        self._stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_fallthroughs": 0,
        }

    # ── Pass-through for all non-cached methods ──────────────

    def __getattr__(self, name):
        """Delegate any unhandled attribute to the underlying API client."""
        return getattr(self._api, name)

"""Symbol search service — looks up stocks by ticker or company name.

Calls FMP's search-symbol (ticker prefix) AND search-name (company name
substring) in parallel, merges + deduplicates results.  Results are cached
in memory for 60 seconds to reduce API calls for repetitive typing.
"""

import asyncio
import logging
import time

from config import get_settings
from data.fmp_client import FMPClient

_log = logging.getLogger(__name__)

# In-memory cache: key = (normalized_query, limit), value = (expires_at, results)
_CACHE: dict[tuple[str, int], tuple[float, list[dict]]] = {}
_CACHE_TTL_SECONDS = 60


async def search_symbols(query: str, limit: int = 10) -> dict:
    """Search for symbols matching the query.

    Calls both FMP search-symbol (ticker match) and search-name (company
    name match) in parallel, merges and deduplicates.

    Returns a dict with ok, query, count, results, cached.
    Raises ValueError on bad input.
    Raises RuntimeError on FMP failure.
    """
    query = (query or "").strip()

    if not query:
        raise ValueError("Query must be 1-60 characters")
    if len(query) > 60:
        raise ValueError("Query must be 1-60 characters")
    if limit < 1 or limit > 20:
        raise ValueError("Limit must be 1-20")

    normalized = query.lower()
    cache_key = (normalized, limit)
    now = time.time()

    cached = _CACHE.get(cache_key)
    if cached and cached[0] > now:
        _log.debug("[search] cache hit for %r", normalized)
        return {
            "ok": True,
            "query": query,
            "count": len(cached[1]),
            "results": cached[1],
            "cached": True,
        }

    # Build FMP client
    settings = get_settings()
    if not settings.fmp_enabled or not settings.fmp_api_key:
        raise RuntimeError("FMP is not configured")

    fmp = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )

    # Fetch 2x limit from each endpoint because we'll filter + deduplicate
    fetch_limit = min(limit * 2, 20)

    try:
        symbol_results, name_results = await asyncio.gather(
            fmp.search_symbol(query=query, limit=fetch_limit),
            fmp.search_name(query=query, limit=fetch_limit),
        )
    except Exception as exc:
        raise RuntimeError(f"FMP search failed: {exc}")

    if symbol_results is None and name_results is None:
        raise RuntimeError("FMP search failed")

    # Merge: ticker matches first (more relevant), then name matches
    raw = (symbol_results or []) + (name_results or [])

    results = []
    seen_symbols = set()

    for item in raw:
        if not isinstance(item, dict):
            continue

        symbol_val = (item.get("symbol") or "").strip().upper()
        if not symbol_val or symbol_val in seen_symbols:
            continue

        # Filter: symbols 1-6 chars, letters and dots only (allows BRK.B)
        if len(symbol_val) > 6:
            continue
        if not all(c.isalpha() or c == "." for c in symbol_val):
            continue

        name = (item.get("name") or "").strip()
        if not name:
            continue

        # FMP fields: exchange (short), exchangeFullName (long)
        exch_short = (item.get("exchange") or "").strip()
        exch_full = (item.get("exchangeFullName") or "").strip()

        seen_symbols.add(symbol_val)
        results.append({
            "symbol": symbol_val,
            "name": name,
            "exchange": exch_full or exch_short,
            "exchange_short": exch_short,
            "currency": item.get("currency") or "USD",
            "type": "stock",
        })

        if len(results) >= limit:
            break

    # Cache
    _CACHE[cache_key] = (now + _CACHE_TTL_SECONDS, results)
    _cleanup_cache(now)

    return {
        "ok": True,
        "query": query,
        "count": len(results),
        "results": results,
        "cached": False,
    }


def _cleanup_cache(now: float):
    expired = [k for k, (exp, _) in _CACHE.items() if exp < now]
    for k in expired:
        del _CACHE[k]

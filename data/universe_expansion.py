"""Phase 2 universe expansion — discovers and onboards quality candidates.

Uses multi-criteria FMP screening + Finnhub IPO calendar to grow
the universe from ~800 to ~2,500 symbols.  Tags each with a tier
and refresh_days for future crawler prioritisation.

This module does NOT modify the crawler.  New symbols are added to
universe_symbols with ``evaluated_at = NULL`` so the crawler's
staleness-based ordering picks them up automatically.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta

from config import get_settings
from data.fmp_client import FMPClient
from data.finnhub_client import FinnhubClient
from db.database import get_session, UniverseSymbol
from sqlalchemy import select, func

_log = logging.getLogger(__name__)

# ── Tier definitions ─────────────────────────────────────────

TIER_DEFINITIONS = {
    "tier_0_watchlist": {
        "description": "Active watchlist — daily refresh",
        "refresh_days": 1,
        "priority": 100,
    },
    "tier_1_large_mid": {
        "description": "Large/mid cap ($10B+) — 3-day refresh",
        "refresh_days": 3,
        "priority": 75,
    },
    "tier_2_breakout_zone": {
        "description": "Breakout zone ($500M-$10B) — 7-day refresh",
        "refresh_days": 7,
        "priority": 50,
    },
    "tier_3_small_cap": {
        "description": "Small cap speculative ($300M-$500M) — 14-day refresh",
        "refresh_days": 14,
        "priority": 25,
    },
    "tier_4_ipo_discovery": {
        "description": "Recent IPOs (<2 years) — 7-day refresh",
        "refresh_days": 7,
        "priority": 60,
    },
}

# ── Exclusion filters ────────────────────────────────────────

_EXCLUDED_NAME_PATTERNS = [
    "acquisition corp",
    "blank check",
    "spac",
    "trust",
    "preferred",
    "warrant",
]

_EXCLUDED_SYMBOL_SUFFIXES = ("W", "U", "R", ".W", ".U", ".R", "-W", "-U")


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def expand_universe(dry_run: bool = True) -> dict:
    """Run the full universe expansion pipeline.

    Args:
        dry_run: If True, report what would be added without inserting.

    Returns summary dict with stats and tier/source breakdowns.
    """
    _log.info("Starting universe expansion (dry_run=%s)", dry_run)

    settings = get_settings()
    fmp = _get_fmp(settings)
    finnhub = _get_finnhub(settings)

    if not fmp:
        return {"error": "FMP client not available (check fmp_enabled / fmp_api_key)"}

    # Current universe
    existing = await _get_existing_symbols()
    _log.info("Existing universe: %d symbols", len(existing))

    # Discover candidates from multiple sources
    breakout = await _discover_breakout_zone(fmp)
    _log.info("Discovered %d breakout zone candidates", len(breakout))

    small = await _discover_small_caps(fmp)
    _log.info("Discovered %d small cap candidates", len(small))

    sector = await _discover_sector_focused(fmp)
    _log.info("Discovered %d sector-focused candidates", len(sector))

    ipos: list[dict] = []
    if finnhub:
        ipos = await _discover_recent_ipos(finnhub)
        _log.info("Discovered %d recent IPO candidates", len(ipos))

    # Deduplicate (same symbol from multiple sources — keep first seen)
    seen: set[str] = set()
    unique: list[dict] = []
    for c in breakout + sector + small + ipos:
        sym = c.get("symbol")
        if sym and sym not in seen:
            seen.add(sym)
            unique.append(c)

    _log.info("Total unique candidates: %d", len(unique))

    # Apply exclusion filters
    filtered = [c for c in unique if not _should_exclude(c)]
    excluded = len(unique) - len(filtered)
    _log.info("After exclusion filters: %d (excluded %d)", len(filtered), excluded)

    # Skip already-in-universe
    new = [c for c in filtered if c["symbol"] not in existing]
    skipped_existing = len(filtered) - len(new)
    _log.info("New candidates: %d (skipped %d existing)", len(new), skipped_existing)

    # Assign tier + refresh_days
    for c in new:
        c["tier"] = _assign_tier(c)
        c["refresh_days"] = TIER_DEFINITIONS[c["tier"]]["refresh_days"]
        c["priority"] = TIER_DEFINITIONS[c["tier"]]["priority"]

    # Breakdowns
    tier_breakdown: dict[str, int] = {}
    source_breakdown: dict[str, int] = {}
    for c in new:
        tier_breakdown[c["tier"]] = tier_breakdown.get(c["tier"], 0) + 1
        src = c.get("discovery_source", "unknown")
        source_breakdown[src] = source_breakdown.get(src, 0) + 1

    added = 0
    if not dry_run:
        added = await _insert_candidates(new)

    return {
        "discovered": len(unique),
        "filtered_out": excluded,
        "skipped_existing": skipped_existing,
        "added": added if not dry_run else len(new),
        "dry_run": dry_run,
        "tier_breakdown": tier_breakdown,
        "source_breakdown": source_breakdown,
    }


# ═══════════════════════════════════════════════════════════════
# DISCOVERY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

async def _discover_breakout_zone(fmp: FMPClient) -> list[dict]:
    """Tier 2 candidates: $500M-$10B (two chunks to stay within FMP limits)."""
    candidates: list[dict] = []

    # Chunk 1: $500M – $2B
    chunk1 = await fmp.stock_screener(
        market_cap_min=500_000_000,
        market_cap_max=2_000_000_000,
        price_min=5.0,
        volume_min=200_000,
        country="US",
        exchange="nyse,nasdaq",
        limit=1000,
    )
    if chunk1:
        for c in chunk1:
            candidates.append(_normalise(c, "fmp_screener_breakout_500M_2B"))

    # Chunk 2: $2B – $10B
    chunk2 = await fmp.stock_screener(
        market_cap_min=2_000_000_000,
        market_cap_max=10_000_000_000,
        price_min=5.0,
        volume_min=200_000,
        country="US",
        exchange="nyse,nasdaq",
        limit=1000,
    )
    if chunk2:
        for c in chunk2:
            candidates.append(_normalise(c, "fmp_screener_breakout_2B_10B"))

    return candidates


async def _discover_small_caps(fmp: FMPClient) -> list[dict]:
    """Tier 3 candidates: $300M-$500M.  Top 200 by volume only."""
    result = await fmp.stock_screener(
        market_cap_min=300_000_000,
        market_cap_max=500_000_000,
        price_min=7.0,
        volume_min=500_000,
        country="US",
        exchange="nyse,nasdaq",
        limit=300,
    )
    if not result:
        return []

    # Sort by volume descending, take top 200
    result.sort(key=lambda c: c.get("volume") or 0, reverse=True)
    return [_normalise(c, "fmp_screener_small_cap") for c in result[:200]]


async def _discover_sector_focused(fmp: FMPClient) -> list[dict]:
    """Sector-biased screens in high-growth sectors — extends cap up to $20B."""
    candidates: list[dict] = []
    target_sectors = [
        "Technology",
        "Healthcare",
        "Communication Services",
        "Consumer Cyclical",
    ]

    for sector in target_sectors:
        result = await fmp.stock_screener(
            sector=sector,
            market_cap_min=500_000_000,
            market_cap_max=20_000_000_000,
            price_min=5.0,
            volume_min=200_000,
            country="US",
            exchange="nyse,nasdaq",
            limit=500,
        )
        if result:
            src = f"fmp_screener_sector_{sector.lower().replace(' ', '_')}"
            for c in result:
                candidates.append(_normalise(c, src))

    return candidates


async def _discover_recent_ipos(finnhub: FinnhubClient) -> list[dict]:
    """IPOs from the last 2 years on NYSE/NASDAQ that actually priced."""
    candidates: list[dict] = []
    try:
        from_date = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        data = await finnhub.get_ipo_calendar(from_date, to_date)
        if not data:
            return []

        ipo_list = data.get("ipoCalendar") or []
        for ipo in ipo_list:
            symbol = ipo.get("symbol")
            exchange = (ipo.get("exchange") or "").upper()
            price = ipo.get("price")

            if not symbol:
                continue
            if "NASDAQ" not in exchange and "NYSE" not in exchange:
                continue
            if not price:
                continue

            candidates.append({
                "symbol": symbol,
                "name": ipo.get("name"),
                "market_cap": None,
                "sector": None,
                "industry": None,
                "price": None,
                "exchange": exchange,
                "discovery_source": "finnhub_ipo_calendar",
                "discovery_metadata": {
                    "ipo_date": ipo.get("date"),
                    "exchange": exchange,
                },
            })
    except Exception as exc:
        _log.warning("IPO discovery failed: %s", exc)

    return candidates


# ═══════════════════════════════════════════════════════════════
# FILTERING  &  TIER ASSIGNMENT
# ═══════════════════════════════════════════════════════════════

def _should_exclude(candidate: dict) -> bool:
    symbol = candidate.get("symbol", "")
    name = (candidate.get("name") or "").lower()

    if len(symbol) > 5:
        return True
    for suffix in _EXCLUDED_SYMBOL_SUFFIXES:
        if symbol.endswith(suffix):
            return True
    for pattern in _EXCLUDED_NAME_PATTERNS:
        if pattern in name:
            return True
    return False


def _assign_tier(candidate: dict) -> str:
    source = candidate.get("discovery_source", "")
    market_cap = candidate.get("market_cap") or 0

    if "ipo" in source:
        return "tier_4_ipo_discovery"
    if market_cap < 500_000_000:
        return "tier_3_small_cap"
    if market_cap < 10_000_000_000:
        return "tier_2_breakout_zone"
    return "tier_1_large_mid"


# ═══════════════════════════════════════════════════════════════
# DB HELPERS
# ═══════════════════════════════════════════════════════════════

async def _get_existing_symbols() -> set[str]:
    async with get_session() as session:
        result = await session.execute(select(UniverseSymbol.symbol))
        return {row[0] for row in result}


async def _insert_candidates(candidates: list[dict]) -> int:
    """Insert new candidates.  Uses INSERT-or-skip to stay idempotent."""
    now = datetime.now(timezone.utc)
    inserted = 0

    async with get_session() as session:
        for c in candidates:
            sym = c.get("symbol")
            if not sym:
                continue

            # Double-check not already present (race protection)
            exists = (await session.execute(
                select(UniverseSymbol.symbol).where(UniverseSymbol.symbol == sym)
            )).scalar_one_or_none()
            if exists:
                continue

            meta_json = json.dumps(c.get("discovery_metadata") or {})
            market_cap = c.get("market_cap")

            session.add(UniverseSymbol(
                symbol=sym,
                company_name=c.get("name"),
                source=c.get("discovery_source", "fmp_screener"),
                market_cap=market_cap,
                market_cap_tier=_classify_market_cap(market_cap),
                sector=c.get("sector"),
                industry=c.get("industry"),
                exchange=c.get("exchange"),
                active=True,
                priority=c.get("priority", 0),
                added_at=now,
                last_screened_at=now,
                tier=c.get("tier"),
                refresh_days=c.get("refresh_days"),
                discovery_source=c.get("discovery_source"),
                discovery_metadata=meta_json,
            ))
            inserted += 1

            # Flush in batches to keep memory stable
            if inserted % 200 == 0:
                await session.flush()

        await session.commit()

    _log.info("Inserted %d new symbols into universe_symbols", inserted)
    return inserted


# ═══════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════

async def get_universe_stats() -> dict:
    """Get universe statistics broken down by tier, source, and sector."""
    async with get_session() as session:
        total = (await session.execute(
            select(func.count()).select_from(UniverseSymbol)
        )).scalar()

        active = (await session.execute(
            select(func.count()).select_from(UniverseSymbol)
            .where(UniverseSymbol.active == True)
        )).scalar()

        # By tier
        tier_rows = (await session.execute(
            select(UniverseSymbol.tier, func.count())
            .group_by(UniverseSymbol.tier)
        )).all()
        by_tier = {(t or "unassigned"): c for t, c in tier_rows}

        # By discovery_source
        src_rows = (await session.execute(
            select(UniverseSymbol.discovery_source, func.count())
            .group_by(UniverseSymbol.discovery_source)
        )).all()
        by_source = {(s or "legacy"): c for s, c in src_rows}

        # By source (original builder source like large_cap, sp500_top100)
        builder_rows = (await session.execute(
            select(UniverseSymbol.source, func.count())
            .group_by(UniverseSymbol.source)
        )).all()
        by_builder_source = {(s or "unknown"): c for s, c in builder_rows}

        # By sector
        sector_rows = (await session.execute(
            select(UniverseSymbol.sector, func.count())
            .group_by(UniverseSymbol.sector)
            .order_by(func.count().desc())
        )).all()
        by_sector = {(s or "unknown"): c for s, c in sector_rows}

    return {
        "total": total,
        "active": active,
        "by_tier": by_tier,
        "by_discovery_source": by_source,
        "by_builder_source": by_builder_source,
        "by_sector": by_sector,
    }


# ═══════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════

def _normalise(fmp_row: dict, source: str) -> dict:
    """Normalise an FMP screener result row into a candidate dict."""
    return {
        "symbol": fmp_row.get("symbol"),
        "name": fmp_row.get("companyName"),
        "market_cap": fmp_row.get("marketCap"),
        "sector": fmp_row.get("sector"),
        "industry": fmp_row.get("industry"),
        "price": fmp_row.get("price"),
        "exchange": fmp_row.get("exchangeShortName"),
        "discovery_source": source,
    }


def _classify_market_cap(market_cap: float | None) -> str:
    if market_cap is None:
        return "unknown"
    if market_cap >= 200_000_000_000:
        return "mega"
    if market_cap >= 10_000_000_000:
        return "large"
    if market_cap >= 2_000_000_000:
        return "mid"
    if market_cap >= 300_000_000:
        return "small"
    if market_cap >= 50_000_000:
        return "micro"
    return "penny"


def _get_fmp(settings) -> FMPClient | None:
    if not settings.fmp_enabled or not settings.fmp_api_key:
        return None
    return FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )


def _get_finnhub(settings) -> FinnhubClient | None:
    if not settings.finnhub_api_key:
        return None
    return FinnhubClient(
        api_key=settings.finnhub_api_key,
        rate_limit=settings.finnhub_rate_limit,
    )

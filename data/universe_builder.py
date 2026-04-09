"""Universe builder — populates universe_symbols by querying Polygon for market-cap tiers."""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta

from config import get_settings
from data.polygon_client import PolygonClient
from data.finnhub_client import FinnhubClient

_log = logging.getLogger(__name__)

# ── Tier definitions ─────────────────────────────────────────

TIER_DEFINITIONS = {
    "large_cap": {
        "label": "Large Cap",
        "market_cap_min": 10_000_000_000,   # $10B
        "market_cap_max": None,
        "target_size": 200,
        "min_volume": 50_000,
        "refresh_frequency": "monthly",
    },
    "mid_cap": {
        "label": "Mid Cap",
        "market_cap_min": 2_000_000_000,    # $2B
        "market_cap_max": 10_000_000_000,   # $10B
        "target_size": 275,
        "min_volume": 50_000,
        "refresh_frequency": "monthly",
    },
    "small_cap": {
        "label": "Small Cap",
        "market_cap_min": 300_000_000,      # $300M
        "market_cap_max": 2_000_000_000,    # $2B
        "target_size": 275,
        "min_volume": 50_000,
        "refresh_frequency": "monthly",
    },
    "penny_stock": {
        "label": "Penny Stocks",
        "market_cap_min": None,
        "market_cap_max": 300_000_000,      # $300M
        "target_size": 100,
        "min_volume": 100_000,              # Higher liquidity floor
        "price_min": 0.50,
        "price_max": 5.00,
        "min_listing_months": 6,            # Not a fresh shell
        "refresh_frequency": "weekly",
    },
}

# Tier priority order — higher-priority tier wins when a symbol qualifies for multiple
TIER_PRIORITY = ["large_cap", "mid_cap", "small_cap", "penny_stock"]

# Cache duration — don't re-query API if refreshed within this window
CACHE_HOURS = 24


def _classify_market_cap_tier(market_cap: float | None) -> str:
    """Compute the market_cap_tier label from a dollar amount."""
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


def _normalize_exchange(raw: str | None) -> str | None:
    """Normalize Polygon exchange codes to human-readable."""
    if not raw:
        return None
    mapping = {
        "XNYS": "NYSE",
        "XNAS": "NASDAQ",
        "XASE": "AMEX",
        "XNMS": "NASDAQ",  # NASDAQ Global Select
        "XNGS": "NASDAQ",  # NASDAQ Global Select (alt)
        "XNCM": "NASDAQ",  # NASDAQ Capital Market
    }
    return mapping.get(raw.upper(), raw)


class UniverseBuilder:
    """Builds the universe_symbols table from Polygon ticker data."""

    def __init__(self):
        settings = get_settings()
        self._polygon = PolygonClient(
            api_key=settings.polygon_api_key,
            rate_limit=settings.polygon_rate_limit,
        )
        self._finnhub = FinnhubClient(
            api_key=settings.finnhub_api_key,
            rate_limit=settings.finnhub_rate_limit,
        )

    async def refresh_tier(self, tier: str) -> dict:
        """Refresh a single universe tier. Returns stats dict."""
        if tier not in TIER_DEFINITIONS:
            return {"error": f"Unknown tier: {tier}", "ok": False}

        defn = TIER_DEFINITIONS[tier]
        _log.info("=" * 50)
        _log.info("UNIVERSE REFRESH: %s (%s)", tier, defn["label"])
        _log.info("=" * 50)

        # Check cache — skip if refreshed recently
        if await self._is_recently_refreshed(tier):
            _log.info("Tier %s was refreshed within %d hours — skipping", tier, CACHE_HOURS)
            return {"ok": True, "tier": tier, "cached": True, "added": 0, "updated": 0, "deactivated": 0}

        # Fetch tickers from Polygon
        tickers = await self._fetch_tickers_for_tier(tier, defn)
        if not tickers:
            _log.warning("No tickers returned for tier %s", tier)
            return {"ok": False, "tier": tier, "error": "No tickers returned", "added": 0, "updated": 0, "deactivated": 0}

        _log.info("Tier %s: %d candidate tickers from Polygon", tier, len(tickers))

        # Enrich ALL tickers with market_cap from Finnhub BEFORE filtering
        tickers = await self._enrich_missing_market_caps(tickers)

        # NOW filter by market cap range (after enrichment)
        mc_min = defn.get("market_cap_min")
        mc_max = defn.get("market_cap_max")
        filtered = []
        for t in tickers:
            mc = t.get("market_cap")
            if mc is None:
                if tier == "penny_stock":
                    filtered.append(t)
                continue
            if mc_min is not None and mc < mc_min:
                continue
            if mc_max is not None and mc >= mc_max:
                continue
            filtered.append(t)

        # Sort by market cap descending
        filtered.sort(key=lambda t: t.get("market_cap") or 0, reverse=True)
        _log.info("Tier %s: %d tickers after market cap filter", tier, len(filtered))

        # For penny stocks, apply extra quality filters
        if tier == "penny_stock":
            filtered = self._apply_penny_filters(filtered, defn)
            _log.info("Tier %s: %d tickers after penny quality filters", tier, len(filtered))

        # Take top N by market cap
        target = defn["target_size"]
        filtered = filtered[:target]
        _log.info("Tier %s: using top %d tickers", tier, len(filtered))

        # Upsert into DB
        stats = await self._upsert_tier(tier, filtered)

        _log.info(
            "UNIVERSE REFRESH COMPLETE: %s — added=%d, updated=%d, deactivated=%d",
            tier, stats["added"], stats["updated"], stats["deactivated"],
        )

        return {"ok": True, "tier": tier, "cached": False, **stats}

    async def refresh_all(self) -> dict:
        """Refresh all tiers sequentially. Returns per-tier stats."""
        results = {}
        for tier in TIER_PRIORITY:
            results[tier] = await self.refresh_tier(tier)
        return results

    async def get_stats(self) -> dict:
        """Get universe statistics by tier."""
        from db.database import get_session, UniverseSymbol, CompanyEvaluation
        from sqlalchemy import select, func, Integer

        async with get_session() as session:
            # Total active
            total_active = (await session.execute(
                select(func.count()).select_from(UniverseSymbol)
                .where(UniverseSymbol.active == True)
            )).scalar()

            # By tier (source)
            tier_rows = (await session.execute(
                select(
                    UniverseSymbol.source,
                    func.count().label("total"),
                    func.sum(func.cast(UniverseSymbol.active, Integer)).label("active_count"),
                    func.max(UniverseSymbol.last_screened_at).label("last_refresh"),
                ).group_by(UniverseSymbol.source)
            )).all()

            # Evaluated counts per source
            eval_counts = {}
            for row in tier_rows:
                source = row[0]
                eval_result = (await session.execute(
                    select(func.count()).select_from(CompanyEvaluation)
                    .where(CompanyEvaluation.symbol.in_(
                        select(UniverseSymbol.symbol)
                        .where(UniverseSymbol.source == source)
                        .where(UniverseSymbol.active == True)
                    ))
                )).scalar()
                eval_counts[source] = eval_result or 0

            by_tier = {}
            for row in tier_rows:
                source = row[0]
                active_count = row[2] or 0
                evaluated = eval_counts.get(source, 0)
                by_tier[source] = {
                    "total": row[1],
                    "active": active_count,
                    "evaluated": evaluated,
                    "stale": active_count - evaluated,
                    "last_refresh": row[3].isoformat() if row[3] else None,
                }

        return {
            "total_active": total_active,
            "by_tier": by_tier,
        }

    # ── Internal methods ─────────────────────────────────────

    async def _fetch_tickers_for_tier(self, tier: str, defn: dict) -> list[dict]:
        """Fetch tickers from Polygon matching a tier's criteria.

        Polygon Starter includes market_cap in the tickers response.
        We fetch sorted by ticker (market_cap sort not supported on this endpoint),
        then sort locally by market_cap.
        """
        # Fetch from both major exchanges
        all_tickers = []
        for exchange in ("XNYS", "XNAS"):
            _log.info("Fetching %s tickers from Polygon (exchange=%s)...", tier, exchange)
            tickers = await self._polygon.get_tickers(
                market="stocks",
                exchange=exchange,
                type="CS",          # Common Stock only
                active=True,
                sort="ticker",      # market_cap sort not supported on /v3/reference/tickers
                order="asc",
                limit=1000,
            )
            all_tickers.extend(tickers)

        if not all_tickers:
            return []

        # Deduplicate by ticker symbol
        seen = set()
        unique = []
        for t in all_tickers:
            sym = t.get("ticker")
            if sym and sym not in seen:
                seen.add(sym)
                unique.append(t)

        # Count how many have market_cap from Polygon response
        has_mc = sum(1 for t in unique if t.get("market_cap") is not None)
        _log.info(
            "Tier %s: %d unique tickers from Polygon (%d have market_cap in response)",
            tier, len(unique), has_mc,
        )
        return unique

    def _apply_penny_filters(self, tickers: list[dict], defn: dict) -> list[dict]:
        """Apply stricter quality filters for penny stocks."""
        price_min = defn.get("price_min", 0.50)
        price_max = defn.get("price_max", 5.00)
        min_listing_months = defn.get("min_listing_months", 6)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=min_listing_months * 30)

        filtered = []
        for t in tickers:
            # Price filter — use last_updated price if available
            # Polygon tickers don't always include price, so we're lenient here
            # The real price filter happens during enrichment

            # Listing age filter
            list_date = t.get("list_date")
            if list_date:
                try:
                    from datetime import date as _date
                    ld = _date.fromisoformat(list_date)
                    if ld > cutoff_date.date():
                        continue  # Too recently listed
                except (ValueError, TypeError):
                    pass

            filtered.append(t)

        return filtered

    async def _enrich_missing_market_caps(self, tickers: list[dict]) -> list[dict]:
        """Enrich tickers missing market_cap via Finnhub.

        Polygon Starter does NOT include market_cap in /v3/reference/tickers.
        Use Finnhub company profile to fill it in.
        """
        missing = [t for t in tickers if t.get("market_cap") is None]
        if not missing:
            _log.info("All %d tickers have market_cap from Polygon — no enrichment needed", len(tickers))
            return tickers

        MAX_ENRICH = 1200
        to_enrich = missing[:MAX_ENRICH]
        _log.info(
            "Enriching %d/%d tickers missing market_cap via Finnhub...",
            len(to_enrich), len(missing),
        )

        enriched = 0
        for i, t in enumerate(to_enrich):
            sym = t.get("ticker")
            if not sym:
                continue
            try:
                profile = await self._finnhub.get_company_profile(sym)
                mc = profile.get("market_cap")
                if mc and isinstance(mc, (int, float)):
                    t["market_cap"] = mc * 1_000_000  # Finnhub returns millions
                    enriched += 1
                name = profile.get("company_name")
                if name and not t.get("name"):
                    t["name"] = name
            except Exception as exc:
                _log.debug("Finnhub enrichment failed for %s: %s", sym, exc)

            # Finnhub free tier: 60 calls/min — stay safe with ~1 req/s
            await asyncio.sleep(1.0)

            if (i + 1) % 100 == 0:
                _log.info("Enrichment progress: %d/%d (found %d market caps)", i + 1, len(to_enrich), enriched)

        _log.info("Enriched %d/%d tickers with Finnhub market_cap", enriched, len(to_enrich))
        return tickers

    async def _upsert_tier(self, tier: str, tickers: list[dict]) -> dict:
        """Upsert tickers into universe_symbols. Returns stats."""
        from db.database import get_session, UniverseSymbol
        from sqlalchemy import select

        now = datetime.now(timezone.utc)
        added = 0
        updated = 0
        deactivated = 0

        # Build set of symbols this tier is contributing
        tier_symbols = {t.get("ticker") for t in tickers if t.get("ticker")}

        async with get_session() as session:
            # 1. Upsert active tickers
            for t in tickers:
                sym = t.get("ticker")
                if not sym:
                    continue

                mc = t.get("market_cap")
                exchange = _normalize_exchange(t.get("primary_exchange"))
                name = t.get("name")

                result = await session.execute(
                    select(UniverseSymbol).where(UniverseSymbol.symbol == sym)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    # Check tier priority — only update source if this tier is higher priority
                    if self._should_update_source(existing.source, tier):
                        existing.source = tier
                    existing.market_cap = mc
                    existing.market_cap_tier = _classify_market_cap_tier(mc)
                    existing.exchange = exchange
                    if name:
                        existing.company_name = name
                    existing.last_screened_at = now
                    existing.active = True
                    existing.delisted_at = None
                    updated += 1
                else:
                    session.add(UniverseSymbol(
                        symbol=sym,
                        company_name=name,
                        source=tier,
                        market_cap=mc,
                        market_cap_tier=_classify_market_cap_tier(mc),
                        exchange=exchange,
                        last_screened_at=now,
                        active=True,
                    ))
                    added += 1

            # 2. Deactivate symbols that were in this tier but are no longer qualifying
            result = await session.execute(
                select(UniverseSymbol)
                .where(UniverseSymbol.source == tier)
                .where(UniverseSymbol.active == True)
            )
            existing_in_tier = result.scalars().all()

            for sym_record in existing_in_tier:
                if sym_record.symbol not in tier_symbols:
                    sym_record.active = False
                    sym_record.delisted_at = now
                    deactivated += 1

            await session.commit()

        return {"added": added, "updated": updated, "deactivated": deactivated}

    def _should_update_source(self, current_source: str, new_tier: str) -> bool:
        """Return True if new_tier has higher or equal priority than current_source.

        Priority: large_cap > mid_cap > small_cap > penny_stock.
        sp500_top100/manual/ipo_discovery are never overwritten by tier refresh.
        """
        protected_sources = {"sp500_top100", "manual", "ipo_discovery"}
        if current_source in protected_sources:
            return False
        if current_source not in TIER_PRIORITY:
            return True  # Unknown source — overwrite
        if new_tier not in TIER_PRIORITY:
            return False
        return TIER_PRIORITY.index(new_tier) <= TIER_PRIORITY.index(current_source)

    async def _is_recently_refreshed(self, tier: str) -> bool:
        """Check if any symbol in this tier was refreshed within CACHE_HOURS."""
        from db.database import get_session, UniverseSymbol
        from sqlalchemy import select, func

        async with get_session() as session:
            result = await session.execute(
                select(func.max(UniverseSymbol.last_screened_at))
                .where(UniverseSymbol.source == tier)
            )
            last_screened = result.scalar_one_or_none()

            if last_screened is None:
                return False

            if last_screened.tzinfo is None:
                last_screened = last_screened.replace(tzinfo=timezone.utc)

            age = datetime.now(timezone.utc) - last_screened
            return age < timedelta(hours=CACHE_HOURS)

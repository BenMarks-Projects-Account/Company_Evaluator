"""Admin API — configuration, universe management."""

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from pydantic import BaseModel
from config import get_settings

router = APIRouter()
_log = logging.getLogger(__name__)

# Track running refresh tasks so we don't double-trigger
_refresh_task: asyncio.Task | None = None


@router.get("/admin/config")
async def get_config():
    s = get_settings()
    return {
        "universe": s.universe,
        "crawler_enabled": s.crawler_enabled,
        "crawler_schedule": s.crawler_schedule,
        "llm_endpoint": s.llm_endpoint,
        "database_url": s.database_url,
        "evaluation_batch_size": s.evaluation_batch_size,
        "refresh_period_days": s.refresh_period_days,
        "pause_between_symbols_sec": s.pause_between_symbols_sec,
    }


@router.get("/admin/universe")
async def get_universe_list():
    from db.database import get_session, UniverseSymbol
    from sqlalchemy import select, desc

    async with get_session() as session:
        result = await session.execute(
            select(UniverseSymbol)
            .order_by(desc(UniverseSymbol.priority), UniverseSymbol.symbol)
        )
        symbols = result.scalars().all()

    active = [s for s in symbols if s.active]
    return {
        "total": len(symbols),
        "active": len(active),
        "symbols": [
            {
                "symbol": s.symbol,
                "company_name": s.company_name,
                "source": s.source,
                "added_at": s.added_at.isoformat() if s.added_at else None,
                "active": s.active,
                "priority": s.priority,
            }
            for s in symbols
        ],
    }


class UniverseAddRequest(BaseModel):
    symbols: list[str]
    source: str = "manual"
    priority: int = 0


class UniverseRemoveRequest(BaseModel):
    symbols: list[str]


@router.post("/admin/universe/add")
async def add_to_universe(req: UniverseAddRequest):
    from db.database import get_session, UniverseSymbol
    from sqlalchemy import select

    added = []
    existing = []
    async with get_session() as session:
        for symbol in req.symbols:
            sym = symbol.upper().strip()
            result = await session.execute(
                select(UniverseSymbol).where(UniverseSymbol.symbol == sym)
            )
            record = result.scalar_one_or_none()
            if record:
                if not record.active:
                    record.active = True
                    record.priority = req.priority
                    added.append(sym)
                else:
                    existing.append(sym)
            else:
                session.add(UniverseSymbol(
                    symbol=sym,
                    source=req.source,
                    priority=req.priority,
                ))
                added.append(sym)
        await session.commit()

    return {"added": added, "already_existed": existing}


@router.post("/admin/universe/remove")
async def remove_from_universe(req: UniverseRemoveRequest):
    from db.database import get_session, UniverseSymbol
    from sqlalchemy import select

    deactivated = []
    async with get_session() as session:
        for symbol in req.symbols:
            sym = symbol.upper().strip()
            result = await session.execute(
                select(UniverseSymbol).where(UniverseSymbol.symbol == sym)
            )
            record = result.scalar_one_or_none()
            if record and record.active:
                record.active = False
                deactivated.append(sym)
        await session.commit()

    return {"deactivated": deactivated}


# ── Smart single-stock add ───────────────────────────────────

class UniverseAddSingleRequest(BaseModel):
    symbol: str


@router.post("/universe/add")
async def add_stock_to_universe(req: UniverseAddSingleRequest):
    """Add a single stock to the universe with profile validation.

    Validates the symbol against Polygon/Finnhub, fetches company
    profile, classifies market-cap tier, and inserts into the universe.
    """
    from db.database import get_session, UniverseSymbol, CompanyEvaluation
    from data.polygon_client import PolygonClient
    from data.finnhub_client import FinnhubClient
    from sqlalchemy import select

    symbol = req.symbol.strip().upper()
    if not symbol or len(symbol) > 10:
        return {"ok": False, "error": "Please provide a valid stock symbol."}

    settings = get_settings()

    # ── 1. Check if already in universe ──────────────────────
    async with get_session() as session:
        result = await session.execute(
            select(UniverseSymbol).where(UniverseSymbol.symbol == symbol)
        )
        existing = result.scalar_one_or_none()

        if existing:
            if not existing.active:
                # Reactivate
                existing.active = True
                existing.priority = 5
                await session.commit()
                return {
                    "ok": True,
                    "action": "reactivated",
                    "symbol": symbol,
                    "company_name": existing.company_name,
                    "source": existing.source,
                    "message": f"{symbol} was previously deactivated. Reactivated and will be re-evaluated.",
                }

            # Already active — check evaluation status
            eval_result = await session.execute(
                select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
            )
            evaluation = eval_result.scalar_one_or_none()
            return {
                "ok": True,
                "action": "exists",
                "symbol": symbol,
                "company_name": existing.company_name,
                "source": existing.source,
                "evaluated": evaluation is not None,
                "last_evaluated": evaluation.evaluated_at.isoformat() if evaluation and evaluation.evaluated_at else None,
                "message": f"{symbol} is already in the universe. Use search to find it.",
            }

    # ── 2. Validate symbol — fetch company profile ───────────
    profile = None

    # Try Polygon first
    try:
        polygon = PolygonClient(api_key=settings.polygon_api_key, rate_limit=settings.polygon_rate_limit)
        ticker_data = await polygon.get_company_details(symbol)
        if ticker_data and not ticker_data.get("error") and ticker_data.get("company_name"):
            profile = {
                "company_name": ticker_data["company_name"],
                "sector": ticker_data.get("sector"),
                "market_cap": ticker_data.get("market_cap"),
                "exchange": ticker_data.get("primary_exchange"),
            }
    except Exception as e:
        _log.warning("Polygon profile failed for %s: %s", symbol, e)

    # Fallback to Finnhub
    if not profile or not profile.get("company_name"):
        try:
            finnhub = FinnhubClient(api_key=settings.finnhub_api_key, rate_limit=settings.finnhub_rate_limit)
            fh_data = await finnhub.get_company_profile(symbol)
            if fh_data and not fh_data.get("error") and fh_data.get("company_name"):
                mc = fh_data.get("market_cap")
                profile = {
                    "company_name": fh_data["company_name"],
                    "sector": fh_data.get("sector"),
                    "market_cap": mc * 1_000_000 if mc else None,  # Finnhub returns millions
                    "exchange": fh_data.get("exchange"),
                }
        except Exception as e:
            _log.warning("Finnhub profile failed for %s: %s", symbol, e)

    if not profile or not profile.get("company_name"):
        return {"ok": False, "error": f"Symbol '{symbol}' not found. Please check the ticker symbol."}

    # ── 3. Classify market-cap tier ──────────────────────────
    mc = profile.get("market_cap") or 0
    if mc >= 10_000_000_000:
        tier = "large_cap"
    elif mc >= 2_000_000_000:
        tier = "mid_cap"
    elif mc >= 300_000_000:
        tier = "small_cap"
    elif mc > 0:
        tier = "penny_stock"
    else:
        tier = "manual"

    # ── 4. Insert into universe ──────────────────────────────
    async with get_session() as session:
        session.add(UniverseSymbol(
            symbol=symbol,
            company_name=profile["company_name"],
            source="manual",
            market_cap=profile.get("market_cap"),
            market_cap_tier=tier,
            sector=profile.get("sector"),
            exchange=profile.get("exchange"),
            active=True,
            priority=5,
            added_at=datetime.now(timezone.utc),
        ))
        await session.commit()

    return {
        "ok": True,
        "action": "added",
        "symbol": symbol,
        "company_name": profile["company_name"],
        "sector": profile.get("sector"),
        "market_cap": profile.get("market_cap"),
        "market_cap_tier": tier,
        "exchange": profile.get("exchange"),
        "message": f"{symbol} added to universe as {tier}. Will be evaluated in the next crawler cycle.",
    }


# ── Universe builder endpoints ───────────────────────────────

class UniverseRefreshRequest(BaseModel):
    tier: str

@router.post("/universe/refresh")
async def refresh_universe_tier(req: UniverseRefreshRequest):
    """Refresh a single universe tier as a background task."""
    global _refresh_task
    from data.universe_builder import UniverseBuilder, TIER_DEFINITIONS

    tier = req.tier.lower().strip()
    if tier not in TIER_DEFINITIONS:
        valid = list(TIER_DEFINITIONS.keys())
        return {"ok": False, "error": f"Unknown tier '{tier}'. Valid: {valid}"}

    if _refresh_task and not _refresh_task.done():
        return {"ok": False, "error": "A universe refresh is already running"}

    async def _run():
        try:
            builder = UniverseBuilder()
            result = await builder.refresh_tier(tier)
            _log.info("Universe refresh '%s' completed: %s", tier, result)
        except Exception as exc:
            _log.error("Universe refresh '%s' failed: %s", tier, exc, exc_info=True)

    _refresh_task = asyncio.create_task(_run())
    return {"ok": True, "status": "started", "tier": tier}


@router.post("/universe/refresh-all")
async def refresh_universe_all():
    """Refresh all universe tiers as a background task."""
    global _refresh_task

    if _refresh_task and not _refresh_task.done():
        return {"ok": False, "error": "A universe refresh is already running"}

    async def _run():
        try:
            from data.universe_builder import UniverseBuilder
            builder = UniverseBuilder()
            results = await builder.refresh_all()
            _log.info("Universe refresh-all completed: %s",
                      {k: v.get('added', 0) for k, v in results.items()})
        except Exception as exc:
            _log.error("Universe refresh-all failed: %s", exc, exc_info=True)

    _refresh_task = asyncio.create_task(_run())
    return {"ok": True, "status": "started", "tiers": ["large_cap", "mid_cap", "small_cap", "penny_stock"]}


@router.get("/universe/refresh-status")
async def refresh_status():
    """Check if a universe refresh is currently running."""
    if _refresh_task is None:
        return {"running": False, "status": "never_started"}
    if _refresh_task.done():
        exc = _refresh_task.exception() if not _refresh_task.cancelled() else None
        return {"running": False, "status": "error" if exc else "completed",
                "error": str(exc) if exc else None}
    return {"running": True, "status": "in_progress"}


@router.get("/universe/stats")
async def get_universe_stats():
    """Get universe statistics by tier."""
    from data.universe_builder import UniverseBuilder

    builder = UniverseBuilder()
    stats = await builder.get_stats()
    return stats

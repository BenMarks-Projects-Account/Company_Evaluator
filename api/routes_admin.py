"""Admin API — configuration, universe management."""

import asyncio
import logging
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

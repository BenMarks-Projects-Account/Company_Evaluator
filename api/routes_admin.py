"""Admin API — configuration, universe management."""

from fastapi import APIRouter
from config import get_settings

router = APIRouter()

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
    }

@router.get("/admin/universe")
async def get_universe_list():
    from data.universe import get_universe, AVAILABLE_UNIVERSES
    settings = get_settings()
    return {
        "active": settings.universe,
        "available": list(AVAILABLE_UNIVERSES.keys()),
        "symbols": get_universe(settings.universe),
        "count": len(get_universe(settings.universe)),
    }

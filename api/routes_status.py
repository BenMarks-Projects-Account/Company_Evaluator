"""Status dashboard API — aggregated system info for the launcher."""

import asyncio
import os
import time
import logging
from datetime import datetime, timezone
from fastapi import APIRouter
from sqlalchemy import select, func, desc

from pipeline.crawler import get_crawler
from pipeline.scheduler import get_scheduler
from db.database import get_session, CompanyEvaluation, UniverseSymbol

router = APIRouter()
_log = logging.getLogger(__name__)

_start_time = time.time()

# ── Dashboard payload cache ─────────────────────────────────
# The launcher polls /api/status/dashboard every 5s. Under crawler
# load this endpoint can stall past the launcher's 3s timeout while
# its SQLAlchemy queries queue behind crawler queries on the same pool.
# A short TTL cache returns the fully-formed payload from memory so
# polls stay responsive regardless of DB contention.
DASHBOARD_TTL_SECONDS = 3.0
_dashboard_cache: dict = {"payload": None, "expires_at": 0.0}
_dashboard_cache_lock = asyncio.Lock()


async def _get_dashboard_payload():
    """Return the dashboard payload, refreshing from DB at most once per TTL."""
    now = time.monotonic()
    if _dashboard_cache["payload"] is not None and now < _dashboard_cache["expires_at"]:
        return _dashboard_cache["payload"]
    async with _dashboard_cache_lock:
        # Double-check after acquiring lock — another coroutine may have refreshed.
        now = time.monotonic()
        if _dashboard_cache["payload"] is not None and now < _dashboard_cache["expires_at"]:
            return _dashboard_cache["payload"]
        t0 = time.monotonic()
        payload = await _build_dashboard_payload()
        query_ms = round((time.monotonic() - t0) * 1000, 1)
        _dashboard_cache["payload"] = payload
        _dashboard_cache["expires_at"] = time.monotonic() + DASHBOARD_TTL_SECONDS
        _log.info(
            "event=dashboard_cache_refresh ttl=%.1f query_ms=%s",
            DASHBOARD_TTL_SECONDS, query_ms,
        )
        return payload


@router.get("/status/dashboard")
async def dashboard():
    """Aggregated status for the launcher dashboard (cached, TTL=3s)."""
    return await _get_dashboard_payload()


async def _build_dashboard_payload():
    """Build the dashboard payload from live sources. Called at most once per TTL."""

    # ── Backend info ─────────────────────────────────────────
    pid = os.getpid()
    uptime_sec = time.time() - _start_time
    mem_mb = None
    cpu_pct = None
    try:
        import psutil
        proc = psutil.Process(pid)
        mem_mb = round(proc.memory_info().rss / 1_048_576, 1)
        cpu_pct = proc.cpu_percent(interval=0)
    except ImportError:
        _log.debug("psutil not installed — memory/CPU stats unavailable")
    except Exception:
        pass

    backend = {
        "status": "running",
        "uptime_seconds": round(uptime_sec),
        "pid": pid,
        "memory_mb": mem_mb,
        "cpu_pct": cpu_pct,
        "port": 8100,
    }

    # ── Crawler info ─────────────────────────────────────────
    crawler = get_crawler()
    scheduler = get_scheduler()
    crawler_status = {**crawler.status, **scheduler.status}

    # ── Universe info ────────────────────────────────────────
    universe = {"total": 0, "active": 0, "by_tier": {}, "last_refresh": None}
    try:
        async with get_session() as session:
            total = (await session.execute(
                select(func.count()).select_from(UniverseSymbol)
            )).scalar() or 0
            active = (await session.execute(
                select(func.count()).select_from(UniverseSymbol)
                .where(UniverseSymbol.active == True)
            )).scalar() or 0

            # By tier: count total and evaluated per source
            tier_rows = (await session.execute(
                select(UniverseSymbol.source, func.count())
                .where(UniverseSymbol.active == True)
                .group_by(UniverseSymbol.source)
            )).all()

            by_tier = {}
            for source, count in tier_rows:
                # Count how many in this tier have been evaluated
                eval_count = (await session.execute(
                    select(func.count()).select_from(CompanyEvaluation)
                    .where(CompanyEvaluation.symbol.in_(
                        select(UniverseSymbol.symbol)
                        .where(UniverseSymbol.active == True)
                        .where(UniverseSymbol.source == source)
                    ))
                )).scalar() or 0
                by_tier[source] = {"total": count, "evaluated": eval_count}

            # Last refresh: most recent added_at
            last_refresh_row = (await session.execute(
                select(func.max(UniverseSymbol.added_at))
            )).scalar()

            universe = {
                "total": total,
                "active": active,
                "by_tier": by_tier,
                "last_refresh": last_refresh_row.isoformat() if last_refresh_row else None,
            }
    except Exception as exc:
        _log.warning("Dashboard: could not load universe stats: %s", exc)

    # ── Recent evaluations from DB ───────────────────────────
    recent_db = []
    try:
        async with get_session() as session:
            rows = (await session.execute(
                select(
                    CompanyEvaluation.symbol,
                    CompanyEvaluation.composite_score,
                    CompanyEvaluation.llm_recommendation,
                    CompanyEvaluation.evaluated_at,
                )
                .order_by(desc(CompanyEvaluation.evaluated_at))
                .limit(10)
            )).all()
            for row in rows:
                recent_db.append({
                    "symbol": row[0],
                    "score": row[1],
                    "recommendation": row[2],
                    "evaluated_at": row[3].isoformat() if row[3] else None,
                })
    except Exception as exc:
        _log.warning("Dashboard: could not load recent evals: %s", exc)

    return {
        "backend": backend,
        "crawler": crawler_status,
        "universe": universe,
        "recent_evaluations": recent_db,
        "recent_activity": crawler_status.get("recent_activity", []),
        "last_error": crawler_status.get("last_error"),
    }

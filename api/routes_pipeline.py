"""Pipeline control API — trigger evaluations, check status."""

import asyncio
from fastapi import APIRouter, Query
from pydantic import BaseModel
from config import get_settings
from pipeline.crawler import get_crawler

router = APIRouter()


class RunRequest(BaseModel):
    symbols: list[str] | None = None
    full_universe: bool = False


@router.get("/pipeline/status")
async def get_pipeline_status():
    """Get current crawler / pipeline status."""
    crawler = get_crawler()
    return crawler.status


@router.post("/pipeline/run")
async def trigger_pipeline_run(req: RunRequest = RunRequest()):
    """Start the crawler asynchronously (non-blocking).

    With full_universe=true or no symbols, the crawler fetches its own
    ordered list from the DB (oldest-evaluated first).
    """
    crawler = get_crawler()

    if crawler._running:
        return {"status": "already_running", **crawler.status}

    symbols = req.symbols  # explicit list overrides auto-ordering
    if not symbols and not req.full_universe:
        return {"error": "Provide symbols list or set full_universe=true"}

    # Run in background — return immediately
    # If symbols is None, crawler will query DB for oldest-first ordering
    asyncio.create_task(crawler.run(symbols if symbols else None))
    return {"status": "started", "mode": "explicit" if symbols else "auto_ordered"}


@router.post("/pipeline/stop")
async def stop_pipeline():
    """Stop the running crawler after the current symbol finishes."""
    crawler = get_crawler()
    if not crawler._running:
        return {"status": "not_running"}
    crawler.stop()
    return {"status": "stop_requested"}


@router.post("/pipeline/pause")
async def pause_pipeline():
    """Pause the crawler after the current symbol finishes."""
    crawler = get_crawler()
    if not crawler._running:
        return {"status": "not_running"}
    if crawler.pause():
        return {"status": "pause_requested"}
    return {"status": "already_paused"}


@router.post("/pipeline/resume")
async def resume_pipeline():
    """Resume a paused crawler."""
    crawler = get_crawler()
    if crawler.resume():
        return {"status": "resumed"}
    return {"status": "not_paused"}


@router.post("/pipeline/evaluate/{symbol}")
async def evaluate_single_company(symbol: str):
    """Evaluate a single company on demand."""
    from pipeline.evaluator import evaluate_company
    result = await evaluate_company(symbol.upper())
    return result

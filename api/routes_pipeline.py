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
    """Start the crawler asynchronously (non-blocking)."""
    crawler = get_crawler()

    if crawler._running:
        return {"status": "already_running", **crawler.status}

    symbols = req.symbols
    if req.full_universe:
        from data.universe import get_universe
        symbols = get_universe(get_settings().universe)
    elif not symbols:
        return {"error": "Provide symbols list or set full_universe=true"}

    # Run in background — return immediately
    asyncio.create_task(crawler.run(symbols))
    return {"status": "started", "symbols": len(symbols)}


@router.post("/pipeline/stop")
async def stop_pipeline():
    """Stop the running crawler after the current symbol finishes."""
    crawler = get_crawler()
    if not crawler._running:
        return {"status": "not_running"}
    crawler.stop()
    return {"status": "stop_requested"}


@router.post("/pipeline/evaluate/{symbol}")
async def evaluate_single_company(symbol: str):
    """Evaluate a single company on demand."""
    from pipeline.evaluator import evaluate_company
    result = await evaluate_company(symbol.upper())
    return result

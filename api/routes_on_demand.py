"""On-Demand analysis routes — /api/on-demand/*."""

import asyncio
import logging
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline.on_demand import (
    create_job,
    get_job,
    get_job_result,
    get_recent_result_by_symbol,
    cancel_job,
    run_on_demand_analysis,
)
from analysis.research_prompt_template import build_research_prompt

router = APIRouter()
_log = logging.getLogger(__name__)

# Only allow 1-5 uppercase alpha characters (standard US ticker format)
_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")


class AnalyzeRequest(BaseModel):
    symbol: str


@router.post("/on-demand/analyze")
async def start_analysis(request: AnalyzeRequest):
    """Kick off an on-demand analysis and return the job ID immediately."""
    symbol = (request.symbol or "").upper().strip()

    if not symbol or not _SYMBOL_RE.match(symbol):
        raise HTTPException(400, f"Invalid symbol format: {request.symbol}")

    job = await create_job(symbol)

    # Fire-and-forget background task
    asyncio.create_task(run_on_demand_analysis(job["job_id"], symbol))

    return job


@router.get("/on-demand/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll current job progress."""
    job = await get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")
    return job


@router.get("/on-demand/jobs/{job_id}/result")
async def get_job_result_endpoint(job_id: str):
    """Fetch the full result payload (only available when status=complete)."""
    job = await get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")

    if job["status"] != "complete":
        raise HTTPException(
            400,
            f"Job {job_id} is not complete (status: {job['status']})",
        )

    result = await get_job_result(job_id)
    if not result:
        raise HTTPException(500, f"Result for job {job_id} is unavailable")

    return result


@router.delete("/on-demand/jobs/{job_id}")
async def cancel_job_endpoint(job_id: str):
    """Cancel a queued or running job."""
    success = await cancel_job(job_id)
    if not success:
        raise HTTPException(404, f"Job {job_id} not found or already finished")
    return {"status": "cancelled"}


@router.get("/on-demand/research-prompt/{symbol}")
async def get_research_prompt(symbol: str):
    """Generate a deep research prompt from the most recent evaluation."""
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(400, f"Invalid symbol format: {symbol}")

    result = await get_recent_result_by_symbol(symbol, max_age_hours=24)
    if not result:
        return {
            "ok": False,
            "symbol": symbol,
            "error": "No recent evaluation found. Run an on-demand analysis first.",
        }

    from datetime import datetime, timezone

    completed_at = result.pop("_completed_at", None)
    age_seconds = None
    if completed_at:
        try:
            ts = completed_at.rstrip("Z")
            if "+" not in ts and not ts.endswith("00:00"):
                ts += "+00:00"
            dt = datetime.fromisoformat(ts)
            age_seconds = (datetime.now(timezone.utc) - dt).total_seconds()
        except Exception:
            pass

    company = result.get("company") or {}

    try:
        prompt_text = build_research_prompt(result)
    except Exception as e:
        company_shape = (
            {k: type(v).__name__ for k, v in company.items()}
            if isinstance(company, dict) else type(company).__name__
        )
        _log.exception(
            "Failed to build research prompt for %s | company field types: %s",
            symbol, company_shape,
        )
        return {
            "ok": False,
            "symbol": symbol,
            "error": f"Failed to build research prompt: {e}",
            "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        }

    return {
        "ok": True,
        "symbol": symbol,
        "company_name": company.get("name", symbol),
        "prompt": prompt_text,
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "data_freshness": completed_at,
        "evaluation_age_seconds": round(age_seconds) if age_seconds else None,
    }

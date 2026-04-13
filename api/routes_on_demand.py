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
    cancel_job,
    run_on_demand_analysis,
)

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

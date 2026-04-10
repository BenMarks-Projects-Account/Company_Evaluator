"""Transcript analysis routes — on-demand earnings call research."""

import logging

from fastapi import APIRouter, HTTPException

router = APIRouter()
_log = logging.getLogger(__name__)


def _get_fmp():
    """Get the shared FMP client (lazy init)."""
    from pipeline.evaluator import _get_fmp_client
    client = _get_fmp_client()
    if client is None:
        raise HTTPException(status_code=503, detail="FMP client not available (check fmp_enabled / fmp_api_key)")
    return client


@router.get("/companies/{symbol}/transcript-analysis")
async def get_transcript_analysis(
    symbol: str,
    year: int | None = None,
    quarter: int | None = None,
):
    """On-demand LLM analysis of an earnings call transcript.

    Fetches the most recent transcript (or a specific quarter) and
    runs structured analysis focused on medium-term investing signals.

    This is slow (30-90s) — the LLM processes ~10K words of transcript.
    """
    from analysis.transcript_analyzer import analyze_transcript

    symbol = symbol.upper()
    fmp = _get_fmp()

    result = await analyze_transcript(symbol, fmp, year, quarter)

    if result.get("error"):
        if "No transcript available" in result["error"]:
            raise HTTPException(status_code=404, detail=result["error"])
        raise HTTPException(status_code=503, detail=result["error"])

    return result


@router.get("/companies/{symbol}/transcript-list")
async def get_available_transcripts(symbol: str):
    """List available earnings call transcripts for a symbol.

    Lightweight metadata-only query — no transcript content returned.
    """
    symbol = symbol.upper()
    fmp = _get_fmp()

    transcripts = await fmp.get_transcript_list(symbol)

    if not transcripts:
        return {"symbol": symbol, "transcripts": []}

    return {"symbol": symbol, "transcripts": transcripts}

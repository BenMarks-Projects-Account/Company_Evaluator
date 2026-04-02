"""Comparable Company Analysis API — /api/valuation/comps."""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from analysis.comps_model import analyze_comps
from db.database import get_session, CompsAnalysis
from sqlalchemy import select

router = APIRouter()
_log = logging.getLogger(__name__)


class CompsRequest(BaseModel):
    symbol: str
    skip_llm: bool = False
    skip_model: bool = False


async def _save_comps(symbol: str, result: dict) -> None:
    """Persist comps analysis to DB (UPSERT)."""
    try:
        verdict = result.get("verdict") or {}
        fair_value = result.get("fair_value") or {}
        confidence = result.get("confidence") or {}
        peer_group = result.get("peer_group") or {}
        subject = result.get("subject") or {}

        async with get_session() as session:
            row = CompsAnalysis(
                symbol=symbol,
                analysis_data=json.dumps(result),
                verdict_status=verdict.get("label"),
                upside_pct=verdict.get("upside_pct"),
                fair_value_composite=fair_value.get("composite_fair_value"),
                confidence=confidence.get("level"),
                peer_count=peer_group.get("count"),
                analyzed_at=datetime.now(timezone.utc).isoformat(),
                current_price_at_analysis=subject.get("current_price"),
            )
            await session.merge(row)
            await session.commit()
        _log.info("Saved comps analysis for %s", symbol)
    except Exception as exc:
        _log.error("Failed to save comps analysis for %s: %s", symbol, exc)


@router.post("/valuation/comps")
async def run_comps(req: CompsRequest):
    """Run comparable-company valuation analysis for a given symbol."""
    symbol = req.symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        raise HTTPException(400, "Invalid symbol")

    skip = req.skip_llm or req.skip_model

    result = await analyze_comps(symbol, skip_llm=skip)

    if not result.get("ok"):
        raise HTTPException(
            422,
            detail=result.get("error", "Comps analysis failed"),
        )

    # Persist the result
    await _save_comps(symbol, result)

    return result


@router.get("/valuation/comps/{symbol}")
async def get_comps_analysis(symbol: str):
    """Retrieve stored comps analysis for a symbol."""
    symbol = symbol.upper().strip()

    async with get_session() as session:
        result = await session.execute(
            select(CompsAnalysis).where(CompsAnalysis.symbol == symbol)
        )
        row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(404, detail=f"No comps analysis for {symbol}")

    data = json.loads(row.analysis_data)
    data["cached"] = True
    data["analyzed_at"] = row.analyzed_at
    return data

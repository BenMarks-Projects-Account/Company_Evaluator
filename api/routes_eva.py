"""EVA/ROIC Valuation API — /api/valuation/eva."""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from analysis.eva_model import analyze_eva
from db.database import get_session, EvaAnalysis
from sqlalchemy import select

router = APIRouter()
_log = logging.getLogger(__name__)


class EvaRequest(BaseModel):
    symbol: str
    skip_llm: bool = False
    skip_model: bool = False


async def _save_eva(symbol: str, result: dict) -> None:
    """Persist EVA analysis to DB (UPSERT)."""
    try:
        roic_analysis = result.get("roic_analysis") or {}
        wacc_data = result.get("wacc") or {}
        eva_data = result.get("eva") or {}
        quality = result.get("quality") or {}

        async with get_session() as session:
            row = EvaAnalysis(
                symbol=symbol,
                analysis_data=json.dumps(result),
                roic=roic_analysis.get("roic"),
                wacc=wacc_data.get("wacc"),
                value_spread=eva_data.get("value_spread"),
                eva_annual=eva_data.get("eva_annual"),
                grade=quality.get("grade"),
                score=quality.get("score"),
                confidence=result.get("confidence"),
                analyzed_at=datetime.now(timezone.utc).isoformat(),
                current_price_at_analysis=result.get("current_price"),
            )
            await session.merge(row)
            await session.commit()
        _log.info("Saved EVA analysis for %s", symbol)
    except Exception as exc:
        _log.error("Failed to save EVA analysis for %s: %s", symbol, exc)


@router.post("/valuation/eva")
async def run_eva(req: EvaRequest):
    """Run EVA/ROIC analysis for a given symbol."""
    symbol = req.symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        raise HTTPException(400, "Invalid symbol")

    skip = req.skip_llm or req.skip_model

    result = await analyze_eva(symbol, skip_llm=skip)

    if not result.get("ok"):
        raise HTTPException(
            422,
            detail=result.get("error", "EVA analysis failed"),
        )

    await _save_eva(symbol, result)

    return result


@router.get("/valuation/eva/{symbol}")
async def get_eva_analysis(symbol: str):
    """Retrieve stored EVA analysis for a symbol."""
    symbol = symbol.upper().strip()

    async with get_session() as session:
        result = await session.execute(
            select(EvaAnalysis).where(EvaAnalysis.symbol == symbol)
        )
        row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(404, detail=f"No EVA analysis for {symbol}")

    data = json.loads(row.analysis_data)
    data["cached"] = True
    data["analyzed_at"] = row.analyzed_at
    return data

"""DCF Valuation API — /api/valuation/dcf."""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from analysis.dcf_model import analyze_dcf
from db.database import get_session, DcfAnalysis
from sqlalchemy import select

router = APIRouter()
_log = logging.getLogger(__name__)


class DcfRequest(BaseModel):
    symbol: str
    skip_llm: bool = False
    skip_model: bool = False


async def _save_dcf(symbol: str, result: dict) -> None:
    """Persist DCF analysis to DB (UPSERT)."""
    try:
        valuation = result.get("valuation") or {}
        inputs = result.get("inputs") or {}

        async with get_session() as session:
            row = DcfAnalysis(
                symbol=symbol,
                analysis_data=json.dumps(result),
                intrinsic_value=valuation.get("intrinsic_value_per_share"),
                upside_pct=valuation.get("upside_pct"),
                verdict=valuation.get("verdict"),
                wacc=inputs.get("wacc"),
                confidence=result.get("confidence"),
                analyzed_at=datetime.now(timezone.utc).isoformat(),
                current_price_at_analysis=valuation.get("current_price"),
            )
            await session.merge(row)
            await session.commit()
        _log.info("Saved DCF analysis for %s", symbol)
    except Exception as exc:
        _log.error("Failed to save DCF analysis for %s: %s", symbol, exc)


@router.post("/valuation/dcf")
async def run_dcf(req: DcfRequest):
    """Run DCF valuation analysis for a given symbol."""
    symbol = req.symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        raise HTTPException(400, "Invalid symbol")

    skip = req.skip_llm or req.skip_model

    result = await analyze_dcf(symbol, skip_llm=skip)

    if not result.get("ok"):
        raise HTTPException(
            422,
            detail=result.get("error", "DCF analysis failed"),
        )

    # Persist the result
    await _save_dcf(symbol, result)

    return result


@router.get("/valuation/dcf/{symbol}")
async def get_dcf_analysis(symbol: str):
    """Retrieve stored DCF analysis for a symbol."""
    symbol = symbol.upper().strip()

    async with get_session() as session:
        result = await session.execute(
            select(DcfAnalysis).where(DcfAnalysis.symbol == symbol)
        )
        row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(404, detail=f"No DCF analysis for {symbol}")

    data = json.loads(row.analysis_data)
    data["cached"] = True
    data["analyzed_at"] = row.analyzed_at
    return data

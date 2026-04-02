"""Entry Point Analysis API — /api/entry-point/analyze."""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from analysis.entry_point import analyze_entry_point
from db.database import get_session, EntryPointAnalysis
from sqlalchemy import select

router = APIRouter()
_log = logging.getLogger(__name__)


class EntryPointRequest(BaseModel):
    symbol: str
    skip_llm: bool = False
    skip_model: bool = False


async def _save_entry_point(symbol: str, result: dict) -> None:
    """Persist entry point analysis to DB (UPSERT)."""
    try:
        async with get_session() as session:
            row = EntryPointAnalysis(
                symbol=symbol,
                analysis_data=json.dumps(result),
                recommendation=result.get("recommendation"),
                conviction=result.get("conviction"),
                composite_score=result.get("composite_score"),
                suggested_entry=result.get("suggested_entry"),
                suggested_stop=result.get("suggested_stop"),
                risk_reward=result.get("risk_reward"),
                analyzed_at=datetime.now(timezone.utc).isoformat(),
                current_price_at_analysis=result.get("current_price"),
            )
            await session.merge(row)
            await session.commit()
        _log.info("Saved entry point analysis for %s", symbol)
    except Exception as exc:
        _log.error("Failed to save entry point analysis for %s: %s", symbol, exc)


@router.post("/entry-point/analyze")
async def analyze(req: EntryPointRequest):
    """Run entry point analysis for a given symbol."""
    symbol = req.symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        raise HTTPException(400, "Invalid symbol")

    # skip_model is an alias for skip_llm
    skip = req.skip_llm or req.skip_model

    result = await analyze_entry_point(symbol, skip_llm=skip)

    if not result.get("ok"):
        raise HTTPException(
            422,
            detail=result.get("error", "Analysis failed"),
        )

    # Persist the result
    await _save_entry_point(symbol, result)

    return result


@router.get("/entry-point/analysis/{symbol}")
async def get_entry_point_analysis(symbol: str):
    """Retrieve stored entry point analysis for a symbol."""
    symbol = symbol.upper().strip()

    async with get_session() as session:
        result = await session.execute(
            select(EntryPointAnalysis).where(EntryPointAnalysis.symbol == symbol)
        )
        row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(404, detail=f"No entry point analysis for {symbol}")

    data = json.loads(row.analysis_data)
    data["cached"] = True
    data["analyzed_at"] = row.analyzed_at
    return data

"""Analyses status API — bulk retrieval of stored analysis summaries."""

from fastapi import APIRouter
from db.database import get_session, EntryPointAnalysis, CompsAnalysis, DcfAnalysis, EvaAnalysis
from sqlalchemy import select

router = APIRouter()


@router.get("/analyses/status")
async def get_analyses_status():
    """Return summary of all stored entry point and comps analyses.

    Used by the TMC dashboard to know which companies have analyses
    available and their headline results.
    """
    entry_point = {}
    comps = {}
    dcf = {}
    eva = {}

    async with get_session() as session:
        # Entry point analyses
        ep_result = await session.execute(select(EntryPointAnalysis))
        for row in ep_result.scalars().all():
            entry_point[row.symbol] = {
                "analyzed_at": row.analyzed_at,
                "recommendation": row.recommendation,
                "conviction": row.conviction,
                "composite_score": row.composite_score,
                "current_price": row.current_price_at_analysis,
            }

        # Comps analyses
        comps_result = await session.execute(select(CompsAnalysis))
        for row in comps_result.scalars().all():
            comps[row.symbol] = {
                "analyzed_at": row.analyzed_at,
                "verdict": row.verdict_status,
                "upside_pct": row.upside_pct,
                "fair_value_composite": row.fair_value_composite,
                "confidence": row.confidence,
                "peer_count": row.peer_count,
                "current_price": row.current_price_at_analysis,
            }

        # DCF analyses
        dcf_result = await session.execute(select(DcfAnalysis))
        for row in dcf_result.scalars().all():
            dcf[row.symbol] = {
                "analyzed_at": row.analyzed_at,
                "verdict": row.verdict,
                "intrinsic_value": row.intrinsic_value,
                "upside_pct": row.upside_pct,
                "wacc": row.wacc,
                "confidence": row.confidence,
                "current_price": row.current_price_at_analysis,
            }

        # EVA analyses
        eva_result = await session.execute(select(EvaAnalysis))
        for row in eva_result.scalars().all():
            eva[row.symbol] = {
                "analyzed_at": row.analyzed_at,
                "roic": row.roic,
                "wacc": row.wacc,
                "value_spread": row.value_spread,
                "eva_annual": row.eva_annual,
                "grade": row.grade,
                "score": row.score,
                "confidence": row.confidence,
                "current_price": row.current_price_at_analysis,
            }

    return {
        "entry_point": entry_point,
        "comps": comps,
        "dcf": dcf,
        "eva": eva,
    }

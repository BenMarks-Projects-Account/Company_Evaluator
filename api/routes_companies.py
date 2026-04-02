"""Company evaluation API — ranked list and individual company detail."""

from datetime import datetime, timezone
from fastapi import APIRouter, Query
from db.database import get_session, CompanyEvaluation
from sqlalchemy import select, desc, func
from config import get_settings

router = APIRouter()


def _staleness_info(evaluated_at, refresh_days: int) -> dict:
    """Compute staleness fields for an evaluation timestamp."""
    if not evaluated_at:
        return {"days_since_evaluation": None, "is_stale": True}
    now = datetime.now(timezone.utc)
    if evaluated_at.tzinfo is None:
        evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)
    days = (now - evaluated_at).total_seconds() / 86400
    return {"days_since_evaluation": round(days, 1), "is_stale": days > refresh_days}

@router.get("/companies/ranked")
async def get_ranked_companies(
    limit: int = Query(50, ge=1, le=500),
    sector: str = Query(None),
    min_score: float = Query(None),
):
    """Get companies ranked by composite score (highest first)."""
    settings = get_settings()
    refresh_days = settings.refresh_period_days

    async with get_session() as session:
        query = select(CompanyEvaluation).order_by(desc(CompanyEvaluation.composite_score))
        
        if sector:
            query = query.where(CompanyEvaluation.sector == sector)
        if min_score:
            query = query.where(CompanyEvaluation.composite_score >= min_score)
        
        query = query.limit(limit)
        result = await session.execute(query)
        companies = result.scalars().all()
        
        return {
            "count": len(companies),
            "refresh_period_days": refresh_days,
            "companies": [
                {
                    "rank": c.rank,
                    "symbol": c.symbol,
                    "company_name": c.company_name,
                    "sector": c.sector,
                    "industry": c.industry,
                    "market_cap": c.market_cap,
                    "composite_score": c.composite_score,
                    "pillar_scores": {
                        "business_quality": c.pillar_1_business_quality,
                        "operational_health": c.pillar_2_operational_health,
                        "capital_allocation": c.pillar_3_capital_allocation,
                        "growth_quality": c.pillar_4_growth_quality,
                        "valuation": c.pillar_5_valuation,
                    },
                    "llm_recommendation": c.llm_recommendation,
                    "llm_conviction": c.llm_conviction,
                    "llm_summary": c.llm_summary,
                    "evaluated_at": c.evaluated_at.isoformat() if c.evaluated_at else None,
                    **_staleness_info(c.evaluated_at, refresh_days),
                }
                for c in companies
            ],
        }

@router.get("/companies/sectors")
async def get_sectors():
    """Get list of sectors with company counts."""
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation.sector,
                   func.count(CompanyEvaluation.symbol),
                   func.avg(CompanyEvaluation.composite_score))
            .group_by(CompanyEvaluation.sector)
            .order_by(desc(func.avg(CompanyEvaluation.composite_score)))
        )
        return {"sectors": [{"sector": r[0], "count": r[1], "avg_score": round(r[2], 1)} for r in result]}

@router.get("/companies/{symbol}")
async def get_company_detail(symbol: str):
    """Get full evaluation detail for a single company."""
    settings = get_settings()

    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol.upper())
        )
        company = result.scalar_one_or_none()
        
        if not company:
            return {"error": f"No evaluation found for {symbol.upper()}"}
        
        return {
            "symbol": company.symbol,
            "company_name": company.company_name,
            "sector": company.sector,
            "industry": company.industry,
            "market_cap": company.market_cap,
            "composite_score": company.composite_score,
            "rank": company.rank,
            "pillar_scores": {
                "business_quality": company.pillar_1_business_quality,
                "operational_health": company.pillar_2_operational_health,
                "capital_allocation": company.pillar_3_capital_allocation,
                "growth_quality": company.pillar_4_growth_quality,
                "valuation": company.pillar_5_valuation,
            },
            "pillar_details": {
                "business_quality": company.pillar_1_detail,
                "operational_health": company.pillar_2_detail,
                "capital_allocation": company.pillar_3_detail,
                "growth_quality": company.pillar_4_detail,
                "valuation": company.pillar_5_detail,
            },
            "llm_analysis": {
                "recommendation": company.llm_recommendation,
                "conviction": company.llm_conviction,
                "summary": company.llm_summary,
                "thesis": company.llm_thesis,
                "risks": company.llm_risks,
                "catalysts": company.llm_catalysts,
            },
            "evaluated_at": company.evaluated_at.isoformat() if company.evaluated_at else None,
            "data_freshness": company.data_freshness,
            **_staleness_info(company.evaluated_at, settings.refresh_period_days),
        }

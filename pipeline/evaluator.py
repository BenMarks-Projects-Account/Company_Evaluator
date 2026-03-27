"""Full evaluation pipeline: data → metrics → score → store."""

import logging
from datetime import datetime, timezone
from data.company_data_service import CompanyDataService
from metrics.composite import compute_composite_score
from analysis.company_analyst import analyze_company
from db.database import get_session, CompanyEvaluation, EvaluationHistory
from sqlalchemy import select

_log = logging.getLogger(__name__)

_data_service = None


def _get_data_service():
    global _data_service
    if _data_service is None:
        _data_service = CompanyDataService()
    return _data_service


async def evaluate_company(symbol: str) -> dict:
    """Full evaluation: fetch → compute → store → return."""
    symbol = symbol.upper()
    _log.info("event=evaluate_start symbol=%s", symbol)

    # Step 1: Fetch data
    data_service = _get_data_service()
    company_data = await data_service.get_company_data(symbol)

    if company_data.get("data_quality") == "degraded":
        return {"symbol": symbol, "status": "degraded", "errors": company_data.get("errors", [])}

    # Step 2: Compute metrics + scores
    try:
        scores = compute_composite_score(company_data)
    except Exception as exc:
        _log.error("event=metrics_failed symbol=%s error=%s", symbol, exc)
        return {"symbol": symbol, "status": "metrics_failed", "error": str(exc)}

    profile = company_data.get("profile", {})

    # Step 3: LLM analysis (graceful — failure doesn't block pipeline)
    llm_result = None
    try:
        llm_result = await analyze_company(symbol, profile, scores)
    except Exception as exc:
        _log.error("event=llm_analysis_error symbol=%s error=%s", symbol, exc)

    # Step 4: Store in database
    async with get_session() as session:
        # Upsert — update existing or insert new
        existing = await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
        )
        eval_record = existing.scalar_one_or_none()

        if eval_record is None:
            eval_record = CompanyEvaluation(symbol=symbol)
            session.add(eval_record)

        # Update all fields
        eval_record.company_name = profile.get("company_name")
        eval_record.sector = profile.get("sector")
        eval_record.industry = profile.get("industry")
        eval_record.market_cap = profile.get("market_cap")
        eval_record.composite_score = scores.get("composite_score")

        ps = scores.get("pillar_scores", {})
        eval_record.pillar_1_business_quality = ps.get("business_quality")
        eval_record.pillar_2_operational_health = ps.get("operational_health")
        eval_record.pillar_3_capital_allocation = ps.get("capital_allocation")
        eval_record.pillar_4_growth_quality = ps.get("growth_quality")
        eval_record.pillar_5_valuation = ps.get("valuation")

        pd = scores.get("pillar_details", {})
        eval_record.pillar_1_detail = pd.get("business_quality")
        eval_record.pillar_2_detail = pd.get("operational_health")
        eval_record.pillar_3_detail = pd.get("capital_allocation")
        eval_record.pillar_4_detail = pd.get("growth_quality")
        eval_record.pillar_5_detail = pd.get("valuation")

        # LLM analysis fields
        if llm_result:
            eval_record.llm_summary = llm_result.get("summary")
            eval_record.llm_recommendation = llm_result.get("recommendation")
            eval_record.llm_conviction = llm_result.get("conviction")
            eval_record.llm_thesis = llm_result.get("thesis")
            eval_record.llm_risks = llm_result.get("risks")
            eval_record.llm_catalysts = llm_result.get("catalysts")

        eval_record.evaluated_at = datetime.now(timezone.utc)
        eval_record.data_freshness = company_data.get("data_quality")

        # Also write to history
        history = EvaluationHistory(
            symbol=symbol,
            composite_score=scores.get("composite_score"),
            evaluated_at=datetime.now(timezone.utc),
            snapshot={
                "pillar_scores": ps,
                "market_cap": profile.get("market_cap"),
            },
        )
        session.add(history)

        await session.commit()

    # Step 5: Update rankings
    await _update_rankings()

    _log.info("event=evaluate_complete symbol=%s score=%s", symbol, scores.get("composite_score"))

    result = {
        "symbol": symbol,
        "status": "complete",
        "composite_score": scores.get("composite_score"),
        "pillar_scores": scores.get("pillar_scores"),
        "company_name": profile.get("company_name"),
        "sector": profile.get("sector"),
        "data_quality": company_data.get("data_quality"),
    }
    if llm_result:
        result["llm_analysis"] = llm_result

    return result


async def _update_rankings():
    """Re-rank all companies by composite_score descending."""
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation)
            .where(CompanyEvaluation.composite_score.isnot(None))
            .order_by(CompanyEvaluation.composite_score.desc())
        )
        companies = result.scalars().all()

        for rank, company in enumerate(companies, 1):
            company.rank = rank

        await session.commit()

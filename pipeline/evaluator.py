"""Full evaluation pipeline: data → metrics → score → store."""

import logging
import time
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
    t0 = time.time()
    _log.info("="*60)
    _log.info("PIPELINE START: %s", symbol)
    _log.info("="*60)

    # Step 1: Fetch data
    _log.info("[%s] Step 1/5: Fetching company data...", symbol)
    t1 = time.time()
    data_service = _get_data_service()
    company_data = await data_service.get_company_data(symbol)
    _log.info("[%s] Step 1/5: Data fetch complete in %.1fs — quality=%s",
              symbol, time.time() - t1, company_data.get("data_quality"))

    sources = company_data.get("sources_used", {})
    _log.info("[%s]   Sources: polygon=%s finnhub=%s yahoo_fallback=%s",
              symbol, sources.get("polygon"), sources.get("finnhub"), sources.get("yahoo_fallback"))

    # Log what data we got
    for key in ["financials_quarterly", "financials_annual", "basic_financials",
                "price_history", "eps_estimates", "price_target",
                "insider_transactions", "analyst_recommendations"]:
        blob = company_data.get(key)
        if blob is None:
            _log.warning("[%s]   %s: MISSING (None)", symbol, key)
        elif isinstance(blob, dict) and blob.get("error"):
            _log.warning("[%s]   %s: ERROR — %s", symbol, key, blob["error"])
        elif isinstance(blob, dict) and "statements" in blob:
            _log.info("[%s]   %s: OK (%d statements)", symbol, key, len(blob.get("statements", [])))
        elif isinstance(blob, dict) and "metrics" in blob:
            _log.info("[%s]   %s: OK (%d metrics)", symbol, key, len(blob.get("metrics", {})))
        else:
            _log.info("[%s]   %s: OK", symbol, key)

    profile = company_data.get("profile", {})
    _log.info("[%s]   Profile: name=%s sector=%s market_cap=%s",
              symbol, profile.get("company_name"), profile.get("sector"), profile.get("market_cap"))

    if company_data.get("data_quality") == "degraded":
        _log.error("[%s] ABORTED: Data quality is 'degraded' — insufficient data to score", symbol)
        return {"symbol": symbol, "status": "degraded", "errors": company_data.get("errors", [])}

    # Step 2: Compute metrics + scores
    _log.info("[%s] Step 2/5: Computing 5-pillar metrics...", symbol)
    t2 = time.time()
    try:
        scores = compute_composite_score(company_data)
    except Exception as exc:
        _log.error("[%s] Step 2/5: METRICS FAILED — %s", symbol, exc, exc_info=True)
        return {"symbol": symbol, "status": "metrics_failed", "error": str(exc)}

    ps = scores.get("pillar_scores", {})
    _log.info("[%s] Step 2/5: Metrics complete in %.1fs — composite=%.1f",
              symbol, time.time() - t2, scores.get("composite_score") or 0)
    _log.info("[%s]   P1 Business Quality:     %s/100", symbol, ps.get("business_quality"))
    _log.info("[%s]   P2 Operational Health:    %s/100", symbol, ps.get("operational_health"))
    _log.info("[%s]   P3 Capital Allocation:    %s/100", symbol, ps.get("capital_allocation"))
    _log.info("[%s]   P4 Growth Quality:        %s/100", symbol, ps.get("growth_quality"))
    _log.info("[%s]   P5 Valuation:             %s/100", symbol, ps.get("valuation"))

    # Log individual metric details per pillar
    for pname, pdata in scores.get("pillar_details", {}).items():
        metrics = pdata.get("metrics", {})
        sub_scores = pdata.get("scores", {})
        if metrics:
            parts = [f"{k}={v}(s:{sub_scores.get(k)})" for k, v in metrics.items()]
            _log.info("[%s]   %s details: %s", symbol, pname, ", ".join(parts))

    # Step 3: LLM analysis (graceful — failure doesn't block pipeline)
    _log.info("[%s] Step 3/5: Running LLM analysis...", symbol)
    t3 = time.time()
    llm_result = None
    try:
        llm_result = await analyze_company(symbol, profile, scores)
    except Exception as exc:
        _log.error("[%s] Step 3/5: LLM FAILED — %s", symbol, exc, exc_info=True)

    if llm_result:
        _log.info("[%s] Step 3/5: LLM complete in %.1fs — rec=%s conviction=%s",
                  symbol, time.time() - t3,
                  llm_result.get("recommendation"), llm_result.get("conviction"))
    else:
        _log.warning("[%s] Step 3/5: LLM returned no result (%.1fs) — pipeline continues without",
                     symbol, time.time() - t3)

    # Step 4: Store in database
    _log.info("[%s] Step 4/5: Saving to database...", symbol)
    t4 = time.time()
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

    _log.info("[%s] Step 4/5: Database save complete in %.1fs (upsert + history)", symbol, time.time() - t4)

    # Step 5: Update rankings
    _log.info("[%s] Step 5/5: Updating rankings...", symbol)
    t5 = time.time()
    await _update_rankings()
    _log.info("[%s] Step 5/5: Rankings updated in %.1fs", symbol, time.time() - t5)

    elapsed_total = time.time() - t0
    _log.info("=" * 60)
    _log.info("PIPELINE COMPLETE: %s — score=%.1f — %.1fs total",
              symbol, scores.get("composite_score") or 0, elapsed_total)
    _log.info("=" * 60)

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

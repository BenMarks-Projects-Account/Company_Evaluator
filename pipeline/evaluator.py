"""Full evaluation pipeline: data → metrics → score → store."""

import json
import logging
import time
from datetime import datetime, timezone
from data.company_data_service import CompanyDataService
from metrics.composite import compute_composite_score, recompute_composite_from_metrics
from metrics.breakout import compute_breakout_score
from metrics.cross_validator import cross_validate_finnhub_metrics
from analysis.company_analyst import analyze_company
from db.database import get_session, CompanyEvaluation, EvaluationHistory
from sqlalchemy import select

_log = logging.getLogger(__name__)
SCORING_VERSION = "0.2.0"

_data_service = None
_fmp_client = None


def _get_data_service():
    global _data_service
    if _data_service is None:
        _data_service = CompanyDataService()
    return _data_service


def _get_fmp_client():
    """Lazily create FMP client if enabled and configured."""
    global _fmp_client
    if _fmp_client is not None:
        return _fmp_client

    from config import get_settings
    settings = get_settings()
    if not settings.fmp_enabled or not settings.fmp_api_key:
        return None

    from data.fmp_client import FMPClient
    _fmp_client = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )
    return _fmp_client


async def evaluate_company(symbol: str, force: bool = False) -> dict:
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
                "price_history",
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

    # Step 1b: FMP cross-validation (optional — adjusts Finnhub metrics before scoring)
    cross_validation_flags = []
    fmp_client = _get_fmp_client()
    if fmp_client:
        _log.info("[%s] Step 1b: FMP cross-validation...", symbol)
        try:
            fmp_data = await fmp_client.get_all_cross_validation_data(symbol)
            bf = company_data.get("basic_financials")
            if bf and isinstance(bf, dict) and "metrics" in bf:
                _, cross_validation_flags = cross_validate_finnhub_metrics(
                    bf["metrics"], fmp_data
                )
                if cross_validation_flags:
                    _log.info("[%s] Step 1b: %d metric(s) adjusted by FMP cross-validation",
                              symbol, len(cross_validation_flags))
                else:
                    _log.info("[%s] Step 1b: FMP cross-validation — all metrics within tolerance",
                              symbol)
            else:
                _log.info("[%s] Step 1b: No Finnhub metrics to cross-validate", symbol)
        except Exception as exc:
            _log.warning("[%s] Step 1b: FMP cross-validation failed (non-blocking) — %s",
                         symbol, exc)

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

    # Step 2b: Compute breakout potential score (parallel to composite)
    breakout_result = compute_breakout_score(company_data)
    if breakout_result.get("filtered_out"):
        _log.info("[%s] Breakout: FILTERED — %s", symbol, breakout_result.get("filter_reason"))
    else:
        _log.info("[%s] Breakout: score=%.1f (completeness=%.0f%%)",
                  symbol, breakout_result.get("score") or 0,
                  breakout_result.get("completeness_pct") or 0)
        for comp_name, comp_data in breakout_result.get("components", {}).items():
            _log.info("[%s]   breakout.%s = %s", symbol, comp_name, comp_data.get("score"))

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
    raw_financials_snapshot = _build_raw_financials_snapshot(company_data, scores)
    structured_errors = _build_errors_snapshot(company_data, scores, cross_validation_flags)
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
        eval_record.evaluation_version = SCORING_VERSION
        eval_record.raw_financials = raw_financials_snapshot
        eval_record.errors = structured_errors

        # Breakout Potential Score (parallel to composite)
        eval_record.breakout_score = breakout_result.get("score")
        eval_record.breakout_components = json.dumps(breakout_result)

        # Also write to history
        history = EvaluationHistory(
            symbol=symbol,
            composite_score=scores.get("composite_score"),
            evaluated_at=datetime.now(timezone.utc),
            snapshot={
                "pillar_scores": ps,
                "market_cap": profile.get("market_cap"),
                "breakout_score": breakout_result.get("score"),
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
        "breakout_score": breakout_result.get("score"),
        "breakout_filtered_out": breakout_result.get("filtered_out"),
        "breakout_filter_reason": breakout_result.get("filter_reason"),
        "company_name": profile.get("company_name"),
        "sector": profile.get("sector"),
        "data_quality": company_data.get("data_quality"),
        "overall_completeness_pct": scores.get("overall_completeness_pct"),
        "missing_pillar_count": scores.get("missing_pillar_count"),
        "data_quality_flags": scores.get("data_quality_flags", []),
        "force": force,
    }
    if llm_result:
        result["llm_analysis"] = llm_result

    return result


def _detail_to_dict(detail):
    if detail is None:
        return {}
    if isinstance(detail, dict):
        return detail
    if isinstance(detail, str):
        try:
            return json.loads(detail)
        except json.JSONDecodeError:
            return {}
    return {}


def _build_raw_financials_snapshot(company_data: dict, scores: dict) -> dict:
    payload = {
        "symbol": company_data.get("symbol"),
        "profile": company_data.get("profile"),
        "financials_quarterly": company_data.get("financials_quarterly"),
        "financials_annual": company_data.get("financials_annual"),
        "basic_financials": company_data.get("basic_financials"),
        "price_history": company_data.get("price_history"),
        "insider_transactions": company_data.get("insider_transactions"),
        "smart_money": company_data.get("smart_money"),
        "analyst_recommendations": company_data.get("analyst_recommendations"),
        "sources_used": company_data.get("sources_used"),
        "data_quality": company_data.get("data_quality"),
    }
    return {
        "fetched_at": company_data.get("fetched_at"),
        "evaluation_version": SCORING_VERSION,
        "sources": company_data.get("source_attribution", {}),
        "company_data": payload,
        "computed_inputs": {
            "biz_quality": _detail_to_dict(scores.get("pillar_details", {}).get("business_quality")).get("raw_metrics", {}),
            "ops_health": _detail_to_dict(scores.get("pillar_details", {}).get("operational_health")).get("raw_metrics", {}),
            "cap_allocation": _detail_to_dict(scores.get("pillar_details", {}).get("capital_allocation")).get("raw_metrics", {}),
            "growth": _detail_to_dict(scores.get("pillar_details", {}).get("growth_quality")).get("raw_metrics", {}),
            "valuation": _detail_to_dict(scores.get("pillar_details", {}).get("valuation")).get("raw_metrics", {}),
        },
    }


def _build_errors_snapshot(company_data: dict, scores: dict,
                           cross_validation_flags: list[dict] | None = None) -> dict:
    fetch_errors = company_data.get("fetch_errors") or []
    data_quality_flags = [
        {**flag, "action": "rejected_treated_as_missing"}
        for flag in (scores.get("data_quality_flags") or [])
    ]

    missing_data_warnings = []
    for pillar_name, pillar_detail in (scores.get("pillar_details") or {}).items():
        completeness_pct = pillar_detail.get("completeness_pct", 0.0) or 0.0
        if completeness_pct < 50:
            missing_data_warnings.append(
                f"{pillar_name} has < 50% data completeness ({completeness_pct:.1f}%)"
            )

    payload = {
        "fetch_errors": fetch_errors,
        "data_quality_flags": data_quality_flags,
        "missing_data_warnings": missing_data_warnings,
        "computational_errors": [],
        "cross_validation_flags": cross_validation_flags or [],
    }

    if not any(payload.values()):
        return {}
    return payload


def _stored_pillar_metrics(company: CompanyEvaluation) -> dict[str, dict]:
    detail_map = {
        "business_quality": company.pillar_1_detail,
        "operational_health": company.pillar_2_detail,
        "capital_allocation": company.pillar_3_detail,
        "growth_quality": company.pillar_4_detail,
        "valuation": company.pillar_5_detail,
    }
    output = {}
    for name, detail in detail_map.items():
        metrics = _detail_to_dict(detail).get("metrics", {})
        output[name] = metrics if isinstance(metrics, dict) else {}
    return output


async def rerank_existing_evaluations() -> dict:
    """Re-score and rerank existing evaluations using stored pillar metrics only."""
    async with get_session() as session:
        result = await session.execute(select(CompanyEvaluation))
        companies = result.scalars().all()

        for company in companies:
            scores = recompute_composite_from_metrics(
                _stored_pillar_metrics(company),
                company.data_freshness or "unknown",
            )

            pillar_scores = scores.get("pillar_scores", {})
            pillar_details = scores.get("pillar_details", {})

            company.composite_score = scores.get("composite_score")
            company.pillar_1_business_quality = pillar_scores.get("business_quality")
            company.pillar_2_operational_health = pillar_scores.get("operational_health")
            company.pillar_3_capital_allocation = pillar_scores.get("capital_allocation")
            company.pillar_4_growth_quality = pillar_scores.get("growth_quality")
            company.pillar_5_valuation = pillar_scores.get("valuation")
            company.pillar_1_detail = pillar_details.get("business_quality")
            company.pillar_2_detail = pillar_details.get("operational_health")
            company.pillar_3_detail = pillar_details.get("capital_allocation")
            company.pillar_4_detail = pillar_details.get("growth_quality")
            company.pillar_5_detail = pillar_details.get("valuation")
            company.evaluation_version = SCORING_VERSION
            existing_errors = _detail_to_dict(company.errors)
            company.errors = {
                "fetch_errors": existing_errors.get("fetch_errors", []),
                "data_quality_flags": [
                    {**flag, "action": "rejected_treated_as_missing"}
                    for flag in (scores.get("data_quality_flags") or [])
                ],
                "missing_data_warnings": [
                    f"{pillar_name} has < 50% data completeness ({(detail.get('completeness_pct', 0.0) or 0.0):.1f}%)"
                    for pillar_name, detail in (scores.get("pillar_details") or {}).items()
                    if (detail.get("completeness_pct", 0.0) or 0.0) < 50
                ],
                "computational_errors": existing_errors.get("computational_errors", []),
            }

        ranked = sorted(
            companies,
            key=lambda company: (company.composite_score is not None, company.composite_score or 0.0),
            reverse=True,
        )
        for index, company in enumerate(ranked, 1):
            company.rank = index

        await session.commit()

    flagged = sum(
        1
        for company in companies
        if _detail_to_dict(company.errors).get("fetch_errors") or _detail_to_dict(company.errors).get("data_quality_flags")
    )
    return {
        "status": "ok",
        "updated": len(companies),
        "flagged_companies": flagged,
        "scoring_version": SCORING_VERSION,
    }


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

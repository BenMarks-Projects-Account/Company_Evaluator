"""Company evaluation API — ranked list and individual company detail."""

import json
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
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
    min_breakout_score: float = Query(None, description="Minimum breakout potential score"),
    sort: str = Query("composite", description="Sort by: composite or breakout"),
):
    """Get companies ranked by composite score (highest first)."""
    settings = get_settings()
    refresh_days = settings.refresh_period_days

    async with get_session() as session:
        if sort == "breakout":
            query = select(CompanyEvaluation).where(
                CompanyEvaluation.breakout_score.isnot(None)
            ).order_by(desc(CompanyEvaluation.breakout_score))
        else:
            query = select(CompanyEvaluation).order_by(desc(CompanyEvaluation.composite_score))
        
        if sector:
            query = query.where(CompanyEvaluation.sector == sector)
        if min_score:
            query = query.where(CompanyEvaluation.composite_score >= min_score)
        if min_breakout_score is not None:
            query = query.where(CompanyEvaluation.breakout_score >= min_breakout_score)
        
        query = query.limit(limit)
        result = await session.execute(query)
        companies = result.scalars().all()

        rows = []
        for company in companies:
            completeness_pct, missing_pillar_count = _pillar_details_summary(company)
            rows.append(
                {
                    "rank": company.rank,
                    "symbol": company.symbol,
                    "company_name": company.company_name,
                    "sector": company.sector,
                    "industry": company.industry,
                    "market_cap": company.market_cap,
                    "composite_score": company.composite_score,
                    "breakout_score": company.breakout_score,
                    "pillar_scores": {
                        "business_quality": company.pillar_1_business_quality,
                        "operational_health": company.pillar_2_operational_health,
                        "capital_allocation": company.pillar_3_capital_allocation,
                        "growth_quality": company.pillar_4_growth_quality,
                        "valuation": company.pillar_5_valuation,
                    },
                    "llm_recommendation": company.llm_recommendation,
                    "llm_conviction": company.llm_conviction,
                    "llm_summary": company.llm_summary,
                    "completeness_pct": completeness_pct,
                    "missing_pillar_count": missing_pillar_count,
                    "evaluated_at": company.evaluated_at.isoformat() if company.evaluated_at else None,
                    **_staleness_info(company.evaluated_at, refresh_days),
                }
            )

        return {
            "count": len(companies),
            "refresh_period_days": refresh_days,
            "companies": rows,
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


@router.get("/companies/data-quality-issues")
async def get_data_quality_issues(
    min_flag_count: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
):
    """Return companies with persisted data quality issues."""
    async with get_session() as session:
        result = await session.execute(select(CompanyEvaluation))
        companies = result.scalars().all()

    rows = []
    for company in companies:
        errors = _normalize_errors(_parse_json_field(company.errors))
        fetch_errors = errors.get("fetch_errors") or []
        data_quality_flags = errors.get("data_quality_flags") or []
        missing_data_warnings = errors.get("missing_data_warnings") or []
        computational_errors = errors.get("computational_errors") or []

        total_flags = (
            len(fetch_errors)
            + len(data_quality_flags)
            + len(missing_data_warnings)
            + len(computational_errors)
        )
        if total_flags < min_flag_count:
            continue

        reason_counts: dict[str, int] = {}
        for flag in data_quality_flags:
            reason = str(flag.get("reason") or "unknown").split(" ", 1)[0]
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

        rows.append(
            {
                "symbol": company.symbol,
                "rank": company.rank,
                "sector": company.sector,
                "market_cap": company.market_cap,
                "composite_score": company.composite_score,
                "flag_count": total_flags,
                "flags_summary": {
                    **reason_counts,
                    "fetch_error": len(fetch_errors),
                    "missing_data_warning": len(missing_data_warnings),
                    "computational_error": len(computational_errors),
                },
                "worst_metrics": [
                    {
                        "metric": flag.get("metric"),
                        "value": flag.get("raw_value"),
                    }
                    for flag in data_quality_flags[:3]
                ],
            }
        )

    rows.sort(key=lambda item: (item["flag_count"], item.get("composite_score") or 0), reverse=True)
    return {
        "total_companies_with_issues": len(rows),
        "companies": rows[:limit],
    }


@router.get("/companies/{symbol}/raw")
async def get_company_raw_data(symbol: str):
    """Return persisted raw evaluation data in a UI-friendly structure."""
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol.upper())
        )
        company = result.scalar_one_or_none()

    if not company:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No evaluation found for symbol '{symbol.upper()}'. "
                f"Use POST /api/admin/backfill/{symbol.upper()} to evaluate it."
            ),
        )

    raw = _parse_json_field(company.raw_financials)
    errors = _normalize_errors(_parse_json_field(company.errors))
    completeness_pct, _ = _pillar_details_summary(company)

    diagnostics_warnings = []
    if raw is None:
        diagnostics_warnings.append(
            f"raw_financials not yet persisted for this evaluation. Use POST /api/admin/backfill/{company.symbol} to populate."
        )

    response = {
        "symbol": company.symbol,
        "company_name": company.company_name,
        "evaluated_at": company.evaluated_at.isoformat() if company.evaluated_at else None,
        "evaluation_version": company.evaluation_version,
        "data_freshness": company.data_freshness,
        "composite": {
            "score": company.composite_score,
            "rank": company.rank,
            "rating": company.llm_recommendation,
            "overall_completeness_pct": completeness_pct,
        },
        "breakout": {
            "score": company.breakout_score,
            "components": _parse_json_field(company.breakout_components),
        },
        "data_sources": _format_data_sources(raw, errors),
        "profile": _format_profile(company, raw),
        "raw_financials": _format_raw_financials(raw),
        "pillars": _format_pillars(company),
        "diagnostics": {
            "warnings": diagnostics_warnings,
            "fetch_errors": errors.get("fetch_errors", []),
            "data_quality_flags": errors.get("data_quality_flags", []),
            "missing_data_warnings": errors.get("missing_data_warnings", []),
            "computational_errors": errors.get("computational_errors", []),
        },
        "llm_analysis": {
            "summary": company.llm_summary,
            "recommendation": company.llm_recommendation,
            "conviction": company.llm_conviction,
            "thesis": company.llm_thesis,
            "risks": _parse_json_field(company.llm_risks),
            "catalysts": _parse_json_field(company.llm_catalysts),
        },
    }

    return response

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
        
        completeness_pct, missing_pillar_count = _pillar_details_summary(company)

        return {
            "symbol": company.symbol,
            "company_name": company.company_name,
            "sector": company.sector,
            "industry": company.industry,
            "market_cap": company.market_cap,
            "composite_score": company.composite_score,
            "rank": company.rank,
            "overall_completeness_pct": completeness_pct,
            "missing_pillar_count": missing_pillar_count,
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
            "breakout": {
                "score": company.breakout_score,
                "components": _parse_json_field(company.breakout_components),
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


@router.post("/companies/{symbol}/evaluate")
async def evaluate_company_alias(symbol: str):
    """Evaluate a single company on demand via the companies namespace."""
    from pipeline.evaluator import evaluate_company

    return await evaluate_company(symbol.upper())


def _pillar_details_summary(company: CompanyEvaluation) -> tuple[float | None, int]:
    details = [
        company.pillar_1_detail,
        company.pillar_2_detail,
        company.pillar_3_detail,
        company.pillar_4_detail,
        company.pillar_5_detail,
    ]
    parsed = []
    for detail in details:
        if isinstance(detail, dict):
            parsed.append(detail)
        elif isinstance(detail, str):
            try:
                parsed.append(json.loads(detail))
            except json.JSONDecodeError:
                parsed.append({})
        else:
            parsed.append({})

    completeness = [d.get("completeness_pct") for d in parsed if d.get("completeness_pct") is not None]
    if not completeness:
        return None, sum(1 for d in parsed if not d.get("metrics"))
    overall = round(sum(completeness) / len(parsed), 1)
    missing_pillars = sum(1 for d in parsed if (d.get("completeness_pct") or 0) == 0)
    return overall, missing_pillars


def _parse_json_field(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _normalize_errors(errors):
    if not errors:
        errors = {}
    elif isinstance(errors, list):
        errors = {"computational_errors": errors}
    elif not isinstance(errors, dict):
        errors = {"computational_errors": [errors]}

    return {
        "fetch_errors": errors.get("fetch_errors", []),
        "data_quality_flags": errors.get("data_quality_flags", []),
        "missing_data_warnings": errors.get("missing_data_warnings", []),
        "computational_errors": errors.get("computational_errors", []),
    }


def _format_data_sources(raw, errors):
    if not raw:
        return None

    fetch_errors = errors.get("fetch_errors", [])
    data_sources = {}
    for name, payload in (raw.get("sources") or {}).items():
        matching_error = next(
            (
                item.get("error")
                for item in fetch_errors
                if item.get("source") == payload.get("provider") and item.get("endpoint") == payload.get("endpoint")
            ),
            None,
        )
        data_sources[name] = {
            "provider": payload.get("provider"),
            "endpoint": payload.get("endpoint"),
            "fetched_at": payload.get("fetched_at"),
            "ok": payload.get("ok"),
            "error": matching_error,
        }
    return data_sources


def _format_profile(company: CompanyEvaluation, raw):
    raw_company_data = (raw or {}).get("company_data", {})
    profile = raw_company_data.get("profile") or {}
    price_history = raw_company_data.get("price_history") or {}
    return {
        "symbol": company.symbol,
        "name": company.company_name,
        "sector": company.sector,
        "industry": company.industry,
        "market_cap": company.market_cap,
        "current_price": price_history.get("current_price"),
        "shares_outstanding": profile.get("shares_outstanding"),
        "country": profile.get("country"),
        "exchange": profile.get("exchange"),
        "employees": profile.get("employees"),
    }


def _statement_sections(statements):
    statements = statements or []
    income_statement = []
    balance_sheet = []
    cash_flow = []

    income_fields = {
        "period", "start_date", "fiscal_period", "fiscal_year", "filing_date",
        "revenue", "cost_of_revenue", "gross_profit", "operating_income", "operating_expenses",
        "net_income", "eps_basic", "eps_diluted", "research_and_development",
        "selling_general_administrative", "income_before_tax", "income_tax",
        "basic_avg_shares", "diluted_avg_shares",
    }
    balance_fields = {
        "period", "start_date", "fiscal_period", "fiscal_year", "filing_date",
        "total_assets", "total_liabilities", "total_equity", "equity_parent", "current_assets",
        "current_liabilities", "noncurrent_assets", "noncurrent_liabilities", "long_term_debt",
        "inventory", "accounts_payable", "fixed_assets",
    }
    cash_fields = {
        "period", "start_date", "fiscal_period", "fiscal_year", "filing_date",
        "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "net_cash_flow", "free_cash_flow",
    }

    for statement in statements:
        income_statement.append({key: statement.get(key) for key in income_fields if key in statement})
        balance_sheet.append({key: statement.get(key) for key in balance_fields if key in statement})
        cash_flow.append({key: statement.get(key) for key in cash_fields if key in statement})

    return {
        "income_statement": income_statement,
        "balance_sheet": balance_sheet,
        "cash_flow": cash_flow,
    }


def _format_raw_financials(raw):
    if not raw:
        return None

    company_data = raw.get("company_data", {})
    quarterly = ((company_data.get("financials_quarterly") or {}).get("statements") or [])
    annual = ((company_data.get("financials_annual") or {}).get("statements") or [])
    quarterly_sections = _statement_sections(quarterly)
    annual_sections = _statement_sections(annual)

    return {
        "income_statement": {
            "annual": annual_sections["income_statement"],
            "quarterly": quarterly_sections["income_statement"],
        },
        "balance_sheet": {
            "annual": annual_sections["balance_sheet"],
            "quarterly": quarterly_sections["balance_sheet"],
        },
        "cash_flow": {
            "annual": annual_sections["cash_flow"],
            "quarterly": quarterly_sections["cash_flow"],
        },
        "finnhub_metrics": (company_data.get("basic_financials") or {}).get("metrics"),
        "analyst_recommendations": company_data.get("analyst_recommendations"),
        "insider_transactions": company_data.get("insider_transactions"),
        "smart_money": company_data.get("smart_money"),
    }


def _format_pillars(company: CompanyEvaluation):
    mapping = [
        ("business_quality", company.pillar_1_business_quality, company.pillar_1_detail),
        ("operational_health", company.pillar_2_operational_health, company.pillar_2_detail),
        ("capital_allocation", company.pillar_3_capital_allocation, company.pillar_3_detail),
        ("growth_quality", company.pillar_4_growth_quality, company.pillar_4_detail),
        ("valuation_expectations", company.pillar_5_valuation, company.pillar_5_detail),
    ]
    pillars = {}
    for name, pillar_score, detail in mapping:
        payload = _parse_json_field(detail) or {}
        metrics = payload.get("metrics") or {}
        sub_scores = payload.get("scores") or {}
        rejected_inputs = payload.get("data_quality_flags") or []
        pillars[name] = {
            "score": pillar_score,
            "raw_score_before_cap": payload.get("raw_score"),
            "cap_applied": payload.get("cap_applied", False),
            "completeness_pct": payload.get("completeness_pct"),
            "metrics": metrics,
            "sub_scores": sub_scores,
            "missing_metrics": [metric for metric, value in metrics.items() if value is None],
            "rejected_inputs": rejected_inputs,
        }
    return pillars

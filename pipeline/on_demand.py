"""On-Demand Company Evaluator.

Orchestrates the full evaluation pipeline plus all action button
analyses (DCF, EVA, Comps, Entry Point, Smart Money, Transcript)
for a single symbol. Tracks progress in the on_demand_jobs table
for polling by the UI.

Persists results as if the crawler had just evaluated the company.
"""

import asyncio
import json
import logging
import secrets
import time
from datetime import datetime, timezone, timedelta

from db.database import get_session, OnDemandJob, CompanyEvaluation, UniverseSymbol
from sqlalchemy import select, delete

_log = logging.getLogger(__name__)

# ─── Step definitions (order matters — these run sequentially) ────────
PIPELINE_STEPS = [
    "Validating symbol and fetching profile",
    "Pulling fundamentals and computing scores",
    "Running smart money analysis",
    "Running DCF valuation",
    "Running EVA/ROIC analysis",
    "Running comparable company analysis",
    "Computing Earnings Power Value",
    "Running entry point analysis",
    "Fetching price targets",
    "Analyzing earnings transcript",
    "Generating LLM thesis",
    "Generating business profile",
    "Computing Piotroski F-Score",
    "Persisting results",
]


# ═══════════════════════════════════════════════════════════════════════
# JOB LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════

def _generate_job_id(symbol: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    suffix = secrets.token_hex(2)
    return f"ondemand_{ts}_{symbol}_{suffix}"


async def create_job(symbol: str) -> dict:
    """Insert a new queued job and return its initial state."""
    job_id = _generate_job_id(symbol)
    now = datetime.now(timezone.utc).isoformat() + "Z"

    async with get_session() as session:
        row = OnDemandJob(
            job_id=job_id,
            symbol=symbol,
            status="queued",
            created_at=now,
            total_steps=len(PIPELINE_STEPS),
            percent=0,
            completed_steps="[]",
        )
        session.add(row)
        await session.commit()

    # Opportunistically clean old jobs
    asyncio.create_task(_cleanup_old_jobs())

    return {
        "job_id": job_id,
        "symbol": symbol,
        "status": "queued",
        "created_at": now,
    }


async def get_job(job_id: str) -> dict | None:
    """Return current job status (without result payload)."""
    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob).where(OnDemandJob.job_id == job_id)
        )
        row = result.scalar_one_or_none()
        if not row:
            return None

        return {
            "job_id": row.job_id,
            "symbol": row.symbol,
            "status": row.status,
            "created_at": row.created_at,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
            "progress": {
                "current_step": row.current_step or "",
                "current_step_index": row.current_step_index or 0,
                "total_steps": row.total_steps or len(PIPELINE_STEPS),
                "percent": row.percent or 0,
            },
            "completed_steps": json.loads(row.completed_steps or "[]"),
            "error": row.error,
        }


async def get_job_result(job_id: str) -> dict | None:
    """Return the full result payload (only if complete)."""
    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob.result_json).where(
                OnDemandJob.job_id == job_id,
                OnDemandJob.status == "complete",
            )
        )
        row = result.scalar_one_or_none()
        if not row:
            return None
        return json.loads(row)


async def get_recent_result_by_symbol(
    symbol: str, max_age_hours: int = 24
) -> dict | None:
    """Return the most recent complete result for *symbol* within *max_age_hours*."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob)
            .where(
                OnDemandJob.symbol == symbol.upper(),
                OnDemandJob.status == "complete",
                OnDemandJob.completed_at > cutoff,
            )
            .order_by(OnDemandJob.completed_at.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
        if not row or not row.result_json:
            return None
        data = json.loads(row.result_json)
        data["_completed_at"] = row.completed_at
        return data


async def cancel_job(job_id: str) -> bool:
    """Mark a queued/running job as cancelled."""
    now = datetime.now(timezone.utc).isoformat() + "Z"
    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob).where(
                OnDemandJob.job_id == job_id,
                OnDemandJob.status.in_(["queued", "running"]),
            )
        )
        row = result.scalar_one_or_none()
        if not row:
            return False
        row.status = "cancelled"
        row.completed_at = now
        await session.commit()
        return True


# ═══════════════════════════════════════════════════════════════════════
# PROGRESS TRACKING (internal helpers)
# ═══════════════════════════════════════════════════════════════════════

async def _update_progress(
    job_id: str,
    step_index: int,
    completed: list[str],
    *,
    status: str = "running",
):
    """Write current progress to the DB so the polling endpoint can read it."""
    step_name = PIPELINE_STEPS[step_index - 1] if 1 <= step_index <= len(PIPELINE_STEPS) else ""
    percent = round((step_index / len(PIPELINE_STEPS)) * 100)

    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob).where(OnDemandJob.job_id == job_id)
        )
        row = result.scalar_one_or_none()
        if not row:
            return
        row.status = status
        row.current_step = step_name
        row.current_step_index = step_index
        row.percent = percent
        row.completed_steps = json.dumps(completed)
        if step_index == 1 and not row.started_at:
            row.started_at = datetime.now(timezone.utc).isoformat() + "Z"
        await session.commit()


async def _mark_complete(job_id: str, result_payload: dict):
    now = datetime.now(timezone.utc).isoformat() + "Z"
    async with get_session() as session:
        res = await session.execute(
            select(OnDemandJob).where(OnDemandJob.job_id == job_id)
        )
        row = res.scalar_one_or_none()
        if not row:
            return
        row.status = "complete"
        row.completed_at = now
        row.percent = 100
        row.current_step = "Complete"
        row.result_json = json.dumps(result_payload, default=str)
        await session.commit()


async def _mark_failed(job_id: str, error: str):
    now = datetime.now(timezone.utc).isoformat() + "Z"
    async with get_session() as session:
        res = await session.execute(
            select(OnDemandJob).where(OnDemandJob.job_id == job_id)
        )
        row = res.scalar_one_or_none()
        if not row:
            return
        row.status = "failed"
        row.completed_at = now
        row.error = error
        await session.commit()


async def _is_cancelled(job_id: str) -> bool:
    async with get_session() as session:
        result = await session.execute(
            select(OnDemandJob.status).where(OnDemandJob.job_id == job_id)
        )
        status = result.scalar_one_or_none()
        return status == "cancelled"


# ═══════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════

async def run_on_demand_analysis(job_id: str, symbol: str):
    """Full on-demand pipeline — runs as a background task.

    Steps:
      1. Validate + profile
      2. Full evaluate_company (data fetch → 5-pillar scoring → DB persist)
      3. Smart money (already fetched inside evaluate_company, extract from raw)
      4-6. DCF / EVA / Comps (parallel)
      7. Entry point analysis
      8. Price targets
      9. Transcript analysis
     10. LLM thesis (already from evaluate_company) + persist result
    """
    completed: list[str] = []
    t0 = time.time()

    try:
        # ── Step 1: Validate symbol ──────────────────────────────
        await _update_progress(job_id, 1, completed)
        if await _is_cancelled(job_id):
            return

        profile = await _fetch_profile(symbol)
        if not profile:
            await _mark_failed(job_id, f"Symbol {symbol} not found in any data source")
            return

        completed.append(PIPELINE_STEPS[0])

        # ── Step 2: Full evaluation pipeline ─────────────────────
        await _update_progress(job_id, 2, completed)
        if await _is_cancelled(job_id):
            return

        from pipeline.evaluator import evaluate_company
        eval_result = await evaluate_company(symbol, force=True)

        if not eval_result or eval_result.get("status") == "degraded":
            await _mark_failed(
                job_id,
                f"Evaluation failed for {symbol}: {eval_result.get('errors') if eval_result else 'no data'}",
            )
            return

        completed.append(PIPELINE_STEPS[1])

        # ── Step 3: Smart money (extract from raw_financials snapshot) ─
        await _update_progress(job_id, 3, completed)
        if await _is_cancelled(job_id):
            return

        smart_money = await _get_smart_money_from_eval(symbol)
        completed.append(PIPELINE_STEPS[2])

        # ── Steps 4-6: DCF, EVA, Comps (parallel) ───────────────
        await _update_progress(job_id, 4, completed)
        if await _is_cancelled(job_id):
            return

        dcf_result, eva_result, comps_result = await asyncio.gather(
            _safe(analyze_dcf_safe, symbol),
            _safe(analyze_eva_safe, symbol),
            _safe(analyze_comps_safe, symbol),
        )

        completed.append(PIPELINE_STEPS[3])  # DCF
        await _update_progress(job_id, 5, completed)
        completed.append(PIPELINE_STEPS[4])  # EVA
        await _update_progress(job_id, 6, completed)
        completed.append(PIPELINE_STEPS[5])  # Comps

        # ── Step 7: Earnings Power Value ─────────────────────────
        await _update_progress(job_id, 7, completed)

        from analysis.epv_model import compute_epv
        from metrics.helpers import get_statements as _get_stmts

        # Extract WACC from prior valuation results — DCF first, EVA fallback
        wacc_for_epv = None
        if dcf_result and dcf_result.get("ok"):
            wacc_for_epv = (dcf_result.get("inputs") or {}).get("wacc")
        if wacc_for_epv is None and eva_result and eva_result.get("ok"):
            wacc_for_epv = (eva_result.get("wacc") or {}).get("wacc")

        # Get annual statements from persisted raw financials
        _db_eval_epv = await _fetch_db_evaluation(symbol)
        _raw_fin_epv = _parse_json((_db_eval_epv or {}).get("raw_financials"))
        _cd_epv = _raw_fin_epv.get("company_data", _raw_fin_epv)
        annual_for_epv = _get_stmts(_cd_epv, "annual")

        # Get diluted shares from the most recent annual record
        diluted_shares_epv = None
        if annual_for_epv:
            diluted_shares_epv = annual_for_epv[0].get("diluted_avg_shares")

        # Derive current price — prefer profile snapshot, fallback to market_cap/shares
        price_for_epv = profile.get("price")
        if not price_for_epv and profile.get("market_cap") and diluted_shares_epv:
            price_for_epv = profile["market_cap"] / diluted_shares_epv

        epv_result = compute_epv(
            annual=annual_for_epv,
            wacc=wacc_for_epv,
            market_cap=profile.get("market_cap"),
            current_price=price_for_epv,
            diluted_shares=diluted_shares_epv,
        )
        completed.append(PIPELINE_STEPS[6])  # EPV

        # ── Step 8: Entry point analysis ─────────────────────────
        await _update_progress(job_id, 8, completed)
        if await _is_cancelled(job_id):
            return

        entry_result = await _safe(analyze_entry_safe, symbol)
        completed.append(PIPELINE_STEPS[7])

        # ── Step 9: Price targets ────────────────────────────────
        await _update_progress(job_id, 9, completed)
        if await _is_cancelled(job_id):
            return

        price_targets = await _safe(fetch_price_targets_safe, symbol)
        completed.append(PIPELINE_STEPS[8])

        # ── Step 10: Transcript analysis ─────────────────────────
        await _update_progress(job_id, 10, completed)
        if await _is_cancelled(job_id):
            return

        transcript = await _safe(analyze_transcript_safe, symbol)
        completed.append(PIPELINE_STEPS[9])

        # ── Step 11: LLM thesis (already generated inside evaluate_company) ──
        await _update_progress(job_id, 11, completed)
        completed.append(PIPELINE_STEPS[10])

        # ── Step 12: Business profile ────────────────────────────
        await _update_progress(job_id, 12, completed)
        if await _is_cancelled(job_id):
            return

        from analysis.business_profile import generate_business_profile
        business_profile = await generate_business_profile(
            symbol=symbol,
            profile=profile,
            evaluation=eval_result,
            comps=comps_result,
        )
        completed.append(PIPELINE_STEPS[11])

        # ── Step 13: Piotroski F-Score ───────────────────────────
        await _update_progress(job_id, 13, completed)

        from analysis.piotroski import compute_piotroski_score

        # Reuse annual statements from EPV step (already fetched)
        piotroski_result = compute_piotroski_score(annual_for_epv)
        completed.append(PIPELINE_STEPS[12])

        # ── Step 14: Persist results ─────────────────────────────
        await _update_progress(job_id, 14, completed)

        # Ensure symbol is in universe
        was_in_universe, tier = await _ensure_in_universe(symbol, profile, eval_result)

        # Fetch persisted evaluation from DB for full detail
        db_eval = await _fetch_db_evaluation(symbol)

        duration = round(time.time() - t0, 1)

        payload = _build_result_payload(
            job_id=job_id,
            symbol=symbol,
            profile=profile,
            eval_result=eval_result,
            db_eval=db_eval,
            smart_money=smart_money,
            dcf=dcf_result,
            eva=eva_result,
            comps=comps_result,
            epv=epv_result,
            entry=entry_result,
            price_targets=price_targets,
            transcript=transcript,
            business_profile=business_profile,
            piotroski=piotroski_result,
            was_in_universe=was_in_universe,
            tier=tier,
            duration=duration,
        )

        completed.append(PIPELINE_STEPS[13])
        await _mark_complete(job_id, payload)
        _log.info(
            "On-demand analysis complete: %s (job=%s) in %.1fs",
            symbol, job_id, duration,
        )

    except Exception as exc:
        _log.exception("On-demand analysis failed: %s (job=%s): %s", symbol, job_id, exc)
        await _mark_failed(job_id, str(exc))


# ═══════════════════════════════════════════════════════════════════════
# SAFE WRAPPERS  (one failure must not kill the pipeline)
# ═══════════════════════════════════════════════════════════════════════

async def _safe(fn, *args):
    """Run *fn* and return None on any exception."""
    try:
        return await fn(*args)
    except Exception as exc:
        _log.warning("On-demand sub-step failed (%s): %s", fn.__name__, exc)
        return None


async def analyze_dcf_safe(symbol: str) -> dict | None:
    from analysis.dcf_model import analyze_dcf
    result = await analyze_dcf(symbol, skip_llm=True)
    if result and result.get("ok"):
        # Persist to DCF table (same as the action-button route)
        await _save_dcf(symbol, result)
        return result
    return None


async def analyze_eva_safe(symbol: str) -> dict | None:
    from analysis.eva_model import analyze_eva
    result = await analyze_eva(symbol, skip_llm=True)
    if result and result.get("ok"):
        await _save_eva(symbol, result)
        return result
    return None


async def analyze_comps_safe(symbol: str) -> dict | None:
    from analysis.comps_model import analyze_comps
    result = await analyze_comps(symbol, skip_llm=True)
    if result and result.get("ok"):
        await _save_comps(symbol, result)
        return result
    return None


async def analyze_entry_safe(symbol: str) -> dict | None:
    from analysis.entry_point import analyze_entry_point
    result = await analyze_entry_point(symbol, skip_llm=True)
    if result and result.get("ok"):
        await _save_entry_point(symbol, result)
        return result
    return None


async def fetch_price_targets_safe(symbol: str) -> dict | None:
    from data.finnhub_client import FinnhubClient
    from config import get_settings

    settings = get_settings()
    if not settings.finnhub_api_key:
        return None
    client = FinnhubClient(settings.finnhub_api_key, settings.finnhub_rate_limit)
    return await client.get_price_target(symbol)


async def analyze_transcript_safe(symbol: str) -> dict | None:
    from pipeline.evaluator import _get_fmp_client
    from analysis.transcript_analyzer import analyze_transcript

    fmp = _get_fmp_client()
    if not fmp:
        return None
    return await analyze_transcript(symbol, fmp)


# ═══════════════════════════════════════════════════════════════════════
# DB PERSISTENCE HELPERS  (reuse existing route save logic)
# ═══════════════════════════════════════════════════════════════════════

async def _save_dcf(symbol: str, result: dict):
    from api.routes_dcf import _save_dcf as save
    await save(symbol, result)


async def _save_eva(symbol: str, result: dict):
    from api.routes_eva import _save_eva as save
    await save(symbol, result)


async def _save_comps(symbol: str, result: dict):
    from api.routes_comps import _save_comps as save
    await save(symbol, result)


async def _save_entry_point(symbol: str, result: dict):
    from api.routes_entry_point import _save_entry_point as save
    await save(symbol, result)


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCH HELPERS
# ═══════════════════════════════════════════════════════════════════════

async def _fetch_profile(symbol: str) -> dict | None:
    """Quick profile fetch from Polygon + Finnhub for validation."""
    from data.polygon_client import PolygonClient
    from data.finnhub_client import FinnhubClient
    from config import get_settings

    settings = get_settings()
    profile: dict = {"symbol": symbol}

    # Polygon details
    if settings.polygon_api_key:
        try:
            poly = PolygonClient(settings.polygon_api_key, settings.polygon_rate_limit)
            details = await poly.get_company_details(symbol)
            if details and not details.get("error"):
                profile.update({
                    "name": details.get("company_name"),
                    "sector": details.get("sector"),
                    "industry": details.get("sector"),  # Polygon only has SIC description
                    "exchange": details.get("primary_exchange"),
                    "market_cap": details.get("market_cap"),
                    "description": details.get("description"),
                    "website": details.get("homepage"),
                    "employees": details.get("employees"),
                    "ipo_date": details.get("list_date"),
                })
        except Exception as exc:
            _log.warning("Polygon profile fetch failed for %s: %s", symbol, exc)

    # Finnhub enrichment
    fh_sector = None
    if settings.finnhub_api_key:
        try:
            fh = FinnhubClient(settings.finnhub_api_key, settings.finnhub_rate_limit)
            fh_profile = await fh.get_company_profile(symbol)
            if fh_profile and not fh_profile.get("error"):
                if not profile.get("name"):
                    profile["name"] = fh_profile.get("name")
                fh_sector = fh_profile.get("sector")  # finnhubIndustry
                profile.setdefault("ipo_date", fh_profile.get("ipo"))
                profile.setdefault("currency", fh_profile.get("currency", "USD"))
                profile.setdefault("country", fh_profile.get("country"))
        except Exception as exc:
            _log.warning("Finnhub profile fetch failed for %s: %s", symbol, exc)

    # FMP profile — clean sector/industry labels (priority over Polygon SIC)
    fmp_sector, fmp_industry = None, None
    if settings.fmp_api_key:
        try:
            from data.fmp_client import FMPClient
            fmp = FMPClient(settings.fmp_api_key)
            fmp_profile = await fmp.get_company_profile(symbol)
            if fmp_profile and not fmp_profile.get("error"):
                fmp_sector = fmp_profile.get("sector")
                fmp_industry = fmp_profile.get("industry")
                if not profile.get("name"):
                    profile["name"] = fmp_profile.get("company_name")
                if not profile.get("employees"):
                    emp = fmp_profile.get("employees")
                    if emp:
                        try:
                            profile["employees"] = int(emp)
                        except (ValueError, TypeError):
                            pass
        except Exception as exc:
            _log.warning("FMP profile fetch failed for %s: %s", symbol, exc)

    # Sector/Industry: FMP (clean labels) → Finnhub → Polygon SIC
    pg_sic = profile.get("sector")  # currently set from Polygon SIC description
    profile["sector"] = fmp_sector or fh_sector or pg_sic or None
    profile["industry"] = fmp_industry or fh_sector or pg_sic or None

    # Current price from Polygon snapshot
    if settings.polygon_api_key:
        try:
            poly = PolygonClient(settings.polygon_api_key, settings.polygon_rate_limit)
            snap = await poly.get_snapshot(symbol)
            if snap:
                profile["price"] = snap.get("last_price")
                profile["shares_outstanding"] = None  # not in snapshot
        except Exception:
            pass

    return profile if profile.get("name") else None


async def _get_smart_money_from_eval(symbol: str) -> dict | None:
    """Extract smart money data from the persisted evaluation."""
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation.raw_financials).where(
                CompanyEvaluation.symbol == symbol
            )
        )
        raw = result.scalar_one_or_none()
        if not raw:
            return None
        if isinstance(raw, str):
            raw = json.loads(raw)
        company_data = raw.get("company_data", raw)
        return company_data.get("smart_money")


async def _fetch_db_evaluation(symbol: str) -> dict | None:
    """Return the persisted CompanyEvaluation as a dict."""
    async with get_session() as session:
        result = await session.execute(
            select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
        )
        row = result.scalar_one_or_none()
        if not row:
            return None
        return {
            "composite_score": row.composite_score,
            "pillar_scores": {
                "business_quality": row.pillar_1_business_quality,
                "operational_health": row.pillar_2_operational_health,
                "capital_allocation": row.pillar_3_capital_allocation,
                "growth_quality": row.pillar_4_growth_quality,
                "valuation": row.pillar_5_valuation,
            },
            "pillar_breakdowns": {
                "business_quality": row.pillar_1_detail,
                "operational_health": row.pillar_2_detail,
                "capital_allocation": row.pillar_3_detail,
                "growth_quality": row.pillar_4_detail,
                "valuation": row.pillar_5_detail,
            },
            "breakout_score": row.breakout_score,
            "breakout_components": _parse_json(row.breakout_components),
            "llm_summary": row.llm_summary,
            "llm_recommendation": row.llm_recommendation,
            "llm_conviction": row.llm_conviction,
            "llm_thesis": row.llm_thesis,
            "llm_risks": row.llm_risks,
            "llm_catalysts": row.llm_catalysts,
            "evaluated_at": row.evaluated_at.isoformat() + "Z" if row.evaluated_at else None,
            "data_freshness": row.data_freshness,
            "rank": row.rank,
            "market_cap": row.market_cap,
            "raw_financials": row.raw_financials,
            "errors": row.errors,
        }


# ═══════════════════════════════════════════════════════════════════════
# UNIVERSE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════

async def _ensure_in_universe(
    symbol: str, profile: dict, eval_result: dict,
) -> tuple[bool, str]:
    """Add symbol to universe if missing; update last_screened_at if present.

    Returns (was_already_in_universe, tier_assigned).
    """
    now = datetime.now(timezone.utc)

    async with get_session() as session:
        result = await session.execute(
            select(UniverseSymbol).where(UniverseSymbol.symbol == symbol)
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.last_screened_at = now
            await session.commit()
            return True, existing.source or "unknown"

        # Insert new
        market_cap = (
            eval_result.get("market_cap")
            or profile.get("market_cap")
        )
        row = UniverseSymbol(
            symbol=symbol,
            company_name=profile.get("name"),
            source="on_demand",
            market_cap=market_cap,
            sector=profile.get("sector"),
            industry=profile.get("industry"),
            exchange=profile.get("exchange"),
            added_at=now,
            last_screened_at=now,
            active=True,
            priority=0,
        )
        session.add(row)
        await session.commit()
        return False, "on_demand"


# ═══════════════════════════════════════════════════════════════════════
# RESULT PAYLOAD BUILDER
# ═══════════════════════════════════════════════════════════════════════

def _build_result_payload(
    *,
    job_id: str,
    symbol: str,
    profile: dict,
    eval_result: dict,
    db_eval: dict | None,
    smart_money: dict | None,
    dcf: dict | None,
    eva: dict | None,
    comps: dict | None,
    epv: dict | None,
    entry: dict | None,
    price_targets: dict | None,
    transcript: dict | None,
    business_profile: dict | None,
    piotroski: dict | None,
    was_in_universe: bool,
    tier: str,
    duration: float,
) -> dict:
    """Assemble the payload returned by GET /jobs/{id}/result."""
    now = datetime.now(timezone.utc).isoformat() + "Z"

    db = db_eval or {}
    pillar_scores = db.get("pillar_scores") or eval_result.get("pillar_scores") or {}
    pillar_breakdowns = db.get("pillar_breakdowns") or {}
    raw_financials = db.get("raw_financials")

    # LLM fields — prefer DB, fall back to inline eval_result
    llm_analysis = eval_result.get("llm_analysis") or {}

    return {
        "job_id": job_id,
        "symbol": symbol,
        "completed_at": now,
        "duration_seconds": duration,

        "company": {
            "symbol": symbol,
            "name": profile.get("name"),
            "sector": profile.get("sector") or db.get("sector"),
            "industry": profile.get("industry"),
            "exchange": profile.get("exchange"),
            "market_cap": profile.get("market_cap") or db.get("market_cap"),
            "shares_outstanding": profile.get("shares_outstanding"),
            "price": profile.get("price"),
            "currency": profile.get("currency", "USD"),
            "description": profile.get("description"),
            "ceo": profile.get("ceo"),
            "employees": profile.get("employees"),
            "website": profile.get("website"),
            "ipo_date": profile.get("ipo_date"),
        },

        "evaluation": {
            "composite_score": eval_result.get("composite_score"),
            "completeness_pct": eval_result.get("overall_completeness_pct"),
            "evaluated_at": db.get("evaluated_at") or now,
            "is_stale": False,
            "rank": db.get("rank"),
            "pillar_scores": pillar_scores,
            "pillar_breakdowns": pillar_breakdowns,
        },

        "breakout": {
            "score": eval_result.get("breakout_score"),
            "filtered_out": eval_result.get("breakout_filtered_out", False),
            "filter_reason": eval_result.get("breakout_filter_reason"),
            "components": db.get("breakout_components") or {},
        },

        "llm_recommendation": {
            "rating": db.get("llm_recommendation") or llm_analysis.get("recommendation"),
            "conviction": db.get("llm_conviction") or llm_analysis.get("conviction"),
            "summary": db.get("llm_summary") or llm_analysis.get("summary"),
            "thesis": db.get("llm_thesis") or llm_analysis.get("thesis"),
            "risks": db.get("llm_risks") or llm_analysis.get("risks") or [],
            "catalysts": db.get("llm_catalysts") or llm_analysis.get("catalysts") or [],
        },

        "smart_money": smart_money,
        "dcf": dcf,
        "eva": eva,
        "comps": comps,
        "epv": epv,
        "entry_analysis": entry,
        "price_targets": price_targets,
        "transcript": transcript,
        "business_profile": business_profile,
        "piotroski_f_score": piotroski,
        "raw_financials": raw_financials,

        "data_sources": {
            "fundamentals": "Polygon",
            "fundamentals_fallback": "FMP",
            "cross_validated_with": "FMP",
            "insider_data": "FMP",
            "macro": "FRED",
            "news": "Finnhub",
            "llm": "LM Studio (local)",
        },

        "metadata": {
            "was_in_universe": was_in_universe,
            "tier_assigned": tier,
            "data_quality": eval_result.get("data_quality"),
            "data_quality_flags": eval_result.get("data_quality_flags") or [],
            "errors": db.get("errors") or {},
        },
    }


# ═══════════════════════════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════════════════════════

async def _cleanup_old_jobs():
    """Delete jobs older than 24 hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat() + "Z"
    try:
        async with get_session() as session:
            result = await session.execute(
                delete(OnDemandJob).where(OnDemandJob.created_at < cutoff)
            )
            if result.rowcount:
                _log.info("Cleaned up %d old on-demand jobs", result.rowcount)
            await session.commit()
    except Exception as exc:
        _log.debug("Job cleanup error (non-critical): %s", exc)


# ═══════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════

def _parse_json(value) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}

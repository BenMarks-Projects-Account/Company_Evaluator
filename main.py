"""Company Evaluator Service — FastAPI entry point."""

import logging
import logging.handlers
import os
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from db.database import init_db
from api.routes_companies import router as companies_router
from api.routes_pipeline import router as pipeline_router
from api.routes_admin import router as admin_router
from api.routes_status import router as status_router
from api.routes_entry_point import router as entry_point_router
from api.routes_comps import router as comps_router
from api.routes_dcf import router as dcf_router
from api.routes_eva import router as eva_router
from api.routes_analyses import router as analyses_router
from api.routes_quote import router as quote_router

# ── Logging setup (console + file) ──────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "company_evaluator.log"

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

_console = logging.StreamHandler()
_console.setFormatter(_fmt)

_file = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8",
)
_file.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_console, _file])
_log = logging.getLogger(__name__)


async def _auto_resume_crawler(settings):
    """Resume crawler ONLY if it was interrupted (status='running')."""
    import asyncio
    from pipeline.crawler import get_crawler, _load_state

    state = _load_state()
    if not state:
        _log.info("No saved crawler state. Use the API/UI to start the crawler.")
        return

    prev_status = state.get("status")
    last_idx = state.get("last_completed_index", -1)
    last_sym = state.get("last_completed_symbol")
    cycle_number = state.get("cycle_number", 1)
    resume_idx = last_idx + 1

    if prev_status == "stopped":
        _log.info("Crawler was stopped by user (last: %s). Use the API/UI to restart.", last_sym)
        return

    if prev_status == "paused":
        _log.info("Crawler was paused (last: %s). Use the API/UI to resume.", last_sym)
        return

    if prev_status != "running":
        _log.info("Crawler state is '%s'. Use the API/UI to start.", prev_status)
        return

    # status == "running" — was interrupted, auto-resume
    # Crawler will fetch its own ordered symbol list from DB
    _log.info(
        "RESUMING crawler: cycle %d, index %d (last completed: %s)",
        cycle_number, resume_idx, last_sym,
    )
    crawler = get_crawler()
    asyncio.create_task(crawler.run(start_index=resume_idx, cycle_number=cycle_number))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    settings = get_settings()
    _log.info("Starting Company Evaluator Service on port %d", settings.port)
    _log.info("Database URL: %s", settings.database_url)
    
    # Initialize database (creates new tables, enables WAL — never drops existing)
    await init_db(settings.database_url)
    
    # Seed universe table from hardcoded list (first run only)
    from data.universe import seed_universe_if_empty
    await seed_universe_if_empty()
    
    # Log row counts for verification
    from db.database import get_session, CompanyEvaluation, EvaluationHistory, UniverseSymbol
    from sqlalchemy import select, func
    async with get_session() as session:
        evals = (await session.execute(select(func.count()).select_from(CompanyEvaluation))).scalar()
        history = (await session.execute(select(func.count()).select_from(EvaluationHistory))).scalar()
        universe = (await session.execute(select(func.count()).select_from(UniverseSymbol))).scalar()
    _log.info("DB status: %d evaluations, %d history records, %d universe symbols", evals, history, universe)
    
    # Start the market-hours scheduler and let it enforce startup state.
    from pipeline.scheduler import start_scheduler
    scheduler = start_scheduler(settings)
    await scheduler.start()
    app.state.crawler_scheduler = scheduler
    
    yield
    
    # ── Graceful shutdown ────────────────────────────────────
    _log.info("Shutting down Company Evaluator Service...")

    scheduler = getattr(app.state, "crawler_scheduler", None)
    if scheduler is not None:
        await scheduler.stop()
    
    from pipeline.crawler import get_crawler
    crawler = get_crawler()
    if crawler._running:
        _log.info("Crawler is running — stopping and saving state for next startup")
        crawler.stop()
        # Give the run loop a moment to exit and save state
        import asyncio
        await asyncio.sleep(2)
    
    from db.database import close_db
    await close_db()
    _log.info("Shutdown complete")

app = FastAPI(
    title="BenTrade Company Evaluator",
    description="Institutional-grade company evaluation service",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow BenTrade frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(companies_router, prefix="/api", tags=["companies"])
app.include_router(pipeline_router, prefix="/api", tags=["pipeline"])
app.include_router(admin_router, prefix="/api", tags=["admin"])
app.include_router(status_router, prefix="/api", tags=["status"])
app.include_router(entry_point_router, prefix="/api", tags=["entry-point"])
app.include_router(comps_router, prefix="/api", tags=["valuation"])
app.include_router(dcf_router, prefix="/api", tags=["valuation"])
app.include_router(eva_router, prefix="/api", tags=["valuation"])
app.include_router(analyses_router, prefix="/api", tags=["analyses"])
app.include_router(quote_router, prefix="/api", tags=["quote"])

@app.get("/health")
async def health():
    return {"status": "ok", "service": "company-evaluator"}

if __name__ == "__main__":
    import uvicorn
    import traceback
    from datetime import datetime
    settings = get_settings()
    try:
        uvicorn.run("main:app", host=settings.host, port=settings.port, reload=settings.debug)
    except Exception as e:
        crash_file = LOG_DIR / f"crash_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        crash_file.write_text(traceback.format_exc(), encoding="utf-8")
        _log.critical("CRASH: %s — see %s", e, crash_file)
        raise

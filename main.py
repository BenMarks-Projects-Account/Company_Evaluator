"""Company Evaluator Service — FastAPI entry point."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from db.database import init_db
from api.routes_companies import router as companies_router
from api.routes_pipeline import router as pipeline_router
from api.routes_admin import router as admin_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    settings = get_settings()
    _log.info("Starting Company Evaluator Service on port %d", settings.port)
    
    # Initialize database
    await init_db(settings.database_url)
    _log.info("Database initialized")
    
    # Start crawler scheduler if enabled
    if settings.crawler_enabled:
        from pipeline.scheduler import start_scheduler
        start_scheduler(settings)
        _log.info("Crawler scheduler started — schedule: %s", settings.crawler_schedule)
    
    yield
    
    _log.info("Shutting down Company Evaluator Service")

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

@app.get("/health")
async def health():
    return {"status": "ok", "service": "company-evaluator"}

if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=settings.debug)

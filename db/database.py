"""SQLite database setup with SQLAlchemy async."""

import logging
import os
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Float, Integer, DateTime, Text, JSON, Boolean, event, text
from datetime import datetime, timezone
from config import sqlite_url_to_path

_log = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

class CompanyEvaluation(Base):
    """Stores the latest evaluation for each company."""
    __tablename__ = "company_evaluations"
    
    symbol = Column(String(10), primary_key=True)
    company_name = Column(String(200))
    sector = Column(String(100))
    industry = Column(String(200))
    market_cap = Column(Float)
    
    # Pillar scores (0-100)
    pillar_1_business_quality = Column(Float)
    pillar_2_operational_health = Column(Float)
    pillar_3_capital_allocation = Column(Float)
    pillar_4_growth_quality = Column(Float)
    pillar_5_valuation = Column(Float)
    composite_score = Column(Float)
    
    # Composite rank
    rank = Column(Integer)
    
    # Metric details (JSON)
    pillar_1_detail = Column(JSON)
    pillar_2_detail = Column(JSON)
    pillar_3_detail = Column(JSON)
    pillar_4_detail = Column(JSON)
    pillar_5_detail = Column(JSON)
    
    # LLM analysis
    llm_summary = Column(Text)
    llm_recommendation = Column(String(20))  # STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
    llm_conviction = Column(Integer)
    llm_thesis = Column(Text)
    llm_risks = Column(JSON)
    llm_catalysts = Column(JSON)
    
    # Breakout Potential Score (parallel to composite)
    breakout_score = Column(Float)
    breakout_components = Column(JSON)

    # Raw data snapshot (JSON — for auditing)
    raw_financials = Column(JSON)
    
    # Metadata
    evaluated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    data_freshness = Column(String(20))  # "fresh", "stale", "partial"
    evaluation_version = Column(String(20), default="0.1.0")
    errors = Column(JSON)

class EvaluationHistory(Base):
    """Historical evaluations for tracking changes over time."""
    __tablename__ = "evaluation_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), index=True)
    composite_score = Column(Float)
    rank = Column(Integer)
    llm_recommendation = Column(String(20))
    evaluated_at = Column(DateTime)
    snapshot = Column(JSON)  # Compact snapshot of key metrics

class EntryPointAnalysis(Base):
    """Stores the latest entry point analysis for each company."""
    __tablename__ = "entry_point_analyses"

    symbol = Column(String(10), primary_key=True)
    analysis_data = Column(Text, nullable=False)        # full JSON response
    recommendation = Column(String(20))                  # ENTER_NOW, WAIT, AVOID
    conviction = Column(Integer)                         # 0-100
    composite_score = Column(Float)
    suggested_entry = Column(Float)
    suggested_stop = Column(Float)
    risk_reward = Column(String(20))
    analyzed_at = Column(String(50), nullable=False)     # ISO timestamp
    current_price_at_analysis = Column(Float)


class CompsAnalysis(Base):
    """Stores the latest comps analysis for each company."""
    __tablename__ = "comps_analyses"

    symbol = Column(String(10), primary_key=True)
    analysis_data = Column(Text, nullable=False)        # full JSON response
    verdict_status = Column(String(30))                  # UNDERVALUED, FAIRLY_VALUED, etc.
    upside_pct = Column(Float)
    fair_value_composite = Column(Float)
    confidence = Column(String(10))
    peer_count = Column(Integer)
    analyzed_at = Column(String(50), nullable=False)     # ISO timestamp
    current_price_at_analysis = Column(Float)


class DcfAnalysis(Base):
    """Stores the latest DCF analysis for each company."""
    __tablename__ = "dcf_analyses"

    symbol = Column(String(10), primary_key=True)
    analysis_data = Column(Text, nullable=False)        # full JSON response
    intrinsic_value = Column(Float)
    upside_pct = Column(Float)
    verdict = Column(String(30))
    wacc = Column(Float)
    confidence = Column(String(10))
    analyzed_at = Column(String(50), nullable=False)     # ISO timestamp
    current_price_at_analysis = Column(Float)


class EvaAnalysis(Base):
    """Stores the latest EVA/ROIC analysis for each company."""
    __tablename__ = "eva_analyses"

    symbol = Column(String(10), primary_key=True)
    analysis_data = Column(Text, nullable=False)        # full JSON response
    roic = Column(Float)
    wacc = Column(Float)
    value_spread = Column(Float)
    eva_annual = Column(Float)
    grade = Column(String(20))
    score = Column(Integer)
    confidence = Column(String(10))
    analyzed_at = Column(String(50), nullable=False)     # ISO timestamp
    current_price_at_analysis = Column(Float)


class UniverseSymbol(Base):
    """Dynamic universe — symbols the crawler evaluates."""
    __tablename__ = "universe_symbols"
    
    symbol = Column(String(10), primary_key=True)
    company_name = Column(String(200))
    source = Column(String(50), default="sp500_top100")  # large_cap, mid_cap, small_cap, penny_stock, sp500_top100, ipo_discovery, manual
    market_cap = Column(Float)
    market_cap_tier = Column(String(20))  # mega, large, mid, small, micro, penny
    sector = Column(String(100))
    industry = Column(String(200))
    exchange = Column(String(20))  # NYSE, NASDAQ
    last_price = Column(Float)
    avg_volume = Column(Float)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_screened_at = Column(DateTime)
    active = Column(Boolean, default=True)
    delisted_at = Column(DateTime)
    priority = Column(Integer, default=0)  # higher = evaluate sooner
    notes = Column(Text)

# Engine and session
_engine = None
_session_factory = None

def _set_sqlite_pragma(dbapi_connection, connection_record):
    """Set SQLite pragmas for safer concurrent access on the NAS share."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA busy_timeout=10000;")
    cursor.execute("PRAGMA cache_size=-64000;")
    cursor.close()

async def init_db(database_url: str):
    global _engine, _session_factory

    if database_url.startswith("sqlite:///"):
        db_path = sqlite_url_to_path(database_url)
        async_url = URL.create("sqlite+aiosqlite", database=db_path)
    else:
        db_path = database_url
        async_url = database_url

    if database_url.startswith("sqlite:///"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    _engine = create_async_engine(async_url, echo=False, connect_args={"timeout": 10})
    _session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    
    # Enable WAL mode on every new connection
    event.listen(_engine.sync_engine, "connect", _set_sqlite_pragma)
    
    # Create tables (only adds new ones — never drops existing)
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Migrate: add missing columns to universe_symbols (preserves existing data)
    await _migrate_universe_symbols()
    
    # Migrate: add breakout columns to company_evaluations
    await _migrate_company_evaluations()
    
    # Log resolved DB path
    _log.info("Database initialized: %s (WAL mode enabled)", db_path)


async def _migrate_universe_symbols():
    """Add columns to universe_symbols that exist in the model but not in the DB.

    Uses ALTER TABLE ADD COLUMN — safe to run repeatedly (skips if column exists).
    """
    _new_columns = {
        "market_cap": "REAL",
        "market_cap_tier": "TEXT",
        "sector": "TEXT",
        "industry": "TEXT",
        "exchange": "TEXT",
        "last_price": "REAL",
        "avg_volume": "REAL",
        "last_screened_at": "TEXT",
        "delisted_at": "TEXT",
        "notes": "TEXT",
    }
    async with _engine.begin() as conn:
        # Get existing column names
        result = await conn.execute(text("PRAGMA table_info(universe_symbols)"))
        existing = {row[1] for row in result}
        
        for col_name, col_type in _new_columns.items():
            if col_name not in existing:
                await conn.execute(text(
                    f"ALTER TABLE universe_symbols ADD COLUMN {col_name} {col_type}"
                ))
                _log.info("Migrated universe_symbols: added column %s (%s)", col_name, col_type)


async def _migrate_company_evaluations():
    """Add breakout columns to company_evaluations if they don't exist yet."""
    _new_columns = {
        "breakout_score": "REAL",
        "breakout_components": "TEXT",
    }
    async with _engine.begin() as conn:
        result = await conn.execute(text("PRAGMA table_info(company_evaluations)"))
        existing = {row[1] for row in result}

        for col_name, col_type in _new_columns.items():
            if col_name not in existing:
                await conn.execute(text(
                    f"ALTER TABLE company_evaluations ADD COLUMN {col_name} {col_type}"
                ))
                _log.info("Migrated company_evaluations: added column %s (%s)", col_name, col_type)


async def close_db():
    """Dispose engine cleanly — call on shutdown."""
    global _engine
    if _engine:
        await _engine.dispose()
        _log.info("Database engine disposed")

from contextlib import asynccontextmanager

@asynccontextmanager
async def get_session():
    """Yield an AsyncSession that auto-closes on exit."""
    session = _session_factory()
    try:
        yield session
    finally:
        await session.close()

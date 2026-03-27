"""SQLite database setup with SQLAlchemy async."""

import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Float, Integer, DateTime, Text, JSON, Boolean
from datetime import datetime, timezone

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

# Engine and session
_engine = None
_session_factory = None

async def init_db(database_url: str):
    global _engine, _session_factory
    
    # SQLite async requires aiosqlite and sqlite+aiosqlite:// prefix
    if database_url.startswith("sqlite:///"):
        async_url = database_url.replace("sqlite:///", "sqlite+aiosqlite:///")
    else:
        async_url = database_url
    
    _engine = create_async_engine(async_url, echo=False)
    _session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    
    # Create tables
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

from contextlib import asynccontextmanager

@asynccontextmanager
async def get_session():
    """Yield an AsyncSession that auto-closes on exit."""
    session = _session_factory()
    try:
        yield session
    finally:
        await session.close()

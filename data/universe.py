"""Company universe definitions — lists of symbols to evaluate."""

import logging

_log = logging.getLogger(__name__)

# Start with a manageable set — expand later
SP500_TOP100 = [
    # Technology
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "AVGO", "TSLA", "AMD", "CRM",
    "ADBE", "INTC", "CSCO", "ORCL", "QCOM", "TXN", "NOW", "IBM", "AMAT", "MU",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY",
    # Financials
    "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "BLK", "SCHW", "AXP",
    # Consumer
    "WMT", "PG", "KO", "PEP", "COST", "MCD", "NKE", "SBUX", "TGT", "HD",
    # Industrials
    "CAT", "DE", "UNP", "HON", "RTX", "BA", "LMT", "GE", "MMM", "UPS",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "HAL",
    # Communication
    "GOOG", "DIS", "CMCSA", "NFLX", "T", "VZ", "TMUS",
    # Materials
    "LIN", "APD", "SHW", "ECL", "NEM",
    # Real Estate
    "AMT", "PLD", "CCI", "EQIX", "SPG",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP",
]

AVAILABLE_UNIVERSES = {
    "sp500_top100": SP500_TOP100,
    "test": ["AAPL", "MSFT", "GOOGL", "AMZN", "JNJ"],  # Quick test set
}

def get_universe(name: str) -> list[str]:
    return AVAILABLE_UNIVERSES.get(name, AVAILABLE_UNIVERSES["test"])


# ── DB-backed universe ──────────────────────────────────────

async def seed_universe_if_empty():
    """Seed the universe_symbols table from SP500_TOP100 (first run only)."""
    from db.database import get_session, UniverseSymbol
    from sqlalchemy import select, func
    
    async with get_session() as session:
        count = (await session.execute(
            select(func.count()).select_from(UniverseSymbol)
        )).scalar()
        
        if count > 0:
            _log.info("Universe table already populated (%d symbols)", count)
            return count
        
        for symbol in SP500_TOP100:
            session.add(UniverseSymbol(symbol=symbol, source="sp500_top100"))
        
        await session.commit()
        _log.info("Seeded universe_symbols with %d symbols from SP500_TOP100", len(SP500_TOP100))
        return len(SP500_TOP100)


async def get_active_symbols() -> list[str]:
    """Get active symbols from the DB universe table (priority DESC, source ASC, symbol ASC)."""
    from db.database import get_session, UniverseSymbol
    from sqlalchemy import select, desc

    async with get_session() as session:
        result = await session.execute(
            select(UniverseSymbol.symbol)
            .where(UniverseSymbol.active == True)
            .order_by(desc(UniverseSymbol.priority), UniverseSymbol.source, UniverseSymbol.symbol)
        )
        return [row[0] for row in result]

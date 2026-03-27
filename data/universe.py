"""Company universe definitions — lists of symbols to evaluate."""

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

"""
Chart data service — builds price + indicator series for the UI.

Fetches daily bars from Polygon, computes SMA 50 and SMA 200,
identifies key levels, and returns a windowed response ready for
the frontend to render.
"""

import logging
import time
from datetime import datetime, timedelta

from config import get_settings
from data.polygon_client import PolygonClient
from data.data_source_router import get_router

_log = logging.getLogger(__name__)

# Timeframe configuration
TIMEFRAMES = {
    "6M": 180,
    "1Y": 365,
    "3Y": 1095,
    "5Y": 1825,
}

# Extra history needed to ensure SMA 200 is computed for the full visible window
SMA_WARMUP_DAYS = 280  # 200 trading days + buffer for weekends/holidays

# In-memory cache: key = (symbol, timeframe), value = (expires_at, payload)
_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
_CACHE_TTL_SECONDS = 60


async def get_chart_data(symbol: str, timeframe: str = "1Y") -> dict:
    """
    Returns chart data payload for the given symbol and timeframe.

    Raises ValueError on invalid input.
    Raises RuntimeError on Polygon failures.
    """
    symbol = (symbol or "").upper().strip()
    if not symbol or not symbol.isalpha() or len(symbol) > 5:
        raise ValueError(f"Invalid symbol: {symbol}")

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            f"Invalid timeframe: {timeframe} (must be one of {list(TIMEFRAMES.keys())})"
        )

    # Check cache
    cache_key = (symbol, timeframe)
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and cached[0] > now:
        _log.debug(f"[charts] cache hit for {symbol} {timeframe}")
        return cached[1]

    # Fetch from Polygon
    visible_days = TIMEFRAMES[timeframe]
    fetch_days = visible_days + SMA_WARMUP_DAYS

    _log.info(f"[charts] fetching {symbol} {timeframe}: {fetch_days} calendar days")

    settings = get_settings()
    polygon = PolygonClient(
        api_key=settings.polygon_api_key,
        rate_limit=settings.polygon_rate_limit,
    )

    # FMP adapter for routed calls
    fmp_bars_fn = None
    if settings.fmp_enabled and settings.fmp_api_key:
        from datetime import date as _date, timedelta as _td
        from data.fmp_client import FMPClient
        _fmp = FMPClient(
            api_key=settings.fmp_api_key,
            base_url=settings.fmp_base_url,
            rate_limit_per_min=settings.fmp_rate_limit_per_min,
        )
        async def _fmp_bars(symbol_arg, days=365):
            from_date = (_date.today() - _td(days=days)).isoformat()
            return await _fmp.get_historical_price_eod(symbol_arg, from_date=from_date)
        fmp_bars_fn = _fmp_bars

    router = get_router()

    bars = await router.route(
        "chart_service.get_raw_bars",
        polygon.get_raw_bars,
        fmp_bars_fn,
        symbol, days=fetch_days,
    )
    if not bars:
        raise RuntimeError(f"No price data available for {symbol}")

    # bars is already sorted oldest-first with keys: date, open, high, low, close, volume
    if not bars:
        raise RuntimeError(f"Empty price series returned from Polygon for {symbol}")

    # Compute SMAs on the full (fetched) series
    sma_50_full = _compute_sma(bars, period=50)
    sma_200_full = _compute_sma(bars, period=200)

    # Find the visible window start date
    today = datetime.utcnow().date()
    visible_start = (today - timedelta(days=visible_days)).isoformat()

    visible_start_idx = 0
    for i, bar in enumerate(bars):
        if bar["date"] >= visible_start:
            visible_start_idx = i
            break

    # Build visible-window arrays
    prices_visible = []
    sma_50_visible = []
    sma_200_visible = []
    for i in range(visible_start_idx, len(bars)):
        prices_visible.append({
            "date": bars[i]["date"],
            "close": round(bars[i]["close"], 4),
        })
        sma_50_visible.append({
            "date": bars[i]["date"],
            "value": round(sma_50_full[i], 4) if sma_50_full[i] is not None else None,
        })
        sma_200_visible.append({
            "date": bars[i]["date"],
            "value": round(sma_200_full[i], 4) if sma_200_full[i] is not None else None,
        })

    # 52-week high/low from the last 252 bars of the FULL series
    last_252 = bars[-252:] if len(bars) >= 252 else bars
    highs = [b["high"] for b in last_252 if b.get("high") is not None]
    lows = [b["low"] for b in last_252 if b.get("low") is not None]
    week_52_high = max(highs) if highs else None
    week_52_low = min(lows) if lows else None

    # Support/resistance from last 60 bars
    support, resistance = _compute_support_resistance(bars[-60:])

    # Build notes
    notes = []
    if prices_visible:
        actual_days = (
            datetime.fromisoformat(prices_visible[-1]["date"])
            - datetime.fromisoformat(prices_visible[0]["date"])
        ).days
    else:
        actual_days = 0

    if actual_days < visible_days * 0.9:
        notes.append(
            f"Only {actual_days} days of history available (requested {visible_days})"
        )
    if sma_200_full[-1] is None:
        notes.append("SMA 200 not computed — insufficient history")

    payload = {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "currency": "USD",
        "prices": prices_visible,
        "sma_50": sma_50_visible,
        "sma_200": sma_200_visible,
        "levels": {
            "support": round(support, 2) if support else None,
            "resistance": round(resistance, 2) if resistance else None,
            "week_52_high": round(week_52_high, 2) if week_52_high else None,
            "week_52_low": round(week_52_low, 2) if week_52_low else None,
        },
        "metadata": {
            "earliest_date": prices_visible[0]["date"] if prices_visible else None,
            "latest_date": prices_visible[-1]["date"] if prices_visible else None,
            "total_bars": len(prices_visible),
            "data_source": "polygon",
            "actual_days_of_history": actual_days,
            "requested_days_of_history": visible_days,
            "notes": notes,
        },
    }

    # Cache it
    _CACHE[cache_key] = (now + _CACHE_TTL_SECONDS, payload)
    _cleanup_cache(now)

    return payload


def _compute_sma(series: list[dict], period: int) -> list[float | None]:
    """
    Simple moving average over the close prices.
    Returns a list aligned to `series` where the first (period-1)
    entries are None.
    """
    result: list[float | None] = []
    closes = [bar["close"] for bar in series]
    running_sum = 0.0

    for i, close in enumerate(closes):
        running_sum += close
        if i >= period:
            running_sum -= closes[i - period]

        if i >= period - 1:
            result.append(running_sum / period)
        else:
            result.append(None)

    return result


def _compute_support_resistance(
    recent_series: list[dict],
) -> tuple[float | None, float | None]:
    """
    Support = lowest low, resistance = highest high in the window.
    """
    if not recent_series:
        return None, None

    highs = [b["high"] for b in recent_series if b.get("high") is not None]
    lows = [b["low"] for b in recent_series if b.get("low") is not None]

    support = min(lows) if lows else None
    resistance = max(highs) if highs else None

    return support, resistance


def _cleanup_cache(now: float):
    """Drop expired cache entries to prevent unbounded growth."""
    expired = [k for k, (exp, _) in _CACHE.items() if exp < now]
    for k in expired:
        del _CACHE[k]

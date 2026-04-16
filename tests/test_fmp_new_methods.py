"""Unit tests for the 4 new FMP client methods added in Phase 1.

Tests use mocked HTTP responses to verify:
  1. get_historical_price_eod() returns bars in Polygon-compatible shape (oldest-first)
  2. get_quote() returns snapshot in Polygon-compatible shape
  3. get_technical_indicator() returns a single float value
  4. get_macd() computes MACD from two EMA calls and returns Polygon-compatible shape
  5. Token-bucket rate limiter works correctly
"""

import asyncio
import time
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from data.fmp_client import FMPClient, _TokenBucketRateLimiter


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def client():
    return FMPClient(api_key="test_key", rate_limit_per_min=6000, rate_limit_safety_pct=1.0)


# ── Mock FMP response data ───────────────────────────────────────────

MOCK_HISTORICAL_EOD = [
    # FMP returns newest-first
    {"date": "2025-04-15", "open": 210.0, "high": 215.0, "low": 208.0, "close": 212.5, "volume": 35000000,
     "change": 2.5, "changePercent": 1.19, "vwap": 211.8},
    {"date": "2025-04-14", "open": 208.0, "high": 211.0, "low": 206.0, "close": 210.0, "volume": 30000000,
     "change": -1.0, "changePercent": -0.47, "vwap": 209.1},
    {"date": "2025-04-11", "open": 211.0, "high": 213.0, "low": 207.0, "close": 211.0, "volume": 28000000,
     "change": 0.5, "changePercent": 0.24, "vwap": 210.5},
]

MOCK_QUOTE = [
    {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "price": 212.5,
        "changesPercentage": 1.19,
        "change": 2.5,
        "dayLow": 208.0,
        "dayHigh": 215.0,
        "yearHigh": 250.0,
        "yearLow": 160.0,
        "marketCap": 3200000000000,
        "priceAvg50": 210.0,
        "priceAvg200": 200.0,
        "exchange": "NASDAQ",
        "volume": 35000000,
        "avgVolume": 50000000,
        "open": 210.0,
        "previousClose": 210.0,
        "eps": 6.5,
        "pe": 32.69,
        "earningsAnnouncement": "2025-04-24",
        "sharesOutstanding": 15000000000,
        "timestamp": 1713200000,
    }
]

MOCK_RSI_RESPONSE = [
    {"date": "2025-04-15 00:00:00", "open": 210.0, "high": 215.0, "low": 208.0,
     "close": 212.5, "volume": 35000000, "rsi": 55.32},
    {"date": "2025-04-14 00:00:00", "open": 208.0, "high": 211.0, "low": 206.0,
     "close": 210.0, "volume": 30000000, "rsi": 48.91},
]

MOCK_SMA_RESPONSE = [
    {"date": "2025-04-15 00:00:00", "open": 210.0, "high": 215.0, "low": 208.0,
     "close": 212.5, "volume": 35000000, "sma": 205.45},
]

MOCK_EMA_FAST = [
    {"date": "2025-04-15 00:00:00", "open": 210.0, "high": 215.0, "low": 208.0,
     "close": 212.5, "volume": 35000000, "ema": 211.0},
    {"date": "2025-04-14 00:00:00", "open": 208.0, "high": 211.0, "low": 206.0,
     "close": 210.0, "volume": 30000000, "ema": 209.8},
    {"date": "2025-04-11 00:00:00", "open": 211.0, "high": 213.0, "low": 207.0,
     "close": 211.0, "volume": 28000000, "ema": 209.2},
    {"date": "2025-04-10 00:00:00", "open": 210.0, "high": 212.0, "low": 208.0,
     "close": 210.5, "volume": 27000000, "ema": 208.9},
    {"date": "2025-04-09 00:00:00", "open": 209.0, "high": 211.0, "low": 207.0,
     "close": 209.0, "volume": 26000000, "ema": 208.5},
    {"date": "2025-04-08 00:00:00", "open": 207.0, "high": 210.0, "low": 206.0,
     "close": 208.0, "volume": 25000000, "ema": 208.0},
    {"date": "2025-04-07 00:00:00", "open": 206.0, "high": 209.0, "low": 205.0,
     "close": 207.0, "volume": 24000000, "ema": 207.5},
    {"date": "2025-04-04 00:00:00", "open": 205.0, "high": 208.0, "low": 204.0,
     "close": 206.0, "volume": 23000000, "ema": 207.0},
    {"date": "2025-04-03 00:00:00", "open": 204.0, "high": 207.0, "low": 203.0,
     "close": 205.0, "volume": 22000000, "ema": 206.5},
    {"date": "2025-04-02 00:00:00", "open": 203.0, "high": 206.0, "low": 202.0,
     "close": 204.0, "volume": 21000000, "ema": 206.0},
    {"date": "2025-04-01 00:00:00", "open": 202.0, "high": 205.0, "low": 201.0,
     "close": 203.0, "volume": 20000000, "ema": 205.5},
    {"date": "2025-03-31 00:00:00", "open": 201.0, "high": 204.0, "low": 200.0,
     "close": 202.0, "volume": 19000000, "ema": 205.0},
    {"date": "2025-03-28 00:00:00", "open": 200.0, "high": 203.0, "low": 199.0,
     "close": 201.0, "volume": 18000000, "ema": 204.5},
    {"date": "2025-03-27 00:00:00", "open": 199.0, "high": 202.0, "low": 198.0,
     "close": 200.0, "volume": 17000000, "ema": 204.0},
]

MOCK_EMA_SLOW = [
    {"date": "2025-04-15 00:00:00", "ema": 207.0},
    {"date": "2025-04-14 00:00:00", "ema": 206.8},
    {"date": "2025-04-11 00:00:00", "ema": 206.5},
    {"date": "2025-04-10 00:00:00", "ema": 206.3},
    {"date": "2025-04-09 00:00:00", "ema": 206.1},
    {"date": "2025-04-08 00:00:00", "ema": 205.9},
    {"date": "2025-04-07 00:00:00", "ema": 205.7},
    {"date": "2025-04-04 00:00:00", "ema": 205.5},
    {"date": "2025-04-03 00:00:00", "ema": 205.3},
    {"date": "2025-04-02 00:00:00", "ema": 205.1},
    {"date": "2025-04-01 00:00:00", "ema": 204.9},
    {"date": "2025-03-31 00:00:00", "ema": 204.7},
    {"date": "2025-03-28 00:00:00", "ema": 204.5},
    {"date": "2025-03-27 00:00:00", "ema": 204.3},
]


# ── Tests: get_historical_price_eod ──────────────────────────────────

@pytest.mark.asyncio
async def test_historical_price_eod_returns_bars_oldest_first(client):
    """Bars must be sorted oldest-first to match Polygon get_raw_bars() shape."""
    client._request = AsyncMock(return_value=MOCK_HISTORICAL_EOD)

    bars = await client.get_historical_price_eod("AAPL", "2025-04-11", "2025-04-15")

    assert bars is not None
    assert len(bars) == 3
    # Oldest first
    assert bars[0]["date"] == "2025-04-11"
    assert bars[-1]["date"] == "2025-04-15"


@pytest.mark.asyncio
async def test_historical_price_eod_field_shape(client):
    """Each bar must have exactly the fields Polygon callers expect."""
    client._request = AsyncMock(return_value=MOCK_HISTORICAL_EOD)

    bars = await client.get_historical_price_eod("AAPL")
    bar = bars[0]

    expected_fields = {"date", "open", "high", "low", "close", "volume"}
    assert set(bar.keys()) == expected_fields


@pytest.mark.asyncio
async def test_historical_price_eod_values(client):
    """Verify actual values are passed through correctly."""
    client._request = AsyncMock(return_value=MOCK_HISTORICAL_EOD)

    bars = await client.get_historical_price_eod("AAPL")
    newest = bars[-1]  # last bar (most recent, since reversed to oldest-first)

    assert newest["close"] == 212.5
    assert newest["volume"] == 35000000
    assert newest["high"] == 215.0


@pytest.mark.asyncio
async def test_historical_price_eod_returns_none_on_empty(client):
    """Empty or None response → None."""
    client._request = AsyncMock(return_value=None)
    assert await client.get_historical_price_eod("AAPL") is None

    client._request = AsyncMock(return_value=[])
    assert await client.get_historical_price_eod("AAPL") is None


@pytest.mark.asyncio
async def test_historical_price_eod_passes_date_params(client):
    """from_date and to_date should be forwarded as 'from' and 'to' params."""
    client._request = AsyncMock(return_value=MOCK_HISTORICAL_EOD)

    await client.get_historical_price_eod("AAPL", "2025-01-01", "2025-04-15")

    client._request.assert_called_once_with(
        "/stable/historical-price-eod/full",
        params={"symbol": "AAPL", "from": "2025-01-01", "to": "2025-04-15"},
    )


# ── Tests: get_quote ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_quote_field_shape_matches_polygon_snapshot(client):
    """Quote must return all fields that Polygon get_snapshot() callers use."""
    client._request = AsyncMock(return_value=MOCK_QUOTE)

    quote = await client.get_quote("AAPL")

    assert quote is not None
    # Fields that Polygon callers depend on
    polygon_required = {
        "symbol", "last_price", "day_open", "day_high", "day_low",
        "day_close", "day_volume", "day_vwap", "prev_close",
        "change", "change_pct",
    }
    assert polygon_required.issubset(set(quote.keys()))


@pytest.mark.asyncio
async def test_quote_values(client):
    """Verify specific field translations."""
    client._request = AsyncMock(return_value=MOCK_QUOTE)

    quote = await client.get_quote("AAPL")

    assert quote["symbol"] == "AAPL"
    assert quote["last_price"] == 212.5
    assert quote["day_open"] == 210.0
    assert quote["day_high"] == 215.0
    assert quote["day_low"] == 208.0
    assert quote["day_close"] == 212.5  # FMP uses "price" for both
    assert quote["day_volume"] == 35000000
    assert quote["prev_close"] == 210.0
    assert quote["change"] == 2.5
    # changesPercentage 1.19 → fraction 0.0119
    assert abs(quote["change_pct"] - 0.0119) < 0.0001


@pytest.mark.asyncio
async def test_quote_none_fields(client):
    """Fields FMP doesn't provide (bid/ask/vwap) should be None."""
    client._request = AsyncMock(return_value=MOCK_QUOTE)

    quote = await client.get_quote("AAPL")

    assert quote["bid"] is None
    assert quote["ask"] is None
    assert quote["day_vwap"] is None


@pytest.mark.asyncio
async def test_quote_returns_none_on_empty(client):
    client._request = AsyncMock(return_value=None)
    assert await client.get_quote("AAPL") is None

    client._request = AsyncMock(return_value=[])
    assert await client.get_quote("AAPL") is None


# ── Tests: get_technical_indicator ────────────────────────────────────

@pytest.mark.asyncio
async def test_technical_indicator_rsi(client):
    """RSI should return a single float value (matching Polygon get_rsi shape)."""
    client._request = AsyncMock(return_value=MOCK_RSI_RESPONSE)

    val = await client.get_technical_indicator("AAPL", "rsi", 14)

    assert val is not None
    assert isinstance(val, float)
    assert abs(val - 55.32) < 0.01


@pytest.mark.asyncio
async def test_technical_indicator_sma(client):
    """SMA should return a single float value (matching Polygon get_sma shape)."""
    client._request = AsyncMock(return_value=MOCK_SMA_RESPONSE)

    val = await client.get_technical_indicator("AAPL", "sma", 50)

    assert val is not None
    assert abs(val - 205.45) < 0.01


@pytest.mark.asyncio
async def test_technical_indicator_passes_params(client):
    """Verify correct endpoint and params are sent."""
    client._request = AsyncMock(return_value=MOCK_RSI_RESPONSE)

    await client.get_technical_indicator("AAPL", "rsi", 14, timeframe="1day")

    client._request.assert_called_once_with(
        "/stable/technical-indicators/rsi",
        params={"symbol": "AAPL", "periodLength": 14, "timeframe": "1day"},
    )


@pytest.mark.asyncio
async def test_technical_indicator_returns_none_on_empty(client):
    client._request = AsyncMock(return_value=None)
    assert await client.get_technical_indicator("AAPL", "rsi", 14) is None

    client._request = AsyncMock(return_value=[])
    assert await client.get_technical_indicator("AAPL", "sma", 50) is None


# ── Tests: get_macd ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_macd_returns_polygon_shape(client):
    """MACD must return {value, signal, histogram} matching Polygon get_macd()."""
    call_count = 0
    async def mock_request(path, params=None):
        nonlocal call_count
        call_count += 1
        if params and params.get("periodLength") == 12:
            return MOCK_EMA_FAST
        elif params and params.get("periodLength") == 26:
            return MOCK_EMA_SLOW
        return None

    client._request = mock_request

    result = await client.get_macd("AAPL")

    assert result is not None
    assert "value" in result
    assert "signal" in result
    assert "histogram" in result
    assert call_count == 2  # one call for fast EMA, one for slow EMA


@pytest.mark.asyncio
async def test_macd_value_computation(client):
    """MACD line = EMA(fast) - EMA(slow) for the most recent date."""
    async def mock_request(path, params=None):
        if params and params.get("periodLength") == 12:
            return MOCK_EMA_FAST
        elif params and params.get("periodLength") == 26:
            return MOCK_EMA_SLOW
        return None

    client._request = mock_request

    result = await client.get_macd("AAPL")

    # Most recent: EMA-12=211.0, EMA-26=207.0 → MACD=4.0
    assert abs(result["value"] - 4.0) < 0.001

    # Signal = EMA(9) of the MACD series, built oldest-first.
    # Common dates (14 total) sorted oldest-first:
    #   03-27, 03-28, 03-31, 04-01, 04-02, 04-03, 04-04, 04-07, 04-08, 04-09, 04-10, 04-11, 04-14, 04-15
    # MACD values (oldest first):
    #   -0.3, 0.0, 0.3, 0.6, 0.9, 1.2, 1.5, 1.8, 2.1, 2.4, 2.6, 2.7, 3.0, 4.0
    import numpy as np
    macd_oldest_first = [-0.3, 0.0, 0.3, 0.6, 0.9, 1.2, 1.5, 1.8, 2.1, 2.4, 2.6, 2.7, 3.0, 4.0]
    # Seed: SMA of first 9 values
    ema = float(np.mean(macd_oldest_first[:9]))
    k = 2.0 / (9 + 1)
    for val in macd_oldest_first[9:]:
        ema = val * k + ema * (1 - k)
    expected_signal = ema
    assert abs(result["signal"] - expected_signal) < 0.01

    # Histogram = MACD line - Signal line
    assert abs(result["histogram"] - (4.0 - expected_signal)) < 0.01


@pytest.mark.asyncio
async def test_macd_returns_none_on_failure(client):
    """If either EMA call fails, MACD should return None."""
    client._request = AsyncMock(return_value=None)
    assert await client.get_macd("AAPL") is None


# ── Tests: Token-bucket rate limiter ──────────────────────────────────

@pytest.mark.asyncio
async def test_rate_limiter_initial_capacity():
    """Limiter should start with full capacity."""
    limiter = _TokenBucketRateLimiter(max_per_min=300, safety_pct=0.80)
    assert limiter.effective_rpm == 240


@pytest.mark.asyncio
async def test_rate_limiter_burst():
    """Should allow burst up to capacity without waiting."""
    limiter = _TokenBucketRateLimiter(max_per_min=60, safety_pct=1.0)
    # capacity=60, so 60 tokens available immediately
    start = time.monotonic()
    for _ in range(10):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    # 10 tokens from a bucket of 60 should be near-instant
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_rate_limiter_safety_margin():
    """80% safety margin on 300 req/min → 240 effective."""
    limiter = _TokenBucketRateLimiter(max_per_min=300, safety_pct=0.80)
    assert limiter.effective_rpm == 240
    assert limiter._capacity == 240

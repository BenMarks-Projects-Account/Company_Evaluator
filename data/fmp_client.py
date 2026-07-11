"""FMP (Financial Modeling Prep) client — primary data source for Company Evaluator.

Covers financial statements, company profiles, price history, real-time quotes,
technical indicators, insider trading, and more.
"""

import asyncio
import logging
import time
from datetime import date, timedelta

import httpx
import numpy as np

_log = logging.getLogger(__name__)


def _coerce_number(value):
    """Coerce value to int/float when possible; return None for empty/unparseable.

    FMP occasionally returns numeric fields as strings (e.g. fullTimeEmployees="164000")
    and downstream formatters like ``f"{x:,}"`` fail on string input with
    ``ValueError: Cannot specify ',' with 's'.``  Apply at the ingestion boundary.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        v = value.replace(",", "").replace("$", "").strip()
        if not v:
            return None
        try:
            if "." in v or "e" in v.lower():
                return float(v)
            return int(v)
        except ValueError:
            return None
    return None


# ── Token-bucket rate limiter ────────────────────────────────────────────

class _TokenBucketRateLimiter:
    """Async token-bucket rate limiter for FMP's 300 req/min limit.

    Parameters
    ----------
    max_per_min : int
        Hard ceiling from the API provider (default 300).
    safety_pct : float
        Fraction of *max_per_min* to actually use (default 0.80 → 240 effective).
    """

    def __init__(self, max_per_min: int = 300, safety_pct: float = 0.80):
        self._capacity = max(1, int(max_per_min * safety_pct))
        self._tokens = float(self._capacity)
        self._refill_rate = self._capacity / 60.0  # tokens per second
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def effective_rpm(self) -> int:
        return self._capacity

    async def acquire(self):
        """Wait until a token is available, then consume one."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # How long until 1 token is available?
                wait = (1.0 - self._tokens) / self._refill_rate
            await asyncio.sleep(wait)

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now


class FMPClient:
    """Client for FMP paid-tier endpoints.

    Used for:
      1. Cross-validation of Finnhub ratio metrics (Tier 2)
      2. Financial statement fallback when Polygon returns empty (Tier 3)
      3. Insider trading + institutional ownership data (Tier 4)
      4. Price history, real-time quotes, and technical indicators (primary)

    Paid tier: 300 requests/min.
    All methods share a single token-bucket rate limiter (default 80 % safety → 240 req/min).
    """

    def __init__(self, api_key: str, base_url: str = "https://financialmodelingprep.com/api/v3",
                 rate_limit_per_min: int = 300, rate_limit_safety_pct: float = 0.80):
        self._api_key = api_key
        self._base_url = base_url
        self._rate_limiter = _TokenBucketRateLimiter(rate_limit_per_min, rate_limit_safety_pct)
        self._calls_today = 0
        self._calls_reset_date = date.today()
        self._disabled_paths: set[str] = set()  # paths that returned 402 (plan limit)

    @property
    def calls_today(self) -> int:
        self._maybe_reset_counter()
        return self._calls_today

    def _maybe_reset_counter(self):
        today = date.today()
        if today != self._calls_reset_date:
            self._calls_today = 0
            self._calls_reset_date = today

    # ── Cross-validation endpoints (Tier 2) ──────────────────

    async def get_company_profile(self, symbol: str) -> dict | None:
        """Fetch company profile with clean sector/industry labels.

        Numeric fields (market_cap, employees) are coerced to numbers because
        the FMP /stable/profile endpoint returns some of them as strings
        (e.g. fullTimeEmployees="164000") and the field name for market cap
        was renamed from "mktCap" to "marketCap" — both shapes are accepted
        for forward/backward compatibility.
        """
        data = await self._request(f"/stable/profile", params={"symbol": symbol})
        if data and isinstance(data, list) and len(data) > 0:
            item = data[0]
            # FMP renamed mktCap → marketCap; accept either for safety.
            mc_raw = item.get("marketCap")
            if mc_raw is None:
                mc_raw = item.get("mktCap")
            return {
                "symbol": item.get("symbol"),
                "company_name": item.get("companyName"),
                "sector": item.get("sector"),
                "industry": item.get("industry"),
                "market_cap": _coerce_number(mc_raw),
                "description": item.get("description"),
                "employees": _coerce_number(item.get("fullTimeEmployees")),
                "website": item.get("website"),
                "country": item.get("country"),
                "exchange": item.get("exchangeShortName"),
            }
        return None

    async def get_key_metrics_ttm(self, symbol: str) -> dict | None:
        """Fetch TTM key metrics (ROIC, netIncomePerShare, etc.)."""
        data = await self._request("/key-metrics-ttm", params={"symbol": symbol})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    async def get_ratios_ttm(self, symbol: str) -> dict | None:
        """Fetch TTM financial ratios (PE, EV/EBITDA, margins, etc.)."""
        data = await self._request("/ratios-ttm", params={"symbol": symbol})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    async def get_financial_growth(self, symbol: str) -> dict | None:
        """Fetch financial growth metrics (revenue growth, EPS growth, etc.)."""
        data = await self._request("/financial-growth", params={"symbol": symbol, "period": "annual", "limit": 1})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    async def get_all_cross_validation_data(self, symbol: str) -> dict:
        """Fetch all FMP data needed for cross-validation in minimal calls.

        Makes 2 API calls: key-metrics-ttm and ratios-ttm.
        """
        result = {"symbol": symbol, "fetched": False, "metrics": {}, "ratios": {}}

        metrics = await self.get_key_metrics_ttm(symbol)
        ratios = await self.get_ratios_ttm(symbol)

        if metrics:
            result["metrics"] = metrics
        if ratios:
            result["ratios"] = ratios

        result["fetched"] = bool(metrics or ratios)
        return result

    # ── Financial statement endpoints (Polygon fallback) ─────

    async def get_income_statement(self, symbol: str, period: str = "quarter",
                                   limit: int = 12) -> list[dict] | None:
        """Fetch income statements from FMP."""
        return await self._request(
            "/income-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_balance_sheet(self, symbol: str, period: str = "quarter",
                                limit: int = 12) -> list[dict] | None:
        """Fetch balance sheet statements from FMP."""
        return await self._request(
            "/balance-sheet-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_cash_flow_statement(self, symbol: str, period: str = "quarter",
                                      limit: int = 12) -> list[dict] | None:
        """Fetch cash flow statements from FMP."""
        return await self._request(
            "/cash-flow-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_full_financials(self, symbol: str, period: str = "quarter",
                                  limit: int = 12) -> dict | None:
        """Fetch all three financial statements.

        Returns ``{"income_statement": [...], "balance_sheet": [...],
        "cash_flow_statement": [...]}`` or None if all three fail.
        """
        income = await self.get_income_statement(symbol, period, limit)
        balance = await self.get_balance_sheet(symbol, period, limit)
        cash_flow = await self.get_cash_flow_statement(symbol, period, limit)

        if not income and not balance and not cash_flow:
            return None

        return {
            "income_statement": income or [],
            "balance_sheet": balance or [],
            "cash_flow_statement": cash_flow or [],
        }

    # ── Insider & institutional endpoints (Tier 4) ───────────

    async def get_insider_trading(self, symbol: str, page: int = 0,
                                  limit: int = 100) -> list[dict] | None:
        """Fetch individual insider transactions."""
        return await self._request(
            "/insider-trading/search",
            params={"symbol": symbol, "page": page, "limit": limit},
        )

    async def get_insider_trading_statistics(self, symbol: str) -> list[dict] | None:
        """Fetch aggregated insider trading statistics by quarter."""
        return await self._request(
            "/insider-trading/statistics",
            params={"symbol": symbol},
        )

    async def get_institutional_ownership(self, symbol: str,
                                           year: int | None = None,
                                           quarter: int | None = None) -> list[dict] | None:
        """Fetch institutional positions summary for a symbol.

        SHORT-CIRCUITED 2026-04-16: FMP's
        ``/institutional-ownership/symbol-positions-summary`` endpoint has
        been returning HTTP 400 for every symbol tested (10/10 in perf
        audit). Returning ``None`` immediately avoids the ~1s round-trip
        and the noisy WARNING log per symbol. Smart-money analysis already
        handles ``None`` institutional data.

        Re-test periodically: if FMP restores the endpoint, remove this
        early return and let the request flow through.
        """
        _log.debug(
            "[FMP] get_institutional_ownership short-circuited for %s "
            "(endpoint broken)",
            symbol,
        )
        return None

    async def _get_institutional_ownership_raw(self, symbol: str,
                                                year: int | None = None,
                                                quarter: int | None = None) -> list[dict] | None:
        """Internal: original institutional-ownership request. Kept so the
        short-circuit above can be removed (or this helper called directly
        in a periodic health check) once FMP fixes the endpoint."""
        p: dict = {"symbol": symbol}
        if year:
            p["year"] = year
        if quarter:
            p["quarter"] = quarter
        return await self._request(
            "/institutional-ownership/symbol-positions-summary",
            params=p,
        )

    async def get_institutional_holders(self, symbol: str,
                                        year: int | None = None,
                                        quarter: int | None = None) -> list[dict] | None:
        """Fetch institutional holders with analytics for a symbol."""
        p: dict = {"symbol": symbol, "page": 0, "limit": 50}
        if year:
            p["year"] = year
        if quarter:
            p["quarter"] = quarter
        return await self._request(
            "/institutional-ownership/extract-analytics/holder",
            params=p,
        )

    # ── Transcript endpoints (on-demand research) ──────────

    async def get_transcript_list(self, symbol: str) -> list[dict] | None:
        """Fetch list of available earnings call transcripts for a symbol."""
        return await self._request(
            "/earning-call-transcript",
            params={"symbol": symbol},
        )

    async def get_earnings_transcript(
        self,
        symbol: str,
        year: int | None = None,
        quarter: int | None = None,
    ) -> dict | None:
        """Fetch a specific earnings call transcript.

        If year/quarter not specified, returns the most recent transcript.
        """
        params: dict = {"symbol": symbol}
        if year is not None:
            params["year"] = year
        if quarter is not None:
            params["quarter"] = quarter

        result = await self._request(
            "/earning-call-transcript",
            params=params,
        )

        # API returns a list — take the first (most recent)
        if isinstance(result, list) and result:
            return result[0]
        return None

    # ── HTTP layer ───────────────────────────────────────────

    async def stock_screener(
        self,
        *,
        market_cap_min: float | None = None,
        market_cap_max: float | None = None,
        price_min: float | None = None,
        volume_min: float | None = None,
        sector: str | None = None,
        country: str = "US",
        exchange: str = "nyse,nasdaq",
        is_actively_trading: bool = True,
        is_etf: bool = False,
        is_fund: bool = False,
        limit: int = 1000,
    ) -> list[dict] | None:
        """FMP stock screener — returns companies matching criteria."""
        params: dict = {}
        if market_cap_min is not None:
            params["marketCapMoreThan"] = int(market_cap_min)
        if market_cap_max is not None:
            params["marketCapLowerThan"] = int(market_cap_max)
        if price_min is not None:
            params["priceMoreThan"] = price_min
        if volume_min is not None:
            params["volumeMoreThan"] = int(volume_min)
        if sector:
            params["sector"] = sector
        if country:
            params["country"] = country
        if exchange:
            params["exchange"] = exchange
        if is_actively_trading:
            params["isActivelyTrading"] = "true"
        if not is_etf:
            params["isEtf"] = "false"
        if not is_fund:
            params["isFund"] = "false"
        params["limit"] = limit

        result = await self._request("/stable/company-screener", params=params)
        if isinstance(result, list):
            return result
        return None

    async def search_symbol(
        self,
        query: str,
        limit: int = 10,
        exchange: str = "NASDAQ,NYSE,AMEX",
    ) -> list[dict] | None:
        """FMP ticker search — matches by ticker prefix.

        Uses /stable/search-symbol. Returns list of dicts with keys:
        symbol, name, currency, exchangeFullName, exchange. Or None on failure.
        """
        params = {"query": query, "limit": limit}
        if exchange:
            params["exchange"] = exchange
        result = await self._request("/stable/search-symbol", params=params)
        if isinstance(result, list):
            return result
        return None

    async def search_name(
        self,
        query: str,
        limit: int = 10,
        exchange: str = "NASDAQ,NYSE,AMEX",
    ) -> list[dict] | None:
        """FMP company name search — matches by company name substring.

        Uses /stable/search-name. Returns same shape as search_symbol.
        """
        params = {"query": query, "limit": limit}
        if exchange:
            params["exchange"] = exchange
        result = await self._request("/stable/search-name", params=params)
        if isinstance(result, list):
            return result
        return None

    # ── Price / quote / technical indicator endpoints ────────

    async def get_historical_price_eod(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[dict] | None:
        """Fetch daily OHLCV bars from FMP's historical-price-eod/full endpoint.

        Returns a list of dicts in the same shape that Polygon callers expect::

            [{date, open, high, low, close, volume}, ...]  (oldest-first)

        Field-name translations from FMP → Polygon caller shape:
          - FMP "date" → kept as "date"
          - FMP "open"/"high"/"low"/"close"/"volume" → kept as-is
          - FMP returns newest-first; we reverse to oldest-first to match
            Polygon's ``get_raw_bars()`` contract.
        """
        params: dict = {"symbol": symbol}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        data = await self._request("/stable/historical-price-eod/full", params=params)

        if not data or not isinstance(data, list):
            return None

        bars = []
        for item in data:
            if "close" not in item or "date" not in item:
                continue
            bars.append({
                "date": item["date"],
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item["close"],
                "volume": item.get("volume", 0),
            })

        # FMP returns newest-first; Polygon callers expect oldest-first
        bars.reverse()
        return bars if bars else None

    async def get_quote(self, symbol: str) -> dict | None:
        """Fetch real-time quote from FMP's /stable/quote endpoint.

        Returns a dict in the same shape that Polygon ``get_snapshot()`` callers
        expect::

            {symbol, last_price, day_open, day_high, day_low, day_close,
             day_volume, day_vwap, prev_close, change, change_pct, ...}

        Field-name translations from FMP → Polygon snapshot shape:
          - FMP "price"        → "last_price"
          - FMP "open"         → "day_open"
          - FMP "dayHigh"      → "day_high"
          - FMP "dayLow"       → "day_low"
          - FMP "price"        → "day_close"  (FMP has no separate intraday close)
          - FMP "volume"       → "day_volume"
          - FMP "avgVolume"    → "avg_volume"  (bonus; not in Polygon shape)
          - FMP "previousClose"→ "prev_close"
          - FMP "change"       → "change"
          - FMP "changesPercentage" → "change_pct" (converted to fraction)
          - FMP has no bid/ask/vwap — those fields are set to None.
        """
        data = await self._request("/stable/quote", params={"symbol": symbol})

        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        q = data[0]
        change_pct_raw = q.get("changesPercentage")
        change_pct = change_pct_raw / 100.0 if change_pct_raw is not None else None

        # Derive change_pct from change / previousClose if not provided
        if change_pct is None:
            change = q.get("change")
            prev_close = q.get("previousClose")
            if change is not None and prev_close and prev_close != 0:
                change_pct = change / prev_close

        return {
            "symbol": q.get("symbol"),
            "last_price": q.get("price"),
            "last_size": None,           # FMP doesn't provide
            "bid": None,                 # FMP quote doesn't include bid/ask
            "ask": None,
            "bid_size": None,
            "ask_size": None,
            "day_open": q.get("open"),
            "day_high": q.get("dayHigh"),
            "day_low": q.get("dayLow"),
            "day_close": q.get("price"),  # FMP uses "price" as current/close
            "day_volume": q.get("volume"),
            "day_vwap": None,            # FMP quote doesn't include VWAP
            "prev_close": q.get("previousClose"),
            "prev_volume": None,         # FMP quote doesn't include prev volume
            "change": q.get("change"),
            "change_pct": change_pct,
            "avg_volume": q.get("avgVolume"),
            "market_cap": q.get("marketCap"),
        }

    async def get_technical_indicator(
        self,
        symbol: str,
        indicator: str,
        period_length: int,
        timeframe: str = "1day",
    ) -> float | None:
        """Fetch a single technical indicator value from FMP.

        Generic wrapper around ``/stable/technical-indicators/{indicator}``.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g. "AAPL").
        indicator : str
            One of "rsi", "sma", "ema", "wma", "dema", "tema",
            "williams", "adx", "standarddeviation".
        period_length : int
            Look-back window (e.g. 14 for RSI, 50 for SMA-50).
        timeframe : str
            Bar interval: "1min", "5min", "15min", "30min",
            "1hour", "4hour", "1day" (default).

        Returns
        -------
        float | None
            The most recent indicator value, or None on failure.
            Matches the return type of Polygon's ``get_rsi()`` and ``get_sma()``.
        """
        data = await self._request(
            f"/stable/technical-indicators/{indicator}",
            params={
                "symbol": symbol,
                "periodLength": period_length,
                "timeframe": timeframe,
            },
        )

        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        # FMP returns newest-first; grab the first entry's indicator value
        entry = data[0]
        return entry.get(indicator)

    async def get_macd(
        self,
        symbol: str,
        timeframe: str = "1day",
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> dict | None:
        """Compute MACD locally from FMP EMA data.

        MACD is not a native FMP endpoint, so we compute it from two EMA calls:

            MACD line = EMA(fast) − EMA(slow)      (default: EMA-12 − EMA-26)
            Signal line = EMA(signal) of the MACD line  (default: 9-period EMA)
            Histogram = MACD line − Signal line

        We seed the EMA with an SMA of the first *signal* MACD values
        (oldest in the window), then apply the standard EMA formula
        forward to produce the most-recent signal value.

        Returns
        -------
        dict | None
            ``{"value": float, "signal": float, "histogram": float}``
            matching Polygon's ``get_macd()`` return shape, or None on failure.
        """
        # We need signal + signal points for EMA warm-up (SMA seed + forward pass)
        lookback = signal * 2 + 5  # generous buffer

        fast_data = await self._request(
            f"/stable/technical-indicators/ema",
            params={
                "symbol": symbol,
                "periodLength": fast,
                "timeframe": timeframe,
            },
        )
        slow_data = await self._request(
            f"/stable/technical-indicators/ema",
            params={
                "symbol": symbol,
                "periodLength": slow,
                "timeframe": timeframe,
            },
        )

        if not fast_data or not slow_data:
            return None
        if not isinstance(fast_data, list) or not isinstance(slow_data, list):
            return None

        # Build date-aligned MACD series (both come newest-first from FMP)
        fast_map = {e["date"]: e.get("ema") for e in fast_data if "date" in e and e.get("ema") is not None}
        slow_map = {e["date"]: e.get("ema") for e in slow_data if "date" in e and e.get("ema") is not None}

        common_dates = sorted(set(fast_map) & set(slow_map), reverse=True)
        if len(common_dates) < signal:
            # Not enough data to compute signal line
            if common_dates:
                macd_val = fast_map[common_dates[0]] - slow_map[common_dates[0]]
                return {"value": macd_val, "signal": None, "histogram": None}
            return None

        # Build MACD series oldest-first for EMA computation
        macd_series = [fast_map[d] - slow_map[d] for d in reversed(common_dates)]

        # Compute EMA of MACD series with period = signal
        # Seed: SMA of first `signal` values
        k = 2.0 / (signal + 1)
        ema = float(np.mean(macd_series[:signal]))
        for val in macd_series[signal:]:
            ema = val * k + ema * (1 - k)

        macd_value = macd_series[-1]  # most recent
        signal_value = ema
        histogram = macd_value - signal_value

        return {
            "value": macd_value,
            "signal": signal_value,
            "histogram": histogram,
        }

    # ── HTTP layer ───────────────────────────────────────────

    async def _request(self, path: str, params: dict | None = None):
        """Make a rate-limited request to FMP API.

        Uses a token-bucket limiter shared across all FMP methods.
        """
        self._maybe_reset_counter()

        # Skip paths that previously returned 402 (plan not included)
        if path in self._disabled_paths:
            return None

        # Wait for a token from the bucket (replaces old per-request sleep)
        await self._rate_limiter.acquire()

        # Route stable endpoints to the stable base URL, v3 endpoints to the configured base
        if path.startswith("/stable/"):
            url = f"https://financialmodelingprep.com{path}"
        else:
            url = f"{self._base_url}{path}"
        req_params = {"apikey": self._api_key}
        if params:
            req_params.update(params)

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, params=req_params)
                self._calls_today += 1

                if resp.status_code == 429:
                    _log.warning("[FMP] 429 rate-limited on %s — backing off 2s", path)
                    await asyncio.sleep(2)
                    await self._rate_limiter.acquire()
                    resp = await client.get(url, params=req_params)
                    self._calls_today += 1
                    if resp.status_code != 200:
                        _log.warning("[FMP] Retry still failed (%d) on %s", resp.status_code, path)
                        return None

                if resp.status_code == 403:
                    _log.warning("[FMP] 403 forbidden on %s (check API key / plan)", path)
                    return None

                if resp.status_code == 402:
                    _log.warning("[FMP] 402 payment required on %s — disabling for this session", path)
                    self._disabled_paths.add(path)
                    return None

                if resp.status_code != 200:
                    _log.warning("[FMP] HTTP %d on %s", resp.status_code, path)
                    return None

                data = resp.json()

                # FMP returns {"Error Message": "..."} on invalid keys/symbols
                if isinstance(data, dict) and "Error Message" in data:
                    _log.warning("[FMP] Error on %s: %s", path, data["Error Message"])
                    return None

                _log.debug("[FMP] %s → %d bytes", path, len(resp.content))
                return data

        except Exception as exc:
            _log.error("[FMP] Request failed %s — %s", path, exc)
            return None

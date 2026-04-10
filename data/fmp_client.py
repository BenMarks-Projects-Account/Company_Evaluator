"""FMP (Financial Modeling Prep) client — cross-validation + Polygon fallback."""

import logging
import time
from datetime import date

import httpx

_log = logging.getLogger(__name__)


class FMPClient:
    """Client for FMP paid-tier endpoints.

    Used for:
      1. Cross-validation of Finnhub ratio metrics (Tier 2)
      2. Financial statement fallback when Polygon returns empty (Tier 3)
      3. Insider trading + institutional ownership data (Tier 4)

    Paid tier: 300 requests/min.
    """

    def __init__(self, api_key: str, base_url: str = "https://financialmodelingprep.com/api/v3",
                 rate_limit_per_min: int = 300):
        self._api_key = api_key
        self._base_url = base_url
        self._min_interval = 60.0 / rate_limit_per_min  # seconds between requests
        self._last_request = 0.0
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
        """Fetch institutional positions summary for a symbol."""
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

    async def _request(self, path: str, params: dict | None = None) -> list | dict | None:
        """Make a rate-limited request to FMP API."""
        import asyncio
        self._maybe_reset_counter()

        # Skip paths that previously returned 402 (plan not included)
        if path in self._disabled_paths:
            return None

        # Per-minute rate limiting
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()

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

"""Polygon.io client for company fundamental data."""

import asyncio
import logging
import time
from datetime import date, timedelta
import httpx
import numpy as np

_log = logging.getLogger(__name__)


class PolygonClient:
    """Fetches financial statements and price data from Polygon.io."""

    def __init__(self, api_key: str, rate_limit: float = 100.0):
        self._api_key = api_key
        self._base_url = "https://api.polygon.io"
        # Polygon Starter: unlimited calls. Keep 100ms courtesy delay.
        self._min_interval = 1.0 / rate_limit  # 0.01s at 100 req/s
        self._last_request = 0.0

    async def get_financials(self, symbol: str, limit: int = 12, timeframe: str = "quarterly") -> dict:
        """Fetch financial statements from Polygon's Financials API.

        Returns income statement, balance sheet, and cash flow data
        sourced from SEC XBRL filings.
        """
        params = {
            "ticker": symbol,
            "timeframe": timeframe,
            "limit": limit,
            "sort": "filing_date",
            "order": "desc",
        }

        data = await self._request("/vX/reference/financials", params)

        if not data or "results" not in data:
            return {"error": "No financial data returned", "results": []}

        statements = []
        for filing in data["results"]:
            fiscal_period = filing.get("fiscal_period", "")
            start_date = filing.get("start_date", "")
            end_date = filing.get("end_date", "")
            filed_date = filing.get("filing_date", "")

            financials = filing.get("financials", {})

            record = {
                "period": end_date,
                "start_date": start_date,
                "fiscal_period": fiscal_period,
                "fiscal_year": filing.get("fiscal_year"),
                "filing_date": filed_date,

                # Income Statement
                "revenue": _extract(financials, "income_statement", "revenues"),
                "cost_of_revenue": _extract(financials, "income_statement", "cost_of_revenue"),
                "gross_profit": _extract(financials, "income_statement", "gross_profit"),
                "operating_income": _extract(financials, "income_statement", "operating_income_loss"),
                "operating_expenses": _extract(financials, "income_statement", "operating_expenses"),
                "net_income": _extract(financials, "income_statement", "net_income_loss"),
                "eps_basic": _extract(financials, "income_statement", "basic_earnings_per_share"),
                "eps_diluted": _extract(financials, "income_statement", "diluted_earnings_per_share"),
                "research_and_development": _extract(financials, "income_statement", "research_and_development"),
                "selling_general_administrative": _extract(financials, "income_statement", "selling_general_and_administrative_expenses"),
                "income_before_tax": _extract(financials, "income_statement", "income_loss_from_continuing_operations_before_tax"),
                "income_tax": _extract(financials, "income_statement", "income_tax_expense_benefit"),
                "basic_avg_shares": _extract(financials, "income_statement", "basic_average_shares"),
                "diluted_avg_shares": _extract(financials, "income_statement", "diluted_average_shares"),

                # Balance Sheet
                "total_assets": _extract(financials, "balance_sheet", "assets"),
                "total_liabilities": _extract(financials, "balance_sheet", "liabilities"),
                "total_equity": _extract(financials, "balance_sheet", "equity"),
                "equity_parent": _extract(financials, "balance_sheet", "equity_attributable_to_parent"),
                "current_assets": _extract(financials, "balance_sheet", "current_assets"),
                "current_liabilities": _extract(financials, "balance_sheet", "current_liabilities"),
                "noncurrent_assets": _extract(financials, "balance_sheet", "noncurrent_assets"),
                "noncurrent_liabilities": _extract(financials, "balance_sheet", "noncurrent_liabilities"),
                "long_term_debt": _extract(financials, "balance_sheet", "long_term_debt"),
                "inventory": _extract(financials, "balance_sheet", "inventory"),
                "accounts_payable": _extract(financials, "balance_sheet", "accounts_payable"),
                "fixed_assets": _extract(financials, "balance_sheet", "fixed_assets"),

                # Cash Flow Statement
                "operating_cash_flow": _extract(financials, "cash_flow_statement", "net_cash_flow_from_operating_activities"),
                "investing_cash_flow": _extract(financials, "cash_flow_statement", "net_cash_flow_from_investing_activities"),
                "financing_cash_flow": _extract(financials, "cash_flow_statement", "net_cash_flow_from_financing_activities"),
                "net_cash_flow": _extract(financials, "cash_flow_statement", "net_cash_flow"),
                "free_cash_flow": None,  # Computed below
            }

            # Compute FCF = operating CF - |investing CF| (approximation since capex not broken out)
            # More conservative: use investing CF as proxy for capex
            if record["operating_cash_flow"] is not None and record["investing_cash_flow"] is not None:
                record["free_cash_flow"] = record["operating_cash_flow"] + record["investing_cash_flow"]

            statements.append(record)

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(statements),
            "statements": statements,
        }

    async def get_company_details(self, symbol: str) -> dict:
        """Get company profile information."""
        data = await self._request(f"/v3/reference/tickers/{symbol}", {})

        if not data or "results" not in data:
            return {"error": "No company data returned"}

        r = data["results"]
        return {
            "symbol": r.get("ticker"),
            "company_name": r.get("name"),
            "market_cap": r.get("market_cap"),
            "sector": r.get("sic_description"),
            "description": r.get("description"),
            "homepage": r.get("homepage_url"),
            "employees": r.get("total_employees"),
            "list_date": r.get("list_date"),
            "locale": r.get("locale"),
            "type": r.get("type"),
        }

    async def get_price_history(self, symbol: str, days: int = 365) -> dict:
        """Get daily price history for return and volatility calculations."""
        end = date.today()
        start = end - timedelta(days=days)

        data = await self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "true", "limit": 5000},
        )

        if not data or "results" not in data:
            return {"error": "No price history returned"}

        bars = data["results"]
        if not bars:
            return {"error": "Empty price history"}

        closes = [b["c"] for b in bars if "c" in b]
        highs = [b["h"] for b in bars if "h" in b]
        lows = [b["l"] for b in bars if "l" in b]
        volumes = [b["v"] for b in bars if "v" in b]

        if len(closes) < 2:
            return {"error": "Insufficient price data"}

        daily_returns = np.diff(closes) / np.array(closes[:-1])

        return {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "data_points": len(closes),
            "current_price": closes[-1],
            "year_high": max(highs) if highs else None,
            "year_low": min(lows) if lows else None,
            "year_return": (closes[-1] / closes[0]) - 1,
            "avg_daily_volume": sum(volumes) / len(volumes) if volumes else None,
            "volatility_annualized": float(np.std(daily_returns) * np.sqrt(252)),
            "max_drawdown": float(_compute_max_drawdown(closes)),
            "sharpe_approx": float(np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252)) if np.std(daily_returns) > 0 else None,
        }

    async def get_raw_bars(self, symbol: str, days: int = 365) -> list[dict] | None:
        """Get raw daily OHLCV bars for technical analysis.

        Returns a list of dicts: [{date, open, high, low, close, volume}, ...]
        sorted oldest-first, or None on failure.
        """
        end = date.today()
        start = end - timedelta(days=days)

        data = await self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "true", "limit": 5000},
        )

        if not data or "results" not in data:
            return None

        bars = data["results"]
        if not bars:
            return None

        result = []
        for b in bars:
            if "c" not in b or "t" not in b:
                continue
            result.append({
                "date": date.fromtimestamp(b["t"] / 1000).isoformat(),
                "open": b.get("o"),
                "high": b.get("h"),
                "low": b.get("l"),
                "close": b["c"],
                "volume": b.get("v", 0),
            })
        return result

    async def get_rsi(self, symbol: str, window: int = 14,
                      timespan: str = "day", limit: int = 1) -> float | None:
        """Get RSI from Polygon's technical indicators endpoint.

        Returns the most recent RSI value, or None on failure.
        """
        data = await self._request(
            f"/v1/indicators/rsi/{symbol}",
            {"timespan": timespan, "window": window, "limit": limit,
             "series_type": "close", "order": "desc"},
        )
        if not data:
            return None
        try:
            values = data.get("results", {}).get("values", [])
            if values:
                return values[0].get("value")
        except (AttributeError, IndexError, TypeError):
            pass
        return None

    async def get_sma(self, symbol: str, window: int = 50,
                      timespan: str = "day", limit: int = 1) -> float | None:
        """Get SMA from Polygon's technical indicators endpoint.

        Returns the most recent SMA value, or None on failure.
        """
        data = await self._request(
            f"/v1/indicators/sma/{symbol}",
            {"timespan": timespan, "window": window, "limit": limit,
             "series_type": "close", "order": "desc"},
        )
        if not data:
            return None
        try:
            values = data.get("results", {}).get("values", [])
            if values:
                return values[0].get("value")
        except (AttributeError, IndexError, TypeError):
            pass
        return None

    async def get_macd(self, symbol: str, timespan: str = "day",
                       limit: int = 1) -> dict | None:
        """Get MACD from Polygon's technical indicators endpoint.

        Returns {value, signal, histogram} or None on failure.
        """
        data = await self._request(
            f"/v1/indicators/macd/{symbol}",
            {"timespan": timespan, "limit": limit,
             "short_window": 12, "long_window": 26, "signal_window": 9,
             "series_type": "close", "order": "desc"},
        )
        if not data:
            return None
        try:
            values = data.get("results", {}).get("values", [])
            if values:
                v = values[0]
                return {
                    "value": v.get("value"),
                    "signal": v.get("signal"),
                    "histogram": v.get("histogram"),
                }
        except (AttributeError, IndexError, TypeError):
            pass
        return None

    async def get_snapshot(self, symbol: str) -> dict | None:
        """Get real-time snapshot (15-min delayed on Starter tier).

        Returns current price, day change, volume, bid/ask, etc.
        """
        data = await self._request(
            f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}", {},
        )
        if not data or "ticker" not in data:
            return None

        t = data["ticker"]
        day = t.get("day", {})
        prev = t.get("prevDay", {})
        last_trade = t.get("lastTrade", {})
        last_quote = t.get("lastQuote", {})

        return {
            "symbol": t.get("ticker"),
            "last_price": last_trade.get("p"),
            "last_size": last_trade.get("s"),
            "bid": last_quote.get("p"),
            "ask": last_quote.get("P"),
            "bid_size": last_quote.get("s"),
            "ask_size": last_quote.get("S"),
            "day_open": day.get("o"),
            "day_high": day.get("h"),
            "day_low": day.get("l"),
            "day_close": day.get("c"),
            "day_volume": day.get("v"),
            "day_vwap": day.get("vw"),
            "prev_close": prev.get("c"),
            "prev_volume": prev.get("v"),
            "change": (day.get("c", 0) - prev.get("c", 0)) if day.get("c") and prev.get("c") else None,
            "change_pct": ((day.get("c", 0) / prev.get("c", 1)) - 1) if day.get("c") and prev.get("c") else None,
        }

    async def get_tickers(
        self,
        market: str = "stocks",
        exchange: str | None = None,
        type: str = "CS",
        active: bool = True,
        sort: str = "ticker",
        order: str = "asc",
        limit: int = 1000,
        search: str | None = None,
    ) -> list[dict]:
        """Fetch tickers from /v3/reference/tickers with cursor pagination.

        Returns a flat list of ticker dicts with fields:
        ticker, name, market_cap, primary_exchange, type, active,
        locale, currency_name, last_updated_utc, etc.
        """
        params = {
            "market": market,
            "type": type,
            "active": str(active).lower(),
            "sort": sort,
            "order": order,
            "limit": limit,
        }
        if exchange:
            params["exchange"] = exchange
        if search:
            params["search"] = search

        all_tickers: list[dict] = []
        next_url: str | None = None
        page = 1

        while True:
            if next_url:
                # Cursor pagination — next_url is a full URL, just append apiKey
                data = await self._request_url(next_url)
            else:
                data = await self._request("/v3/reference/tickers", params)

            if not data:
                break

            results = data.get("results", [])
            all_tickers.extend(results)
            _log.info("Polygon tickers page %d: got %d tickers (total so far: %d)",
                       page, len(results), len(all_tickers))

            # Check for next page cursor
            next_url = data.get("next_url")
            if not next_url or not results:
                break
            page += 1

        return all_tickers

    async def get_all_snapshots(self) -> list[dict] | None:
        """Get snapshots of ALL US stock tickers in one call.

        Returns a list of snapshot dicts with current price, volume, day change.
        Useful for bulk market cap / price lookups during universe refresh.
        """
        data = await self._request(
            "/v2/snapshot/locale/us/markets/stocks/tickers", {},
        )
        if not data or "tickers" not in data:
            return None

        results = []
        for t in data["tickers"]:
            day = t.get("day", {})
            prev = t.get("prevDay", {})
            results.append({
                "ticker": t.get("ticker"),
                "last_price": t.get("lastTrade", {}).get("p"),
                "day_volume": day.get("v"),
                "day_close": day.get("c"),
                "prev_close": prev.get("c"),
                "change_pct": t.get("todaysChangePerc"),
            })
        _log.info("Polygon snapshots: got %d ticker snapshots", len(results))
        return results

    async def _request(self, path: str, params: dict) -> dict | None:
        """Make a rate-limited request to Polygon API."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()

        url = f"{self._base_url}{path}"
        params["apiKey"] = self._api_key

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params)

                if resp.status_code == 429:
                    _log.warning("Polygon 429 rate-limited on %s — retrying in 2s", path)
                    await asyncio.sleep(2)
                    resp = await client.get(url, params=params)

                if resp.status_code != 200:
                    _log.warning("Polygon HTTP %d on %s", resp.status_code, path)
                    return None

                data = resp.json()
                _log.debug("Polygon %s → %d bytes", path, len(resp.content))
                return data
        except Exception as exc:
            _log.error("Polygon request failed %s — %s", path, exc)
            return None

    async def _request_url(self, url: str) -> dict | None:
        """Make a rate-limited request to a full URL (for cursor pagination)."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()

        separator = "&" if "?" in url else "?"
        full_url = f"{url}{separator}apiKey={self._api_key}"

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(full_url)

                if resp.status_code == 429:
                    _log.warning("Polygon 429 rate-limited on paginated URL — retrying in 2s")
                    await asyncio.sleep(2)
                    resp = await client.get(full_url)

                if resp.status_code != 200:
                    _log.warning("Polygon HTTP %d on paginated URL", resp.status_code)
                    return None

                data = resp.json()
                return data
        except Exception as exc:
            _log.error("Polygon paginated request failed — %s", exc)
            return None


def _extract(financials: dict, statement: str, field: str) -> float | None:
    """Safely extract a value from Polygon's nested financials structure."""
    try:
        return financials.get(statement, {}).get(field, {}).get("value")
    except (AttributeError, TypeError):
        return None


def _compute_max_drawdown(closes: list) -> float:
    """Compute maximum drawdown from a price series."""
    peak = closes[0]
    max_dd = 0.0
    for c in closes:
        if c > peak:
            peak = c
        dd = (c - peak) / peak
        if dd < max_dd:
            max_dd = dd
    return max_dd

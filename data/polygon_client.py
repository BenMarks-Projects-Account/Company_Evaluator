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

    def __init__(self, api_key: str, rate_limit: float = 5.0):
        self._api_key = api_key
        self._base_url = "https://api.polygon.io"
        self._min_interval = 1.0 / rate_limit
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
                    _log.warning("event=polygon_rate_limited path=%s", path)
                    await asyncio.sleep(5)
                    resp = await client.get(url, params=params)

                if resp.status_code != 200:
                    _log.warning("event=polygon_error path=%s status=%d", path, resp.status_code)
                    return None

                return resp.json()
        except Exception as exc:
            _log.error("event=polygon_request_failed path=%s error=%s", path, exc)
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

"""Finnhub client for company ratios, estimates, and alternative data."""

import asyncio
import logging
import time
import httpx

_log = logging.getLogger(__name__)


class FinnhubClient:
    """Fetches pre-computed ratios, estimates, and alternative data from Finnhub."""

    def __init__(self, api_key: str, rate_limit: float = 30.0):
        self._api_key = api_key
        self._base_url = "https://finnhub.io/api/v1"
        self._min_interval = 1.0 / rate_limit
        self._last_request = 0.0

    async def get_basic_financials(self, symbol: str) -> dict:
        """Get 117 pre-computed financial ratios and metrics.

        Includes: PE, PB, PS, EV/EBITDA, ROE, ROA, margins, growth rates,
        dividend yield, beta, and many more.
        """
        data = await self._request("/stock/metric", {"symbol": symbol, "metric": "all"})

        if not data or "metric" not in data:
            return {"error": "No metrics returned", "metrics": {}, "series": {}}

        return {
            "metrics": data.get("metric", {}),
            "series": data.get("series", {}),
        }

    async def get_company_profile(self, symbol: str) -> dict:
        """Get company profile information."""
        data = await self._request("/stock/profile2", {"symbol": symbol})

        if not data:
            return {"error": "No profile returned"}

        return {
            "symbol": data.get("ticker"),
            "company_name": data.get("name"),
            "sector": data.get("finnhubIndustry"),
            "country": data.get("country"),
            "exchange": data.get("exchange"),
            "ipo_date": data.get("ipo"),
            "market_cap": data.get("marketCapitalization"),  # In millions
            "shares_outstanding": data.get("shareOutstanding"),  # In millions
            "website": data.get("weburl"),
            "logo": data.get("logo"),
            "phone": data.get("phone"),
        }

    async def get_eps_estimates(self, symbol: str, freq: str = "quarterly") -> dict:
        """Get consensus EPS estimates."""
        data = await self._request("/stock/eps-estimate", {"symbol": symbol, "freq": freq})

        if not data or "data" not in data:
            return {"error": "No estimates returned", "estimates": []}

        return {
            "symbol": data.get("symbol"),
            "freq": data.get("freq"),
            "estimates": [
                {
                    "period": e.get("period"),
                    "eps_avg": e.get("epsAvg"),
                    "eps_high": e.get("epsHigh"),
                    "eps_low": e.get("epsLow"),
                    "number_analysts": e.get("numberAnalysts"),
                }
                for e in data.get("data", [])[:8]
            ],
        }

    async def get_price_target(self, symbol: str) -> dict:
        """Get analyst consensus price target."""
        data = await self._request("/stock/price-target", {"symbol": symbol})

        if not data:
            return {"error": "No price target returned"}

        return {
            "target_high": data.get("targetHigh"),
            "target_low": data.get("targetLow"),
            "target_mean": data.get("targetMean"),
            "target_median": data.get("targetMedian"),
            "last_updated": data.get("lastUpdated"),
        }

    async def get_insider_transactions(self, symbol: str) -> dict:
        """Get insider buy/sell transactions."""
        data = await self._request("/stock/insider-transactions", {"symbol": symbol})

        if not data or "data" not in data:
            return {"error": "No insider data", "transactions": [], "net_activity": "unknown"}

        transactions = data.get("data", [])[:30]

        buys = sum(1 for t in transactions if t.get("change", 0) > 0)
        sells = sum(1 for t in transactions if t.get("change", 0) < 0)
        buy_value = sum(
            t.get("transactionPrice", 0) * t.get("change", 0)
            for t in transactions if t.get("change", 0) > 0
        )
        sell_value = sum(
            abs(t.get("transactionPrice", 0) * t.get("change", 0))
            for t in transactions if t.get("change", 0) < 0
        )

        if buys > sells + 2:
            net_activity = "net_buying"
        elif sells > buys + 2:
            net_activity = "net_selling"
        else:
            net_activity = "neutral"

        return {
            "transaction_count": len(transactions),
            "buys": buys,
            "sells": sells,
            "buy_value": round(buy_value, 2),
            "sell_value": round(sell_value, 2),
            "net_activity": net_activity,
        }

    async def get_peers(self, symbol: str) -> list:
        """Get list of peer company symbols."""
        data = await self._request("/stock/peers", {"symbol": symbol})
        return data if isinstance(data, list) else []

    async def get_recommendation_trends(self, symbol: str) -> dict:
        """Get analyst recommendation trends."""
        data = await self._request("/stock/recommendation", {"symbol": symbol})

        if not data or not isinstance(data, list) or len(data) == 0:
            return {"error": "No recommendations", "latest": {}}

        latest = data[0]
        return {
            "period": latest.get("period"),
            "strong_buy": latest.get("strongBuy", 0),
            "buy": latest.get("buy", 0),
            "hold": latest.get("hold", 0),
            "sell": latest.get("sell", 0),
            "strong_sell": latest.get("strongSell", 0),
        }

    async def _request(self, path: str, params: dict) -> dict | list | None:
        """Make a rate-limited request to Finnhub API."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()

        url = f"{self._base_url}{path}"
        params["token"] = self._api_key

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, params=params)

                if resp.status_code == 429:
                    _log.warning("event=finnhub_rate_limited path=%s", path)
                    await asyncio.sleep(2)
                    resp = await client.get(url, params=params)

                if resp.status_code != 200:
                    _log.warning("event=finnhub_error path=%s status=%d", path, resp.status_code)
                    return None

                return resp.json()
        except Exception as exc:
            _log.error("event=finnhub_request_failed path=%s error=%s", path, exc)
            return None

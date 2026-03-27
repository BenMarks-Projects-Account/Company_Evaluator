"""Unified company data service — orchestrates all data sources."""

import asyncio
import logging
from config import get_settings

_log = logging.getLogger(__name__)


class CompanyDataService:
    """Fetches all data needed for company evaluation from multiple sources.

    Source priority:
      1. Polygon  — financial statements (SEC XBRL), price history
      2. Finnhub  — 117 ratios, estimates, insiders, peers, price targets
      3. Yahoo    — fallback ONLY when Polygon financials fail
    """

    def __init__(self):
        settings = get_settings()

        from data.polygon_client import PolygonClient
        from data.finnhub_client import FinnhubClient

        self._polygon = (
            PolygonClient(settings.polygon_api_key, settings.polygon_rate_limit)
            if settings.polygon_api_key
            else None
        )
        self._finnhub = (
            FinnhubClient(settings.finnhub_api_key, settings.finnhub_rate_limit)
            if settings.finnhub_api_key
            else None
        )
        self._yahoo_enabled = settings.yahoo_enabled

    async def get_company_data(self, symbol: str) -> dict:
        """Fetch ALL data needed for company evaluation."""
        _log.info("event=company_data_start symbol=%s", symbol)
        yahoo_used = False

        # === POLYGON: Financial statements + price history ===
        financials_quarterly = None
        financials_annual = None
        price_history = None
        company_details = None

        if self._polygon:
            financials_quarterly = await self._safe(
                "polygon_financials_q",
                self._polygon.get_financials, symbol, limit=12, timeframe="quarterly",
            )
            financials_annual = await self._safe(
                "polygon_financials_a",
                self._polygon.get_financials, symbol, limit=5, timeframe="annual",
            )
            price_history = await self._safe(
                "polygon_prices",
                self._polygon.get_price_history, symbol, days=365,
            )
            company_details = await self._safe(
                "polygon_details",
                self._polygon.get_company_details, symbol,
            )

        # === FINNHUB: Ratios, estimates, insiders, peers ===
        basic_financials = None
        profile = None
        eps_estimates = None
        price_target = None
        insiders = None
        peers = None
        recommendations = None

        if self._finnhub:
            basic_financials = await self._safe(
                "finnhub_metrics", self._finnhub.get_basic_financials, symbol,
            )
            profile = await self._safe(
                "finnhub_profile", self._finnhub.get_company_profile, symbol,
            )
            eps_estimates = await self._safe(
                "finnhub_estimates", self._finnhub.get_eps_estimates, symbol,
            )
            price_target = await self._safe(
                "finnhub_target", self._finnhub.get_price_target, symbol,
            )
            insiders = await self._safe(
                "finnhub_insiders", self._finnhub.get_insider_transactions, symbol,
            )
            peers = await self._safe(
                "finnhub_peers", self._finnhub.get_peers, symbol,
            )
            recommendations = await self._safe(
                "finnhub_recs", self._finnhub.get_recommendation_trends, symbol,
            )

        # === YAHOO FALLBACK: Only if critical data is missing ===
        if self._yahoo_enabled:
            if not financials_quarterly or financials_quarterly.get("error"):
                _log.info("event=yahoo_fallback symbol=%s reason=polygon_financials_failed", symbol)
                yahoo_data = await self._fetch_yahoo_fallback(symbol)
                if yahoo_data:
                    yahoo_used = True
                    if not financials_quarterly or financials_quarterly.get("error"):
                        financials_quarterly = yahoo_data.get("income_statement")
                    if not price_history or price_history.get("error"):
                        price_history = yahoo_data.get("price_history")

        # === MERGE PROFILE from best source ===
        merged_profile = self._merge_profile(company_details, profile)

        # === BUILD UNIFIED RESULT ===
        result = {
            "symbol": symbol,
            "profile": merged_profile,
            "financials_quarterly": financials_quarterly,
            "financials_annual": financials_annual,
            "basic_financials": basic_financials,
            "price_history": price_history,
            "eps_estimates": eps_estimates,
            "price_target": price_target,
            "insider_transactions": insiders,
            "peers": peers or [],
            "analyst_recommendations": recommendations,
            "sources_used": {
                "polygon": self._polygon is not None,
                "finnhub": self._finnhub is not None,
                "yahoo_fallback": yahoo_used,
            },
            "data_quality": self._assess_quality(financials_quarterly, basic_financials, price_history),
        }

        _log.info(
            "event=company_data_complete symbol=%s quality=%s yahoo_fallback=%s",
            symbol, result["data_quality"], yahoo_used,
        )
        return result

    def _merge_profile(self, polygon_details, finnhub_profile) -> dict:
        """Merge company profile from multiple sources, preferring non-None values."""
        merged = {
            "company_name": None,
            "sector": None,
            "industry": None,
            "market_cap": None,
            "employees": None,
            "description": None,
            "website": None,
            "country": None,
        }

        # Finnhub profile (preferred for sector, name)
        if finnhub_profile and not finnhub_profile.get("error"):
            merged["company_name"] = finnhub_profile.get("company_name")
            merged["sector"] = finnhub_profile.get("sector")
            merged["market_cap"] = finnhub_profile.get("market_cap")
            if merged["market_cap"]:
                merged["market_cap"] *= 1_000_000  # Finnhub returns in millions
            merged["website"] = finnhub_profile.get("website")
            merged["country"] = finnhub_profile.get("country")

        # Polygon fills gaps
        if polygon_details and not polygon_details.get("error"):
            if not merged["company_name"]:
                merged["company_name"] = polygon_details.get("company_name")
            if not merged["sector"]:
                merged["sector"] = polygon_details.get("sector")
            if not merged["market_cap"]:
                merged["market_cap"] = polygon_details.get("market_cap")
            if not merged["description"]:
                merged["description"] = polygon_details.get("description")
            if not merged["employees"]:
                merged["employees"] = polygon_details.get("employees")
            if not merged["website"]:
                merged["website"] = polygon_details.get("homepage")

        return merged

    def _assess_quality(self, financials, metrics, prices) -> str:
        """Rate data completeness across key sources."""
        score = 0
        if financials and not financials.get("error"):
            score += 1
        if metrics and not metrics.get("error"):
            score += 1
        if prices and not prices.get("error"):
            score += 1

        if score == 3:
            return "full"
        if score >= 2:
            return "good"
        if score >= 1:
            return "partial"
        return "degraded"

    async def _safe(self, name: str, func, *args, **kwargs):
        """Call a data fetch function with error handling."""
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            _log.warning("event=data_fetch_failed source=%s error=%s", name, exc)
            return {"error": str(exc)}

    async def _fetch_yahoo_fallback(self, symbol: str) -> dict | None:
        """Yahoo Finance fallback — only used when primary sources fail."""
        try:
            from data.yahoo_client import YahooFinanceClient
            client = YahooFinanceClient(rate_limit=1.0)
            return await client.get_company_data(symbol)
        except Exception as exc:
            _log.warning("event=yahoo_fallback_failed symbol=%s error=%s", symbol, exc)
            return None

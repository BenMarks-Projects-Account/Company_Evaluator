"""Unified company data service — orchestrates all data sources."""

import asyncio
import logging
from datetime import datetime, timezone
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
        import time
        t0 = time.time()
        _log.info("[%s] DATA: Begin fetching from all sources...", symbol)
        yahoo_used = False
        fetched_at = datetime.now(timezone.utc).isoformat()
        source_attribution: dict[str, dict] = {}
        fetch_errors: list[dict] = []

        # === POLYGON: Financial statements + price history ===
        financials_quarterly = None
        financials_annual = None
        price_history = None
        company_details = None

        if self._polygon:
            _log.info("[%s] DATA: Fetching Polygon quarterly financials...", symbol)
            financials_quarterly = await self._safe(
                "polygon_financials_q",
                self._polygon.get_financials, symbol, limit=12, timeframe="quarterly",
                provider="polygon",
                endpoint="/vX/reference/financials",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Polygon annual financials...", symbol)
            financials_annual = await self._safe(
                "polygon_financials_a",
                self._polygon.get_financials, symbol, limit=8, timeframe="annual",
                provider="polygon",
                endpoint="/vX/reference/financials",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Polygon price history...", symbol)
            price_history = await self._safe(
                "polygon_prices",
                self._polygon.get_price_history, symbol, days=365,
                provider="polygon",
                endpoint="/v2/aggs/ticker/{symbol}/range/1/day",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Polygon company details...", symbol)
            company_details = await self._safe(
                "polygon_details",
                self._polygon.get_company_details, symbol,
                provider="polygon",
                endpoint="/v3/reference/tickers/{symbol}",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
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
            _log.info("[%s] DATA: Fetching Finnhub basic financials (117 ratios)...", symbol)
            basic_financials = await self._safe(
                "finnhub_metrics", self._finnhub.get_basic_financials, symbol,
                provider="finnhub",
                endpoint="/stock/metric",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub company profile...", symbol)
            profile = await self._safe(
                "finnhub_profile", self._finnhub.get_company_profile, symbol,
                provider="finnhub",
                endpoint="/stock/profile2",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub EPS estimates...", symbol)
            eps_estimates = await self._safe(
                "finnhub_estimates", self._finnhub.get_eps_estimates, symbol,
                provider="finnhub",
                endpoint="/stock/eps-estimate",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub price target...", symbol)
            price_target = await self._safe(
                "finnhub_target", self._finnhub.get_price_target, symbol,
                provider="finnhub",
                endpoint="/stock/price-target",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub insider transactions...", symbol)
            insiders = await self._safe(
                "finnhub_insiders", self._finnhub.get_insider_transactions, symbol,
                provider="finnhub",
                endpoint="/stock/insider-transactions",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub peers...", symbol)
            peers = await self._safe(
                "finnhub_peers", self._finnhub.get_peers, symbol,
                provider="finnhub",
                endpoint="/stock/peers",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            _log.info("[%s] DATA: Fetching Finnhub recommendations...", symbol)
            recommendations = await self._safe(
                "finnhub_recs", self._finnhub.get_recommendation_trends, symbol,
                provider="finnhub",
                endpoint="/stock/recommendation",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )

        # === YAHOO FALLBACK: Only if critical data is missing ===
        if self._yahoo_enabled:
            if not financials_quarterly or financials_quarterly.get("error"):
                _log.info("event=yahoo_fallback symbol=%s reason=polygon_financials_failed", symbol)
                yahoo_data = await self._fetch_yahoo_fallback(symbol)
                if yahoo_data:
                    yahoo_used = True
                    source_attribution["yahoo_fallback"] = {
                        "provider": "yahoo",
                        "endpoint": "yfinance",
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "ok": True,
                        "used": True,
                    }
                    if not financials_quarterly or financials_quarterly.get("error"):
                        financials_quarterly = yahoo_data.get("income_statement")
                    if not price_history or price_history.get("error"):
                        price_history = yahoo_data.get("price_history")
                else:
                    fetch_errors.append({
                        "source": "yahoo",
                        "endpoint": "yfinance",
                        "error": "Yahoo fallback returned no data",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })

        # === MERGE PROFILE from best source ===
        merged_profile = self._merge_profile(company_details, profile)

        # === BUILD UNIFIED RESULT ===
        result = {
            "symbol": symbol,
            "fetched_at": fetched_at,
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
            "source_attribution": source_attribution,
            "fetch_errors": fetch_errors,
            "data_quality": self._assess_quality(financials_quarterly, basic_financials, price_history),
        }

        _log.info(
            "[%s] DATA: Complete in %.1fs — quality=%s yahoo_fallback=%s",
            symbol, time.time() - t0, result["data_quality"], yahoo_used,
        )
        return result

    def _merge_profile(self, polygon_details, finnhub_profile) -> dict:
        """Merge company profile from multiple sources.

        Polygon is PRIMARY (paid tier has reliable market_cap, description,
        employees). Finnhub supplements with sector/industry names.
        """
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

        # Polygon details (primary for name, market_cap, description, employees)
        if polygon_details and not polygon_details.get("error"):
            merged["company_name"] = polygon_details.get("company_name")
            merged["sector"] = polygon_details.get("sector")
            merged["market_cap"] = polygon_details.get("market_cap")
            merged["description"] = polygon_details.get("description")
            merged["employees"] = polygon_details.get("employees")
            merged["website"] = polygon_details.get("homepage")

        # Finnhub fills gaps (better sector names, country)
        if finnhub_profile and not finnhub_profile.get("error"):
            if not merged["company_name"]:
                merged["company_name"] = finnhub_profile.get("company_name")
            if not merged["sector"] or merged["sector"] == polygon_details.get("sector"):
                # Finnhub sector names are often more descriptive
                fh_sector = finnhub_profile.get("sector")
                if fh_sector:
                    merged["sector"] = fh_sector
            if not merged["market_cap"]:
                mc = finnhub_profile.get("market_cap")
                if mc:
                    merged["market_cap"] = mc * 1_000_000  # Finnhub returns millions
            if not merged["website"]:
                merged["website"] = finnhub_profile.get("website")
            merged["country"] = finnhub_profile.get("country")

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

    async def _safe(
        self,
        name: str,
        func,
        *args,
        provider: str | None = None,
        endpoint: str | None = None,
        source_attribution: dict | None = None,
        fetch_errors: list | None = None,
        **kwargs,
    ):
        """Call a data fetch function with error handling."""
        import time
        t = time.time()
        fetched_at = datetime.now(timezone.utc).isoformat()
        try:
            result = await func(*args, **kwargs)
            elapsed = time.time() - t
            ok = not (isinstance(result, dict) and result.get("error"))
            if source_attribution is not None:
                source_attribution[name] = {
                    "provider": provider,
                    "endpoint": endpoint,
                    "fetched_at": fetched_at,
                    "ok": ok,
                }
            if isinstance(result, dict) and result.get("error"):
                _log.warning("  └─ %s: FAILED in %.1fs — %s", name, elapsed, result["error"])
                if fetch_errors is not None:
                    fetch_errors.append({
                        "source": provider,
                        "endpoint": endpoint,
                        "error": result["error"],
                        "timestamp": fetched_at,
                    })
            else:
                _log.info("  └─ %s: OK in %.1fs", name, elapsed)
            return result
        except Exception as exc:
            _log.warning("  └─ %s: EXCEPTION in %.1fs — %s", name, time.time() - t, exc)
            if source_attribution is not None:
                source_attribution[name] = {
                    "provider": provider,
                    "endpoint": endpoint,
                    "fetched_at": fetched_at,
                    "ok": False,
                }
            if fetch_errors is not None:
                fetch_errors.append({
                    "source": provider,
                    "endpoint": endpoint,
                    "error": str(exc),
                    "timestamp": fetched_at,
                })
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

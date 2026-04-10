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

        # FMP — statement fallback when Polygon is empty
        from data.fmp_client import FMPClient
        self._fmp = (
            FMPClient(
                api_key=settings.fmp_api_key,
                base_url=settings.fmp_base_url,
                rate_limit_per_min=settings.fmp_rate_limit_per_min,
            )
            if settings.fmp_enabled and settings.fmp_api_key
            else None
        )

    async def get_company_data(self, symbol: str) -> dict:
        """Fetch ALL data needed for company evaluation."""
        import time
        t0 = time.time()
        _log.info("[%s] DATA: Begin fetching from all sources...", symbol)
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

        # === FINNHUB: Ratios, profile, insiders, recommendations ===
        basic_financials = None
        profile = None
        insiders = None
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
            _log.info("[%s] DATA: Fetching Finnhub insider transactions...", symbol)
            insiders = await self._safe(
                "finnhub_insiders", self._finnhub.get_insider_transactions, symbol,
                provider="finnhub",
                endpoint="/stock/insider-transactions",
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

        # === FMP FALLBACK: Only if Polygon financials are empty ===
        q_patched_fmp = False
        a_patched_fmp = False
        if self._fmp:
            q_empty = _is_empty_financials(financials_quarterly)
            a_empty = _is_empty_financials(financials_annual)

            if q_empty or a_empty:
                _log.info(
                    "event=fmp_fallback symbol=%s reason=polygon_empty "
                    "q_empty=%s a_empty=%s",
                    symbol, q_empty, a_empty,
                )
                from data.fmp_normalizer import normalize_fmp_to_scorer_shape

                if q_empty:
                    fmp_q_raw = await self._safe(
                        "fmp_financials_q",
                        self._fmp.get_full_financials, symbol, period="quarter", limit=12,
                        provider="fmp",
                        endpoint="/v3/income-statement+balance-sheet+cash-flow (quarter)",
                        source_attribution=source_attribution,
                        fetch_errors=fetch_errors,
                    )
                    if fmp_q_raw and not fmp_q_raw.get("error"):
                        normalized = normalize_fmp_to_scorer_shape(fmp_q_raw)
                        if normalized.get("statements"):
                            financials_quarterly = normalized
                            q_patched_fmp = True
                            _log.info(
                                "[%s] FMP quarterly fallback: %d statements",
                                symbol, len(normalized["statements"]),
                            )

                if a_empty:
                    fmp_a_raw = await self._safe(
                        "fmp_financials_a",
                        self._fmp.get_full_financials, symbol, period="annual", limit=10,
                        provider="fmp",
                        endpoint="/v3/income-statement+balance-sheet+cash-flow (annual)",
                        source_attribution=source_attribution,
                        fetch_errors=fetch_errors,
                    )
                    if fmp_a_raw and not fmp_a_raw.get("error"):
                        normalized = normalize_fmp_to_scorer_shape(fmp_a_raw)
                        if normalized.get("statements"):
                            financials_annual = normalized
                            a_patched_fmp = True
                            _log.info(
                                "[%s] FMP annual fallback: %d statements",
                                symbol, len(normalized["statements"]),
                            )

        # === YAHOO OWNERSHIP ENRICHMENT (lightweight) ===
        yahoo_ownership = None
        if self._yahoo_enabled:
            yahoo_ownership = await self._fetch_yahoo_ownership(symbol)

        # === FMP SMART MONEY: Insider transactions + Institutional ownership ===
        smart_money = None
        if self._fmp:
            try:
                from data.smart_money_analyzer import (
                    analyze_insider_activity,
                    analyze_institutional_ownership,
                )

                _log.info("[%s] DATA: Fetching FMP insider trading...", symbol)
                insider_txns = await self._safe(
                    "fmp_insider_trading", self._fmp.get_insider_trading, symbol,
                    provider="fmp", endpoint="/insider-trading/search",
                    source_attribution=source_attribution,
                    fetch_errors=fetch_errors,
                )
                insider_stats = await self._safe(
                    "fmp_insider_stats", self._fmp.get_insider_trading_statistics, symbol,
                    provider="fmp", endpoint="/insider-trading/statistics",
                    source_attribution=source_attribution,
                    fetch_errors=fetch_errors,
                )
                _log.info("[%s] DATA: Fetching FMP institutional ownership...", symbol)
                institutional = await self._safe(
                    "fmp_institutional", self._fmp.get_institutional_ownership, symbol,
                    provider="fmp", endpoint="/institutional-ownership/symbol-positions-summary",
                    source_attribution=source_attribution,
                    fetch_errors=fetch_errors,
                )

                insider_analysis = analyze_insider_activity(
                    insider_txns or [], insider_stats or [], lookback_days=180,
                )
                institutional_analysis = analyze_institutional_ownership(
                    institutional or [],
                )

                smart_money = {
                    "insider_activity": insider_analysis,
                    "institutional_ownership": institutional_analysis,
                    "_source": "fmp",
                }
                _log.info(
                    "[%s] Smart money: insider=%s(%d txns) institutional=%s",
                    symbol,
                    insider_analysis.get("signal"),
                    insider_analysis.get("transaction_count", 0),
                    institutional_analysis.get("trend"),
                )
            except Exception as exc:
                _log.warning("event=smart_money_failed symbol=%s error=%s", symbol, exc)

        # === INSIDER DATA: prefer FMP smart money, fall back to Finnhub ===
        if smart_money and smart_money.get("insider_activity", {}).get("score") is not None:
            insider_signal = smart_money["insider_activity"]["signal"]
            insiders = {
                "net_activity": _signal_to_legacy(insider_signal),
                "transaction_count": smart_money["insider_activity"]["transaction_count"],
                "buys": smart_money["insider_activity"]["buy_count"],
                "sells": smart_money["insider_activity"]["sell_count"],
                "buy_value": smart_money["insider_activity"]["buy_value"],
                "sell_value": smart_money["insider_activity"]["sell_value"],
                "net_shares_180d": smart_money["insider_activity"]["net_shares"],
                "_source": "fmp_smart_money",
            }

        # === MERGE PROFILE from best source ===
        merged_profile = self._merge_profile(company_details, profile, yahoo_ownership)

        # === BUILD UNIFIED RESULT ===
        result = {
            "symbol": symbol,
            "fetched_at": fetched_at,
            "profile": merged_profile,
            "financials_quarterly": financials_quarterly,
            "financials_annual": financials_annual,
            "basic_financials": basic_financials,
            "price_history": price_history,
            "insider_transactions": insiders,
            "smart_money": smart_money,
            "analyst_recommendations": recommendations,
            "sources_used": {
                "polygon": self._polygon is not None,
                "finnhub": self._finnhub is not None,
                "fmp_fallback": q_patched_fmp or a_patched_fmp,
                "financials_quarterly": "fmp (polygon empty)" if q_patched_fmp else "polygon",
                "financials_annual": "fmp (polygon empty)" if a_patched_fmp else "polygon",
                "insider_transactions": "fmp_smart_money" if (smart_money and smart_money.get("insider_activity", {}).get("score") is not None) else "finnhub",
                "smart_money": "fmp" if smart_money else None,
            },
            "source_attribution": source_attribution,
            "fetch_errors": fetch_errors,
            "data_quality": self._assess_quality(financials_quarterly, basic_financials, price_history),
        }

        _log.info(
            "[%s] DATA: Complete in %.1fs — quality=%s fmp_fallback=%s",
            symbol, time.time() - t0, result["data_quality"],
            q_patched_fmp or a_patched_fmp,
        )
        return result

    def _merge_profile(self, polygon_details, finnhub_profile, yahoo_ownership=None) -> dict:
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
            "exchange": None,
            "shares_outstanding": None,
            "institutional_ownership_pct": None,
            "insider_ownership_pct": None,
        }

        pg = polygon_details if polygon_details and not polygon_details.get("error") else {}
        fh = finnhub_profile if finnhub_profile and not finnhub_profile.get("error") else {}

        # Polygon details (primary for name, market_cap, description, employees)
        if pg:
            merged["company_name"] = pg.get("company_name")
            merged["sector"] = pg.get("sector")
            merged["market_cap"] = pg.get("market_cap")
            merged["description"] = pg.get("description")
            merged["employees"] = pg.get("employees")
            merged["website"] = pg.get("homepage")
            merged["exchange"] = pg.get("primary_exchange")

        # Finnhub fills gaps (better sector names, country)
        if fh:
            if not merged["company_name"]:
                merged["company_name"] = fh.get("company_name")
            if not merged["sector"] or merged["sector"] == pg.get("sector"):
                fh_sector = fh.get("sector")
                if fh_sector:
                    merged["sector"] = fh_sector
            if not merged["market_cap"]:
                mc = fh.get("market_cap")
                if mc:
                    merged["market_cap"] = mc * 1_000_000  # Finnhub returns millions
            if not merged["website"]:
                merged["website"] = fh.get("website")
            merged["country"] = fh.get("country")
            if not merged["exchange"]:
                merged["exchange"] = fh.get("exchange")

        # Industry: prefer Polygon SIC description, fall back to Finnhub
        merged["industry"] = pg.get("sector") or fh.get("sector")

        # Shares outstanding: prefer Finnhub (always present), no Polygon equivalent in this endpoint
        fh_shares = fh.get("shares_outstanding")
        if fh_shares:
            merged["shares_outstanding"] = fh_shares * 1_000_000  # Finnhub uses millions

        # Yahoo ownership enrichment
        if yahoo_ownership:
            merged["institutional_ownership_pct"] = yahoo_ownership.get("institutional_ownership_pct")
            merged["insider_ownership_pct"] = yahoo_ownership.get("insider_ownership_pct")

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

    async def _fetch_yahoo_ownership(self, symbol: str) -> dict | None:
        """Lightweight Yahoo fetch for ownership stats only."""
        try:
            import asyncio
            import yfinance as yf

            loop = asyncio.get_event_loop()
            ticker = await loop.run_in_executor(None, yf.Ticker, symbol)
            info = await loop.run_in_executor(None, lambda: ticker.info or {})
            result = {
                "institutional_ownership_pct": info.get("heldPercentInstitutions"),
                "insider_ownership_pct": info.get("heldPercentInsiders"),
            }
            if result["institutional_ownership_pct"] is not None or result["insider_ownership_pct"] is not None:
                _log.info("[%s] Yahoo ownership: inst=%.1f%% insider=%.1f%%",
                          symbol,
                          (result["institutional_ownership_pct"] or 0) * 100,
                          (result["insider_ownership_pct"] or 0) * 100)
                return result
            return None
        except Exception as exc:
            _log.warning("event=yahoo_ownership_failed symbol=%s error=%s", symbol, exc)
            return None


def _is_empty_financials(fin: dict | None) -> bool:
    """Return True if financials data is missing, errored, or has no statements."""
    if not fin or fin.get("error"):
        return True
    stmts = fin.get("statements", [])
    return len(stmts) == 0


def _signal_to_legacy(signal: str) -> str:
    """Map FMP smart money signal to legacy net_activity format."""
    return {
        "strong_buying": "net_buying",
        "buying": "net_buying",
        "routine_selling": "neutral",
        "elevated_selling": "net_selling",
        "heavy_selling": "net_selling",
        "no_activity": "neutral",
        "no_data": "unknown",
    }.get(signal, "unknown")

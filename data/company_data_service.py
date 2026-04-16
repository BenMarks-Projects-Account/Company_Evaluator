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
        from data.data_source_router import get_router

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
        self._fmp = None
        if settings.fmp_enabled and settings.fmp_api_key:
            raw_fmp = FMPClient(
                api_key=settings.fmp_api_key,
                base_url=settings.fmp_base_url,
                rate_limit_per_min=settings.fmp_rate_limit_per_min,
            )
            # Wrap with bulk cache if available
            if settings.enable_bulk_cache:
                from pathlib import Path
                cache_db = Path(settings.database_path).parent / "company_eval_bulk.db"
                if cache_db.exists():
                    from bulk.bulk_cache import BulkCache
                    from bulk.cache_lookup import BulkCacheLookup
                    from bulk.cached_fmp_client import CachedFMPClient
                    cache = BulkCache(str(cache_db))
                    lookup = BulkCacheLookup(cache)
                    if lookup.is_available():
                        self._fmp = CachedFMPClient(raw_fmp, lookup)
                        _log.info("CompanyDataService FMP using bulk cache")
                    else:
                        self._fmp = raw_fmp
                else:
                    self._fmp = raw_fmp
            else:
                self._fmp = raw_fmp

        self._router = get_router()

    async def get_company_data(self, symbol: str) -> dict:
        """Fetch ALL data needed for company evaluation.

        Phase 1 fires every independent fetch concurrently via
        ``asyncio.gather`` (Polygon financials/prices/details, Finnhub
        metrics/profile/insiders/recs, Yahoo ownership, FMP insider
        trading + stats + profile). Phase 2 runs the FMP financials
        fallback only if Polygon returned empty. Phase 3 is purely
        local processing (smart-money analysis + profile merge).

        Every fetch is still wrapped in ``_safe``, so individual
        failures return ``{"error": ...}`` / ``None`` instead of
        cancelling the whole batch.
        """
        import time
        t0 = time.time()
        _log.info("[%s] DATA: Begin fetching from all sources (parallel)...", symbol)
        fetched_at = datetime.now(timezone.utc).isoformat()
        source_attribution: dict[str, dict] = {}
        fetch_errors: list[dict] = []

        # ── Phase 1: all independent fetches ──────────────────────
        tasks: dict[str, object] = {}

        if self._polygon:
            tasks["financials_quarterly"] = self._safe(
                "polygon_financials_q",
                self._routed_get_financials, symbol, limit=12, timeframe="quarterly",
                provider="polygon",
                endpoint="/vX/reference/financials",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["financials_annual"] = self._safe(
                "polygon_financials_a",
                self._routed_get_financials, symbol, limit=8, timeframe="annual",
                provider="polygon",
                endpoint="/vX/reference/financials",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["price_history"] = self._safe(
                "polygon_prices",
                self._routed_get_price_history, symbol, days=365,
                provider="polygon",
                endpoint="/v2/aggs/ticker/{symbol}/range/1/day",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["company_details"] = self._safe(
                "polygon_details",
                self._routed_get_company_details, symbol,
                provider="polygon",
                endpoint="/v3/reference/tickers/{symbol}",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )

        if self._finnhub:
            tasks["basic_financials"] = self._safe(
                "finnhub_metrics", self._finnhub.get_basic_financials, symbol,
                provider="finnhub",
                endpoint="/stock/metric",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["profile"] = self._safe(
                "finnhub_profile", self._finnhub.get_company_profile, symbol,
                provider="finnhub",
                endpoint="/stock/profile2",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["insiders"] = self._safe(
                "finnhub_insiders", self._finnhub.get_insider_transactions, symbol,
                provider="finnhub",
                endpoint="/stock/insider-transactions",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["recommendations"] = self._safe(
                "finnhub_recs", self._finnhub.get_recommendation_trends, symbol,
                provider="finnhub",
                endpoint="/stock/recommendation",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )

        if self._yahoo_enabled:
            tasks["yahoo_ownership"] = self._fetch_yahoo_ownership(symbol)

        if self._fmp:
            tasks["fmp_insider_txns"] = self._safe(
                "fmp_insider_trading", self._fmp.get_insider_trading, symbol,
                provider="fmp", endpoint="/insider-trading/search",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["fmp_insider_stats"] = self._safe(
                "fmp_insider_stats", self._fmp.get_insider_trading_statistics, symbol,
                provider="fmp", endpoint="/insider-trading/statistics",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )
            tasks["fmp_profile"] = self._safe(
                "fmp_profile", self._fmp.get_company_profile, symbol,
                provider="fmp",
                endpoint="/v3/profile/{symbol}",
                source_attribution=source_attribution,
                fetch_errors=fetch_errors,
            )

        keys = list(tasks.keys())
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        bundle: dict = {}
        for key, res in zip(keys, results):
            if isinstance(res, Exception):
                _log.warning("[%s] DATA: gather task %s raised: %s", symbol, key, res)
                bundle[key] = None
            else:
                bundle[key] = res

        financials_quarterly = bundle.get("financials_quarterly")
        financials_annual = bundle.get("financials_annual")
        price_history = bundle.get("price_history")
        company_details = bundle.get("company_details")
        basic_financials = bundle.get("basic_financials")
        profile = bundle.get("profile")
        insiders = bundle.get("insiders")
        recommendations = bundle.get("recommendations")
        yahoo_ownership = bundle.get("yahoo_ownership")
        insider_txns = bundle.get("fmp_insider_txns")
        insider_stats = bundle.get("fmp_insider_stats")
        fmp_profile = bundle.get("fmp_profile")

        # Institutional ownership: FMP endpoint short-circuited (2026-04-16;
        # see FMPClient.get_institutional_ownership). Keep variable None so
        # downstream smart-money analysis falls through cleanly.
        institutional = None

        _log.info(
            "[%s] DATA: Phase 1 parallel fetch complete in %.1fs",
            symbol, time.time() - t0,
        )

        # ── Phase 2: FMP financials fallback (depends on Polygon result) ──
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

                fallback_tasks: dict[str, object] = {}
                if q_empty:
                    fallback_tasks["q"] = self._safe(
                        "fmp_financials_q",
                        self._fmp.get_full_financials, symbol, period="quarter", limit=12,
                        provider="fmp",
                        endpoint="/v3/income-statement+balance-sheet+cash-flow (quarter)",
                        source_attribution=source_attribution,
                        fetch_errors=fetch_errors,
                    )
                if a_empty:
                    fallback_tasks["a"] = self._safe(
                        "fmp_financials_a",
                        self._fmp.get_full_financials, symbol, period="annual", limit=10,
                        provider="fmp",
                        endpoint="/v3/income-statement+balance-sheet+cash-flow (annual)",
                        source_attribution=source_attribution,
                        fetch_errors=fetch_errors,
                    )
                fb_keys = list(fallback_tasks.keys())
                fb_results = await asyncio.gather(*fallback_tasks.values(), return_exceptions=True)
                for key, res in zip(fb_keys, fb_results):
                    if isinstance(res, Exception) or res is None or (isinstance(res, dict) and res.get("error")):
                        continue
                    normalized = normalize_fmp_to_scorer_shape(res)
                    if not normalized.get("statements"):
                        continue
                    if key == "q":
                        financials_quarterly = normalized
                        q_patched_fmp = True
                        _log.info(
                            "[%s] FMP quarterly fallback: %d statements",
                            symbol, len(normalized["statements"]),
                        )
                    elif key == "a":
                        financials_annual = normalized
                        a_patched_fmp = True
                        _log.info(
                            "[%s] FMP annual fallback: %d statements",
                            symbol, len(normalized["statements"]),
                        )

        # ── Phase 3: local post-processing (smart-money + profile merge) ──
        smart_money = None
        if self._fmp:
            try:
                from data.smart_money_analyzer import (
                    analyze_insider_activity,
                    analyze_institutional_ownership,
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
        merged_profile = self._merge_profile(company_details, profile, yahoo_ownership, fmp_profile=fmp_profile)

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

    def _merge_profile(self, polygon_details, finnhub_profile, yahoo_ownership=None, fmp_profile=None) -> dict:
        """Merge company profile from multiple sources.

        Polygon is PRIMARY (paid tier has reliable market_cap, description,
        employees). FMP provides clean sector/industry labels.
        Finnhub supplements with country and fallback fields.
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
        fmp = fmp_profile if fmp_profile and not fmp_profile.get("error") else {}

        # Polygon details (primary for name, market_cap, description, employees)
        if pg:
            merged["company_name"] = pg.get("company_name")
            merged["market_cap"] = pg.get("market_cap")
            merged["description"] = pg.get("description")
            merged["employees"] = pg.get("employees")
            merged["website"] = pg.get("homepage")
            merged["exchange"] = pg.get("primary_exchange")

        # Sector & Industry: FMP first (clean labels), then Finnhub, then Polygon SIC
        fmp_sector = fmp.get("sector") if fmp else None
        fmp_industry = fmp.get("industry") if fmp else None
        fh_sector = fh.get("sector") if fh else None  # finnhubIndustry
        pg_sic = pg.get("sector") if pg else None  # raw SIC description

        merged["sector"] = fmp_sector or fh_sector or pg_sic or None
        merged["industry"] = fmp_industry or fh_sector or pg_sic or None

        # Finnhub fills gaps (name, market_cap, country)
        if fh:
            if not merged["company_name"]:
                merged["company_name"] = fh.get("company_name")
            if not merged["market_cap"]:
                mc = fh.get("market_cap")
                if mc:
                    merged["market_cap"] = mc * 1_000_000  # Finnhub returns millions
            if not merged["website"]:
                merged["website"] = fh.get("website")
            merged["country"] = fh.get("country")
            if not merged["exchange"]:
                merged["exchange"] = fh.get("exchange")

        # FMP fills remaining gaps
        if fmp:
            if not merged["company_name"]:
                merged["company_name"] = fmp.get("company_name")
            if not merged["employees"]:
                emp = fmp.get("employees")
                if emp:
                    try:
                        merged["employees"] = int(emp)
                    except (TypeError, ValueError):
                        pass
            if not merged["country"]:
                merged["country"] = fmp.get("country")

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

    # ── FMP adapter helpers ────────────────────────────────────

    async def _fmp_get_financials(self, symbol: str, limit: int = 12, timeframe: str = "quarterly"):
        """Adapter: FMP get_full_financials + normalizer → Polygon shape."""
        from data.fmp_normalizer import normalize_fmp_to_scorer_shape
        period = "quarter" if timeframe == "quarterly" else "annual"
        raw = await self._fmp.get_full_financials(symbol, period=period, limit=limit)
        if not raw:
            return {"error": "No financial data returned", "results": []}
        normalized = normalize_fmp_to_scorer_shape(raw)
        # Add wrapper fields that Polygon includes at top level
        stmts = normalized.get("statements", [])
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(stmts),
            "statements": stmts,
        }

    async def _fmp_get_price_history(self, symbol: str, days: int = 365):
        """Adapter: FMP EOD bars → Polygon get_price_history derived-stats shape."""
        import numpy as np
        from datetime import date, timedelta

        end = date.today()
        start = end - timedelta(days=days)
        bars = await self._fmp.get_historical_price_eod(
            symbol, from_date=start.isoformat(), to_date=end.isoformat()
        )
        if not bars:
            return {"error": "No price history returned"}

        closes = [b["close"] for b in bars if b.get("close") is not None]
        highs = [b["high"] for b in bars if b.get("high") is not None]
        lows = [b["low"] for b in bars if b.get("low") is not None]
        volumes = [b["volume"] for b in bars if b.get("volume") is not None]

        if len(closes) < 2:
            return {"error": "Insufficient price data"}

        daily_returns = np.diff(closes) / np.array(closes[:-1])

        # Max drawdown: peak-to-trough
        peak = closes[0]
        max_dd = 0.0
        for c in closes:
            if c > peak:
                peak = c
            dd = (c - peak) / peak
            if dd < max_dd:
                max_dd = dd

        std = float(np.std(daily_returns))
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
            "max_drawdown": float(max_dd),
            "sharpe_approx": float(np.mean(daily_returns) / std * np.sqrt(252)) if std > 0 else None,
        }

    # ── Routed Polygon call-site wrappers ────────────────────
    async def _routed_get_financials(self, symbol: str, **kwargs):
        return await self._router.route(
            "company_data_service.get_financials",
            self._polygon.get_financials,
            self._fmp_get_financials if self._fmp else None,
            symbol, **kwargs,
        )

    async def _routed_get_price_history(self, symbol: str, **kwargs):
        return await self._router.route(
            "company_data_service.get_price_history",
            self._polygon.get_price_history,
            self._fmp_get_price_history if self._fmp else None,
            symbol, **kwargs,
        )

    async def _routed_get_company_details(self, symbol: str):
        return await self._router.route(
            "company_data_service.get_company_details",
            self._polygon.get_company_details,
            self._fmp_get_company_details if self._fmp else None,
            symbol,
        )

    async def _fmp_get_company_details(self, symbol: str):
        """Adapter: FMP get_company_profile → Polygon get_company_details shape."""
        raw = await self._fmp.get_company_profile(symbol)
        if not raw:
            return None
        return {
            "symbol": raw.get("symbol"),
            "company_name": raw.get("company_name"),
            "market_cap": raw.get("market_cap"),
            "sector": raw.get("sector"),
            "primary_exchange": raw.get("exchange"),
            "description": raw.get("description"),
            "homepage": raw.get("website"),
            "employees": raw.get("employees"),
            "list_date": None,  # FMP doesn't provide IPO date on this endpoint
            "locale": None,     # FMP doesn't provide locale
            "type": None,       # FMP doesn't provide security type
        }

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

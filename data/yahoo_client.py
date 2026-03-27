"""Yahoo Finance data client for company fundamental data."""

import asyncio
import logging
import time
from datetime import datetime, date, timedelta
from typing import Optional
import yfinance as yf
import pandas as pd

_log = logging.getLogger(__name__)


class YahooFinanceClient:
    """Fetches fundamental data for company evaluation."""
    
    def __init__(self, rate_limit: float = 2.0):
        self._min_interval = 1.0 / rate_limit
        self._last_request = 0.0
        self._cache = {}
        self._cache_ttl = 3600  # 1 hour cache for financial data
    
    async def get_company_data(self, symbol: str) -> dict:
        """Fetch ALL data needed for company evaluation.
        
        Returns a comprehensive dict with all financial data,
        or partial data with errors noted if some fetches fail.
        """
        _log.info("event=fetch_company_start symbol=%s", symbol)
        start = time.time()
        errors = []
        
        # Run synchronous yfinance calls in executor
        loop = asyncio.get_event_loop()
        
        try:
            ticker = await loop.run_in_executor(None, self._get_ticker, symbol)
        except Exception as exc:
            _log.error("event=ticker_failed symbol=%s error=%s", symbol, exc)
            return {"symbol": symbol, "error": str(exc), "data": None}
        
        # Fetch each data type with individual error handling
        profile = await loop.run_in_executor(None, self._fetch_profile, ticker, symbol)
        income = await loop.run_in_executor(None, self._fetch_income_statement, ticker, symbol)
        balance = await loop.run_in_executor(None, self._fetch_balance_sheet, ticker, symbol)
        cashflow = await loop.run_in_executor(None, self._fetch_cash_flow, ticker, symbol)
        stats = await loop.run_in_executor(None, self._fetch_key_stats, ticker, symbol)
        prices = await loop.run_in_executor(None, self._fetch_price_history, ticker, symbol)
        insiders = await loop.run_in_executor(None, self._fetch_insider_transactions, ticker, symbol)
        
        # Collect errors
        for dataset_name, dataset in [("profile", profile), ("income", income), 
                                        ("balance", balance), ("cashflow", cashflow),
                                        ("stats", stats), ("prices", prices), ("insiders", insiders)]:
            if dataset and dataset.get("error"):
                errors.append(f"{dataset_name}: {dataset['error']}")
        
        elapsed = time.time() - start
        _log.info("event=fetch_company_complete symbol=%s elapsed_s=%.1f errors=%d", symbol, elapsed, len(errors))
        
        return {
            "symbol": symbol,
            "fetched_at": datetime.now().isoformat(),
            "fetch_duration_s": round(elapsed, 1),
            "profile": profile,
            "income_statement": income,
            "balance_sheet": balance,
            "cash_flow": cashflow,
            "key_stats": stats,
            "price_history": prices,
            "insider_transactions": insiders,
            "errors": errors,
            "data_quality": "full" if len(errors) == 0 else "partial" if len(errors) < 4 else "degraded",
        }
    
    def _get_ticker(self, symbol: str) -> yf.Ticker:
        self._rate_limit()
        return yf.Ticker(symbol)
    
    def _fetch_profile(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            info = ticker.info or {}
            return {
                "company_name": info.get("longName") or info.get("shortName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "description": info.get("longBusinessSummary"),
                "employees": info.get("fullTimeEmployees"),
                "country": info.get("country"),
                "website": info.get("website"),
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_income_statement(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            quarterly = ticker.quarterly_income_stmt
            annual = ticker.income_stmt
            
            return {
                "quarterly": self._df_to_records(quarterly) if quarterly is not None else [],
                "annual": self._df_to_records(annual) if annual is not None else [],
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_balance_sheet(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            quarterly = ticker.quarterly_balance_sheet
            annual = ticker.balance_sheet
            
            return {
                "quarterly": self._df_to_records(quarterly) if quarterly is not None else [],
                "annual": self._df_to_records(annual) if annual is not None else [],
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_cash_flow(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            quarterly = ticker.quarterly_cashflow
            annual = ticker.cashflow
            
            return {
                "quarterly": self._df_to_records(quarterly) if quarterly is not None else [],
                "annual": self._df_to_records(annual) if annual is not None else [],
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_key_stats(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            info = ticker.info or {}
            
            return {
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "peg_ratio": info.get("pegRatio"),
                "price_to_book": info.get("priceToBook"),
                "price_to_sales": info.get("priceToSalesTrailing12Months"),
                "ev_to_ebitda": info.get("enterpriseToEbitda"),
                "ev_to_revenue": info.get("enterpriseToRevenue"),
                "profit_margin": info.get("profitMargins"),
                "operating_margin": info.get("operatingMargins"),
                "gross_margin": info.get("grossMargins"),
                "return_on_equity": info.get("returnOnEquity"),
                "return_on_assets": info.get("returnOnAssets"),
                "revenue_growth": info.get("revenueGrowth"),
                "earnings_growth": info.get("earningsGrowth"),
                "dividend_yield": info.get("dividendYield"),
                "payout_ratio": info.get("payoutRatio"),
                "beta": info.get("beta"),
                "short_percent_of_float": info.get("shortPercentOfFloat"),
                "insider_percent_held": info.get("heldPercentInsiders"),
                "institutional_percent_held": info.get("heldPercentInstitutions"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "float_shares": info.get("floatShares"),
                "book_value": info.get("bookValue"),
                "current_price": info.get("currentPrice") or info.get("regularMarketPrice"),
                "target_mean_price": info.get("targetMeanPrice"),
                "recommendation_mean": info.get("recommendationMean"),
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_price_history(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            hist = ticker.history(period="1y")
            
            if hist.empty:
                return {"error": "No price history available"}
            
            return {
                "start_date": hist.index[0].strftime("%Y-%m-%d"),
                "end_date": hist.index[-1].strftime("%Y-%m-%d"),
                "data_points": len(hist),
                "current_price": float(hist["Close"].iloc[-1]),
                "year_high": float(hist["High"].max()),
                "year_low": float(hist["Low"].min()),
                "year_return": float((hist["Close"].iloc[-1] / hist["Close"].iloc[0]) - 1),
                "avg_volume": int(hist["Volume"].mean()),
                "volatility_annualized": float(hist["Close"].pct_change().std() * (252 ** 0.5)),
                "max_drawdown": float(self._compute_max_drawdown(hist["Close"])),
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _fetch_insider_transactions(self, ticker, symbol) -> dict:
        try:
            self._rate_limit()
            insiders = ticker.insider_transactions
            
            if insiders is None or insiders.empty:
                return {"transactions": [], "net_activity": "unknown"}
            
            # Summarize last 6 months
            recent = insiders.head(20)  # Most recent 20 transactions
            
            buys = len(recent[recent["Text"].str.contains("Purchase|Buy", case=False, na=False)]) if "Text" in recent.columns else 0
            sells = len(recent[recent["Text"].str.contains("Sale|Sell", case=False, na=False)]) if "Text" in recent.columns else 0
            
            if buys > sells + 2:
                net_activity = "net_buying"
            elif sells > buys + 2:
                net_activity = "net_selling"
            else:
                net_activity = "neutral"
            
            return {
                "transaction_count": len(recent),
                "buys": buys,
                "sells": sells,
                "net_activity": net_activity,
            }
        except Exception as exc:
            return {"error": str(exc)}
    
    def _rate_limit(self):
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()
    
    def _df_to_records(self, df) -> list:
        """Convert a yfinance financial statement DataFrame to list of dicts."""
        if df is None or df.empty:
            return []
        
        records = []
        for col in df.columns:
            period_date = col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col)
            record = {"period": period_date}
            for idx in df.index:
                val = df.loc[idx, col]
                # Convert numpy types to Python native
                if pd.isna(val):
                    record[str(idx)] = None
                elif hasattr(val, "item"):
                    record[str(idx)] = val.item()
                else:
                    record[str(idx)] = val
            records.append(record)
        
        return records
    
    @staticmethod
    def _compute_max_drawdown(prices) -> float:
        """Compute maximum drawdown from a price series."""
        peak = prices.expanding(min_periods=1).max()
        drawdown = (prices - peak) / peak
        return float(drawdown.min())

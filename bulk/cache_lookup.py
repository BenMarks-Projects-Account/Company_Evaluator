"""
Cache lookup layer for FMP bulk data.

Translates between the BulkCache's SQLite row format and the
response shapes expected by FMPClient callers.  This is the
'shape adapter' that makes cache-vs-API transparent.

Key contracts (must match FMPClient return shapes):
  - get_company_profile  → dict (10 curated keys) or None
  - get_key_metrics_ttm  → dict (raw) or None
  - get_ratios_ttm       → dict (raw) or None
  - get_financial_growth  → dict (single row) or None
  - get_income_statement  → list[dict] or None
  - get_balance_sheet     → list[dict] or None
  - get_cash_flow_statement → list[dict] or None
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from .bulk_cache import BulkCache

logger = logging.getLogger(__name__)


class BulkCacheLookup:
    """Translate bulk cache rows to FMPClient response shapes."""

    def __init__(self, cache: BulkCache):
        self.cache = cache

    # ── Profile ──────────────────────────────────────────────

    def get_profile(self, symbol: str) -> Optional[dict]:
        """Return FMPClient.get_company_profile() shape: curated dict.

        Profile is paginated across 4 parts in bulk. Try each.
        FMPClient maps raw FMP fields to clean names; we do the same.
        """
        for part in range(4):
            row = self.cache.get_row_for_symbol("profile", symbol, part=part)
            if row:
                return {
                    "symbol": row.get("symbol"),
                    "company_name": row.get("companyName"),
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "market_cap": self._to_number(row.get("marketCap")),
                    "description": row.get("description"),
                    "employees": self._to_number(row.get("fullTimeEmployees")),
                    "website": row.get("website"),
                    "country": row.get("country"),
                    "exchange": row.get("exchange"),
                }
        return None

    # ── TTM metrics / ratios ─────────────────────────────────

    def get_key_metrics_ttm(self, symbol: str) -> Optional[dict]:
        """Return FMPClient.get_key_metrics_ttm() shape: raw dict."""
        row = self.cache.get_row_for_symbol("key_metrics_ttm", symbol)
        if row:
            return self._convert_types(row)
        return None

    def get_ratios_ttm(self, symbol: str) -> Optional[dict]:
        """Return FMPClient.get_ratios_ttm() shape: raw dict."""
        row = self.cache.get_row_for_symbol("ratios_ttm", symbol)
        if row:
            return self._convert_types(row)
        return None

    # ── Financial growth ─────────────────────────────────────

    # Mapping from bulk growth column names → FMP /financial-growth API keys.
    # The API merges income, balance-sheet, and cash-flow growth into one dict
    # with different naming conventions.
    _GROWTH_KEY_MAP = {
        # income_growth_annual →
        "growthRevenue": "revenueGrowth",
        "growthGrossProfit": "grossProfitGrowth",
        "growthOperatingIncome": "operatingIncomeGrowth",
        "growthNetIncome": "netIncomeGrowth",
        "growthEPS": "epsgrowth",
        "growthEPSDiluted": "epsdilutedGrowth",
        "growthWeightedAverageShsOut": "weightedAverageSharesGrowth",
        "growthWeightedAverageShsOutDil": "weightedAverageSharesDilutedGrowth",
        "growthResearchAndDevelopmentExpenses": "rdexpenseGrowth",
        "growthSellingAndMarketingExpenses": "sgaexpensesGrowth",
        "growthEBITDA": "ebitdaGrowth",
        "growthEBIT": "ebitgrowth",
        # balance_growth_annual →
        "growthTotalAssets": "assetGrowth",
        "growthTotalDebt": "debtGrowth",
        "growthInventory": "inventoryGrowth",
        "growthNetReceivables": "receivablesGrowth",
        "growthTotalStockholdersEquity": "bookValueperShareGrowth",
        # cash_flow_growth_annual →
        "growthFreeCashFlow": "freeCashFlowGrowth",
        "growthOperatingCashFlow": "operatingCashFlowGrowth",
        "growthCapitalExpenditure": "growthCapitalExpenditure",
        "growthDividendsPaid": "dividendsPerShareGrowth",
    }

    def get_financial_growth(self, symbol: str) -> Optional[dict]:
        """Return FMPClient.get_financial_growth() shape: single dict.

        The per-symbol API calls /financial-growth?period=annual&limit=1
        and returns data[0].  Bulk splits growth into three tables:
        income_growth_annual, balance_growth_annual, cash_flow_growth_annual.
        We merge the most recent year from all three and rename keys to match
        the API response shape.
        """
        current_year = datetime.now().year
        merged: dict = {}
        found_any = False

        for offset in range(5):
            year = current_year - offset
            rows_found = 0
            for table in (
                "income_growth_annual",
                "balance_growth_annual",
                "cash_flow_growth_annual",
            ):
                row = self.cache.get_row_for_symbol(table, symbol, year=year)
                if row:
                    rows_found += 1
                    for bulk_key, api_key in self._GROWTH_KEY_MAP.items():
                        if bulk_key in row and row[bulk_key] is not None:
                            merged[api_key] = row[bulk_key]
            if rows_found > 0:
                found_any = True
                # Carry over metadata from whatever row we found
                for table in (
                    "income_growth_annual",
                    "balance_growth_annual",
                    "cash_flow_growth_annual",
                ):
                    row = self.cache.get_row_for_symbol(table, symbol, year=year)
                    if row:
                        for meta_key in ("symbol", "date", "period", "fiscalYear", "reportedCurrency"):
                            if meta_key in row and meta_key not in merged:
                                merged[meta_key] = row[meta_key]
                        break
                break  # Use the most recent year that has data

        if not found_any:
            return None
        return self._convert_types(merged)

    # ── Financial statements (multi-year) ────────────────────

    def get_income_statement(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> Optional[List[dict]]:
        """Return list[dict] matching FMPClient.get_income_statement().

        Only serves annual from cache. Quarterly falls through to API.
        """
        if period != "annual":
            return None
        return self._get_multi_year(symbol, "income_statement_annual", limit)

    def get_balance_sheet(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> Optional[List[dict]]:
        if period != "annual":
            return None
        return self._get_multi_year(symbol, "balance_sheet_annual", limit)

    def get_cash_flow_statement(
        self, symbol: str, period: str = "quarter", limit: int = 12,
    ) -> Optional[List[dict]]:
        if period != "annual":
            return None
        return self._get_multi_year(symbol, "cash_flow_annual", limit)

    # ── Helpers ──────────────────────────────────────────────

    def _get_multi_year(
        self, symbol: str, endpoint_base: str, limit: int,
    ) -> Optional[List[dict]]:
        """Assemble multi-year statements from yearly cache tables.

        Returns list sorted newest-first (FMP's default ordering).
        """
        current_year = datetime.now().year
        results = []

        for offset in range(min(limit, 5)):  # bulk stores 5 years
            year = current_year - offset
            row = self.cache.get_row_for_symbol(
                endpoint_base, symbol, year=year
            )
            if row:
                results.append(self._convert_types(row))

        if not results:
            return None
        return results

    def _convert_types(self, row: Dict[str, Any]) -> dict:
        """Convert SQLite string values to appropriate Python types.

        Bulk CSV loads everything as strings. FMP per-symbol API
        returns proper types. Bridge the gap here.
        """
        result = {}
        for key, value in row.items():
            if value is None or value == "":
                result[key] = None
            elif isinstance(value, (int, float)):
                result[key] = value
            elif isinstance(value, str):
                result[key] = self._to_number(value) if self._looks_numeric(value) else value
            else:
                result[key] = value
        return result

    @staticmethod
    def _looks_numeric(value: str) -> bool:
        """Check if a string looks like a number."""
        if not value:
            return False
        v = value.strip()
        if not v:
            return False
        # Quick reject for obvious non-numbers
        if v[0].isalpha():
            return False
        try:
            float(v)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _to_number(value):
        """Convert a value to int or float if possible."""
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            try:
                if "." in v or "e" in v.lower():
                    return float(v)
                return int(v)
            except (ValueError, TypeError):
                return value
        return value

    # ── Observability ────────────────────────────────────────

    def is_available(self) -> bool:
        """Check if cache has any data at all."""
        summary = self.cache.get_refresh_summary()
        return summary.get("table_count", 0) > 0

    def is_stale(self, hours: int = 24) -> bool:
        """Check if cache needs refresh."""
        return self.cache.is_stale(hours=hours)

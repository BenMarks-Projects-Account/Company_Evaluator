"""Normalize FMP statement data to the scorer's expected flat shape.

The scorer expects::

    data["financials_quarterly"] = {
        "statements": [
            {
                "period": "Q4 2025",
                "fiscal_year": 2025,
                "fiscal_period": "Q4",
                "revenue": ...,
                "gross_profit": ...,
                # ... flat fields
                "_source": "fmp"
            }
        ]
    }

FMP returns three separate arrays (income, balance, cash_flow)
that need to be aligned by date and merged into flat period records.
"""

from __future__ import annotations

import logging

_log = logging.getLogger(__name__)

# ── FMP field name → scorer flat field name ──────────────────────────────

INCOME_STATEMENT_FIELDS = {
    "revenue":                                    "revenue",
    "costOfRevenue":                              "cost_of_revenue",
    "grossProfit":                                "gross_profit",
    "operatingIncome":                            "operating_income",
    "operatingExpenses":                          "operating_expenses",
    "researchAndDevelopmentExpenses":              "research_and_development",
    "sellingGeneralAndAdministrativeExpenses":     "selling_general_administrative",
    "interestExpense":                            "interest_expense",
    "incomeTaxExpense":                           "income_tax",
    "incomeBeforeTax":                            "income_before_tax",
    "netIncome":                                  "net_income",
    "eps":                                        "eps_basic",
    "epsDiluted":                                 "eps_diluted",
    "weightedAverageShsOut":                      "basic_avg_shares",
    "weightedAverageShsOutDil":                   "diluted_avg_shares",
    "ebitda":                                     "ebitda",
}

BALANCE_SHEET_FIELDS = {
    "totalAssets":                                "total_assets",
    "totalCurrentAssets":                         "current_assets",
    "cashAndCashEquivalents":                     "cash_and_equivalents",
    "shortTermInvestments":                       "short_term_investments",
    "inventory":                                  "inventory",
    "totalLiabilities":                           "total_liabilities",
    "totalCurrentLiabilities":                    "current_liabilities",
    "longTermDebt":                               "long_term_debt",
    "shortTermDebt":                              "short_term_debt",
    "totalStockholdersEquity":                    "total_equity",
    "commonStock":                                "common_stock",
    "retainedEarnings":                           "retained_earnings",
    "goodwill":                                   "goodwill",
    "intangibleAssets":                           "intangible_assets",
}

CASH_FLOW_FIELDS = {
    "operatingCashFlow":                          "operating_cash_flow",
    "freeCashFlow":                               "free_cash_flow",
    "capitalExpenditure":                         "capex",
    "netCashProvidedByInvestingActivities":        "investing_cash_flow",
    "netCashProvidedByFinancingActivities":        "financing_cash_flow",
    "depreciationAndAmortization":                "depreciation_amortization",
    "stockBasedCompensation":                     "stock_based_comp",
    "commonDividendsPaid":                        "dividends_paid",
}


def normalize_fmp_to_scorer_shape(fmp_data: dict) -> dict:
    """Convert FMP financial data to the scorer's expected shape.

    Args:
        fmp_data: Output from ``FMPClient.get_full_financials()``, with keys
                  ``income_statement``, ``balance_sheet``, ``cash_flow_statement``.

    Returns:
        ``{"statements": [<flat record>, ...]}`` sorted most-recent-first.
    """
    if not fmp_data:
        return {"statements": []}

    income_records = fmp_data.get("income_statement") or []
    balance_records = fmp_data.get("balance_sheet") or []
    cash_flow_records = fmp_data.get("cash_flow_statement") or []

    # Index records by date for alignment
    income_by_date = {r["date"]: r for r in income_records if r.get("date")}
    balance_by_date = {r["date"]: r for r in balance_records if r.get("date")}
    cash_flow_by_date = {r["date"]: r for r in cash_flow_records if r.get("date")}

    all_dates = sorted(
        set(income_by_date) | set(balance_by_date) | set(cash_flow_by_date),
        reverse=True,
    )

    statements = []
    for date_str in all_dates:
        income = income_by_date.get(date_str, {})
        balance = balance_by_date.get(date_str, {})
        cash_flow = cash_flow_by_date.get(date_str, {})

        # Use whichever record has period/calendar info
        primary = income or balance or cash_flow

        record: dict = {
            "period": _build_period_label(primary),
            "fiscal_year": _safe_int(primary.get("fiscalYear") or primary.get("calendarYear")),
            "fiscal_period": primary.get("period"),  # FY, Q1, Q2, Q3, Q4
            "filing_date": primary.get("fillingDate") or primary.get("acceptedDate"),
            "_source": "fmp",
        }

        # Map income statement fields
        for fmp_field, scorer_field in INCOME_STATEMENT_FIELDS.items():
            val = income.get(fmp_field)
            if val is not None and scorer_field not in record:
                record[scorer_field] = _to_float(val)

        # Map balance sheet fields
        for fmp_field, scorer_field in BALANCE_SHEET_FIELDS.items():
            val = balance.get(fmp_field)
            if val is not None and scorer_field not in record:
                record[scorer_field] = _to_float(val)

        # Map cash flow fields
        for fmp_field, scorer_field in CASH_FLOW_FIELDS.items():
            val = cash_flow.get(fmp_field)
            if val is not None and scorer_field not in record:
                record[scorer_field] = _to_float(val)

        # Compute free_cash_flow if FMP didn't provide it
        if record.get("free_cash_flow") is None:
            ocf = record.get("operating_cash_flow")
            capex = record.get("capex")
            if ocf is not None and capex is not None:
                record["free_cash_flow"] = ocf - abs(capex)

        statements.append(record)

    _log.info("FMP normalizer: produced %d statements", len(statements))
    return {"statements": statements}


def _build_period_label(record: dict) -> str:
    """Build a human-readable period label like 'Q4 2025' or 'FY 2024'."""
    year = record.get("fiscalYear") or record.get("calendarYear")
    period = record.get("period", "")

    if not year:
        return record.get("date", "unknown")

    if period in ("Q1", "Q2", "Q3", "Q4"):
        return f"{period} {year}"
    if period == "FY":
        return f"FY {year}"

    return f"{period} {year}".strip()


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

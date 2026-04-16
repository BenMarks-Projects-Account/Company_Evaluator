"""
Bulk endpoint definitions for FMP Ultimate API.

Each endpoint is declared with its URL template, expected
response characteristics, and caching behavior.
"""
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class BulkEndpoint:
    """Declarative definition of a bulk data endpoint."""

    # Logical name (used for cache table naming)
    name: str

    # URL template (supports Python format strings for parameters)
    # Example: "/stable/income-statement-bulk?year={year}&period={period}"
    url_template: str

    # Category grouping for display/logging
    category: str  # "profile" | "financials" | "metrics" | "scores" | ...

    # Approximate response size (for adaptive spacing)
    # small < 5MB, medium 5-25MB, large 25-65MB, xlarge > 65MB
    size_class: str  # "small" | "medium" | "large" | "xlarge"

    # Whether this endpoint requires pagination (part=0, part=1, ...)
    paginated: bool = False
    max_parts: int = 0  # How many parts to fetch (e.g., 4 for profile-bulk)

    # For year-based endpoints, how many historical years to fetch
    years_back: int = 0

    # Column that identifies the symbol in the CSV (for filtering)
    symbol_column: str = "symbol"


# All bulk endpoints we'll fetch in a daily refresh
BULK_ENDPOINTS: List[BulkEndpoint] = [
    # === Company profile (paginated, 4 parts) ===
    BulkEndpoint(
        name="profile",
        url_template="/stable/profile-bulk?part={part}",
        category="profile",
        size_class="medium",
        paginated=True,
        max_parts=4,
    ),

    # === Financial statements (5 years of annual) ===
    BulkEndpoint(
        name="income_statement_annual",
        url_template="/stable/income-statement-bulk?year={year}&period=annual",
        category="financials",
        size_class="medium",
        years_back=5,
    ),
    BulkEndpoint(
        name="balance_sheet_annual",
        url_template="/stable/balance-sheet-statement-bulk?year={year}&period=annual",
        category="financials",
        size_class="large",
        years_back=5,
    ),
    BulkEndpoint(
        name="cash_flow_annual",
        url_template="/stable/cash-flow-statement-bulk?year={year}&period=annual",
        category="financials",
        size_class="medium",
        years_back=5,
    ),

    # === Growth statements (5 years) ===
    BulkEndpoint(
        name="income_growth_annual",
        url_template="/stable/income-statement-growth-bulk?year={year}&period=annual",
        category="growth",
        size_class="large",
        years_back=5,
    ),
    BulkEndpoint(
        name="balance_growth_annual",
        url_template="/stable/balance-sheet-statement-growth-bulk?year={year}&period=annual",
        category="growth",
        size_class="large",
        years_back=5,
    ),
    BulkEndpoint(
        name="cash_flow_growth_annual",
        url_template="/stable/cash-flow-statement-growth-bulk?year={year}&period=annual",
        category="growth",
        size_class="large",
        years_back=5,
    ),

    # === TTM metrics and ratios ===
    BulkEndpoint(
        name="key_metrics_ttm",
        url_template="/stable/key-metrics-ttm-bulk",
        category="metrics",
        size_class="large",
    ),
    BulkEndpoint(
        name="ratios_ttm",
        url_template="/stable/ratios-ttm-bulk",
        category="metrics",
        size_class="xlarge",
    ),

    # === Scores and valuations ===
    BulkEndpoint(
        name="scores",
        url_template="/stable/scores-bulk",
        category="scores",
        size_class="small",
    ),
    BulkEndpoint(
        name="dcf",
        url_template="/stable/dcf-bulk",
        category="valuations",
        size_class="small",
    ),
    BulkEndpoint(
        name="rating",
        url_template="/stable/rating-bulk",
        category="scores",
        size_class="small",
    ),

    # === Peers ===
    BulkEndpoint(
        name="peers",
        url_template="/stable/peers-bulk",
        category="peers",
        size_class="small",
    ),

    # === Analyst data ===
    BulkEndpoint(
        name="price_target_summary",
        url_template="/stable/price-target-summary-bulk",
        category="analyst",
        size_class="small",
    ),
    BulkEndpoint(
        name="upgrades_downgrades",
        url_template="/stable/upgrades-downgrades-consensus-bulk",
        category="analyst",
        size_class="small",
    ),

    # === Earnings surprises (3 years) ===
    BulkEndpoint(
        name="earnings_surprises",
        url_template="/stable/earnings-surprises-bulk?year={year}",
        category="earnings",
        size_class="small",
        years_back=3,
    ),

    # === EOD prices (today's snapshot) ===
    BulkEndpoint(
        name="eod_snapshot",
        url_template="/stable/eod-bulk?date={date}",
        category="price",
        size_class="small",
    ),
]

# Spacing recommendations by size class (seconds between calls)
SPACING_BY_SIZE = {
    "small": 3,
    "medium": 5,
    "large": 8,
    "xlarge": 8,
}

# Backoff on 429 rate limit
RATE_LIMIT_BACKOFF_SECONDS = 15
MAX_RETRIES = 3

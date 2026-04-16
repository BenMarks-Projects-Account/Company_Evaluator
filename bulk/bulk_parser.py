"""
CSV parser for bulk endpoint responses.

Parses CSV text into filtered DataFrames containing only universe
symbols. Non-universe rows are discarded immediately to reduce
memory footprint (bulk responses contain 22K-82K rows, but we
only care about ~3K).
"""
import io
import logging
from dataclasses import dataclass
from typing import Optional, Set

import pandas as pd

from .bulk_endpoints import BulkEndpoint
from .bulk_fetcher import FetchResult

logger = logging.getLogger(__name__)


@dataclass
class ParseResult:
    """Result of parsing a fetch response."""
    endpoint_name: str
    part: Optional[int]
    year: Optional[int]
    dataframe: Optional[pd.DataFrame]
    original_row_count: int
    filtered_row_count: int
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return self.dataframe is not None and self.error is None


class BulkParser:
    """Parses bulk CSV responses and filters to universe symbols."""

    def __init__(self, universe_symbols: Set[str]):
        """Initialize with the set of symbols we care about."""
        self.universe = {s.upper() for s in universe_symbols}
        logger.info(f"Parser initialized with {len(self.universe)} universe symbols")

    def parse(self, fetch_result: FetchResult, endpoint: BulkEndpoint) -> ParseResult:
        """Parse a fetch result into a filtered DataFrame."""
        if not fetch_result.is_success:
            return ParseResult(
                endpoint_name=fetch_result.endpoint_name,
                part=fetch_result.part,
                year=fetch_result.year,
                dataframe=None,
                original_row_count=0,
                filtered_row_count=0,
                error=fetch_result.error or "Fetch failed",
            )

        try:
            df = pd.read_csv(io.StringIO(fetch_result.csv_text), low_memory=False)
            original_count = len(df)

            # Verify symbol column exists
            symbol_col = endpoint.symbol_column
            if symbol_col not in df.columns:
                return ParseResult(
                    endpoint_name=fetch_result.endpoint_name,
                    part=fetch_result.part,
                    year=fetch_result.year,
                    dataframe=None,
                    original_row_count=original_count,
                    filtered_row_count=0,
                    error=(
                        f"Missing symbol column '{symbol_col}'. "
                        f"Available: {list(df.columns)[:10]}"
                    ),
                )

            # Normalize symbol case and filter to universe
            df[symbol_col] = df[symbol_col].astype(str).str.upper()
            df_filtered = df[df[symbol_col].isin(self.universe)].copy()
            filtered_count = len(df_filtered)

            label = endpoint.name
            if fetch_result.part is not None:
                label += f" part={fetch_result.part}"
            if fetch_result.year is not None:
                label += f" year={fetch_result.year}"

            logger.info(
                f"Parsed {label}: {original_count} rows -> "
                f"{filtered_count} universe rows "
                f"({filtered_count / max(original_count, 1) * 100:.1f}%)"
            )

            return ParseResult(
                endpoint_name=fetch_result.endpoint_name,
                part=fetch_result.part,
                year=fetch_result.year,
                dataframe=df_filtered,
                original_row_count=original_count,
                filtered_row_count=filtered_count,
            )

        except Exception as e:
            logger.error(f"Parse error on {endpoint.name}: {e}")
            return ParseResult(
                endpoint_name=fetch_result.endpoint_name,
                part=fetch_result.part,
                year=fetch_result.year,
                dataframe=None,
                original_row_count=0,
                filtered_row_count=0,
                error=f"Parse error: {e}",
            )

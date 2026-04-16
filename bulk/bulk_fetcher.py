"""
HTTP fetcher for FMP bulk endpoints with adaptive rate limiting.

Handles:
- Bandwidth-based throttling (429 responses from large downloads)
- Adaptive spacing based on response size class
- Retry with exponential backoff on transient failures
"""
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple

import requests

from .bulk_endpoints import (
    BulkEndpoint,
    BULK_ENDPOINTS,
    SPACING_BY_SIZE,
    RATE_LIMIT_BACKOFF_SECONDS,
    MAX_RETRIES,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """Result of a single bulk endpoint fetch."""
    endpoint_name: str
    part: Optional[int]
    year: Optional[int]
    url: str
    status_code: int
    csv_text: str
    size_bytes: int
    elapsed_seconds: float
    retry_count: int = 0
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return self.status_code == 200 and self.csv_text and self.error is None

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)


class BulkFetcher:
    """Fetches FMP bulk endpoints with adaptive rate limiting."""

    BASE_URL = "https://financialmodelingprep.com"
    REQUEST_TIMEOUT = 180  # seconds — bulk downloads can be large

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key required")
        self.api_key = api_key
        self.session = requests.Session()

    def fetch_endpoint(
        self,
        endpoint: BulkEndpoint,
        part: Optional[int] = None,
        year: Optional[int] = None,
        date_str: Optional[str] = None,
    ) -> FetchResult:
        """Fetch a single bulk endpoint invocation."""
        url_path = endpoint.url_template
        if "{part}" in url_path:
            url_path = url_path.replace("{part}", str(part))
        if "{year}" in url_path:
            url_path = url_path.replace("{year}", str(year))
        if "{date}" in url_path:
            url_path = url_path.replace("{date}", date_str)

        separator = "&" if "?" in url_path else "?"
        url = f"{self.BASE_URL}{url_path}{separator}apikey={self.api_key}"

        return self._fetch_with_retry(endpoint, url, part=part, year=year)

    def _fetch_with_retry(
        self,
        endpoint: BulkEndpoint,
        url: str,
        part: Optional[int],
        year: Optional[int],
    ) -> FetchResult:
        """Perform HTTP GET with retry on 429."""
        retry_count = 0
        last_error = None

        for attempt in range(MAX_RETRIES):
            start = time.time()
            try:
                response = self.session.get(url, timeout=self.REQUEST_TIMEOUT)
                elapsed = time.time() - start

                if response.status_code == 200:
                    return FetchResult(
                        endpoint_name=endpoint.name,
                        part=part,
                        year=year,
                        url=self._sanitize_url(url),
                        status_code=200,
                        csv_text=response.text,
                        size_bytes=len(response.content),
                        elapsed_seconds=elapsed,
                        retry_count=retry_count,
                    )

                if response.status_code == 429:
                    retry_count += 1
                    logger.warning(
                        f"Rate limited on {endpoint.name} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"waiting {RATE_LIMIT_BACKOFF_SECONDS}s"
                    )
                    time.sleep(RATE_LIMIT_BACKOFF_SECONDS)
                    continue

                # Other error
                return FetchResult(
                    endpoint_name=endpoint.name,
                    part=part,
                    year=year,
                    url=self._sanitize_url(url),
                    status_code=response.status_code,
                    csv_text="",
                    size_bytes=0,
                    elapsed_seconds=elapsed,
                    retry_count=retry_count,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )

            except requests.RequestException as e:
                elapsed = time.time() - start
                last_error = str(e)
                retry_count += 1
                logger.warning(
                    f"Request failed on {endpoint.name} "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue

                return FetchResult(
                    endpoint_name=endpoint.name,
                    part=part,
                    year=year,
                    url=self._sanitize_url(url),
                    status_code=0,
                    csv_text="",
                    size_bytes=0,
                    elapsed_seconds=elapsed,
                    retry_count=retry_count,
                    error=last_error,
                )

        # All retries exhausted (429 loop)
        return FetchResult(
            endpoint_name=endpoint.name,
            part=part,
            year=year,
            url=self._sanitize_url(url),
            status_code=429,
            csv_text="",
            size_bytes=0,
            elapsed_seconds=0,
            retry_count=retry_count,
            error="Max retries exhausted on 429",
        )

    def _sanitize_url(self, url: str) -> str:
        """Remove API key from URL for logging."""
        return re.sub(r"apikey=[^&]+", "apikey=***", url)

    def fetch_all(self) -> List[FetchResult]:
        """
        Fetch all bulk endpoints with adaptive spacing.

        Expands parameterized endpoints (paginated, year-based) into
        concrete fetch calls. Spaces calls by size class to minimize
        429 rate limiting.
        """
        results: List[FetchResult] = []
        calls_to_make = self._expand_calls()

        total = len(calls_to_make)
        logger.info(f"Starting bulk fetch: {total} calls")

        for i, (endpoint, params) in enumerate(calls_to_make, 1):
            label = endpoint.name
            if "part" in params and params["part"] is not None:
                label += f" part={params['part']}"
            if "year" in params and params["year"] is not None:
                label += f" year={params['year']}"

            logger.info(f"[{i}/{total}] Fetching {label}")

            result = self.fetch_endpoint(endpoint, **params)
            results.append(result)

            if result.is_success:
                retries_note = f" (retries: {result.retry_count})" if result.retry_count else ""
                logger.info(
                    f"  -> {result.size_mb:.1f} MB in {result.elapsed_seconds:.1f}s{retries_note}"
                )
            else:
                logger.error(
                    f"  -> FAILED: {result.error or f'HTTP {result.status_code}'}"
                )

            # Adaptive spacing based on size class (skip after last call)
            if i < total:
                spacing = SPACING_BY_SIZE.get(endpoint.size_class, 5)
                time.sleep(spacing)

        successful = sum(1 for r in results if r.is_success)
        total_mb = sum(r.size_mb for r in results if r.is_success)
        logger.info(
            f"Bulk fetch complete: {successful}/{total} successful, "
            f"{total_mb:.1f} MB total"
        )

        return results

    def _expand_calls(self) -> List[Tuple[BulkEndpoint, dict]]:
        """Expand parameterized endpoints into concrete fetch calls."""
        calls: List[Tuple[BulkEndpoint, dict]] = []
        current_year = datetime.now().year
        eod_date = self._last_business_day()

        for endpoint in BULK_ENDPOINTS:
            if endpoint.paginated:
                for part in range(endpoint.max_parts):
                    calls.append((endpoint, {"part": part}))
            elif endpoint.years_back > 0:
                # Most recent filing year first, then older
                for offset in range(endpoint.years_back):
                    year = current_year - 1 - offset
                    calls.append((endpoint, {"year": year}))
            elif "{date}" in endpoint.url_template:
                calls.append((endpoint, {"date_str": eod_date}))
            else:
                calls.append((endpoint, {}))

        return calls

    @staticmethod
    def _last_business_day() -> str:
        """Return the most recent weekday (Mon-Fri) before today.

        EOD bulk data is only available after a market's close.
        At overnight-crawl time (2 AM), today's US data doesn't
        exist yet, so we always use the previous business day.
        """
        d = date.today() - timedelta(days=1)
        while d.weekday() >= 5:  # Saturday=5, Sunday=6
            d -= timedelta(days=1)
        return d.isoformat()

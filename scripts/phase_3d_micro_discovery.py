"""Phase 3d — Micro Discovery Tier Expansion ($50M-$500M Tech/Healthcare).

Adds up to 1,000 new symbols (600 Tech + 400 Healthcare ceilings) to
``universe_symbols`` under a new ``tier_5_micro_discovery`` tier.

Discovery uses the same bounded waiver as Phase 3b: exactly three calls to
FMP's ``/stable/company-screener`` endpoint (Technology, Communication
Services, Healthcare), ``limit=10000``.

Live-fetch waiver (Phase 3d Option B):
  Per-symbol live FMP calls are permitted ONLY for screener-returned symbols
  absent from the bulk cache.  Capped at ``_LIVE_FETCH_CAP`` (500) per run;
  symbols beyond the cap (sorted by screener market_cap descending) are
  rejected as ``live_fetched_rejected_cap``.  Allowed live endpoints:
  ``/stable/profile`` and ``key-metrics-ttm`` only.  No financials, balance
  sheets, cash flow, or estimates — those remain bulk-cache only.

  Successful fetches are written back to the bulk cache (``bulk_profile_p3``
  and ``bulk_key_metrics_ttm``) so subsequent phase runs benefit from the
  newly-populated coverage.

Filters (short-circuit on first failure):
  1. Exchange ∈ {NYSE, NASDAQ}, country=US, symbol shape, not already in
     ``universe_symbols``.
  2. Sector bucket: Technology → tech; Communication Services →
     tech (Internet Content + Electronic Gaming only); Healthcare →
     healthcare.
  3. Market cap re-validated from bulk profile: $50M ≤ cap ≤ $500M.
  4. Gross margin > 0 — TTM if available (key-metrics-ttm), else most
     recent annual ``grossProfit/revenue`` from bulk income statements.
     Live-fetched candidates can only resolve gross margin via TTM (no
     income-statement endpoint is permitted under the waiver).

Within each bucket, survivors are ranked by market cap descending.  Ceilings
are 600 Tech + 400 Healthcare.  Filters are NEVER relaxed to hit the target;
shortfalls are accepted as-is.

Default mode is ``--dry-run``.  Pass ``--execute`` to commit in a single
transaction and write ``logs/phase_3d_micro_discovery_report.json``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import select  # noqa: E402

from config import get_settings  # noqa: E402
from db.database import UniverseSymbol, get_session, init_db  # noqa: E402

_log = logging.getLogger(__name__)


# ── Security: API-key redaction across logs + reports ──────────
# Carried over verbatim from scripts/phase_3b_expansion.py — DO NOT remove.

_APIKEY_RE = re.compile(r"apikey=[^&\s\"']+", re.IGNORECASE)


def _redact(text: str) -> str:
    if not isinstance(text, str):
        return text
    return _APIKEY_RE.sub("apikey=***REDACTED***", text)


class _ApiKeyRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        try:
            msg = record.getMessage()
            if "apikey=" in msg.lower():
                record.msg = _redact(msg)
                record.args = ()
        except Exception:
            pass
        return True


def _redact_obj(obj):
    if isinstance(obj, str):
        return _redact(obj)
    if isinstance(obj, dict):
        return {k: _redact_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_obj(v) for v in obj]
    return obj


def _install_security_filters() -> None:
    redaction = _ApiKeyRedactionFilter()
    logging.getLogger().addFilter(redaction)
    for name in ("httpx", "httpcore", "data.fmp_client"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        lg.addFilter(redaction)


# ── Tunables ───────────────────────────────────────────────────

_SOURCE = "phase_3d_micro_discovery"
_DISCOVERY_SOURCE = "screener_micro_tech_healthcare"
_TIER = "tier_5_micro_discovery"
_PRIORITY = 5

_MARKET_CAP_MIN = 50_000_000.0
_MARKET_CAP_MAX = 500_000_000.0
_GROSS_MARGIN_MIN = 0.0  # strictly greater than 0; checked with >

_TARGET_TECH = 600
_TARGET_HEALTHCARE = 400

_TECH_BUCKET = "tech"
_HC_BUCKET = "healthcare"

# Live-fetch waiver (Option B): per-symbol calls capped at 500 per run.
_LIVE_FETCH_CAP = 500

# Bulk cache target tables for write-back.
_PROFILE_WRITE_TABLE = "bulk_profile_p3"
_KM_TTM_TABLE = "bulk_key_metrics_ttm"
_RATIOS_TTM_TABLE = "bulk_ratios_ttm"

_ALLOWED_EXCHANGES = {"NYSE", "NASDAQ"}

# Same Comms restriction as Phase 3b.
_COMMS_ALLOWED_INDUSTRIES = {
    "Internet Content & Information",
    "Electronic Gaming & Multimedia",
}

# Dotted warrant/unit/right markers only — single trailing uppercase letters
# (NOW, SNOW, FLOW) must NOT be rejected.  Lowercase rejection handled
# separately in the filter loop.
_BAD_SYMBOL_RE = re.compile(r"^[A-Z]+\.(W|WS|U|R)$")

# Reason codes
R_EXCHANGE = "filter_1_exchange"
R_COUNTRY = "filter_1_country"
R_SYMBOL_SHAPE = "filter_1_symbol_shape"
R_ALREADY_IN_UNIVERSE = "filter_1_already_in_universe"
R_SECTOR = "filter_2_sector"
R_MARKET_CAP = "filter_3_market_cap"
R_GROSS_MARGIN = "filter_4_gross_margin"
R_MISSING_GROSS_MARGIN = "missing_gross_margin"
R_NOT_IN_BULK_CACHE = "not_in_bulk_cache"
R_MISSING_PROFILE = "missing_data_profile"
R_MISSING_INCOME = "missing_data_income_statement"

# Live-fetch funnel stages (counted separately from the bulk-cache filter funnel).
LF_SUCCESS = "live_fetched_success"
LF_FAILED = "live_fetched_failed"
LF_REJECTED_CAP = "live_fetched_rejected_cap"

_FILTER_ORDER = [
    R_EXCHANGE, R_COUNTRY, R_SYMBOL_SHAPE, R_ALREADY_IN_UNIVERSE,
    R_NOT_IN_BULK_CACHE, R_MISSING_PROFILE,
    R_SECTOR,
    R_MARKET_CAP,
    R_MISSING_INCOME, R_MISSING_GROSS_MARGIN, R_GROSS_MARGIN,
]


# ── Candidate ──────────────────────────────────────────────────

@dataclass
class Candidate:
    symbol: str
    screener_row: dict = field(default_factory=dict)
    bucket: Optional[str] = None
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    country: Optional[str] = None
    market_cap: Optional[float] = None
    market_cap_tier: Optional[str] = None
    gross_margin_ttm: Optional[float] = None
    gross_margin_source: Optional[str] = None  # "key_metrics_ttm" | "annual_y####" | "live_key_metrics_ttm"
    latest_price: Optional[float] = None
    average_volume: Optional[float] = None
    rejected_reason: Optional[str] = None
    passed_filters: list[str] = field(default_factory=list)
    live_fetched: bool = False  # True if data was sourced via the live-fetch waiver


def _classify_market_cap_tier(market_cap: Optional[float]) -> str:
    """Phase 3a thresholds — only ``small`` or ``micro`` valid here."""
    if market_cap is None:
        return "unknown"
    if market_cap >= 300_000_000:
        return "small"
    if market_cap >= 50_000_000:
        return "micro"
    return "penny"


# ── Bulk cache reader ──────────────────────────────────────────

class BulkReader:
    """Direct sqlite3 reader with in-memory per-table symbol indexes."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._tables: set[str] = {
            r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self._index_cache: dict[str, dict[str, dict]] = {}
        # Per-symbol full live key-metrics-ttm dict (preserves grossProfitMarginTTM
        # which the cached schema does not include).
        self._live_km_full: dict[str, dict] = {}
        # Per-symbol full live ratios-ttm dict (the source of grossProfitMarginTTM
        # — the key_metrics_ttm endpoint does NOT include margin fields).
        self._live_ratios_full: dict[str, dict] = {}
        # Cache table column lists for write-back schema alignment.
        self._table_columns: dict[str, list[str]] = {}

    def _index(self, table: str) -> dict[str, dict]:
        if table in self._index_cache:
            return self._index_cache[table]
        if table not in self._tables:
            self._index_cache[table] = {}
            return self._index_cache[table]
        idx: dict[str, dict] = {}
        for row in self._conn.execute(f'SELECT * FROM "{table}"'):
            d = dict(row)
            sym = (d.get("symbol") or "").upper()
            if sym and sym not in idx:
                idx[sym] = d
        self._index_cache[table] = idx
        _log.debug("Indexed %s: %d rows", table, len(idx))
        return idx

    def profile(self, symbol: str) -> Optional[dict]:
        for part in range(4):
            row = self._index(f"bulk_profile_p{part}").get(symbol.upper())
            if row:
                return row
        return None

    def income(self, symbol: str, year: int) -> Optional[dict]:
        return self._index(f"bulk_income_statement_annual_y{year}").get(symbol.upper())

    def key_metrics_ttm(self, symbol: str) -> Optional[dict]:
        return self._index("bulk_key_metrics_ttm").get(symbol.upper())

    def ratios_ttm(self, symbol: str) -> Optional[dict]:
        return self._index("bulk_ratios_ttm").get(symbol.upper())

    def available_years(self, endpoint_base: str) -> list[int]:
        years = []
        prefix = f"bulk_{endpoint_base}_y"
        for name in self._tables:
            if name.startswith(prefix):
                try:
                    years.append(int(name[len(prefix):]))
                except ValueError:
                    continue
        return sorted(years, reverse=True)

    # ── Write-back support for the live-fetch waiver ──────────

    def _columns_for(self, table: str) -> list[str]:
        if table in self._table_columns:
            return self._table_columns[table]
        if table not in self._tables:
            self._table_columns[table] = []
            return []
        cols = [r[1] for r in self._conn.execute(f'PRAGMA table_info("{table}")')]
        self._table_columns[table] = cols
        return cols

    def upsert_row(self, table: str, row: dict) -> bool:
        """Append a row to ``table`` aligned to its existing column schema.

        Bulk cache tables have no PRIMARY KEY (created via pandas.to_sql with
        ``if_exists='replace'``), so this is a plain INSERT.  The live-fetch
        waiver only writes symbols that were absent from the cache by
        construction, so duplicate-symbol rows are not produced.  In-memory
        index is updated in lock-step so subsequent lookups in this run see
        the freshly-written row.  This method does NOT commit — callers
        must invoke ``flush()`` to persist a batch in a single transaction.
        """
        if table not in self._tables:
            _log.warning("upsert_row: table %s does not exist; skipping", table)
            return False
        cols = self._columns_for(table)
        if not cols:
            return False
        aligned = {c: row.get(c) for c in cols}
        placeholders = ",".join("?" for _ in cols)
        col_list = ",".join(f'"{c}"' for c in cols)
        try:
            self._conn.execute(
                f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})',
                [aligned[c] for c in cols],
            )
        except Exception as exc:
            _log.warning("upsert_row %s failed for %s: %s", table, row.get("symbol"), exc)
            return False
        # Update the in-memory index so re-runs of the filter pipeline see this row.
        sym = (aligned.get("symbol") or "").upper()
        if sym:
            idx = self._index(table)
            if sym not in idx:  # don't shadow an existing row
                idx[sym] = aligned
        return True

    def flush(self) -> None:
        """Commit any pending writes in a single transaction."""
        try:
            self._conn.commit()
        except Exception as exc:
            _log.warning("BulkReader.flush() failed: %s", exc)

    def store_live_km_full(self, symbol: str, km_full: dict) -> None:
        """Stash the full live key-metrics-ttm response for in-run gross margin lookup."""
        self._live_km_full[symbol.upper()] = km_full

    def live_km_full(self, symbol: str) -> Optional[dict]:
        return self._live_km_full.get(symbol.upper())

    def store_live_ratios_full(self, symbol: str, ratios_full: dict) -> None:
        """Stash the full live ratios-ttm response for in-run gross margin lookup."""
        self._live_ratios_full[symbol.upper()] = ratios_full

    def live_ratios_full(self, symbol: str) -> Optional[dict]:
        return self._live_ratios_full.get(symbol.upper())

    def close(self):
        self._conn.close()


def _to_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Live screener (3 calls, bounded waiver) ────────────────────

async def _run_screener_waiver() -> tuple[list[dict], dict[str, int]]:
    from data.fmp_client import FMPClient

    settings = get_settings()
    if not (settings.fmp_enabled and settings.fmp_api_key):
        raise RuntimeError(
            "FMP is not enabled — screener waiver requires fmp_enabled=True "
            "and a valid fmp_api_key"
        )
    client = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )

    sectors = [
        ("Technology", "tech"),
        ("Communication Services", "comms"),
        ("Healthcare", "healthcare"),
    ]
    merged: list[dict] = []
    counts: dict[str, int] = {}

    for sector_name, label in sectors:
        _log.info("Live screener call: sector=%s (limit=10000)", sector_name)
        rows = await client.stock_screener(
            sector=sector_name,
            market_cap_min=_MARKET_CAP_MIN,
            market_cap_max=_MARKET_CAP_MAX,
            country="US",
            exchange="nyse,nasdaq",
            is_actively_trading=True,
            is_etf=False,
            is_fund=False,
            limit=10000,
        ) or []
        counts[f"screener_{label}_returned"] = len(rows)
        _log.info("  screener returned %d rows for %s", len(rows), sector_name)
        if len(rows) >= 10000:
            _log.warning(
                "Sector %s hit the 10,000-row ceiling — additional pages "
                "are not retrievable without widening filters. Proceeding "
                "with the %d rows returned.",
                sector_name, len(rows),
            )
        for r in rows:
            r["_requested_sector"] = sector_name
            merged.append(r)

    return merged, counts


# ── Filter pipeline ────────────────────────────────────────────

def _resolve_gross_margin(
    sym: str,
    bulk: BulkReader,
    history_years: list[int],
) -> tuple[Optional[float], Optional[str], Optional[str]]:
    """Return (gross_margin, source_label, reject_reason).

    Tries (in order) the in-run live ratios-ttm response, the in-run live
    key-metrics-ttm response (legacy, no margin fields but kept for parity),
    cached ``bulk_ratios_ttm``, cached ``bulk_key_metrics_ttm`` (legacy),
    then walks annual income statements from newest year backwards.  Rejects
    with ``missing_gross_margin`` only if no source yields a value.
    """
    live_ratios = bulk.live_ratios_full(sym)
    if live_ratios:
        gm = _to_float(live_ratios.get("grossProfitMarginTTM"))
        if gm is not None:
            return gm, "live_ratios_ttm", None

    live_km = bulk.live_km_full(sym)
    if live_km:
        gm = _to_float(live_km.get("grossProfitMarginTTM"))
        if gm is not None:
            return gm, "live_key_metrics_ttm", None

    ratios = bulk.ratios_ttm(sym)
    if ratios:
        gm = _to_float(ratios.get("grossProfitMarginTTM"))
        if gm is not None:
            return gm, "ratios_ttm", None

    km = bulk.key_metrics_ttm(sym)
    if km:
        gm = _to_float(km.get("grossProfitMarginTTM"))
        if gm is not None:
            return gm, "key_metrics_ttm", None

    if not history_years:
        return None, None, R_MISSING_INCOME

    saw_income = False
    for year in history_years:
        row = bulk.income(sym, year)
        if row is None:
            continue
        saw_income = True
        revenue = _to_float(row.get("revenue"))
        gross_profit = _to_float(row.get("grossProfit"))
        if revenue is None or revenue <= 0:
            continue  # divide-by-zero guard per spec
        if gross_profit is None:
            continue
        return gross_profit / revenue, f"annual_y{year}", None

    if not saw_income:
        return None, None, R_MISSING_INCOME
    return None, None, R_MISSING_GROSS_MARGIN


def _apply_filters(
    screener_rows: list[dict],
    existing_symbols: set[str],
    bulk: BulkReader,
    *,
    skip_not_in_cache_collect: bool = False,
) -> tuple[list[Candidate], dict[str, int], list[Candidate]]:
    """Run the bulk-cache filter pipeline.

    Returns ``(survivors, reject_counts, not_in_cache_rejects)``.
    ``not_in_cache_rejects`` is the list of Candidate objects rejected
    specifically with reason ``not_in_bulk_cache`` — these are the input
    pool for the live-fetch waiver.  When ``skip_not_in_cache_collect`` is
    True (used during the second pass after live-fetch), the list is
    returned empty.
    """
    reject_counts: dict[str, int] = {k: 0 for k in _FILTER_ORDER}
    survivors: list[Candidate] = []
    not_in_cache_rejects: list[Candidate] = []
    history_years = bulk.available_years("income_statement_annual")
    seen_symbols: set[str] = set()
    existing_upper = {s.upper() for s in existing_symbols}

    for row in screener_rows:
        sym = (row.get("symbol") or "").strip()
        if not sym:
            continue
        sym_upper = sym.upper()
        if sym_upper in seen_symbols:
            continue  # dedupe within screener union
        seen_symbols.add(sym_upper)

        cand = Candidate(symbol=sym, screener_row=row)

        # Filter 1a: exchange (re-validated below from profile)
        exch = (row.get("exchangeShortName") or row.get("exchange") or "").upper()
        if exch not in _ALLOWED_EXCHANGES:
            cand.rejected_reason = R_EXCHANGE
            reject_counts[R_EXCHANGE] += 1
            continue
        cand.passed_filters.append(R_EXCHANGE)

        # Filter 1b: country
        country = (row.get("country") or "").upper()
        if country and country != "US":
            cand.rejected_reason = R_COUNTRY
            reject_counts[R_COUNTRY] += 1
            continue
        cand.country = country or "US"
        cand.passed_filters.append(R_COUNTRY)

        # Filter 1c: symbol shape (lowercase or dotted warrant/unit/right)
        if any(c.islower() for c in sym) or _BAD_SYMBOL_RE.match(sym):
            cand.rejected_reason = R_SYMBOL_SHAPE
            reject_counts[R_SYMBOL_SHAPE] += 1
            continue
        cand.passed_filters.append(R_SYMBOL_SHAPE)

        # Filter 1d: not already in universe
        if sym_upper in existing_upper:
            cand.rejected_reason = R_ALREADY_IN_UNIVERSE
            reject_counts[R_ALREADY_IN_UNIVERSE] += 1
            continue
        cand.passed_filters.append(R_ALREADY_IN_UNIVERSE)

        # Bulk profile lookup (required)
        profile = bulk.profile(sym)
        if profile is None:
            cand.rejected_reason = R_NOT_IN_BULK_CACHE
            reject_counts[R_NOT_IN_BULK_CACHE] += 1
            if not skip_not_in_cache_collect:
                not_in_cache_rejects.append(cand)
            continue

        cand.company_name = profile.get("companyName") or row.get("companyName")
        cand.sector = (profile.get("sector") or row.get("sector") or "").strip()
        cand.industry = (profile.get("industry") or row.get("industry") or "").strip()
        cand.exchange = (profile.get("exchange") or exch).upper()
        cand.country = (profile.get("country") or cand.country or "").upper()
        cand.market_cap = _to_float(profile.get("marketCap"))
        cand.latest_price = _to_float(profile.get("price"))
        cand.average_volume = _to_float(profile.get("averageVolume"))

        # Re-validate exchange + country from authoritative profile
        if cand.exchange not in _ALLOWED_EXCHANGES:
            cand.rejected_reason = R_EXCHANGE
            reject_counts[R_EXCHANGE] += 1
            continue
        if cand.country != "US":
            cand.rejected_reason = R_COUNTRY
            reject_counts[R_COUNTRY] += 1
            continue
        if not cand.company_name or not cand.sector:
            cand.rejected_reason = R_MISSING_PROFILE
            reject_counts[R_MISSING_PROFILE] += 1
            continue

        # Filter 2: sector bucket
        if cand.sector == "Technology":
            cand.bucket = _TECH_BUCKET
        elif cand.sector == "Communication Services":
            if cand.industry in _COMMS_ALLOWED_INDUSTRIES:
                cand.bucket = _TECH_BUCKET
            else:
                cand.rejected_reason = R_SECTOR
                reject_counts[R_SECTOR] += 1
                continue
        elif cand.sector == "Healthcare":
            cand.bucket = _HC_BUCKET
        else:
            cand.rejected_reason = R_SECTOR
            reject_counts[R_SECTOR] += 1
            continue
        cand.passed_filters.append(R_SECTOR)

        # Filter 3: market cap
        if cand.market_cap is None or not (
            _MARKET_CAP_MIN <= cand.market_cap <= _MARKET_CAP_MAX
        ):
            cand.rejected_reason = R_MARKET_CAP
            reject_counts[R_MARKET_CAP] += 1
            continue
        cand.market_cap_tier = _classify_market_cap_tier(cand.market_cap)
        cand.passed_filters.append(R_MARKET_CAP)

        # Filter 4: gross margin > 0
        gm, gm_source, gm_reject = _resolve_gross_margin(sym, bulk, history_years)
        if gm_reject:
            cand.rejected_reason = gm_reject
            reject_counts[gm_reject] += 1
            continue
        cand.gross_margin_ttm = gm
        cand.gross_margin_source = gm_source
        if gm is None or gm <= _GROSS_MARGIN_MIN:
            cand.rejected_reason = R_GROSS_MARGIN
            reject_counts[R_GROSS_MARGIN] += 1
            continue
        cand.passed_filters.append(R_GROSS_MARGIN)

        survivors.append(cand)

    return survivors, reject_counts, not_in_cache_rejects


# ── Live-fetch waiver (Option B) ─────────────────────────

async def _run_live_waiver(
    not_in_cache: list[Candidate],
    bulk: BulkReader,
    max_fetch: int = _LIVE_FETCH_CAP,
) -> tuple[list[str], dict[str, int], list[str]]:
    """Per-symbol live FMP fetch for symbols absent from bulk cache.

    Sorts ``not_in_cache`` by screener-reported market cap descending, takes
    up to ``_LIVE_FETCH_CAP`` symbols, and fetches ``/stable/profile`` +
    ``key-metrics-ttm`` for each.  Successful fetches are written back to the
    bulk cache (``bulk_profile_p3``, ``bulk_key_metrics_ttm``) and stashed in
    ``BulkReader._live_km_full`` so the second-pass filter can see
    grossProfitMarginTTM.

    Errors and rate-limits are logged and counted; the run does not retry
    aggressively and does not crash.

    Returns ``(eligible_symbols, lf_counts, captured_urls)``:
      - ``eligible_symbols``: symbols that successfully populated the cache
        and should be re-run through the filter pipeline.
      - ``lf_counts``: ``{LF_SUCCESS, LF_FAILED, LF_REJECTED_CAP}`` counts.
      - ``captured_urls``: any URL strings observed during live fetches
        (always pass through ``_redact_obj`` before persisting).
    """
    from data.fmp_client import FMPClient

    counts = {LF_SUCCESS: 0, LF_FAILED: 0, LF_REJECTED_CAP: 0}
    eligible: list[str] = []
    captured_urls: list[str] = []

    if not not_in_cache:
        return eligible, counts, captured_urls

    def _cap_for(c: Candidate) -> float:
        try:
            return float(c.screener_row.get("marketCap") or 0.0)
        except Exception:
            return 0.0

    ranked = sorted(not_in_cache, key=_cap_for, reverse=True)
    in_scope = ranked[:max_fetch]
    overflow = ranked[max_fetch:]
    counts[LF_REJECTED_CAP] = len(overflow)
    if overflow:
        _log.info(
            "Live-fetch waiver cap hit: %d symbols deferred (max=%d)",
            len(overflow), max_fetch,
        )

    settings = get_settings()
    if not (settings.fmp_enabled and settings.fmp_api_key):
        raise RuntimeError(
            "Live-fetch waiver requires fmp_enabled=True and fmp_api_key"
        )
    client = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )

    _log.info("Live-fetch waiver: starting %d per-symbol fetches", len(in_scope))

    for i, cand in enumerate(in_scope):
        sym = cand.symbol
        try:
            profile_resp = await client._request(
                "/stable/profile", params={"symbol": sym}
            )
            km_resp = await client._request(
                "/stable/key-metrics-ttm", params={"symbol": sym}
            )
            ratios_resp = await client._request(
                "/stable/ratios-ttm", params={"symbol": sym}
            )
        except Exception as exc:
            _log.warning(
                "Live fetch failed for %s: %s (continuing)", sym, _redact(str(exc))
            )
            counts[LF_FAILED] += 1
            continue

        prof_row: Optional[dict] = None
        if isinstance(profile_resp, list) and profile_resp:
            prof_row = profile_resp[0]
        km_row: Optional[dict] = None
        if isinstance(km_resp, list) and km_resp:
            km_row = km_resp[0]
        ratios_row: Optional[dict] = None
        if isinstance(ratios_resp, list) and ratios_resp:
            ratios_row = ratios_resp[0]

        if not prof_row or not prof_row.get("symbol"):
            counts[LF_FAILED] += 1
            continue

        # Normalise symbol casing to match cache convention.
        prof_row = dict(prof_row)
        prof_row["symbol"] = (prof_row.get("symbol") or sym).upper()
        bulk.upsert_row(_PROFILE_WRITE_TABLE, prof_row)

        if km_row:
            km_row = dict(km_row)
            km_row["symbol"] = (km_row.get("symbol") or sym).upper()
            bulk.upsert_row(_KM_TTM_TABLE, km_row)
            bulk.store_live_km_full(sym, km_row)

        if ratios_row:
            ratios_row = dict(ratios_row)
            ratios_row["symbol"] = (ratios_row.get("symbol") or sym).upper()
            bulk.upsert_row(_RATIOS_TTM_TABLE, ratios_row)
            bulk.store_live_ratios_full(sym, ratios_row)

        counts[LF_SUCCESS] += 1
        eligible.append(sym)

        if (i + 1) % 25 == 0:
            _log.info(
                "  live-fetch progress: %d/%d (success=%d, failed=%d)",
                i + 1, len(in_scope), counts[LF_SUCCESS], counts[LF_FAILED],
            )
            # Periodic flush so we don't lose progress on a crash and don't
            # hold the writer lock for the entire run.
            bulk.flush()

    bulk.flush()
    _log.info(
        "Live-fetch waiver complete: success=%d, failed=%d, rejected_cap=%d",
        counts[LF_SUCCESS], counts[LF_FAILED], counts[LF_REJECTED_CAP],
    )
    return eligible, counts, captured_urls


# ── Ranking (market cap descending, no quality score) ──────────

def _rank_and_trim(
    survivors: list[Candidate],
) -> tuple[list[Candidate], list[Candidate], dict[str, int]]:
    tech = [c for c in survivors if c.bucket == _TECH_BUCKET]
    hc = [c for c in survivors if c.bucket == _HC_BUCKET]
    tech.sort(key=lambda c: c.market_cap or 0.0, reverse=True)
    hc.sort(key=lambda c: c.market_cap or 0.0, reverse=True)
    stats = {
        "tech_candidates": len(tech),
        "healthcare_candidates": len(hc),
        "tech_target": _TARGET_TECH,
        "healthcare_target": _TARGET_HEALTHCARE,
    }
    return tech[:_TARGET_TECH], hc[:_TARGET_HEALTHCARE], stats


# ── Persistence ────────────────────────────────────────────────

async def _load_existing_symbols() -> set[str]:
    async with get_session() as session:
        result = await session.execute(select(UniverseSymbol.symbol))
        return {row[0] for row in result}


async def _insert_candidates(selected: list[Candidate]) -> int:
    if not selected:
        return 0
    now = datetime.now(timezone.utc)
    inserted = 0
    async with get_session() as session:
        try:
            for c in selected:
                metadata = {
                    "gross_margin_ttm": c.gross_margin_ttm,
                    "gross_margin_source": c.gross_margin_source,
                    "market_cap_at_discovery": c.market_cap,
                    "screener_source": c.screener_row.get("_requested_sector"),
                    "bucket": c.bucket,
                }
                session.add(UniverseSymbol(
                    symbol=c.symbol,
                    company_name=c.company_name,
                    source=_SOURCE,
                    market_cap=c.market_cap,
                    market_cap_tier=c.market_cap_tier,
                    sector=c.sector,
                    industry=c.industry,
                    exchange=c.exchange,
                    last_price=c.latest_price,
                    avg_volume=c.average_volume,
                    added_at=now,
                    last_screened_at=now,
                    active=True,
                    priority=_PRIORITY,
                    tier=_TIER,
                    discovery_source=_DISCOVERY_SOURCE,
                    discovery_metadata=json.dumps(metadata),
                ))
                inserted += 1
            await session.commit()
            _log.info("Inserted %d new rows in a single transaction", inserted)
        except Exception:
            await session.rollback()
            raise
    return inserted


# ── Reporting ──────────────────────────────────────────────────

def _candidate_brief(c: Candidate) -> dict:
    return {
        "symbol": c.symbol,
        "company_name": c.company_name,
        "sector": c.sector,
        "industry": c.industry,
        "exchange": c.exchange,
        "market_cap": c.market_cap,
        "market_cap_tier": c.market_cap_tier,
        "tier": _TIER,
        "bucket": c.bucket,
        "gross_margin_ttm": c.gross_margin_ttm,
        "gross_margin_source": c.gross_margin_source,
        "live_fetched": c.live_fetched,
    }


def _summarize(
    screener_counts: dict[str, int],
    total_screener_rows: int,
    existing_universe_count: int,
    new_vs_existing: int,
    reject_counts: dict[str, int],
    live_counts: dict[str, int],
    tech_selected: list[Candidate],
    hc_selected: list[Candidate],
    bucket_stats: dict[str, int],
    inserted_count: int,
    executed: bool,
    sample_size: int = 10,
) -> dict:
    selected = tech_selected + hc_selected
    sample = selected[:sample_size] if not executed else []
    cap_tier_counts: dict[str, int] = {}
    live_fetched_in_selection = 0
    for c in selected:
        cap_tier_counts[c.market_cap_tier or "unknown"] = (
            cap_tier_counts.get(c.market_cap_tier or "unknown", 0) + 1
        )
        if c.live_fetched:
            live_fetched_in_selection += 1
    return {
        "mode": "execute" if executed else "dry_run",
        "screener_waiver": {
            **screener_counts,
            "total_screener_rows": total_screener_rows,
            "existing_universe_rows": existing_universe_count,
            "total_new_vs_existing_universe": new_vs_existing,
        },
        "filter_funnel": {k: reject_counts.get(k, 0) for k in _FILTER_ORDER},
        "live_fetch_waiver": {
            "cap_per_run": _LIVE_FETCH_CAP,
            LF_SUCCESS: live_counts.get(LF_SUCCESS, 0),
            LF_FAILED: live_counts.get(LF_FAILED, 0),
            LF_REJECTED_CAP: live_counts.get(LF_REJECTED_CAP, 0),
            "survivors_from_live_fetch": live_fetched_in_selection,
        },
        "bucket_stats": bucket_stats,
        "achieved": {
            "tech_selected": len(tech_selected),
            "healthcare_selected": len(hc_selected),
            "total_selected": len(selected),
            "tech_shortfall": max(0, _TARGET_TECH - len(tech_selected)),
            "healthcare_shortfall": max(0, _TARGET_HEALTHCARE - len(hc_selected)),
        },
        "market_cap_tier_distribution": cap_tier_counts,
        "inserted_rows": inserted_count,
        "spot_sample_first_10": [_candidate_brief(c) for c in sample],
        "selected_symbols": [_candidate_brief(c) for c in selected],
    }


# ── Orchestration ──────────────────────────────────────────────

async def run_expansion(execute: bool, max_live_fetch: int = _LIVE_FETCH_CAP) -> dict:
    settings = get_settings()
    await init_db(settings.database_url)

    configured = (getattr(settings, "bulk_cache_path", "") or "").strip()
    bulk_path = Path(configured) if configured else Path(settings.database_path).parent / "company_eval_bulk.db"
    if not bulk_path.exists():
        raise RuntimeError(f"Bulk cache DB not found at {bulk_path}")

    bulk = BulkReader(str(bulk_path))
    try:
        screener_rows, screener_counts = await _run_screener_waiver()
        total_screener_rows = len(screener_rows)

        existing_symbols = await _load_existing_symbols()
        _log.info("Existing universe has %d rows", len(existing_symbols))

        existing_upper = {s.upper() for s in existing_symbols}
        new_vs_existing = sum(
            1 for r in screener_rows
            if (r.get("symbol") or "").upper() not in existing_upper
        )

        # ---- Pass 1: bulk-cache-only filter pipeline ----
        survivors, reject_counts, not_in_cache = _apply_filters(
            screener_rows, existing_symbols, bulk,
        )
        _log.info(
            "Pass 1 (bulk-cache only): survivors=%d, not_in_bulk_cache=%d",
            len(survivors), len(not_in_cache),
        )

        # ---- Pass 2: live-fetch waiver for not_in_cache symbols ----
        live_counts = {LF_SUCCESS: 0, LF_FAILED: 0, LF_REJECTED_CAP: 0}
        if not_in_cache:
            eligible_syms, live_counts, _urls = await _run_live_waiver(
                not_in_cache, bulk, max_fetch=max_live_fetch,
            )
            if eligible_syms:
                eligible_set = {s.upper() for s in eligible_syms}
                second_pass_rows = [
                    r for r in screener_rows
                    if (r.get("symbol") or "").upper() in eligible_set
                ]
                # Re-run filters; suppress not_in_cache collection on the second pass
                # (already handled).  Reject counts from this pass are merged in.
                pass2_survivors, pass2_rejects, _ = _apply_filters(
                    second_pass_rows, existing_symbols, bulk,
                    skip_not_in_cache_collect=True,
                )
                # Tag survivors as live-fetched for downstream provenance.
                for c in pass2_survivors:
                    c.live_fetched = True
                survivors.extend(pass2_survivors)
                # Merge reject counts (excluding not_in_bulk_cache so we don't
                # double-count the original pass-1 figure).
                for k, v in pass2_rejects.items():
                    if k == R_NOT_IN_BULK_CACHE:
                        continue
                    reject_counts[k] = reject_counts.get(k, 0) + v
                _log.info(
                    "Pass 2 (post-live-fetch): survivors=%d, additional rejects=%d",
                    len(pass2_survivors),
                    sum(v for k, v in pass2_rejects.items() if k != R_NOT_IN_BULK_CACHE),
                )

        _log.info("Total filter survivors: %d", len(survivors))

        tech_selected, hc_selected, bucket_stats = _rank_and_trim(survivors)
        selected = tech_selected + hc_selected
        _log.info(
            "Selected: tech=%d/%d, healthcare=%d/%d",
            len(tech_selected), _TARGET_TECH,
            len(hc_selected), _TARGET_HEALTHCARE,
        )

        inserted = 0
        if execute:
            inserted = await _insert_candidates(selected)

        return _summarize(
            screener_counts=screener_counts,
            total_screener_rows=total_screener_rows,
            existing_universe_count=len(existing_symbols),
            new_vs_existing=new_vs_existing,
            reject_counts=reject_counts,
            live_counts=live_counts,
            tech_selected=tech_selected,
            hc_selected=hc_selected,
            bucket_stats=bucket_stats,
            inserted_count=inserted,
            executed=execute,
        )
    finally:
        bulk.close()


# ── CLI ────────────────────────────────────────────────────────

def _print_summary(summary: dict) -> None:
    _log.info("──────── Phase 3d micro discovery summary ────────")
    sw = summary["screener_waiver"]
    _log.info("Live screener waiver (3 calls):")
    for k in ("screener_tech_returned", "screener_comms_returned", "screener_healthcare_returned"):
        _log.info("  %-40s %d", k + ":", sw.get(k, 0))
    _log.info("  %-40s %d", "total_screener_rows:", sw["total_screener_rows"])
    _log.info("  %-40s %d", "existing_universe_rows:", sw["existing_universe_rows"])
    _log.info("  %-40s %d", "total_new_vs_existing_universe:", sw["total_new_vs_existing_universe"])

    _log.info("Filter funnel (rejection counts):")
    for k, v in summary["filter_funnel"].items():
        if v:
            _log.info("  %-40s %d", k + ":", v)

    lf = summary.get("live_fetch_waiver", {})
    if lf:
        _log.info("Live-fetch waiver (cap=%d per run):", lf.get("cap_per_run", 0))
        _log.info("  %-40s %d", "live_fetched_success:", lf.get(LF_SUCCESS, 0))
        _log.info("  %-40s %d", "live_fetched_failed:", lf.get(LF_FAILED, 0))
        _log.info("  %-40s %d", "live_fetched_rejected_cap:", lf.get(LF_REJECTED_CAP, 0))
        _log.info("  %-40s %d", "survivors_from_live_fetch:", lf.get("survivors_from_live_fetch", 0))

    bs = summary["bucket_stats"]
    _log.info("Bucket stats: tech_candidates=%d, healthcare_candidates=%d",
              bs.get("tech_candidates", 0), bs.get("healthcare_candidates", 0))

    ach = summary["achieved"]
    _log.info("Achieved: tech=%d/%d (shortfall %d), healthcare=%d/%d (shortfall %d), total=%d",
              ach["tech_selected"], _TARGET_TECH, ach["tech_shortfall"],
              ach["healthcare_selected"], _TARGET_HEALTHCARE, ach["healthcare_shortfall"],
              ach["total_selected"])

    cap_dist = summary.get("market_cap_tier_distribution", {})
    if cap_dist:
        _log.info("Market cap tier distribution:")
        for tier, count in sorted(cap_dist.items()):
            _log.info("  %-10s %d", tier + ":", count)

    if summary.get("spot_sample_first_10"):
        _log.info("Spot sample (first 10 selected):")
        for row in summary["spot_sample_first_10"]:
            _log.info(
                "  %-6s %-30s %-12s %-25s cap=%.2fM gm=%s",
                row.get("symbol") or "",
                (row.get("company_name") or "")[:30],
                row.get("bucket") or "",
                (row.get("industry") or "")[:25],
                (row.get("market_cap") or 0) / 1e6,
                f"{row.get('gross_margin_ttm'):.3f}" if row.get("gross_margin_ttm") is not None else "n/a",
            )

    _log.info("inserted_rows: %d", summary["inserted_rows"])
    _log.info("───────────────────────────────────────────────────")


def _write_report(summary: dict, dry_run: bool) -> Path:
    name = "phase_3d_micro_discovery_report.dryrun.json" if dry_run else "phase_3d_micro_discovery_report.json"
    report_path = _PROJECT_ROOT / "logs" / name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    scrubbed = _redact_obj(summary)
    serialized = json.dumps(scrubbed, indent=2, sort_keys=True, default=str)
    # Final paranoia gate: verify no ``apikey=`` survived redaction.
    if "apikey=" in serialized.lower() and "apikey=***redacted***" not in serialized.lower():
        raise RuntimeError(
            "Refusing to write report: an unredacted apikey= string was "
            "detected in the serialised summary."
        )
    report_path.write_text(serialized)
    _log.info("Wrote report: %s", report_path)
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 3d micro discovery tier expansion")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="Default — run filters and report without writing.")
    mode.add_argument("--execute", action="store_true",
                      help="Commit in a single transaction and write the JSON report.")
    parser.add_argument("--max-live-fetch", type=int, default=_LIVE_FETCH_CAP,
                        help=f"Override the per-run live-fetch cap (default {_LIVE_FETCH_CAP}). "
                             f"Use a small value (e.g. 20) to validate end-to-end behaviour quickly.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
    )
    _install_security_filters()

    execute = bool(args.execute)
    _log.info("Phase 3d micro discovery starting (mode=%s, max_live_fetch=%d)",
              "EXECUTE" if execute else "DRY-RUN", args.max_live_fetch)

    summary = asyncio.run(run_expansion(execute=execute, max_live_fetch=args.max_live_fetch))
    _print_summary(summary)
    _write_report(summary, dry_run=not execute)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

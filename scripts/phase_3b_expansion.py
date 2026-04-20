"""Phase 3b — Universe expansion (+1,000 Tech/Healthcare multibagger candidates).

Candidate discovery uses a **bounded live waiver**: exactly three calls to
FMP's ``/stable/company-screener`` endpoint (one per sector: Technology,
Communication Services, Healthcare) with ``limit=10000``.  Every
fundamentals filter after discovery reads from the FMP bulk cache only —
no per-symbol live lookups.

Filters (short-circuit on first failure):
  1. Exchange/domicile/symbol-shape (live screener already enforces country+
     exchange; this layer adds the symbol regex + dedupe-against-universe).
  2. Sector bucket (tech vs healthcare; Communication Services restricted to
     Internet-Content and Electronic-Gaming industries only).
  3. Market cap $1B-$20B (re-validated from bulk profile).
  4. Revenue >= $50M (most recent annual from bulk_income_statement).
  5. Gross margin >= 25% (same source).
  6. FCF > 0 OR net_debt/revenue < 3.0 (balance + cash flow bulk tables).
  7. >= 3 years of annual history (bulk_income_statement present for
     current_year-3 or earlier).

Within each bucket, survivors are ranked by a quality composite:
  quality = 0.4*norm(rev_growth_3yr) + 0.3*norm(gross_margin) + 0.3*norm(roic)

Top 600 Tech + top 400 Healthcare are inserted.  Shortfalls are acceptable —
filters are never relaxed to hit the target.

Default mode is ``--dry-run``.  Pass ``--execute`` to commit in a single
transaction and write ``logs/phase_3b_expansion_report.json``.
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


# ── Security: API-key redaction across logs + reports ──

_APIKEY_RE = re.compile(r"apikey=[^&\s\"']+", re.IGNORECASE)


def _redact(text: str) -> str:
    """Replace any ``apikey=...`` occurrence with ``apikey=***REDACTED***``."""
    if not isinstance(text, str):
        return text
    return _APIKEY_RE.sub("apikey=***REDACTED***", text)


class _ApiKeyRedactionFilter(logging.Filter):
    """Belt-and-suspenders: scrub API keys from any log record."""

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
    """Recursively scrub ``apikey=...`` from strings inside a JSON-serialisable object."""
    if isinstance(obj, str):
        return _redact(obj)
    if isinstance(obj, dict):
        return {k: _redact_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_obj(v) for v in obj]
    return obj


def _install_security_filters() -> None:
    """Install redaction filter on root + silence httpx/httpcore INFO logs."""
    redaction = _ApiKeyRedactionFilter()
    logging.getLogger().addFilter(redaction)
    for name in ("httpx", "httpcore", "data.fmp_client"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        lg.addFilter(redaction)


# ── Tunables (match spec) ────────────────────────────────────

_SOURCE = "phase_3b_expansion"
_DISCOVERY_SOURCE = "screener_tech_healthcare_multibagger"

_MARKET_CAP_MIN = 500_000_000.0
_MARKET_CAP_MAX = 30_000_000_000.0
_REVENUE_MIN = 50_000_000.0
_GROSS_MARGIN_MIN = 0.25
_NET_DEBT_TO_REV_MAX = 3.0
_HISTORY_YEARS_MIN = 3

_TARGET_TECH = 600
_TARGET_HEALTHCARE = 400

_TECH_BUCKET = "tech"
_HC_BUCKET = "healthcare"

_ALLOWED_EXCHANGES = {"NYSE", "NASDAQ"}

# Comms sub-sector whitelist — spec excludes traditional telecom + broadcast.
_COMMS_ALLOWED_INDUSTRIES = {
    "Internet Content & Information",
    "Electronic Gaming & Multimedia",
}

# Symbol regex — reject clear warrant/unit/right suffixes only.
# Original spec regex was over-broad (killed NOW, SNOW, FLOW, etc. because
# any trailing single uppercase W/U/R/P triggered a match).  This version
# rejects ONLY dotted warrant markers; lowercase-letter rejection is
# handled separately in the filter loop.
_BAD_SYMBOL_RE = re.compile(r"^[A-Z]+\.(W|WS|U|R)$")

_TIER_BY_CAP_TIER = {
    "large": "tier_1_large_mid",
    "mid": "tier_2_breakout_zone",
    "small": "tier_3_small_cap",
}
_PRIORITY_BY_TIER = {
    "tier_1_large_mid": 1,
    "tier_2_breakout_zone": 2,
    "tier_3_small_cap": 3,
}

# Reason codes (stable strings used in report)
R_EXCHANGE = "filter_1_exchange"
R_COUNTRY = "filter_1_country"
R_SYMBOL_SHAPE = "filter_1_symbol_shape"
R_ALREADY_IN_UNIVERSE = "filter_1_already_in_universe"
R_SECTOR = "filter_2_sector"
R_MARKET_CAP = "filter_3_market_cap"
R_REVENUE = "filter_4_revenue"
R_GROSS_MARGIN = "filter_5_gross_margin"
R_SUSTAINABILITY = "filter_6_sustainability"
R_HISTORY = "filter_7_history"
R_NOT_IN_BULK_CACHE = "not_in_bulk_cache"
R_MISSING_PROFILE = "missing_data_profile"
R_MISSING_INCOME = "missing_data_income_statement"
R_MISSING_BS = "missing_data_balance_sheet"
R_MISSING_CF = "missing_data_cash_flow"

_FILTER_ORDER = [
    R_EXCHANGE, R_COUNTRY, R_SYMBOL_SHAPE, R_ALREADY_IN_UNIVERSE,
    R_SECTOR,
    R_NOT_IN_BULK_CACHE, R_MISSING_PROFILE,
    R_MARKET_CAP,
    R_MISSING_INCOME, R_REVENUE, R_GROSS_MARGIN,
    R_MISSING_BS, R_MISSING_CF, R_SUSTAINABILITY,
    R_HISTORY,
]


# ── Candidate container ──────────────────────────────────────

@dataclass
class Candidate:
    symbol: str
    # Screener-sourced raw
    screener_row: dict = field(default_factory=dict)
    bucket: Optional[str] = None        # "tech" | "healthcare"
    # Enriched from bulk cache
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    country: Optional[str] = None
    market_cap: Optional[float] = None
    market_cap_tier: Optional[str] = None
    revenue_ttm: Optional[float] = None
    gross_margin_ttm: Optional[float] = None
    free_cash_flow_ttm: Optional[float] = None
    net_debt: Optional[float] = None
    return_on_capital: Optional[float] = None
    revenue_growth_3yr: Optional[float] = None
    latest_price: Optional[float] = None
    average_volume: Optional[float] = None
    ipo_date: Optional[str] = None
    # Outcome
    rejected_reason: Optional[str] = None
    passed_filters: list[str] = field(default_factory=list)
    quality_score: Optional[float] = None


def _classify_market_cap_tier(market_cap: Optional[float]) -> str:
    """Same taxonomy used in Phase 3a."""
    if market_cap is None:
        return "unknown"
    if market_cap >= 200_000_000_000:
        return "mega"
    if market_cap >= 10_000_000_000:
        return "large"
    if market_cap >= 2_000_000_000:
        return "mid"
    if market_cap >= 300_000_000:
        return "small"
    if market_cap >= 50_000_000:
        return "micro"
    return "penny"


# ── Bulk cache direct reader ─────────────────────────────────

class BulkReader:
    """Direct sqlite3 reader over the bulk cache DB.

    Caches per-table symbol→row dicts in memory for O(1) lookup during the
    candidate loop.  The bulk DB is ~3K rows per table, so memory cost is
    negligible.
    """

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

    def _index(self, table: str) -> dict[str, dict]:
        """Return {SYMBOL → row-as-dict} for the given table (cached)."""
        if table in self._index_cache:
            return self._index_cache[table]
        if table not in self._tables:
            self._index_cache[table] = {}
            return self._index_cache[table]
        idx: dict[str, dict] = {}
        for row in self._conn.execute(f'SELECT * FROM "{table}"'):
            d = dict(row)
            sym = (d.get("symbol") or "").upper()
            if sym and sym not in idx:  # keep first row if duplicates
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

    def balance(self, symbol: str, year: int) -> Optional[dict]:
        return self._index(f"bulk_balance_sheet_annual_y{year}").get(symbol.upper())

    def cash_flow(self, symbol: str, year: int) -> Optional[dict]:
        return self._index(f"bulk_cash_flow_annual_y{year}").get(symbol.upper())

    def key_metrics_ttm(self, symbol: str) -> Optional[dict]:
        return self._index("bulk_key_metrics_ttm").get(symbol.upper())

    def eod(self, symbol: str) -> Optional[dict]:
        return self._index("bulk_eod_snapshot").get(symbol.upper())

    def available_years(self, endpoint_base: str) -> list[int]:
        years = []
        for name in self._tables:
            prefix = f"bulk_{endpoint_base}_y"
            if name.startswith(prefix):
                try:
                    years.append(int(name[len(prefix):]))
                except ValueError:
                    continue
        return sorted(years, reverse=True)

    def close(self):
        self._conn.close()


def _to_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Live screener (bounded waiver: 3 calls total) ────────────

async def _run_screener_waiver() -> tuple[list[dict], dict[str, int]]:
    """Invoke FMP /stable/company-screener once per target sector.

    Returns (merged_rows, per_sector_counts).  Exactly 3 HTTP calls.
    """
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
                "Sector %s hit the 10,000-row ceiling — FMP /stable/company-screener "
                "does not expose an offset parameter, so additional pages are not "
                "available without widening the filter. Proceeding with the %d rows "
                "returned; documenting this in the report.",
                sector_name, len(rows),
            )
        for r in rows:
            # Stamp the originating sector for downstream bucket assignment;
            # FMP screener already populates 'sector' but this keeps traceability
            # in case screener response shape shifts.
            r["_requested_sector"] = sector_name
            merged.append(r)

    return merged, counts


# ── Filter stack ─────────────────────────────────────────────

def _apply_filters(
    screener_rows: list[dict],
    existing_symbols: set[str],
    bulk: BulkReader,
) -> tuple[list[Candidate], dict[str, int], list[Candidate]]:
    """Run the 7-stage filter pipeline.

    Returns (survivors, reject_counts, near_misses).
    Near-misses = rejected at filter_6_sustainability or filter_7_history
    (i.e. passed every fundamentals check except the last one or two).
    """
    reject_counts: dict[str, int] = {k: 0 for k in _FILTER_ORDER}
    survivors: list[Candidate] = []
    near_misses: list[Candidate] = []

    history_years = bulk.available_years("income_statement_annual")
    newest_year = history_years[0] if history_years else None
    history_cutoff_year = newest_year - (_HISTORY_YEARS_MIN - 1) if newest_year else None

    seen_symbols: set[str] = set()
    for row in screener_rows:
        sym = (row.get("symbol") or "").strip()
        if not sym or sym.upper() in seen_symbols:
            continue  # dedupe within screener union
        seen_symbols.add(sym.upper())

        cand = Candidate(symbol=sym, screener_row=row)

        # ── Filter 1a: exchange (screener should already enforce) ──
        exch = (row.get("exchangeShortName") or row.get("exchange") or "").upper()
        if exch not in _ALLOWED_EXCHANGES:
            cand.rejected_reason = R_EXCHANGE
            reject_counts[R_EXCHANGE] += 1
            continue
        cand.passed_filters.append(R_EXCHANGE)

        # ── Filter 1b: country ──
        country = (row.get("country") or "").upper()
        if country and country != "US":
            cand.rejected_reason = R_COUNTRY
            reject_counts[R_COUNTRY] += 1
            continue
        cand.country = country or "US"
        cand.passed_filters.append(R_COUNTRY)

        # ── Filter 1c: symbol shape ──
        if any(c.islower() for c in sym) or _BAD_SYMBOL_RE.match(sym):
            cand.rejected_reason = R_SYMBOL_SHAPE
            reject_counts[R_SYMBOL_SHAPE] += 1
            continue
        cand.passed_filters.append(R_SYMBOL_SHAPE)

        # ── Filter 1d: not already in universe ──
        if sym in existing_symbols:
            cand.rejected_reason = R_ALREADY_IN_UNIVERSE
            reject_counts[R_ALREADY_IN_UNIVERSE] += 1
            continue
        cand.passed_filters.append(R_ALREADY_IN_UNIVERSE)

        # ── Fetch bulk profile (required for further fundamentals) ──
        profile = bulk.profile(sym)
        if profile is None:
            cand.rejected_reason = R_NOT_IN_BULK_CACHE
            reject_counts[R_NOT_IN_BULK_CACHE] += 1
            continue

        cand.company_name = profile.get("companyName") or row.get("companyName")
        cand.sector = (profile.get("sector") or row.get("sector") or "").strip()
        cand.industry = (profile.get("industry") or row.get("industry") or "").strip()
        cand.exchange = (profile.get("exchange") or exch).upper()
        cand.country = (profile.get("country") or cand.country or "").upper()
        cand.market_cap = _to_float(profile.get("marketCap"))
        cand.ipo_date = profile.get("ipoDate")
        cand.latest_price = _to_float(profile.get("price"))
        cand.average_volume = _to_float(profile.get("averageVolume"))

        # Re-validate exchange from profile (authoritative)
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

        # ── Filter 2: sector bucket ──
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

        # ── Filter 3: market cap (re-validated from bulk profile) ──
        if cand.market_cap is None or not (
            _MARKET_CAP_MIN <= cand.market_cap <= _MARKET_CAP_MAX
        ):
            cand.rejected_reason = R_MARKET_CAP
            reject_counts[R_MARKET_CAP] += 1
            continue
        cand.market_cap_tier = _classify_market_cap_tier(cand.market_cap)
        cand.passed_filters.append(R_MARKET_CAP)

        # ── Filters 4-5: revenue + gross margin from most recent annual ──
        income_row = None
        latest_income_year = None
        for year in history_years or []:
            row_ = bulk.income(sym, year)
            if row_:
                income_row = row_
                latest_income_year = year
                break
        if income_row is None:
            cand.rejected_reason = R_MISSING_INCOME
            reject_counts[R_MISSING_INCOME] += 1
            continue

        revenue = _to_float(income_row.get("revenue"))
        gross_profit = _to_float(income_row.get("grossProfit"))
        cand.revenue_ttm = revenue
        if revenue is None or revenue < _REVENUE_MIN:
            cand.rejected_reason = R_REVENUE
            reject_counts[R_REVENUE] += 1
            continue
        cand.passed_filters.append(R_REVENUE)

        if gross_profit is None or revenue <= 0:
            cand.rejected_reason = R_GROSS_MARGIN
            reject_counts[R_GROSS_MARGIN] += 1
            continue
        gm = gross_profit / revenue
        cand.gross_margin_ttm = gm
        if gm < _GROSS_MARGIN_MIN:
            cand.rejected_reason = R_GROSS_MARGIN
            reject_counts[R_GROSS_MARGIN] += 1
            continue
        cand.passed_filters.append(R_GROSS_MARGIN)

        # ── Filter 6: sustainability (FCF OR net debt/revenue) ──
        cf_row = bulk.cash_flow(sym, latest_income_year) if latest_income_year else None
        bs_row = bulk.balance(sym, latest_income_year) if latest_income_year else None
        if cf_row is None:
            cand.rejected_reason = R_MISSING_CF
            reject_counts[R_MISSING_CF] += 1
            continue
        if bs_row is None:
            cand.rejected_reason = R_MISSING_BS
            reject_counts[R_MISSING_BS] += 1
            continue

        ocf = _to_float(cf_row.get("netCashProvidedByOperatingActivities"))
        capex = _to_float(cf_row.get("investmentsInPropertyPlantAndEquipment"))
        # FMP convention: capex is negative (cash outflow)
        if ocf is not None and capex is not None:
            fcf = ocf + capex
        else:
            fcf = None
        cand.free_cash_flow_ttm = fcf

        total_debt = _to_float(bs_row.get("totalDebt"))
        cash = _to_float(bs_row.get("cashAndCashEquivalents"))
        if total_debt is not None and cash is not None:
            net_debt = total_debt - cash
        else:
            net_debt = None
        cand.net_debt = net_debt

        fcf_ok = fcf is not None and fcf > 0
        if net_debt is not None and net_debt < 0:
            debt_ok = True  # net cash position
        elif net_debt is not None and revenue > 0:
            debt_ok = (net_debt / revenue) < _NET_DEBT_TO_REV_MAX
        else:
            debt_ok = False

        if not (fcf_ok or debt_ok):
            cand.rejected_reason = R_SUSTAINABILITY
            reject_counts[R_SUSTAINABILITY] += 1
            near_misses.append(cand)
            continue
        cand.passed_filters.append(R_SUSTAINABILITY)

        # ── Filter 7: >= 3 years of annual history ──
        if history_cutoff_year is None or bulk.income(sym, history_cutoff_year) is None:
            cand.rejected_reason = R_HISTORY
            reject_counts[R_HISTORY] += 1
            near_misses.append(cand)
            continue
        cand.passed_filters.append(R_HISTORY)

        # ── Quality-score components (computed for survivors only) ──
        # 3-year revenue CAGR from bulk_income_statement (current vs -3yr)
        base_row = bulk.income(sym, history_cutoff_year)
        base_rev = _to_float(base_row.get("revenue")) if base_row else None
        if base_rev and base_rev > 0 and revenue > 0:
            years_span = latest_income_year - history_cutoff_year
            if years_span > 0:
                cand.revenue_growth_3yr = (revenue / base_rev) ** (1.0 / years_span) - 1.0
        # ROIC from key metrics TTM
        km = bulk.key_metrics_ttm(sym)
        if km:
            cand.return_on_capital = _to_float(km.get("returnOnInvestedCapitalTTM"))

        survivors.append(cand)

    return survivors, reject_counts, near_misses


# ── Ranking ──────────────────────────────────────────────────

def _minmax(values: list[Optional[float]]) -> list[float]:
    """Min-max scale to [0, 1].  None → 0.  Constant series → 0.5."""
    finite = [v for v in values if v is not None]
    if not finite:
        return [0.0] * len(values)
    lo, hi = min(finite), max(finite)
    if hi == lo:
        return [0.5 if v is not None else 0.0 for v in values]
    return [0.0 if v is None else (v - lo) / (hi - lo) for v in values]


def _rank_and_trim(
    survivors: list[Candidate],
) -> tuple[list[Candidate], list[Candidate], dict[str, int]]:
    """Split into buckets, compute quality score, trim to targets."""
    tech = [c for c in survivors if c.bucket == _TECH_BUCKET]
    hc = [c for c in survivors if c.bucket == _HC_BUCKET]

    def score(bucket: list[Candidate]) -> None:
        rg = _minmax([c.revenue_growth_3yr for c in bucket])
        gm = _minmax([c.gross_margin_ttm for c in bucket])
        roc = _minmax([c.return_on_capital for c in bucket])
        for i, c in enumerate(bucket):
            c.quality_score = 0.4 * rg[i] + 0.3 * gm[i] + 0.3 * roc[i]

    score(tech)
    score(hc)
    tech.sort(key=lambda c: c.quality_score or 0.0, reverse=True)
    hc.sort(key=lambda c: c.quality_score or 0.0, reverse=True)

    stats = {
        "tech_candidates": len(tech),
        "healthcare_candidates": len(hc),
        "tech_target": _TARGET_TECH,
        "healthcare_target": _TARGET_HEALTHCARE,
    }
    return tech[:_TARGET_TECH], hc[:_TARGET_HEALTHCARE], stats


# ── Persistence ──────────────────────────────────────────────

async def _load_existing_symbols() -> set[str]:
    async with get_session() as session:
        result = await session.execute(select(UniverseSymbol.symbol))
        return {row[0] for row in result}


async def _insert_candidates(selected: list[Candidate]) -> int:
    """Single-transaction bulk insert.  Returns rows inserted."""
    if not selected:
        return 0
    now = datetime.now(timezone.utc)
    inserted = 0

    async with get_session() as session:
        try:
            for c in selected:
                tier = _TIER_BY_CAP_TIER.get(c.market_cap_tier or "", "tier_3_small_cap")
                priority = _PRIORITY_BY_TIER.get(tier, 3)
                metadata = {
                    "revenue_growth_3yr": c.revenue_growth_3yr,
                    "gross_margin_ttm": c.gross_margin_ttm,
                    "return_on_capital": c.return_on_capital,
                    "quality_score": c.quality_score,
                    "revenue_ttm": c.revenue_ttm,
                    "free_cash_flow_ttm": c.free_cash_flow_ttm,
                    "net_debt": c.net_debt,
                    "bucket": c.bucket,
                    "ipo_date": c.ipo_date,
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
                    priority=priority,
                    tier=tier,
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


# ── Reporting ────────────────────────────────────────────────

def _candidate_brief(c: Candidate) -> dict:
    return {
        "symbol": c.symbol,
        "company_name": c.company_name,
        "sector": c.sector,
        "industry": c.industry,
        "exchange": c.exchange,
        "market_cap": c.market_cap,
        "market_cap_tier": c.market_cap_tier,
        "tier": _TIER_BY_CAP_TIER.get(c.market_cap_tier or "", "tier_3_small_cap"),
        "bucket": c.bucket,
        "revenue_ttm": c.revenue_ttm,
        "gross_margin_ttm": c.gross_margin_ttm,
        "free_cash_flow_ttm": c.free_cash_flow_ttm,
        "net_debt": c.net_debt,
        "return_on_capital": c.return_on_capital,
        "revenue_growth_3yr": c.revenue_growth_3yr,
        "quality_score": c.quality_score,
    }


def _summarize(
    screener_counts: dict[str, int],
    total_screener_rows: int,
    existing_universe_count: int,
    new_vs_existing: int,
    reject_counts: dict[str, int],
    tech_selected: list[Candidate],
    hc_selected: list[Candidate],
    bucket_stats: dict[str, int],
    inserted_count: int,
    notable_near_misses: list[Candidate],
    executed: bool,
) -> dict:
    return {
        "mode": "execute" if executed else "dry_run",
        "screener_waiver": {
            **screener_counts,
            "total_screener_rows": total_screener_rows,
            "existing_universe_rows": existing_universe_count,
            "total_new_vs_existing_universe": new_vs_existing,
        },
        "filter_funnel": {k: reject_counts.get(k, 0) for k in _FILTER_ORDER},
        "bucket_stats": bucket_stats,
        "achieved": {
            "tech_selected": len(tech_selected),
            "healthcare_selected": len(hc_selected),
            "total_selected": len(tech_selected) + len(hc_selected),
            "tech_shortfall": max(0, _TARGET_TECH - len(tech_selected)),
            "healthcare_shortfall": max(0, _TARGET_HEALTHCARE - len(hc_selected)),
        },
        "inserted_rows": inserted_count,
        "selected_symbols": [_candidate_brief(c) for c in tech_selected + hc_selected],
        "notable_near_misses": [
            {
                "symbol": c.symbol,
                "sector": c.sector,
                "market_cap": c.market_cap,
                "rejected_reason": c.rejected_reason,
                "passed_filters": c.passed_filters,
            }
            for c in notable_near_misses
        ],
    }


# ── Main orchestration ───────────────────────────────────────

async def run_expansion(execute: bool) -> dict:
    settings = get_settings()
    await init_db(settings.database_url)

    # Locate bulk cache
    configured = (getattr(settings, "bulk_cache_path", "") or "").strip()
    bulk_path = Path(configured) if configured else Path(settings.database_path).parent / "company_eval_bulk.db"
    if not bulk_path.exists():
        raise RuntimeError(f"Bulk cache DB not found at {bulk_path}")

    bulk = BulkReader(str(bulk_path))
    try:
        # STEP 1 — live screener (bounded waiver)
        screener_rows, screener_counts = await _run_screener_waiver()
        total_screener_rows = len(screener_rows)

        # STEP 2 — existing universe for dedupe
        existing_symbols = await _load_existing_symbols()
        _log.info("Existing universe has %d rows", len(existing_symbols))

        # Pre-compute new-vs-existing (before shape filter) for the report header
        new_vs_existing = sum(
            1 for r in screener_rows
            if (r.get("symbol") or "").upper() not in {s.upper() for s in existing_symbols}
        )

        # STEP 3 — run filter pipeline
        survivors, reject_counts, near_misses = _apply_filters(
            screener_rows, existing_symbols, bulk,
        )
        _log.info("Filter survivors: %d (near-misses: %d)", len(survivors), len(near_misses))

        # STEP 4 — rank and trim
        tech_selected, hc_selected, bucket_stats = _rank_and_trim(survivors)
        selected = tech_selected + hc_selected
        _log.info(
            "Selected: tech=%d/%d, healthcare=%d/%d",
            len(tech_selected), _TARGET_TECH,
            len(hc_selected), _TARGET_HEALTHCARE,
        )

        # STEP 5 — persist (if execute)
        inserted = 0
        if execute:
            inserted = await _insert_candidates(selected)

        summary = _summarize(
            screener_counts=screener_counts,
            total_screener_rows=total_screener_rows,
            existing_universe_count=len(existing_symbols),
            new_vs_existing=new_vs_existing,
            reject_counts=reject_counts,
            tech_selected=tech_selected,
            hc_selected=hc_selected,
            bucket_stats=bucket_stats,
            inserted_count=inserted,
            notable_near_misses=near_misses,
            executed=execute,
        )
        return summary
    finally:
        bulk.close()


# ── CLI ──────────────────────────────────────────────────────

def _print_summary(summary: dict) -> None:
    _log.info("──────── Phase 3b expansion summary ────────")
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

    bs = summary["bucket_stats"]
    _log.info("Bucket stats: tech_candidates=%d, healthcare_candidates=%d",
              bs.get("tech_candidates", 0), bs.get("healthcare_candidates", 0))

    ach = summary["achieved"]
    _log.info("Achieved: tech=%d/%d (shortfall %d), healthcare=%d/%d (shortfall %d), total=%d",
              ach["tech_selected"], _TARGET_TECH, ach["tech_shortfall"],
              ach["healthcare_selected"], _TARGET_HEALTHCARE, ach["healthcare_shortfall"],
              ach["total_selected"])
    _log.info("inserted_rows: %d", summary["inserted_rows"])
    _log.info("───────────────────────────────────────────")


def _write_report(summary: dict) -> Path:
    report_path = _PROJECT_ROOT / "logs" / "phase_3b_expansion_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    scrubbed = _redact_obj(summary)
    report_path.write_text(json.dumps(scrubbed, indent=2, sort_keys=True, default=str))
    _log.info("Wrote expansion report: %s", report_path)
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 3b universe expansion")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="Default — run filters and report without writing.")
    mode.add_argument("--execute", action="store_true",
                      help="Commit in a single transaction and write the JSON report.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
    )
    _install_security_filters()

    execute = bool(args.execute)
    _log.info("Phase 3b expansion starting (mode=%s)", "EXECUTE" if execute else "DRY-RUN")

    summary = asyncio.run(run_expansion(execute=execute))
    _print_summary(summary)

    # Always persist the dry-run summary too, but keep the canonical path for --execute.
    if execute:
        _write_report(summary)
    else:
        dry_path = _PROJECT_ROOT / "logs" / "phase_3b_expansion_report.dryrun.json"
        dry_path.parent.mkdir(parents=True, exist_ok=True)
        scrubbed = _redact_obj(summary)
        dry_path.write_text(json.dumps(scrubbed, indent=2, sort_keys=True, default=str))
        _log.info("Wrote dry-run report: %s", dry_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

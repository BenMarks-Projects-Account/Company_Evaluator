"""Phase 3 Universe Expansion — ~1,710 → ~3,500 symbols.

Sources: FMP stock screener + Polygon reference tickers.
Filters: US-domiciled, $250M+ market cap, common stock only, no ADRs.

Usage:
    # Dry run — just show candidates, don't insert
    python _phase3_universe_expansion.py

    # Actually insert
    python _phase3_universe_expansion.py --commit
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import get_settings
from data.fmp_client import FMPClient
from data.polygon_client import PolygonClient


# ── Config ────────────────────────────────────────────────────
MARKET_CAP_FLOOR = 250_000_000       # $250M
FMP_SCREENER_LIMIT = 5000            # per-call limit
EXISTING_FILE = "existing_universe.json"
FMP_FILE = "fmp_candidates.json"
POLYGON_FILE = "polygon_candidates.json"
MERGED_FILE = "expansion_candidates.json"

SKIP_NAME_PATTERNS = [
    "SPAC", "ACQUISITION", "BLANK CHECK", "MERGER CORP",
    "WARRANT", "RIGHTS", "UNIT", " TRUST",
    "LIQUIDATING",
]

# Symbols with dots/hyphens are usually preferred shares, warrants, or units
SYMBOL_MAX_LEN = 5


# ── Helpers ───────────────────────────────────────────────────

def _clean_symbol(s: str) -> str:
    return s.upper().strip()


def _is_valid_symbol(s: str) -> bool:
    if not s or len(s) > SYMBOL_MAX_LEN:
        return False
    if "." in s or "-" in s or " " in s:
        return False
    if not s.isalpha():
        return False
    return True


def _is_spac_or_junk(name: str) -> bool:
    upper = (name or "").upper()
    return any(pat in upper for pat in SKIP_NAME_PATTERNS)


def assign_tier(market_cap):
    """Consistent with existing tiers in universe_expansion.py."""
    if market_cap is None:
        return "tier_2_breakout_zone"  # default for unknowns
    if market_cap >= 10_000_000_000:
        return "tier_1_large_mid"
    if market_cap >= 500_000_000:
        return "tier_2_breakout_zone"
    return "tier_3_small_cap"


def assign_refresh_days(tier: str) -> int:
    return {
        "tier_1_large_mid": 3,
        "tier_2_breakout_zone": 7,
        "tier_3_small_cap": 14,
    }.get(tier, 7)


def assign_priority(tier: str) -> int:
    return {
        "tier_1_large_mid": 75,
        "tier_2_breakout_zone": 50,
        "tier_3_small_cap": 25,
    }.get(tier, 50)


def classify_market_cap(mc):
    if mc is None:
        return "unknown"
    if mc >= 200_000_000_000:
        return "mega"
    if mc >= 10_000_000_000:
        return "large"
    if mc >= 2_000_000_000:
        return "mid"
    if mc >= 300_000_000:
        return "small"
    if mc >= 50_000_000:
        return "micro"
    return "penny"


# ── Step 1: Load existing symbols ────────────────────────────

def load_existing() -> set[str]:
    with open(EXISTING_FILE, "r") as f:
        return set(json.load(f))


# ── Step 2: Pull FMP candidates ──────────────────────────────

async def pull_fmp_candidates(settings) -> list[dict]:
    """Pull all US common stocks above $250M from FMP screener."""
    fmp = FMPClient(
        api_key=settings.fmp_api_key,
        base_url=settings.fmp_base_url,
        rate_limit_per_min=settings.fmp_rate_limit_per_min,
    )

    all_results = []

    # FMP screener: market cap ranges to get comprehensive coverage
    ranges = [
        (MARKET_CAP_FLOOR, 500_000_000, "250M-500M"),
        (500_000_000, 2_000_000_000, "500M-2B"),
        (2_000_000_000, 10_000_000_000, "2B-10B"),
        (10_000_000_000, 50_000_000_000, "10B-50B"),
        (50_000_000_000, None, "50B+"),
    ]

    for mc_min, mc_max, label in ranges:
        print(f"  FMP screener: {label}...", end=" ", flush=True)
        results = await fmp.stock_screener(
            market_cap_min=mc_min,
            market_cap_max=mc_max,
            country="US",
            exchange="nyse,nasdaq,amex",
            is_actively_trading=True,
            is_etf=False,
            is_fund=False,
            limit=FMP_SCREENER_LIMIT,
        )
        if results:
            print(f"{len(results)} results")
            all_results.extend(results)
        else:
            print("0 results (or error)")

    print(f"\n  FMP total raw: {len(all_results)}")

    # Save raw results
    with open(FMP_FILE, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"  Saved to {FMP_FILE}")

    return all_results


# ── Step 3: Pull Polygon candidates ──────────────────────────

async def pull_polygon_candidates(settings) -> list[dict]:
    """Pull all active US common stock tickers from Polygon reference."""
    polygon = PolygonClient(
        api_key=settings.polygon_api_key,
        rate_limit=settings.polygon_rate_limit,
    )

    print("  Polygon reference tickers (all pages)...", flush=True)
    all_tickers = await polygon.get_tickers(
        market="stocks",
        type="CS",
        active=True,
        limit=1000,
    )
    print(f"  Polygon total raw: {len(all_tickers)}")

    # Filter to US locale
    us_tickers = [t for t in all_tickers if t.get("locale", "").lower() == "us"]
    print(f"  After US locale filter: {len(us_tickers)}")

    # Exchange distribution
    exchanges = {}
    for t in us_tickers:
        ex = t.get("primary_exchange", "unknown")
        exchanges[ex] = exchanges.get(ex, 0) + 1
    print("  Exchange distribution:")
    for ex, count in sorted(exchanges.items(), key=lambda x: -x[1])[:10]:
        print(f"    {ex}: {count}")

    with open(POLYGON_FILE, "w") as f:
        json.dump(us_tickers, f, indent=2)
    print(f"  Saved to {POLYGON_FILE}")

    return us_tickers


# ── Step 4: Merge, deduplicate, filter ────────────────────────

def merge_and_filter(fmp_data: list[dict], polygon_data: list[dict],
                     existing: set[str]) -> dict[str, dict]:
    """Merge both sources, deduplicate, filter, return final candidates."""

    merged = {}

    # FMP first — richer data
    for c in fmp_data:
        symbol = _clean_symbol(c.get("symbol", ""))
        if not _is_valid_symbol(symbol):
            continue
        country = (c.get("country") or "").upper()
        if country not in ("US", "USA", "UNITED STATES"):
            continue
        merged[symbol] = {
            "symbol": symbol,
            "company_name": c.get("companyName", ""),
            "market_cap": c.get("marketCap"),
            "sector": c.get("sector", ""),
            "industry": c.get("industry", ""),
            "exchange": c.get("exchangeShortName", c.get("exchange", "")),
            "source": "fmp_screener_phase3",
        }

    fmp_count = len(merged)
    print(f"  FMP valid US symbols: {fmp_count}")

    # Polygon fills gaps
    poly_added = 0
    for t in polygon_data:
        symbol = _clean_symbol(t.get("ticker", ""))
        if not _is_valid_symbol(symbol):
            continue
        if symbol not in merged:
            merged[symbol] = {
                "symbol": symbol,
                "company_name": t.get("name", ""),
                "market_cap": t.get("market_cap"),
                "sector": "",
                "industry": "",
                "exchange": t.get("primary_exchange", ""),
                "source": "polygon_ref_phase3",
            }
            poly_added += 1

    print(f"  Polygon-only additions: {poly_added}")
    print(f"  Merged unique symbols: {len(merged)}")

    # Remove existing universe members
    new_only = {s: d for s, d in merged.items() if s not in existing}
    print(f"  After removing existing ({len(existing)}): {len(new_only)}")

    # Market cap filter: keep $250M+ or unknown (for Polygon-only without market cap)
    mc_filtered = {}
    below_floor = 0
    no_mcap = 0
    for s, d in new_only.items():
        mc = d.get("market_cap")
        if mc is None:
            no_mcap += 1
            mc_filtered[s] = d
        elif mc >= MARKET_CAP_FLOOR:
            mc_filtered[s] = d
        else:
            below_floor += 1

    print(f"  Below ${MARKET_CAP_FLOOR/1e6:.0f}M removed: {below_floor}")
    print(f"  Unknown market cap (kept): {no_mcap}")
    print(f"  After market cap filter: {len(mc_filtered)}")

    # Remove SPACs, blank-check companies, etc.
    spac_removed = 0
    final = {}
    for s, d in mc_filtered.items():
        if _is_spac_or_junk(d.get("company_name", "")):
            spac_removed += 1
            continue
        final[s] = d

    print(f"  SPAC/junk removed: {spac_removed}")
    print(f"  FINAL CANDIDATE COUNT: {len(final)}")

    return final


# ── Step 5: Assign tiers ─────────────────────────────────────

def assign_tiers(candidates: dict[str, dict]) -> dict[str, dict]:
    for d in candidates.values():
        mc = d.get("market_cap")
        d["tier"] = assign_tier(mc)
        d["market_cap_tier"] = classify_market_cap(mc)
        d["refresh_days"] = assign_refresh_days(d["tier"])
        d["priority"] = assign_priority(d["tier"])
    return candidates


# ── Step 6: Insert into DB ───────────────────────────────────

def insert_candidates(candidates: list[dict], db_path: str) -> tuple[int, int]:
    """Insert candidates into universe_symbols. Returns (inserted, skipped)."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    inserted = 0
    skipped = 0

    for c in candidates:
        symbol = c["symbol"]
        # Double-check existence
        cursor.execute("SELECT COUNT(*) FROM universe_symbols WHERE symbol = ?", (symbol,))
        if cursor.fetchone()[0] > 0:
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO universe_symbols
                (symbol, company_name, sector, industry, exchange,
                 market_cap, market_cap_tier, source, active, added_at,
                 priority, tier, refresh_days, discovery_source, discovery_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                c.get("company_name", ""),
                c.get("sector", ""),
                c.get("industry", ""),
                c.get("exchange", ""),
                c.get("market_cap"),
                c.get("market_cap_tier", "unknown"),
                c.get("source", "phase3_expansion"),
                True,
                now,
                c.get("priority", 50),
                c.get("tier", "tier_2_breakout_zone"),
                c.get("refresh_days", 7),
                c.get("source", "phase3_expansion"),
                json.dumps({"phase": 3, "added": now}),
            ))
            inserted += 1
        except Exception as e:
            print(f"  FAIL {symbol}: {e}")
            skipped += 1

        # Commit every 200 rows
        if inserted % 200 == 0 and inserted > 0:
            conn.commit()

    conn.commit()
    conn.close()
    return inserted, skipped


# ── Step 7: Verify ───────────────────────────────────────────

def verify(db_path: str):
    import sqlite3

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM universe_symbols WHERE active=1")
    total = c.fetchone()[0]
    print(f"\n  Total active universe: {total}")

    c.execute("SELECT tier, COUNT(*) FROM universe_symbols WHERE active=1 GROUP BY tier ORDER BY tier")
    print("  Tier distribution:")
    for tier, count in c.fetchall():
        print(f"    {tier}: {count}")

    c.execute("SELECT market_cap_tier, COUNT(*) FROM universe_symbols WHERE active=1 GROUP BY market_cap_tier ORDER BY COUNT(*) DESC LIMIT 10")
    print("  Market cap tier distribution:")
    for mct, count in c.fetchall():
        print(f"    {mct}: {count}")

    c.execute("SELECT source, COUNT(*) FROM universe_symbols WHERE active=1 GROUP BY source ORDER BY COUNT(*) DESC LIMIT 15")
    print("  Source distribution:")
    for src, count in c.fetchall():
        print(f"    {src}: {count}")

    c.execute("SELECT sector, COUNT(*) FROM universe_symbols WHERE active=1 GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 15")
    print("  Top sectors:")
    for sec, count in c.fetchall():
        print(f"    {sec or 'Unknown'}: {count}")

    conn.close()


# ── Main ──────────────────────────────────────────────────────

async def main():
    commit = "--commit" in sys.argv
    settings = get_settings()

    print("=" * 60)
    print("Phase 3 Universe Expansion")
    print(f"Mode: {'COMMIT (will insert)' if commit else 'DRY RUN (view only)'}")
    print("=" * 60)

    # Step 1: Load existing
    print("\n[1] Loading existing universe...")
    existing = load_existing()
    print(f"  Existing symbols: {len(existing)}")

    # Step 2: Pull FMP
    print("\n[2] Pulling FMP screener candidates...")
    fmp_data = await pull_fmp_candidates(settings)

    # Step 3: Pull Polygon
    print("\n[3] Pulling Polygon reference tickers...")
    polygon_data = await pull_polygon_candidates(settings)

    # Step 4: Merge
    print("\n[4] Merging, deduplicating, filtering...")
    candidates = merge_and_filter(fmp_data, polygon_data, existing)

    # Step 5: Assign tiers
    print("\n[5] Assigning tiers...")
    candidates = assign_tiers(candidates)

    # Tier distribution
    tiers = {}
    for d in candidates.values():
        t = d["tier"]
        tiers[t] = tiers.get(t, 0) + 1
    print("  Tier distribution of new candidates:")
    for t, count in sorted(tiers.items()):
        print(f"    {t}: {count}")

    # Sector distribution
    sectors = {}
    for d in candidates.values():
        sec = d.get("sector") or "Unknown"
        sectors[sec] = sectors.get(sec, 0) + 1
    print("  Sector distribution:")
    for sec, count in sorted(sectors.items(), key=lambda x: -x[1])[:15]:
        print(f"    {sec}: {count}")

    # Source distribution
    sources = {}
    for d in candidates.values():
        src = d["source"]
        sources[src] = sources.get(src, 0) + 1
    print("  Source distribution:")
    for src, count in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"    {src}: {count}")

    # Market cap unknowns
    no_mc = sum(1 for d in candidates.values() if d.get("market_cap") is None)
    print(f"  Unknown market cap: {no_mc}")

    # Summary
    print(f"\n  === SUMMARY ===")
    print(f"  Current universe: {len(existing)}")
    print(f"  New candidates: {len(candidates)}")
    print(f"  Projected total: {len(existing) + len(candidates)}")

    # Save candidates
    with open(MERGED_FILE, "w") as f:
        json.dump(list(candidates.values()), f, indent=2)
    print(f"  Saved to {MERGED_FILE}")

    if not commit:
        print("\n  *** DRY RUN — no database changes made ***")
        print("  Re-run with --commit to insert into the database.")
        return

    # Step 6: Insert
    from config import sqlite_url_to_path
    db_path = sqlite_url_to_path(settings.database_url)
    print(f"\n[6] Inserting into {db_path}...")
    candidate_list = list(candidates.values())
    inserted, skipped = insert_candidates(candidate_list, db_path)
    print(f"  Inserted: {inserted}")
    print(f"  Skipped: {skipped}")

    # Step 7: Verify
    print("\n[7] Verifying...")
    verify(db_path)


if __name__ == "__main__":
    asyncio.run(main())

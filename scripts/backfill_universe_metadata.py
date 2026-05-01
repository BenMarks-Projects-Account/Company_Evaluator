"""Phase 3a — Universe metadata backfill.

One-shot migration that brings ``universe_symbols`` (active rows only) into a
consistent state ahead of Phase 3c:

  * Rule B — rename legacy ``market_cap_tier`` strings (``large_cap`` → ``large``,
    ``small_cap`` → ``small``).
  * Rule C — for rows whose ``market_cap_tier`` is ``unknown`` or NULL, re-read
    the current market cap from the FMP bulk cache and reclassify into one of
    ``mega/large/mid/small/micro/penny``.  No live HTTP calls.
  * Rule A — for rows whose ``tier`` is NULL, map the (post-Rule-B/C)
    ``market_cap_tier`` to the Phase-2 expansion ``tier`` column.

Symbols whose market cap cannot be resolved from the bulk cache fall back to
``tier_3_small_cap`` (logged at WARN).

Default mode is ``--dry-run`` (read-only).  Pass ``--execute`` to apply writes
in a single transaction; the summary is then written to
``logs/phase_3a_backfill_report.json``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Allow ``python scripts/backfill_universe_metadata.py`` from project root.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import select  # noqa: E402

from config import get_settings  # noqa: E402
from db.database import UniverseSymbol, get_session, init_db  # noqa: E402

_log = logging.getLogger(__name__)


# ── Classification helpers ──────────────────────────────────

def _classify_market_cap_tier(market_cap: Optional[float]) -> str:
    """Mirror the existing taxonomy in ``data/universe_builder.py``."""
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


# Map post-Rule-B/C ``market_cap_tier`` → Phase-2 ``tier``.
# ``unknown`` is intentionally absent — unresolved rows fall back to
# ``tier_3_small_cap`` via the explicit default in Pass 3 (logged at WARN).
_TIER_MAP = {
    "mega": "tier_1_large_mid",
    "large": "tier_1_large_mid",
    "mid": "tier_1_large_mid",
    "small": "tier_3_small_cap",
    "micro": "tier_3_small_cap",
    "penny": "tier_3_small_cap",
}

_LEGACY_MARKET_CAP_RENAMES = {"large_cap": "large", "small_cap": "small"}

_UNRESOLVED_DEFAULT_TIER = "tier_3_small_cap"


# ── Bulk cache adapter ──────────────────────────────────────

def _build_bulk_lookup():
    """Construct a ``BulkCacheLookup`` over the local bulk cache DB.

    Returns ``None`` if the bulk cache file is missing or empty — the caller
    must treat every Rule-C row as unresolved in that case.
    """
    settings = get_settings()
    configured = (getattr(settings, "bulk_cache_path", "") or "").strip()
    if configured:
        cache_db = Path(configured)
    else:
        cache_db = Path(settings.database_path).parent / "company_eval_bulk.db"

    if not cache_db.exists():
        _log.warning("Bulk cache DB not found at %s — all Rule-C rows will be unresolved", cache_db)
        return None

    from bulk.bulk_cache import BulkCache
    from bulk.cache_lookup import BulkCacheLookup

    lookup = BulkCacheLookup(BulkCache(str(cache_db)))
    if not lookup.is_available():
        _log.warning("Bulk cache at %s has no tables — all Rule-C rows will be unresolved", cache_db)
        return None

    _log.info("Bulk cache loaded from %s", cache_db)
    return lookup


# ── Core backfill ───────────────────────────────────────────

async def run_backfill(execute: bool) -> dict:
    """Compute (and optionally apply) all three rule passes.

    Returns the structured summary dict described in the Phase-3a spec.
    """
    settings = get_settings()
    await init_db(settings.database_url)

    lookup = _build_bulk_lookup()

    summary = {
        "rows_scanned": 0,
        "null_tier_updated": 0,
        "legacy_renamed": 0,
        "unknown_reclassified": 0,
        "unknown_remaining": 0,
        "unknown_defaulted_to_tier_3": 0,
        "unknown_defaulted_symbols": [],  # list[str]
        "unresolved_symbols": [],         # list[{symbol, reason}]
        "errors": [],                     # list[{symbol, error}]
        "executed": execute,
    }

    async with get_session() as session:
        result = await session.execute(
            select(UniverseSymbol).where(UniverseSymbol.active == True)  # noqa: E712
        )
        rows = list(result.scalars().all())
        summary["rows_scanned"] = len(rows)
        _log.info("Scanned %d active universe rows", len(rows))

        try:
            for row in rows:
                original_market_cap_tier = row.market_cap_tier
                original_tier = row.tier

                # ── Pass 1 — Rule B: legacy rename ──────────
                if row.market_cap_tier in _LEGACY_MARKET_CAP_RENAMES:
                    new_label = _LEGACY_MARKET_CAP_RENAMES[row.market_cap_tier]
                    _log.debug("Rule B: %s %s → %s", row.symbol, row.market_cap_tier, new_label)
                    row.market_cap_tier = new_label
                    summary["legacy_renamed"] += 1

                # ── Pass 2 — Rule C: refresh unknown / NULL caps ──
                if row.market_cap_tier in (None, "unknown"):
                    profile = lookup.get_profile(row.symbol) if lookup else None
                    market_cap = (profile or {}).get("market_cap") if profile else None

                    if market_cap is not None and isinstance(market_cap, (int, float)):
                        new_label = _classify_market_cap_tier(float(market_cap))
                        _log.debug(
                            "Rule C: %s market_cap=%s → %s",
                            row.symbol, market_cap, new_label,
                        )
                        row.market_cap = float(market_cap)
                        row.market_cap_tier = new_label
                        summary["unknown_reclassified"] += 1
                    else:
                        reason = (
                            "not found in bulk cache"
                            if profile is None
                            else "market cap value null"
                        )
                        _log.warning(
                            "Rule C unresolved: symbol=%s reason=%s — defaulting tier to %s",
                            row.symbol, reason, _UNRESOLVED_DEFAULT_TIER,
                        )
                        row.market_cap_tier = "unknown"
                        summary["unresolved_symbols"].append(
                            {"symbol": row.symbol, "reason": reason}
                        )

                # ── Pass 3 — Rule A: backfill NULL tier ─────
                if row.tier is None:
                    mapped_tier = _TIER_MAP.get(row.market_cap_tier)
                    if mapped_tier is None:
                        # Reaches here only when market_cap_tier is "unknown"
                        # (unresolved by Rule C). Per spec, default to tier_3.
                        mapped_tier = _UNRESOLVED_DEFAULT_TIER
                        summary["unknown_defaulted_to_tier_3"] += 1
                        summary["unknown_defaulted_symbols"].append(row.symbol)
                    row.tier = mapped_tier
                    summary["null_tier_updated"] += 1

                # Track rows that remain ``unknown`` for visibility.
                if row.market_cap_tier == "unknown":
                    summary["unknown_remaining"] += 1

                # Revert in-memory mutation when dry-running so SQLAlchemy
                # doesn't try to flush anything if the session is reused.
                if not execute:
                    row.market_cap_tier = original_market_cap_tier
                    row.tier = original_tier

            if execute:
                await session.commit()
                _log.info("Committed all updates in a single transaction")
            else:
                # Defensive — nothing should be dirty, but make it explicit.
                await session.rollback()
                _log.info("Dry-run complete — no writes performed")

        except Exception as exc:  # rollback on ANY failure
            await session.rollback()
            _log.exception("Backfill aborted — rolled back: %s", exc)
            summary["errors"].append({"phase": "apply", "error": str(exc)})
            raise

    return summary


# ── CLI plumbing ────────────────────────────────────────────

def _print_summary(summary: dict) -> None:
    _log.info("──────── Phase 3a backfill summary ────────")
    _log.info("rows_scanned:                  %d", summary["rows_scanned"])
    _log.info("null_tier_updated:             %d", summary["null_tier_updated"])
    _log.info("legacy_renamed:                %d", summary["legacy_renamed"])
    _log.info("unknown_reclassified:          %d", summary["unknown_reclassified"])
    _log.info("unknown_remaining:             %d", summary["unknown_remaining"])
    _log.info("unknown_defaulted_to_tier_3:   %d", summary["unknown_defaulted_to_tier_3"])
    if summary["unknown_defaulted_symbols"]:
        _log.info(
            "Defaulted symbols (review for misclassifications): %s",
            ", ".join(summary["unknown_defaulted_symbols"]),
        )
    if summary["errors"]:
        _log.warning("errors: %s", summary["errors"])
    _log.info("───────────────────────────────────────────")


def _write_report(summary: dict) -> Path:
    report_path = _PROJECT_ROOT / "logs" / "phase_3a_backfill_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    _log.info("Wrote backfill report: %s", report_path)
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 3a universe metadata backfill")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true",
        help="Default. Compute changes and log them without writing.",
    )
    mode.add_argument(
        "--execute", action="store_true",
        help="Apply changes in a single transaction and write the JSON report.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
    )

    execute = bool(args.execute)
    mode_label = "EXECUTE" if execute else "DRY-RUN"
    _log.info("Phase 3a backfill starting (mode=%s)", mode_label)

    summary = asyncio.run(run_backfill(execute=execute))
    _print_summary(summary)

    if execute:
        _write_report(summary)

    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

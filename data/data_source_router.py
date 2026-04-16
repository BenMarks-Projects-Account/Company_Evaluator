"""Data source router — dispatches calls to Polygon, FMP, or both (shadow mode).

Reads per-call-site config from Settings.get_data_source(key).
Logs every dispatch at DEBUG level and writes structured diffs to
``logs/data_source_diff.log`` when running in shadow mode.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from config import get_settings

_log = logging.getLogger(__name__)

# ── Diff thresholds (percentage) ─────────────────────────────
_PRICE_THRESHOLD_PCT = 0.5
_INDICATOR_THRESHOLD_PCT = 1.0

_DIFF_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
_DIFF_LOG_PATH = os.path.join(_DIFF_LOG_DIR, "data_source_diff.log")


def _ensure_diff_log_dir():
    os.makedirs(_DIFF_LOG_DIR, exist_ok=True)


# ── Field comparison helpers ─────────────────────────────────

def _pct_delta(a: float, b: float) -> float | None:
    """Percentage delta between two numbers.  Returns None if base is zero."""
    if a == 0 and b == 0:
        return 0.0
    if a == 0:
        return None
    return abs(b - a) / abs(a) * 100.0


def _compare_scalar(key: str, polygon_val: Any, fmp_val: Any, threshold_pct: float) -> dict:
    """Compare a single field.  Auto-detects numeric vs categorical."""
    entry: dict[str, Any] = {"field": key}

    if polygon_val is None and fmp_val is None:
        entry["status"] = "both_none"
        return entry
    if polygon_val is None:
        entry["status"] = "missing_in_polygon"
        entry["fmp"] = fmp_val
        return entry
    if fmp_val is None:
        entry["status"] = "missing_in_fmp"
        entry["polygon"] = polygon_val
        return entry

    entry["polygon"] = polygon_val
    entry["fmp"] = fmp_val

    if isinstance(polygon_val, (int, float)) and isinstance(fmp_val, (int, float)):
        abs_delta = fmp_val - polygon_val
        pct = _pct_delta(polygon_val, fmp_val)
        entry["abs_delta"] = round(abs_delta, 6)
        entry["pct_delta"] = round(pct, 4) if pct is not None else None
        entry["exceeds_threshold"] = (pct is not None and pct > threshold_pct)
    else:
        entry["match"] = (polygon_val == fmp_val)

    return entry


def _threshold_for_field(field: str) -> float:
    """Return the percentage threshold for a given field name."""
    price_fields = {"open", "high", "low", "close", "last_price", "day_open",
                    "day_high", "day_low", "day_close", "prev_close", "change",
                    "day_vwap", "value"}
    id_fields = {"date", "symbol"}
    if field in id_fields:
        return 0.0
    if field in price_fields:
        return _PRICE_THRESHOLD_PCT
    return _INDICATOR_THRESHOLD_PCT


def compare_responses(polygon_resp: Any, fmp_resp: Any, call_site_key: str) -> dict:
    """Build a machine-readable diff between Polygon and FMP responses.

    Returns a dict suitable for JSON-lines logging.
    """
    diff: dict[str, Any] = {
        "call_site": call_site_key,
        "polygon_type": type(polygon_resp).__name__,
        "fmp_type": type(fmp_resp).__name__,
    }

    # --- Scalar / None -----------------------------------------------
    if polygon_resp is None and fmp_resp is None:
        diff["status"] = "both_none"
        return diff
    if polygon_resp is None:
        diff["status"] = "polygon_none"
        return diff
    if fmp_resp is None:
        diff["status"] = "fmp_none"
        return diff

    # --- Numeric scalars (RSI, SMA, etc.) ----------------------------
    if isinstance(polygon_resp, (int, float)) and isinstance(fmp_resp, (int, float)):
        diff["fields"] = [_compare_scalar("value", polygon_resp, fmp_resp, _INDICATOR_THRESHOLD_PCT)]
        return diff

    # --- Dicts (snapshot, macd, etc.) --------------------------------
    if isinstance(polygon_resp, dict) and isinstance(fmp_resp, dict):
        all_keys = sorted(set(polygon_resp.keys()) | set(fmp_resp.keys()))
        fields = []
        for k in all_keys:
            if k.startswith("_"):
                continue
            pv = polygon_resp.get(k)
            fv = fmp_resp.get(k)
            # For list-valued fields (e.g. "statements"), compare counts
            # instead of opaque deep equality
            if isinstance(pv, list) or isinstance(fv, list):
                plen = len(pv) if isinstance(pv, list) else None
                flen = len(fv) if isinstance(fv, list) else None
                entry: dict[str, Any] = {"field": k, "polygon_count": plen, "fmp_count": flen}
                if plen is not None and flen is not None:
                    entry["count_match"] = (plen == flen)
                fields.append(entry)
                continue
            th = _threshold_for_field(k)
            fields.append(_compare_scalar(k, pv, fv, th))
        diff["fields"] = fields
        return diff

    # --- Lists of bars (get_raw_bars, get_historical_price_eod) ------
    if isinstance(polygon_resp, list) and isinstance(fmp_resp, list):
        # Index by date for per-bar comparison
        poly_by_date = {b["date"]: b for b in polygon_resp if isinstance(b, dict) and "date" in b}
        fmp_by_date = {b["date"]: b for b in fmp_resp if isinstance(b, dict) and "date" in b}
        all_dates = sorted(set(poly_by_date.keys()) | set(fmp_by_date.keys()))

        diff["polygon_bar_count"] = len(polygon_resp)
        diff["fmp_bar_count"] = len(fmp_resp)

        bar_diffs = []
        for dt in all_dates:
            pb = poly_by_date.get(dt)
            fb = fmp_by_date.get(dt)
            if pb and fb:
                bar_entry = {"date": dt, "fields": []}
                for fld in ("open", "high", "low", "close", "volume"):
                    bar_entry["fields"].append(
                        _compare_scalar(fld, pb.get(fld), fb.get(fld), _threshold_for_field(fld))
                    )
                bar_diffs.append(bar_entry)
            elif pb:
                bar_diffs.append({"date": dt, "status": "missing_in_fmp"})
            else:
                bar_diffs.append({"date": dt, "status": "missing_in_polygon"})

        diff["bar_diffs_sample"] = bar_diffs[:10]  # cap to avoid huge logs
        diff["total_dates"] = len(all_dates)
        diff["dates_only_polygon"] = len([d for d in all_dates if d not in fmp_by_date])
        diff["dates_only_fmp"] = len([d for d in all_dates if d not in poly_by_date])
        return diff

    # --- Fallback: stringify both ------------------------------------
    diff["status"] = "type_mismatch_or_unhandled"
    return diff


def _write_diff_log(entry: dict):
    """Append a JSON-lines entry to the diff log file."""
    _ensure_diff_log_dir()
    try:
        with open(_DIFF_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as exc:
        _log.warning("Failed to write diff log: %s", exc)


# ── Main router ──────────────────────────────────────────────

class DataSourceRouter:
    """Routes data-source calls to Polygon, FMP, or both based on config.

    Usage::

        result = await router.route(
            "entry_point.get_snapshot",
            polygon_fn=polygon.get_snapshot,
            fmp_fn=fmp.get_quote,
            symbol=symbol,
        )
    """

    async def route(
        self,
        call_site_key: str,
        polygon_fn: Callable[..., Coroutine],
        fmp_fn: Callable[..., Coroutine] | None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Dispatch to the configured source for *call_site_key*."""
        settings = get_settings()
        source = settings.get_data_source(call_site_key)

        t0 = time.perf_counter()

        if source == "fmp":
            if fmp_fn is None:
                _log.warning(
                    "data_source_router: %s → fmp requested but no fmp_fn provided, falling back to polygon",
                    call_site_key,
                )
                result = await polygon_fn(*args, **kwargs)
                source = "polygon"
            else:
                result = await fmp_fn(*args, **kwargs)
            elapsed = (time.perf_counter() - t0) * 1000
            _log.debug("data_source_router: %s → %s (%.0fms)", call_site_key, source, elapsed)
            return result

        if source == "shadow":
            return await self._shadow(call_site_key, polygon_fn, fmp_fn, *args, **kwargs)

        # Default: polygon
        result = await polygon_fn(*args, **kwargs)
        elapsed = (time.perf_counter() - t0) * 1000
        _log.debug("data_source_router: %s → polygon (%.0fms)", call_site_key, elapsed)
        return result

    async def _shadow(
        self,
        call_site_key: str,
        polygon_fn: Callable[..., Coroutine],
        fmp_fn: Callable[..., Coroutine] | None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call both sources, return Polygon result, log comparison."""
        t0 = time.perf_counter()

        polygon_result = None
        fmp_result = None
        polygon_error = None
        fmp_error = None

        # Run both concurrently
        async def _call_polygon():
            return await polygon_fn(*args, **kwargs)

        async def _call_fmp():
            if fmp_fn is None:
                return None
            return await fmp_fn(*args, **kwargs)

        poly_task = asyncio.create_task(_call_polygon())
        fmp_task = asyncio.create_task(_call_fmp())

        try:
            polygon_result = await poly_task
        except Exception as exc:
            polygon_error = str(exc)
            _log.warning("data_source_router: shadow polygon error for %s: %s", call_site_key, exc)

        try:
            fmp_result = await fmp_task
        except Exception as exc:
            fmp_error = str(exc)
            _log.warning("data_source_router: shadow fmp error for %s: %s", call_site_key, exc)

        elapsed = (time.perf_counter() - t0) * 1000
        _log.debug("data_source_router: %s → shadow (%.0fms)", call_site_key, elapsed)

        # Extract symbol from args/kwargs for logging
        symbol = kwargs.get("symbol") or (args[0] if args else None)

        # Compute diff
        diff: dict = {}
        if polygon_result is not None and fmp_result is not None:
            diff = compare_responses(polygon_result, fmp_result, call_site_key)
        elif polygon_result is None and fmp_result is None:
            diff = {"status": "both_none"}
        elif polygon_result is None:
            diff = {"status": "polygon_none"}
        else:
            diff = {"status": "fmp_none"}

        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "call_site": call_site_key,
            "symbol": str(symbol) if symbol else None,
            "source_returned": "polygon",
            "elapsed_ms": round(elapsed, 1),
            "polygon_error": polygon_error,
            "fmp_error": fmp_error,
            "diff": diff,
        }
        _write_diff_log(log_entry)

        # Always return Polygon result in shadow mode
        return polygon_result


# Module-level singleton
_router = DataSourceRouter()


def get_router() -> DataSourceRouter:
    """Return the module-level DataSourceRouter instance."""
    return _router

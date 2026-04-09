"""Stock quote endpoint — real-time(ish) price via Polygon → Finnhub fallback."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from config import get_settings
from data.polygon_client import PolygonClient
from data.finnhub_client import FinnhubClient

_log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/quote/{symbol}")
async def get_quote(symbol: str):
    """Fetch current stock quote. Polygon snapshot first, Finnhub fallback."""
    symbol = symbol.upper().strip()
    settings = get_settings()

    # ── Try Polygon snapshot ─────────────────────────────────
    try:
        polygon = PolygonClient(api_key=settings.polygon_api_key, rate_limit=settings.polygon_rate_limit)
        snap = await polygon.get_snapshot(symbol)
        if snap and snap.get("last_price"):
            price = snap["last_price"]
            prev_close = snap.get("prev_close")
            change = snap.get("change")
            change_pct = snap.get("change_pct")

            # Derive change from prev_close if snapshot didn't compute it
            if change is None and prev_close and prev_close > 0:
                change = round(price - prev_close, 2)
            if change_pct is None and prev_close and prev_close > 0:
                change_pct = round((price / prev_close - 1) * 100, 2)

            return {
                "ok": True,
                "symbol": symbol,
                "price": round(price, 2),
                "change": round(change, 2) if change is not None else None,
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
                "volume": snap.get("day_volume"),
                "source": "polygon",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        _log.warning("Polygon quote failed for %s: %s", symbol, e)

    # ── Fallback: Finnhub quote ──────────────────────────────
    try:
        finnhub = FinnhubClient(api_key=settings.finnhub_api_key, rate_limit=settings.finnhub_rate_limit)
        data = await finnhub.get_quote(symbol)
        if data and data.get("c"):
            return {
                "ok": True,
                "symbol": symbol,
                "price": round(data["c"], 2),
                "change": round(data.get("d", 0), 2),
                "change_pct": round(data.get("dp", 0), 2),
                "volume": None,
                "source": "finnhub",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        _log.warning("Finnhub quote failed for %s: %s", symbol, e)

    return {"ok": False, "error": f"Could not fetch quote for {symbol}"}

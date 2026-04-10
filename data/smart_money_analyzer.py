"""Analyze insider and institutional data to produce smart money signals.

Takes raw FMP data and produces:
- Insider activity signal (buying/selling/neutral)
- Insider conviction score (transaction sizes, unique buyers, officer buys)
- Institutional momentum signal (accumulating/distributing/neutral)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

_log = logging.getLogger(__name__)


# ── Insider Activity ─────────────────────────────────────────


def analyze_insider_activity(
    transactions: list[dict],
    statistics: list[dict],
    lookback_days: int = 180,
) -> dict:
    """Analyze insider transactions over a recent lookback period.

    Returns dict with signal, counts, values, and a 0-100 sub-score.
    """
    empty = {
        "signal": "no_data",
        "transaction_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "net_shares": 0,
        "buy_value": 0.0,
        "sell_value": 0.0,
        "net_value": 0.0,
        "unique_buyers": 0,
        "officer_buys": 0,
        "director_buys": 0,
        "ten_pct_owner_buys": 0,
        "score": None,
        "_lookback_days": lookback_days,
    }

    if not transactions:
        return empty

    cutoff = datetime.now() - timedelta(days=lookback_days)

    buys: list[dict] = []
    sells: list[dict] = []
    unique_buyers: set[str] = set()

    for tx in transactions:
        tx_date_str = tx.get("transactionDate") or tx.get("filingDate")
        if not tx_date_str:
            continue
        try:
            tx_date = datetime.fromisoformat(tx_date_str.split("T")[0])
        except (ValueError, AttributeError):
            continue
        if tx_date < cutoff:
            continue

        tx_type = (tx.get("transactionType") or "").upper()
        shares = tx.get("securitiesTransacted") or 0
        price = tx.get("price") or 0
        owner_type = (tx.get("typeOfOwner") or "").lower()
        owner_name = tx.get("reportingName") or ""

        if not shares:
            continue

        # Only count intentional market transactions:
        #   P-Purchase = open-market buy (the strongest insider signal)
        #   S-Sale     = open-market sell
        # Ignore: M-Exempt (option exercise), A-Award (grant),
        #         F-InKind (tax withholding), G-Gift
        is_buy = tx_type.startswith("P")
        is_sell = tx_type.startswith("S")

        if not is_buy and not is_sell:
            continue

        entry = {
            "shares": shares,
            "value": shares * price,
            "owner_type": owner_type,
            "owner_name": owner_name,
        }

        if is_buy:
            buys.append(entry)
            unique_buyers.add(owner_name)
        elif is_sell:
            sells.append(entry)

    buy_count = len(buys)
    sell_count = len(sells)
    buy_value = sum(b["value"] for b in buys)
    sell_value = sum(s["value"] for s in sells)
    net_shares = sum(b["shares"] for b in buys) - sum(s["shares"] for s in sells)

    officer_buys = sum(1 for b in buys if "officer" in b["owner_type"])
    director_buys = sum(1 for b in buys if "director" in b["owner_type"])
    ten_pct_owner_buys = sum(1 for b in buys if "10%" in b["owner_type"])

    signal = _classify_insider_signal(
        buy_count, sell_count, buy_value, sell_value, len(unique_buyers),
        statistics,
    )
    sub_score = _compute_insider_score(
        signal, buy_count, sell_count, len(unique_buyers),
        officer_buys, director_buys, ten_pct_owner_buys,
    )

    return {
        "signal": signal,
        "transaction_count": buy_count + sell_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "net_shares": net_shares,
        "buy_value": round(buy_value, 2),
        "sell_value": round(sell_value, 2),
        "net_value": round(buy_value - sell_value, 2),
        "unique_buyers": len(unique_buyers),
        "officer_buys": officer_buys,
        "director_buys": director_buys,
        "ten_pct_owner_buys": ten_pct_owner_buys,
        "score": sub_score,
        "_lookback_days": lookback_days,
    }


def _classify_insider_signal(
    buy_count: int,
    sell_count: int,
    buy_value: float,
    sell_value: float,
    unique_buyers: int,
    statistics: list[dict] | None = None,
) -> str:
    if buy_count == 0 and sell_count == 0:
        return "no_activity"

    # Any open-market purchases (P-Purchase) are rare and meaningful
    if buy_count > 0:
        if unique_buyers >= 3 and buy_value > sell_value * 2:
            return "strong_buying"
        if buy_count > sell_count and buy_value > sell_value:
            return "strong_buying"
        if buy_count >= 1:
            return "buying"

    # Sells only — check if selling is unusual vs historical pattern
    # At most companies, routine compensation selling is NORMAL.
    # Only flag as negative when selling is elevated vs recent quarters.
    avg_quarterly_sales = _avg_quarterly_sales(statistics)
    if avg_quarterly_sales and avg_quarterly_sales > 0:
        # Compare 180-day sell count (~2 quarters) to avg quarterly rate
        expected_sells = avg_quarterly_sales * 2
        sell_ratio = sell_count / expected_sells
        if sell_ratio > 2.0:
            return "heavy_selling"  # 2x+ above normal
        if sell_ratio > 1.5:
            return "elevated_selling"
        return "routine_selling"

    # No statistics available — use simple heuristic
    # Sells-only without context defaults to routine (not alarming)
    return "routine_selling"


def _avg_quarterly_sales(statistics: list[dict] | None) -> float | None:
    """Compute average quarterly sale count from statistics data."""
    if not statistics:
        return None
    sales = [s.get("totalSales", 0) for s in statistics if s.get("totalSales")]
    if not sales:
        return None
    # Use last 8 quarters if available
    return sum(sales[:8]) / len(sales[:8])


def _compute_insider_score(
    signal: str,
    buy_count: int,
    sell_count: int,
    unique_buyers: int,
    officer_buys: int,
    director_buys: int,
    ten_pct_owner_buys: int,
) -> float | None:
    """0-100 sub-score. Heavy reward for clustered insider buying,
    mild penalty for cluster selling, neutral for no activity."""
    if signal in ("no_data", "no_activity"):
        return None

    base = {
        "strong_buying": 90,
        "buying": 70,
        "routine_selling": 55,     # Normal compensation selling — slightly above neutral
        "elevated_selling": 40,    # Noticeably above-average selling
        "heavy_selling": 25,       # 2x+ above historical selling rate
    }
    result = base.get(signal, 50)

    if officer_buys >= 2:
        result += 5
    if director_buys >= 2:
        result += 3
    if ten_pct_owner_buys >= 1:
        result += 5

    return min(100.0, float(result))


# ── Institutional Ownership ──────────────────────────────────


def analyze_institutional_ownership(snapshots: list[dict]) -> dict:
    """Analyze institutional ownership trends from FMP snapshots.

    Returns dict with current pct, QoQ changes, trend, and 0-100 sub-score.
    """
    empty = {
        "current_pct": None,
        "current_holders": None,
        "pct_change_qoq": None,
        "holder_change_qoq": None,
        "trend": "no_data",
        "new_positions_qoq": None,
        "increased_positions_qoq": None,
        "reduced_positions_qoq": None,
        "closed_positions_qoq": None,
        "put_call_ratio": None,
        "score": None,
    }

    if not snapshots:
        return empty

    current = snapshots[0]

    current_pct = _safe_float(current.get("ownershipPercent"))
    last_pct = _safe_float(current.get("lastOwnershipPercent"))
    pct_change = (current_pct - last_pct) if (current_pct is not None and last_pct is not None) else None

    current_holders = current.get("investorsHolding")
    last_holders = current.get("lastInvestorsHolding")
    holder_change = (current_holders - last_holders) if (current_holders is not None and last_holders is not None) else None

    new_positions = current.get("newPositions") or 0
    increased = current.get("increasedPositions") or 0
    reduced = current.get("reducedPositions") or 0
    closed = current.get("closedPositions") or 0

    accumulating = new_positions + increased
    distributing = closed + reduced

    if accumulating > distributing * 1.5:
        trend = "accumulating"
    elif distributing > accumulating * 1.5:
        trend = "distributing"
    else:
        trend = "stable"

    sub_score = _compute_institutional_score(
        trend, pct_change, holder_change, accumulating, distributing,
    )

    return {
        "current_pct": current_pct,
        "current_holders": current_holders,
        "pct_change_qoq": _safe_round(pct_change),
        "holder_change_qoq": holder_change,
        "trend": trend,
        "new_positions_qoq": new_positions,
        "increased_positions_qoq": increased,
        "reduced_positions_qoq": reduced,
        "closed_positions_qoq": closed,
        "put_call_ratio": _safe_float(current.get("putCallRatio")),
        "score": sub_score,
    }


def _compute_institutional_score(
    trend: str,
    pct_change: float | None,
    holder_change: int | None,
    accumulating: int,
    distributing: int,
) -> float | None:
    if trend == "no_data":
        return None

    base = {"accumulating": 75, "stable": 55, "distributing": 35}
    result = base.get(trend, 50)

    if pct_change is not None and pct_change > 2.0:
        result += 10
    elif pct_change is not None and pct_change < -2.0:
        result -= 10

    if holder_change is not None and holder_change > 10:
        result += 5
    elif holder_change is not None and holder_change < -10:
        result -= 5

    return max(0.0, min(100.0, float(result)))


# ── Helpers ──────────────────────────────────────────────────


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_round(value: float | None, decimals: int = 4) -> float | None:
    return round(value, decimals) if value is not None else None

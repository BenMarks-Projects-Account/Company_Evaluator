"""Entry Point Analysis Engine — determines if NOW is a good time to buy.

Computes a composite entry-point score from four components:
  1. Technical Analysis (35%) — RSI, MAs, 52-week position, volume, S/R
  2. Market Context   (25%) — SPY trend, VIX, regime
  3. Valuation Timing (25%) — evaluator score vs price action
  4. Catalyst         (15%) — earnings proximity, recent events

Returns a recommendation: ENTER_NOW, WAIT, or AVOID.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

from config import get_settings
from data.polygon_client import PolygonClient
from data.finnhub_client import FinnhubClient

_log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  Technical Indicator Functions (no external TA libraries)
# ═══════════════════════════════════════════════════════════════

def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    """Wilder's RSI from a list of closing prices."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_sma(closes: list[float], period: int) -> float | None:
    """Simple moving average of the last `period` closes."""
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def find_swing_levels(highs: list[float], lows: list[float],
                      lookback: int = 60) -> dict:
    """Find recent support/resistance from swing highs and lows.

    Uses local min/max within rolling windows of 5 bars.
    """
    h = highs[-lookback:] if len(highs) >= lookback else highs
    lo = lows[-lookback:] if len(lows) >= lookback else lows
    n = len(h)

    swing_highs: list[float] = []
    swing_lows: list[float] = []
    win = 5

    for i in range(win, n - win):
        if h[i] == max(h[i - win : i + win + 1]):
            swing_highs.append(h[i])
        if lo[i] == min(lo[i - win : i + win + 1]):
            swing_lows.append(lo[i])

    # Fallback: use overall min/max of last 20 bars
    recent_support = min(lo[-20:]) if lo else None
    recent_resistance = max(h[-20:]) if h else None

    if swing_lows:
        recent_support = min(swing_lows[-3:])  # nearest support cluster
    if swing_highs:
        recent_resistance = max(swing_highs[-3:])

    return {
        "support": recent_support,
        "resistance": recent_resistance,
        "swing_lows": sorted(set(swing_lows))[-5:] if swing_lows else [],
        "swing_highs": sorted(set(swing_highs))[-5:] if swing_highs else [],
    }


# ═══════════════════════════════════════════════════════════════
#  Component Scorers
# ═══════════════════════════════════════════════════════════════

def score_technical(bars: list[dict]) -> dict:
    """Compute 0-100 technical score from daily price bars."""
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars if b.get("high")]
    lows = [b["low"] for b in bars if b.get("low")]
    volumes = [b["volume"] for b in bars if b.get("volume")]

    signals: list[dict] = []
    sub_scores: dict[str, float] = {}

    price = closes[-1]

    # ── RSI ───────────────────────────────────────────────────
    rsi = compute_rsi(closes)
    if rsi is not None:
        # Peak score at RSI ≈ 35 (ideal buy zone), decay toward 0 and 100
        if rsi <= 35:
            rsi_score = min(100, 60 + (35 - rsi) * 1.5)  # oversold bonus
        elif rsi <= 55:
            rsi_score = 80 - (rsi - 35) * 1.5  # neutral zone
        elif rsi <= 70:
            rsi_score = 50 - (rsi - 55) * 2.0  # getting warm
        else:
            rsi_score = max(0, 20 - (rsi - 70) * 1.5)  # overbought penalty

        rsi_signal = "oversold" if rsi < 30 else "overbought" if rsi > 70 else "neutral"
        sub_scores["rsi"] = rsi_score
        direction = "bullish" if rsi < 45 else "bearish" if rsi > 65 else "neutral"
        signals.append({
            "signal": f"RSI {rsi_signal} at {rsi:.0f}",
            "direction": direction,
            "weight": "high",
        })
    else:
        sub_scores["rsi"] = 50

    # ── Moving Average Position ───────────────────────────────
    sma_20 = compute_sma(closes, 20)
    sma_50 = compute_sma(closes, 50)
    sma_200 = compute_sma(closes, 200)

    ma_score = 50  # default
    ma_position = "unknown"
    ma_signal_text = "insufficient data"

    if sma_50 is not None and sma_200 is not None:
        above_50 = price > sma_50
        above_200 = price > sma_200
        golden = sma_50 > sma_200

        if above_50 and above_200:
            ma_position = "above_both"
            ma_score = 55  # uptrend but not special entry
            ma_signal_text = "uptrend (above 50 & 200 SMA)"

            if sma_20 and price < sma_20:
                # Pullback to 20-day in uptrend — great entry
                ma_position = "above_50_below_20"
                ma_score = 85
                ma_signal_text = "pullback to 20-day SMA in uptrend"

        elif not above_50 and above_200:
            # Below 50 but above 200 — pullback in larger uptrend
            ma_position = "below_50_above_200"
            ma_score = 75
            ma_signal_text = "pullback to 50-day SMA (bullish)"
        elif not above_50 and not above_200:
            ma_position = "below_both"
            ma_score = 25
            ma_signal_text = "downtrend (below both MAs)"
        else:
            # above 50 but below 200 — recovery attempt
            ma_position = "above_50_below_200"
            ma_score = 45
            ma_signal_text = "recovering but below 200-day"

        # Golden/death cross adjustments
        if golden:
            ma_score = min(100, ma_score + 10)
        else:
            ma_score = max(0, ma_score - 10)
    elif sma_50 is not None:
        if price > sma_50:
            ma_score = 60
            ma_position = "above_50"
            ma_signal_text = "above 50-day SMA"
        else:
            ma_score = 40
            ma_position = "below_50"
            ma_signal_text = "below 50-day SMA"

    sub_scores["ma_position"] = ma_score
    signals.append({
        "signal": f"Price {ma_signal_text}",
        "direction": "bullish" if ma_score >= 60 else "bearish" if ma_score <= 40 else "neutral",
        "weight": "high",
    })

    # ── 52-Week Range Position ────────────────────────────────
    if highs and lows:
        year_high = max(highs)
        year_low = min(lows)
        range_width = year_high - year_low
        if range_width > 0:
            percentile = (price - year_low) / range_width
        else:
            percentile = 0.5

        # Best entry: 30-50th percentile. Penalty at extremes.
        if percentile <= 0.20:
            range_score = 55  # very low — could be value trap
        elif percentile <= 0.40:
            range_score = 85  # sweet spot — discounted
        elif percentile <= 0.60:
            range_score = 70  # mid-range, neutral-good
        elif percentile <= 0.80:
            range_score = 45  # getting expensive
        else:
            range_score = 20  # near highs — chasing

        sub_scores["52w_range"] = range_score
        signals.append({
            "signal": f"52-week percentile {percentile:.0%}",
            "direction": "bullish" if percentile < 0.5 else "bearish" if percentile > 0.8 else "neutral",
            "weight": "medium",
        })
    else:
        sub_scores["52w_range"] = 50
        percentile = None

    # ── Volume Analysis ───────────────────────────────────────
    vol_score = 50
    vol_signal = "neutral"
    if len(volumes) >= 25:
        avg_vol_20 = sum(volumes[-20:]) / 20
        recent_vol = sum(volumes[-5:]) / 5
        vol_ratio = recent_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0

        # Check if volume declining on pullback (healthy)
        recent_price_change = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] != 0 else 0

        if recent_price_change < -0.02 and vol_ratio < 0.8:
            vol_score = 80
            vol_signal = "declining_on_pullback"
        elif recent_price_change < -0.02 and vol_ratio > 1.5:
            vol_score = 25
            vol_signal = "surging_on_decline"
        elif vol_ratio > 2.0:
            vol_score = 40
            vol_signal = "unusually_high"
        elif vol_ratio < 0.5:
            vol_score = 55
            vol_signal = "unusually_low"
        else:
            vol_score = 60
            vol_signal = "normal"

    sub_scores["volume"] = vol_score
    signals.append({
        "signal": f"Volume {vol_signal.replace('_', ' ')}",
        "direction": "bullish" if vol_score >= 70 else "bearish" if vol_score <= 30 else "neutral",
        "weight": "medium",
    })

    # ── Support / Resistance ──────────────────────────────────
    sr = find_swing_levels(highs, lows)
    support = sr["support"]
    resistance = sr["resistance"]
    sr_score = 50
    near_support = False

    if support and resistance and support < price:
        dist_to_support = (price - support) / price
        dist_to_resistance = (resistance - price) / price if resistance > price else 0

        if dist_to_support < 0.03:
            sr_score = 85
            near_support = True
        elif dist_to_support < 0.06:
            sr_score = 70
            near_support = True
        elif dist_to_resistance < 0.02:
            sr_score = 25  # right at resistance
        else:
            sr_score = 55

    sub_scores["support_resistance"] = sr_score
    if near_support:
        signals.append({
            "signal": f"Near support at ${support:.2f}",
            "direction": "bullish",
            "weight": "high",
        })

    # ── Composite ─────────────────────────────────────────────
    weights = {
        "rsi": 0.25,
        "ma_position": 0.25,
        "52w_range": 0.20,
        "volume": 0.15,
        "support_resistance": 0.15,
    }
    total_score = sum(sub_scores[k] * weights[k] for k in weights if k in sub_scores)

    return {
        "score": round(total_score, 1),
        "rsi": round(rsi, 1) if rsi is not None else None,
        "rsi_signal": rsi_signal if rsi is not None else None,
        "ma_position": ma_position,
        "ma_signal": ma_signal_text,
        "sma_20": round(sma_20, 2) if sma_20 else None,
        "sma_50": round(sma_50, 2) if sma_50 else None,
        "sma_200": round(sma_200, 2) if sma_200 else None,
        "percentile_52w": round(percentile, 3) if percentile is not None else None,
        "volume_signal": vol_signal,
        "near_support": near_support,
        "support_level": round(support, 2) if support else None,
        "resistance_level": round(resistance, 2) if resistance else None,
        "sub_scores": {k: round(v, 1) for k, v in sub_scores.items()},
        "signals": signals,
    }


def score_market_context(spy_bars: list[dict] | None,
                         vix_bars: list[dict] | None) -> dict:
    """Compute 0-100 market context score from SPY and VIX data."""
    signals: list[dict] = []
    sub_scores: dict[str, float] = {}

    # ── SPY Trend ─────────────────────────────────────────────
    spy_score = 50
    spy_rsi_val = None
    spy_trend = "unknown"

    if spy_bars and len(spy_bars) >= 50:
        spy_closes = [b["close"] for b in spy_bars]
        spy_price = spy_closes[-1]
        spy_rsi_val = compute_rsi(spy_closes)
        spy_sma_20 = compute_sma(spy_closes, 20)
        spy_sma_50 = compute_sma(spy_closes, 50)

        if spy_rsi_val is not None:
            if spy_rsi_val > 75:
                spy_score = 25
                spy_trend = "overbought"
            elif spy_rsi_val > 60:
                spy_score = 55
                spy_trend = "uptrend"
            elif spy_rsi_val > 40:
                spy_score = 65
                spy_trend = "neutral"
            elif spy_rsi_val > 25:
                spy_score = 70  # pullback in market — contrarian opportunity
                spy_trend = "pullback"
            else:
                spy_score = 50  # panic — could go either way
                spy_trend = "oversold"

        if spy_sma_20 and spy_sma_50:
            if spy_price > spy_sma_20 > spy_sma_50:
                spy_score = min(100, spy_score + 10)
                spy_trend = "strong_uptrend"
            elif spy_price < spy_sma_20 and spy_price < spy_sma_50:
                spy_score = max(0, spy_score - 15)
                spy_trend = "downtrend"

        signals.append({
            "signal": f"Market SPY: {spy_trend} (RSI {spy_rsi_val:.0f})" if spy_rsi_val else f"Market SPY: {spy_trend}",
            "direction": "bullish" if spy_score >= 60 else "bearish" if spy_score < 40 else "neutral",
            "weight": "medium",
        })

    sub_scores["spy_trend"] = spy_score

    # ── VIX Level ─────────────────────────────────────────────
    vix_score = 50
    vix_val = None

    if vix_bars and len(vix_bars) >= 1:
        vix_val = vix_bars[-1]["close"]

        if vix_val < 15:
            vix_score = 60  # calm market, favorable
        elif vix_val < 20:
            vix_score = 65  # normal, good
        elif vix_val < 25:
            vix_score = 55  # slightly elevated
        elif vix_val < 35:
            vix_score = 70  # elevated fear — contrarian opportunity
        else:
            vix_score = 60  # panic — strong contrarian but risky

        signals.append({
            "signal": f"VIX at {vix_val:.1f}",
            "direction": "bullish" if vix_val > 25 else "neutral" if vix_val < 20 else "neutral",
            "weight": "medium",
        })

    sub_scores["vix"] = vix_score

    # ── Simple Regime ─────────────────────────────────────────
    if spy_score >= 60 and vix_score >= 55:
        regime = "RISK_ON"
    elif spy_score <= 35 or (vix_val and vix_val > 30):
        regime = "RISK_OFF"
    else:
        regime = "NEUTRAL"

    total = sub_scores["spy_trend"] * 0.6 + sub_scores["vix"] * 0.4

    return {
        "score": round(total, 1),
        "spy_rsi": round(spy_rsi_val, 1) if spy_rsi_val is not None else None,
        "spy_trend": spy_trend,
        "vix": round(vix_val, 1) if vix_val is not None else None,
        "regime": regime,
        "sub_scores": {k: round(v, 1) for k, v in sub_scores.items()},
        "signals": signals,
    }


def score_valuation_timing(evaluation: dict | None,
                           current_price: float,
                           price_target: dict | None) -> dict:
    """Compute 0-100 valuation timing score using evaluator data."""
    signals: list[dict] = []
    sub_scores: dict[str, float] = {}

    eval_score = None
    eval_rating = None
    price_vs_value = "unknown"
    momentum_vs_value = "unknown"

    # ── Evaluator Quality ─────────────────────────────────────
    if evaluation:
        eval_score = evaluation.get("composite_score")
        eval_rating = evaluation.get("llm_recommendation")

        if eval_score is not None:
            # Strong fundamentals deserve better entry scores
            if eval_score >= 75:
                quality = 90
                momentum_vs_value = "strong_fundamentals"
            elif eval_score >= 60:
                quality = 70
                momentum_vs_value = "good_fundamentals"
            elif eval_score >= 45:
                quality = 50
                momentum_vs_value = "average_fundamentals"
            else:
                quality = 25
                momentum_vs_value = "weak_fundamentals"
            sub_scores["fundamentals"] = quality

            signals.append({
                "signal": f"Evaluator: {eval_rating or 'N/A'} ({eval_score:.1f})",
                "direction": "bullish" if eval_score >= 65 else "bearish" if eval_score < 45 else "neutral",
                "weight": "high",
            })
    if "fundamentals" not in sub_scores:
        sub_scores["fundamentals"] = 50

    # ── Price vs Analyst Target ───────────────────────────────
    target_score = 50
    if price_target and price_target.get("target_mean"):
        target_mean = price_target["target_mean"]
        upside = (target_mean - current_price) / current_price

        if upside > 0.25:
            target_score = 90
            price_vs_value = "significantly_below_target"
        elif upside > 0.10:
            target_score = 75
            price_vs_value = "below_target"
        elif upside > 0:
            target_score = 60
            price_vs_value = "near_target"
        elif upside > -0.10:
            target_score = 40
            price_vs_value = "above_target"
        else:
            target_score = 20
            price_vs_value = "significantly_above_target"

        signals.append({
            "signal": f"Analyst target ${target_mean:.2f} ({upside:+.1%})",
            "direction": "bullish" if upside > 0.10 else "bearish" if upside < -0.10 else "neutral",
            "weight": "medium",
        })

    sub_scores["price_vs_target"] = target_score

    total = sub_scores["fundamentals"] * 0.6 + sub_scores["price_vs_target"] * 0.4

    return {
        "score": round(total, 1),
        "evaluator_score": round(eval_score, 1) if eval_score else None,
        "evaluator_rating": eval_rating,
        "price_vs_value": price_vs_value,
        "momentum_vs_value": momentum_vs_value,
        "sub_scores": {k: round(v, 1) for k, v in sub_scores.items()},
        "signals": signals,
    }


def score_catalyst(earnings: list[dict], current_price: float) -> dict:
    """Compute 0-100 catalyst score based on earnings proximity."""
    signals: list[dict] = []
    today = date.today()

    next_earnings_date = None
    last_earnings = None
    days_to_earnings = None
    days_since_earnings = None
    earnings_signal = "no_data"

    for e in earnings:
        edate_str = e.get("date")
        if not edate_str:
            continue
        try:
            edate = date.fromisoformat(edate_str)
        except ValueError:
            continue

        if edate >= today and (next_earnings_date is None or edate < next_earnings_date):
            next_earnings_date = edate
        if edate < today and (last_earnings is None or edate > last_earnings.get("_date", date.min)):
            last_earnings = {**e, "_date": edate}

    if next_earnings_date:
        days_to_earnings = (next_earnings_date - today).days

    if last_earnings:
        days_since_earnings = (today - last_earnings["_date"]).days

    # ── Scoring ───────────────────────────────────────────────
    score = 60  # base

    if days_to_earnings is not None:
        if days_to_earnings <= 7:
            score = 20
            earnings_signal = "imminent"
            signals.append({
                "signal": f"Earnings in {days_to_earnings} days — binary event risk",
                "direction": "bearish",
                "weight": "high",
            })
        elif days_to_earnings <= 14:
            score = 35
            earnings_signal = "approaching"
            signals.append({
                "signal": f"Earnings in {days_to_earnings} days — consider waiting",
                "direction": "bearish",
                "weight": "medium",
            })
        elif days_to_earnings <= 30:
            score = 55
            earnings_signal = "upcoming"
            signals.append({
                "signal": f"Earnings in {days_to_earnings} days",
                "direction": "neutral",
                "weight": "low",
            })
        else:
            score = 70
            earnings_signal = "clear"
            signals.append({
                "signal": f"{days_to_earnings} days to earnings — clear",
                "direction": "neutral",
                "weight": "low",
            })

    # Bonus if earnings just passed with a beat
    if last_earnings and days_since_earnings and days_since_earnings <= 7:
        eps_actual = last_earnings.get("eps_actual")
        eps_est = last_earnings.get("eps_estimate")
        if eps_actual is not None and eps_est is not None and eps_actual > eps_est:
            score = min(100, score + 20)
            earnings_signal = "post_beat"
            signals.append({
                "signal": f"Earnings beat {days_since_earnings}d ago (${eps_actual} vs ${eps_est})",
                "direction": "bullish",
                "weight": "high",
            })
        elif eps_actual is not None and eps_est is not None and eps_actual < eps_est:
            score = max(0, score - 15)
            earnings_signal = "post_miss"
            signals.append({
                "signal": f"Earnings miss {days_since_earnings}d ago",
                "direction": "bearish",
                "weight": "high",
            })

    if not signals:
        signals.append({
            "signal": "No earnings date data available",
            "direction": "neutral",
            "weight": "low",
        })

    return {
        "score": round(score, 1),
        "next_earnings": next_earnings_date.isoformat() if next_earnings_date else None,
        "days_to_earnings": days_to_earnings,
        "earnings_signal": earnings_signal,
        "signals": signals,
    }


# ═══════════════════════════════════════════════════════════════
#  Entry Price Suggestions
# ═══════════════════════════════════════════════════════════════

def resolve_price_target(
    finnhub_target: dict | None,
    current_price: float,
    evaluator_score: float | None = None,
    sma_200: float | None = None,
) -> tuple[float, str]:
    """Resolve a price target from the best available source.

    Fallback order:
      1. Analyst consensus (Finnhub)
      2. Evaluator-implied (if score > 70)
      3. SMA-200 mean reversion (if below)
      4. Default +10%
    """
    # Try 1: Analyst consensus from Finnhub
    if finnhub_target and not finnhub_target.get("error"):
        mean = finnhub_target.get("target_mean")
        if mean and isinstance(mean, (int, float)) and mean > 0:
            return float(mean), "analyst_consensus"

    # Try 2: Evaluator-implied target
    if evaluator_score is not None and evaluator_score > 70:
        upside_pct = 0.10 + (evaluator_score - 70) * 0.0075
        return round(current_price * (1 + upside_pct), 2), "evaluator_implied"

    # Try 3: Technical — 200 SMA reversion if current price below it
    if sma_200 is not None and current_price < sma_200:
        return round(sma_200, 2), "sma_200_reversion"

    # Try 4: Default +10%
    return round(current_price * 1.10, 2), "default_10pct"


def compute_entry_prices(price: float, tech: dict,
                         finnhub_target: dict | None,
                         evaluator_score: float | None = None) -> dict:
    """Compute suggested entry price, stop loss, and risk/reward."""
    sma_20 = tech.get("sma_20")
    sma_50 = tech.get("sma_50")
    sma_200 = tech.get("sma_200")
    support = tech.get("support_level")
    near_support = tech.get("near_support", False)

    # ── Suggested entry ───────────────────────────────────────
    if near_support and support:
        suggested_entry = round(support * 1.005, 2)
    elif "pullback" in (tech.get("ma_signal") or "") and sma_20:
        suggested_entry = round(sma_20, 2)
    elif sma_50 and price > sma_50:
        # Trending up — suggest waiting for a small dip
        suggested_entry = round(price * 0.985, 2)
    else:
        suggested_entry = round(price * 0.995, 2)

    # ── Stop loss ─────────────────────────────────────────────
    stop_candidates = [v for v in [support, sma_50] if v and v < price]
    if stop_candidates:
        suggested_stop = round(min(stop_candidates) * 0.98, 2)
    else:
        suggested_stop = round(price * 0.93, 2)  # 7% max stop

    # ── Price target (3-layer fallback) ───────────────────────
    target_price, target_source = resolve_price_target(
        finnhub_target, price, evaluator_score, sma_200,
    )

    # ── Risk/Reward ───────────────────────────────────────────
    risk = suggested_entry - suggested_stop
    reward = target_price - suggested_entry
    if risk > 0 and reward > 0:
        rr = f"{reward / risk:.1f}:1"
    elif risk > 0:
        rr = "0:1"
    else:
        rr = "N/A"

    return {
        "suggested_entry": suggested_entry,
        "suggested_stop": suggested_stop,
        "price_target": round(target_price, 2),
        "price_target_source": target_source,
        "risk_reward": rr,
    }


# ═══════════════════════════════════════════════════════════════
#  Recommendation Logic
# ═══════════════════════════════════════════════════════════════

def compute_recommendation(
    tech: dict, market: dict, valuation: dict, catalyst: dict,
) -> tuple[str, float, str]:
    """Return (recommendation, conviction, summary).

    Hard rules override the composite. Otherwise composite drives:
      ≥ 70 → ENTER_NOW
      ≥ 50 → WAIT
      < 50 → AVOID
    """
    rsi = tech.get("rsi")
    spy_rsi = market.get("spy_rsi")
    days_to_earnings = catalyst.get("days_to_earnings")
    earnings_signal = catalyst.get("earnings_signal", "")

    t_score = tech["score"]
    m_score = market["score"]
    v_score = valuation["score"]
    c_score = catalyst["score"]

    composite = (
        t_score * 0.35 +
        m_score * 0.25 +
        v_score * 0.25 +
        c_score * 0.15
    )

    # ── Hard-rule overrides ───────────────────────────────────
    if rsi is not None and rsi > 80:
        return "AVOID", min(composite, 25), "Extremely overbought (RSI > 80)"

    if days_to_earnings is not None and days_to_earnings <= 7:
        return "WAIT", min(composite, 40), "Earnings announcement imminent"

    if spy_rsi is not None and spy_rsi > 75 and t_score < 50:
        return "WAIT", min(composite, 45), "Market overbought and stock not in buy zone"

    # ── Composite-based ───────────────────────────────────────
    if composite >= 70:
        rec = "ENTER_NOW"
        summary = f"Strong entry signal ({composite:.0f}/100)"
    elif composite >= 50:
        rec = "WAIT"
        summary = f"Marginal — consider waiting for pullback ({composite:.0f}/100)"
    else:
        rec = "AVOID"
        summary = f"Unfavorable entry conditions ({composite:.0f}/100)"

    # Enrich summary with the dominant signal
    if t_score >= 75 and "pullback" in (tech.get("ma_signal") or ""):
        summary = f"Pullback in uptrend, {summary.split('(')[0].strip()}"
    elif rsi is not None and rsi < 35:
        summary = f"Oversold bounce opportunity, {summary.split('(')[0].strip()}"

    return rec, round(composite, 1), summary


# ═══════════════════════════════════════════════════════════════
#  LLM Enhancement (optional)
# ═══════════════════════════════════════════════════════════════

async def llm_entry_analysis(
    symbol: str, tech: dict, market: dict, valuation: dict,
    catalyst: dict, recommendation: str, conviction: float,
    summary: str, price: float,
) -> dict | None:
    """Call LLM for a detailed entry point analysis with key levels.

    Returns parsed dict with recommendation, conviction, analysis,
    key_levels, agrees_with_engine — or None on failure.
    """
    import json as _json
    from analysis.llm_client import call_llm

    system_prompt = (
        "You are a technical analyst. Analyze entry point data and respond in JSON only."
    )

    user_prompt = f"""Entry point analysis for {symbol} at ${price:.2f}:

Engine: {recommendation} (conviction {conviction:.0f}/100) — {summary}
RSI: {tech.get('rsi')} | MAs: {tech.get('ma_signal')} | 52W: {tech.get('percentile_52w')}
Support: ${tech.get('support_level') or 'N/A'} | Resistance: ${tech.get('resistance_level') or 'N/A'}
Volume: {tech.get('volume_signal')} | Near Support: {tech.get('near_support')}
SPY RSI: {market.get('spy_rsi')} | VIX: {market.get('vix')} | Regime: {market.get('regime')}
Evaluator: {valuation.get('evaluator_rating')} ({valuation.get('evaluator_score')})
Earnings: {catalyst.get('next_earnings')} ({catalyst.get('days_to_earnings')} days) — {catalyst.get('earnings_signal')}

Respond in this JSON format only:
{{"recommendation":"ENTER_NOW/WAIT/AVOID","conviction":0-100,"analysis":"2-3 paragraphs: technical setup with prices, market conditions, risks/catalysts","key_levels":{{"strong_buy_below":price,"take_profit_at":price,"stop_loss_at":price}},"agrees_with_engine":true/false}}"""

    try:
        raw = await call_llm(system_prompt, user_prompt, max_tokens=1000)
        if not raw:
            return None

        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines)

        parsed = _json.loads(text)

        # Validate expected fields
        result = {
            "recommendation": str(parsed.get("recommendation", "WAIT")),
            "conviction": min(100, max(0, int(parsed.get("conviction", 50)))),
            "analysis": str(parsed.get("analysis", "")),
            "key_levels": parsed.get("key_levels", {}),
            "agrees_with_engine": bool(parsed.get("agrees_with_engine", True)),
        }
        return result

    except _json.JSONDecodeError as exc:
        _log.warning("LLM entry analysis JSON parse failed: %s", exc)
        # Try to salvage — return raw text as analysis
        if raw:
            return {
                "recommendation": recommendation,
                "conviction": int(conviction),
                "analysis": raw.strip()[:2000],
                "key_levels": {},
                "agrees_with_engine": True,
            }
        return None
    except Exception as exc:
        _log.warning("LLM entry analysis failed: %s", exc)
        return None


# ═══════════════════════════════════════════════════════════════
#  Main Analyzer
# ═══════════════════════════════════════════════════════════════

async def analyze_entry_point(symbol: str, skip_llm: bool = False) -> dict:
    """Full entry point analysis for a symbol.

    Fetches price bars, market data, evaluator data, and earnings.
    Computes component scores and an overall recommendation.
    """
    settings = get_settings()
    polygon = PolygonClient(
        api_key=settings.polygon_api_key,
        rate_limit=settings.polygon_rate_limit,
    )
    finnhub = FinnhubClient(
        api_key=settings.finnhub_api_key,
        rate_limit=settings.finnhub_rate_limit,
    )

    symbol = symbol.upper().strip()
    _log.info("Entry point analysis starting for %s", symbol)

    # ── Fetch data concurrently — Polygon Starter has unlimited calls ─
    bars_task = asyncio.create_task(polygon.get_raw_bars(symbol, days=365))
    spy_bars_task = asyncio.create_task(polygon.get_raw_bars("SPY", days=120))

    # VIX — try Polygon index ticker I:VIX first
    vix_bars_task = asyncio.create_task(polygon.get_raw_bars("I:VIX", days=60))

    # Polygon TA indicators (primary — faster and more reliable)
    polygon_rsi_task = asyncio.create_task(polygon.get_rsi(symbol, window=14))
    polygon_sma20_task = asyncio.create_task(polygon.get_sma(symbol, window=20))
    polygon_sma50_task = asyncio.create_task(polygon.get_sma(symbol, window=50))
    polygon_sma200_task = asyncio.create_task(polygon.get_sma(symbol, window=200))
    polygon_macd_task = asyncio.create_task(polygon.get_macd(symbol))
    spy_rsi_task = asyncio.create_task(polygon.get_rsi("SPY", window=14))

    # Polygon snapshot for near-real-time quote (15-min delayed)
    snapshot_task = asyncio.create_task(polygon.get_snapshot(symbol))

    # Finnhub calls (concurrent — different rate limiter)
    price_target_task = asyncio.create_task(finnhub.get_price_target(symbol))
    earnings_task = asyncio.create_task(finnhub.get_earnings_calendar(symbol))

    # Await all
    bars = await bars_task
    if not bars or len(bars) < 30:
        return {
            "ok": False,
            "symbol": symbol,
            "error": f"Insufficient price data ({len(bars) if bars else 0} bars, need 30+)",
        }

    spy_bars = await spy_bars_task

    # VIX: try I:VIX, then compute proxy from SPY realized vol
    vix_bars = await vix_bars_task
    vix_proxy_used = False
    if not vix_bars or len(vix_bars) < 5:
        # Compute VIX proxy from SPY 20-day realized volatility (annualized)
        if spy_bars and len(spy_bars) >= 25:
            spy_closes = [b["close"] for b in spy_bars]
            import numpy as np
            rets = np.diff(spy_closes[-21:]) / np.array(spy_closes[-21:-1])
            realized_vol = float(np.std(rets) * np.sqrt(252) * 100)
            vix_bars = [{"close": realized_vol}]
            vix_proxy_used = True
            _log.info("VIX proxy from SPY realized vol: %.1f", realized_vol)
        else:
            vix_bars = None

    polygon_rsi = await polygon_rsi_task
    polygon_sma20 = await polygon_sma20_task
    polygon_sma50 = await polygon_sma50_task
    polygon_sma200 = await polygon_sma200_task
    polygon_macd = await polygon_macd_task
    spy_rsi_polygon = await spy_rsi_task
    snapshot = await snapshot_task

    price_target = await price_target_task
    earnings = await earnings_task

    # Evaluator data from DB
    evaluation = None
    try:
        from db.database import get_session, CompanyEvaluation
        from sqlalchemy import select
        async with get_session() as session:
            result = await session.execute(
                select(CompanyEvaluation).where(CompanyEvaluation.symbol == symbol)
            )
            row = result.scalar_one_or_none()
            if row:
                evaluation = {
                    "composite_score": row.composite_score,
                    "llm_recommendation": row.llm_recommendation,
                    "llm_conviction": row.llm_conviction,
                    "pillar_1": row.pillar_1_business_quality,
                    "pillar_2": row.pillar_2_operational_health,
                    "pillar_3": row.pillar_3_capital_allocation,
                    "pillar_4": row.pillar_4_growth_quality,
                    "pillar_5": row.pillar_5_valuation,
                }
    except Exception as exc:
        _log.warning("Could not load evaluator data for %s: %s", symbol, exc)

    # Use snapshot price if available (more recent), else last bar close
    current_price = bars[-1]["close"]
    if snapshot and snapshot.get("last_price"):
        current_price = snapshot["last_price"]

    # ── Compute component scores ──────────────────────────────
    tech = score_technical(bars)

    # Override manual TA with Polygon TA endpoints where available
    if polygon_rsi is not None:
        tech["rsi"] = round(polygon_rsi, 1)
        _log.info("Using Polygon RSI: %.1f (manual was: %s)", polygon_rsi, tech.get("rsi"))
    if polygon_sma20 is not None:
        tech["sma_20"] = round(polygon_sma20, 2)
    if polygon_sma50 is not None:
        tech["sma_50"] = round(polygon_sma50, 2)
    if polygon_sma200 is not None:
        tech["sma_200"] = round(polygon_sma200, 2)
    if polygon_macd is not None:
        tech["macd"] = polygon_macd

    market = score_market_context(spy_bars, vix_bars)
    # Override SPY RSI with Polygon TA if available
    if spy_rsi_polygon is not None:
        market["spy_rsi"] = round(spy_rsi_polygon, 1)

    val = score_valuation_timing(evaluation, current_price, price_target)
    cat = score_catalyst(earnings, current_price)

    # ── Recommendation ────────────────────────────────────────
    rec, conviction, summary = compute_recommendation(tech, market, val, cat)

    # ── Entry prices ──────────────────────────────────────────
    eval_score = evaluation.get("composite_score") if evaluation else None
    entry = compute_entry_prices(current_price, tech, price_target, eval_score)

    # ── Optional LLM analysis ─────────────────────────────────
    llm_result = None
    if not skip_llm:
        llm_result = await llm_entry_analysis(
            symbol, tech, market, val, cat,
            rec, conviction, summary, current_price,
        )

    # ── Aggregate signals ─────────────────────────────────────
    all_signals = (
        tech.get("signals", []) +
        market.get("signals", []) +
        val.get("signals", []) +
        cat.get("signals", [])
    )

    composite = round(
        tech["score"] * 0.35 +
        market["score"] * 0.25 +
        val["score"] * 0.25 +
        cat["score"] * 0.15,
        1,
    )

    _log.info(
        "Entry point analysis complete for %s: %s (%.1f) — tech=%.0f mkt=%.0f val=%.0f cat=%.0f",
        symbol, rec, composite,
        tech["score"], market["score"], val["score"], cat["score"],
    )

    return {
        "ok": True,
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recommendation": rec,
        "conviction": conviction,
        "summary": summary,
        "composite_score": composite,
        "current_price": round(current_price, 2),
        "components": {
            "technical": tech,
            "market_context": market,
            "valuation_timing": val,
            "catalyst": cat,
        },
        "suggested_entry": entry["suggested_entry"],
        "suggested_stop": entry["suggested_stop"],
        "price_target": entry["price_target"],
        "price_target_source": entry["price_target_source"],
        "risk_reward": entry["risk_reward"],
        "signals": all_signals,
        # LLM analysis
        "llm_available": llm_result is not None,
        "llm_recommendation": llm_result["recommendation"] if llm_result else None,
        "llm_conviction": llm_result["conviction"] if llm_result else None,
        "llm_analysis": llm_result["analysis"] if llm_result else None,
        "llm_key_levels": llm_result["key_levels"] if llm_result else None,
        "llm_agrees_with_engine": llm_result["agrees_with_engine"] if llm_result else None,
        "data_sources": {
            "polygon_ta": polygon_rsi is not None,
            "polygon_snapshot": snapshot is not None and snapshot.get("last_price") is not None,
            "vix_proxy": vix_proxy_used,
            "polygon_macd": polygon_macd is not None,
        },
    }

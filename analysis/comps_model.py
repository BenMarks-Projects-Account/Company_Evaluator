"""Comparable Company Analysis (Comps) — on-demand valuation model.

Fetches multiples for a subject company and its peer group from Finnhub,
computes median peer multiples, derives implied fair values, and returns
a structured verdict (UNDERVALUED → OVERVALUED).

NOT integrated into the crawler or pillar scoring — runs on demand only.
"""

import asyncio
import logging
import statistics
from datetime import datetime, timezone

from config import get_settings
from data.finnhub_client import FinnhubClient
from db.database import get_session, CompanyEvaluation
from sqlalchemy import select
from analysis.llm_client import call_llm

_log = logging.getLogger(__name__)

# ── Multiple definitions ─────────────────────────────────────
# Each entry: (display_name, finnhub_key, per_share_key, invert_better)
#   invert_better=True means lower multiple = cheaper = better
MULTIPLES = [
    ("P/E",         "peBasicExclExtraTTM", "epsTTM",                    True),
    ("EV/EBITDA",   "evEbitdaTTM",         None,                        True),
    ("P/S",         "psTTM",               "revenuePerShareTTM",        True),
    ("P/FCF",       "pfcfShareTTM",        "cashFlowPerShareTTM",       True),
    ("P/B",         "pbQuarterly",         "bookValuePerShareQuarterly", True),
    ("EV/Revenue",  "evRevenueTTM",        None,                        True),
    ("PEG",         "pegTTM",              None,                        True),
]

# Sane bounds — reject negative or absurd multiples
_MIN_MULTIPLE = 0.1
_MAX_MULTIPLE = 500.0

# Hard caps per multiple — values above these are speculative / distortive
_MULTIPLE_CAPS: dict[str, float] = {
    "peBasicExclExtraTTM":  100.0,   # P/E > 100x is speculative
    "evEbitdaTTM":          80.0,    # EV/EBITDA > 80x is extreme
    "psTTM":                40.0,    # P/S > 40x is hyper-growth territory
    "pfcfShareTTM":         80.0,    # P/FCF > 80x is extreme
    "pbQuarterly":          30.0,    # P/B > 30x is unusual
    "evRevenueTTM":         40.0,    # EV/Revenue > 40x is extreme
    "pegTTM":                5.0,    # PEG > 5x is overvalued by any measure
}

COURTESY_DELAY = 0.05  # 50 ms between Finnhub calls


# ═════════════════════════════════════════════════════════════
#  Public entry point
# ═════════════════════════════════════════════════════════════

async def analyze_comps(symbol: str, *, skip_llm: bool = False) -> dict:
    """Run comparable-company analysis for *symbol*.

    Returns dict with keys: ok, symbol, subject, peer_group, multiples_comparison,
    fair_value, verdict, confidence, llm_narrative (if not skipped), analyzed_at.
    """
    symbol = symbol.upper().strip()
    settings = get_settings()
    fh = FinnhubClient(api_key=settings.finnhub_api_key, rate_limit=settings.finnhub_rate_limit)

    # ── 1. Fetch subject profile + metrics ───────────────────
    profile_task = asyncio.create_task(fh.get_company_profile(symbol))
    metrics_task = asyncio.create_task(fh.get_basic_financials(symbol))

    profile = await profile_task
    if profile.get("error") or not profile.get("sector"):
        return {"ok": False, "symbol": symbol, "error": "Could not fetch company profile"}

    subject_metrics = await metrics_task
    subject_m = subject_metrics.get("metrics", {})
    if not subject_m:
        return {"ok": False, "symbol": symbol, "error": "No financial metrics available"}

    subject_mcap = profile.get("market_cap")  # in millions
    subject_sector = profile.get("sector")
    subject_name = profile.get("company_name", symbol)

    # ── 2. Build peer list ───────────────────────────────────
    peers = await _build_peer_list(fh, symbol, subject_sector, subject_mcap)
    if len(peers) < 2:
        return _insufficient_data_result(
            symbol, subject_name, subject_sector, subject_mcap,
            peers, f"Only {len(peers)} peer(s) found — need at least 2 for comps",
        )

    # ── 3. Fetch peer metrics (sequential with courtesy delay) ─
    peer_data = await _fetch_peer_metrics(fh, peers)
    valid_peers = {s: d for s, d in peer_data.items() if d.get("metrics")}

    # Validate peers: market-cap similarity + data quality
    valid_peers = _validate_peers(valid_peers, subject_mcap, subject_m)

    if len(valid_peers) < 2:
        return _insufficient_data_result(
            symbol, subject_name, subject_sector, subject_mcap,
            list(valid_peers.keys()),
            f"Only {len(valid_peers)} comparable peer(s) after filtering — need at least 2",
        )

    # ── 4. Compute multiples comparison ──────────────────────
    comparison = _compute_multiples_comparison(subject_m, valid_peers)

    # ── 5. Derive fair values ────────────────────────────────
    current_price = subject_m.get("52WeekHighDate") and None  # not a price
    # Use market cap / shares outstanding for current price proxy
    shares = profile.get("shares_outstanding")  # millions
    if subject_mcap and shares and shares > 0:
        current_price = (subject_mcap / shares)  # both in millions → price
    if not current_price or current_price <= 0:
        current_price = subject_m.get("52WeekHigh", 0) * 0.5 + subject_m.get("52WeekLow", 0) * 0.5
        if not current_price or current_price <= 0:
            current_price = None

    fair_values = _derive_fair_values(subject_m, comparison, current_price)

    # ── 5b. Sanity check ────────────────────────────────────
    _apply_fair_value_sanity(fair_values, current_price)

    # ── 6. Verdict ───────────────────────────────────────────
    verdict_info = _compute_verdict(fair_values, current_price)

    # ── 7. Confidence ────────────────────────────────────────
    confidence = _compute_confidence(len(valid_peers), comparison, subject_sector)

    # Lower confidence if fair value sanity check flagged an issue
    if fair_values.get("sanity") and fair_values["sanity"] != "ok":
        if confidence["level"] == "HIGH":
            confidence["level"] = "MEDIUM"
            confidence["score_pct"] = min(confidence["score_pct"], 60)
        elif confidence["level"] == "MEDIUM":
            confidence["level"] = "LOW"
            confidence["score_pct"] = min(confidence["score_pct"], 40)
        confidence["warning"] = fair_values.get("warning")

    # ── 8. Build peer summary table ──────────────────────────
    peer_table = _build_peer_table(valid_peers)

    # ── 9. Optional LLM narrative ────────────────────────────
    llm_narrative = None
    if not skip_llm:
        llm_narrative = await _llm_comps_narrative(
            symbol, subject_name, subject_sector, subject_mcap,
            current_price, comparison, fair_values, verdict_info,
            peer_table, confidence,
        )

    return {
        "ok": True,
        "symbol": symbol,
        "subject": {
            "name": subject_name,
            "sector": subject_sector,
            "market_cap_m": round(subject_mcap, 1) if subject_mcap else None,
            "current_price": round(current_price, 2) if current_price else None,
            "shares_outstanding_m": round(shares, 2) if shares else None,
        },
        "peer_group": {
            "count": len(valid_peers),
            "symbols": sorted(valid_peers.keys()),
            "details": peer_table,
        },
        "multiples_comparison": comparison,
        "fair_value": fair_values,
        "verdict": verdict_info,
        "confidence": confidence,
        "llm_narrative": llm_narrative,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


def _insufficient_data_result(
    symbol: str, name: str, sector: str | None, mcap: float | None,
    peers: list[str], reason: str,
) -> dict:
    """Return a degraded-but-valid result when peer data is insufficient.

    Returns ``ok: True`` with ``verdict.label = "INSUFFICIENT_DATA"`` so
    the endpoint doesn't 422 and the result gets persisted.
    """
    return {
        "ok": True,
        "symbol": symbol,
        "subject": {
            "name": name,
            "sector": sector,
            "market_cap_m": round(mcap, 1) if mcap else None,
            "current_price": None,
            "shares_outstanding_m": None,
        },
        "peer_group": {
            "count": len(peers),
            "symbols": sorted(peers),
            "details": [],
        },
        "multiples_comparison": [],
        "fair_value": {"composite_fair_value": None, "detail": []},
        "verdict": {
            "label": "INSUFFICIENT_DATA",
            "upside_pct": None,
            "description": reason,
        },
        "confidence": {"level": "NONE", "score_pct": 0},
        "llm_narrative": None,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


# ═════════════════════════════════════════════════════════════
#  Peer selection
# ═════════════════════════════════════════════════════════════

async def _build_peer_list(
    fh: FinnhubClient, symbol: str, sector: str, mcap: float | None,
) -> list[str]:
    """Combine DB sector peers + Finnhub peers, deduplicate, cap at 15.

    DB peers (same sector, similar mcap) are preferred because they come
    from our evaluated universe.  Finnhub peers supplement.
    """
    db_peers: list[str] = []
    fh_peers_raw: list[str] = []

    # Source 1 (preferred): DB — same sector, market-cap within 0.2x–5x
    if sector and mcap and mcap > 0:
        try:
            async with get_session() as session:
                lo = mcap * 0.2
                hi = mcap * 5.0
                stmt = (
                    select(CompanyEvaluation.symbol)
                    .where(CompanyEvaluation.sector == sector)
                    .where(CompanyEvaluation.market_cap >= lo)
                    .where(CompanyEvaluation.market_cap <= hi)
                    .where(CompanyEvaluation.symbol != symbol)
                )
                rows = (await session.execute(stmt)).scalars().all()
                db_peers = [r for r in rows]
        except Exception as exc:
            _log.warning("DB peer query failed: %s", exc)

    # Source 2: Finnhub peers
    fh_peers_raw = await fh.get_peers(symbol)
    fh_peers = [p.upper() for p in fh_peers_raw if isinstance(p, str) and p.upper() != symbol]

    # Merge: DB peers first, then Finnhub peers that aren't already included
    seen: set[str] = set()
    ordered: list[str] = []
    for p in db_peers:
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    for p in fh_peers:
        if p not in seen:
            seen.add(p)
            ordered.append(p)

    return ordered[:15]  # cap at 15; filtering later will trim further


# ═════════════════════════════════════════════════════════════
#  Data fetching
# ═════════════════════════════════════════════════════════════

async def _fetch_peer_metrics(fh: FinnhubClient, peers: list[str]) -> dict:
    """Fetch basic_financials for each peer, sequentially with courtesy delay."""
    results = {}
    for sym in peers:
        try:
            data = await fh.get_basic_financials(sym)
            results[sym] = data
        except Exception as exc:
            _log.warning("Failed to fetch metrics for peer %s: %s", sym, exc)
            results[sym] = {"metrics": {}}
        await asyncio.sleep(COURTESY_DELAY)
    return results


# ═════════════════════════════════════════════════════════════
#  Peer validation & filtering
# ═════════════════════════════════════════════════════════════

def _validate_peers(
    valid_peers: dict[str, dict],
    subject_mcap: float | None,
    subject_metrics: dict,
) -> dict[str, dict]:
    """Filter peers by market-cap similarity and data quality.

    Applies progressively relaxed market-cap windows until at least
    3 peers pass.  Also excludes peers with extreme / negative P/E
    that indicate non-comparable business models.
    """
    subject_pe = subject_metrics.get("peBasicExclExtraTTM")
    subject_profitable = subject_pe is not None and subject_pe > 0

    def _peer_passes(sym: str, pdata: dict, lo: float, hi: float) -> bool:
        m = pdata.get("metrics", {})
        peer_mcap = m.get("marketCapitalization")

        # Market-cap gate
        if peer_mcap is not None and subject_mcap and subject_mcap > 0:
            if not (lo <= peer_mcap <= hi):
                return False
        # Minimum $1 B market cap (in millions from Finnhub)
        if peer_mcap is not None and peer_mcap < 1_000:
            return False

        # If subject is profitable, exclude peers with negative or
        # extremely high P/E (>150) — they are structurally different.
        peer_pe = m.get("peBasicExclExtraTTM")
        if subject_profitable and peer_pe is not None:
            if peer_pe < 0 or peer_pe > 150:
                return False

        return True

    # Progressive relaxation: 0.2x–5x → 0.1x–10x → accept all
    for lo_mult, hi_mult in [(0.2, 5.0), (0.1, 10.0)]:
        lo = (subject_mcap or 0) * lo_mult
        hi = (subject_mcap or 1e18) * hi_mult
        filtered = {
            s: d for s, d in valid_peers.items()
            if _peer_passes(s, d, lo, hi)
        }
        if len(filtered) >= 3:
            return dict(list(filtered.items())[:12])  # cap at 12

    # Last resort — keep all peers with metrics but log warning
    _log.warning(
        "Could not find 3+ peers after validation for mcap=%.0fM — "
        "using %d unfiltered peers", subject_mcap or 0, len(valid_peers),
    )
    return dict(list(valid_peers.items())[:12])


# ═════════════════════════════════════════════════════════════
#  Multiples comparison
# ═════════════════════════════════════════════════════════════

def _compute_multiples_comparison(
    subject_metrics: dict, peer_data: dict[str, dict],
) -> list[dict]:
    """For each multiple, compute subject value, peer median/mean/range, and premium/discount.

    Applies hard caps per multiple, then IQR outlier removal on peer values
    before computing the median.
    """
    results = []

    for name, key, _per_share_key, _invert in MULTIPLES:
        subject_val = subject_metrics.get(key)

        # Collect valid peer values with hard-cap filtering
        cap = _MULTIPLE_CAPS.get(key, _MAX_MULTIPLE)
        peer_vals = []
        for _sym, pdata in peer_data.items():
            v = pdata.get("metrics", {}).get(key)
            if v is not None and _MIN_MULTIPLE <= v <= cap:
                peer_vals.append(v)

        if not peer_vals:
            results.append({
                "name": name, "key": key,
                "subject": _round(subject_val),
                "peer_median": None, "peer_mean": None,
                "peer_min": None, "peer_max": None,
                "premium_pct": None, "usable": False,
            })
            continue

        # IQR outlier removal (need ≥3 values)
        cleaned = _iqr_filter(peer_vals) if len(peer_vals) >= 3 else peer_vals

        median_val = statistics.median(cleaned)
        mean_val = statistics.mean(cleaned)

        premium_pct = None
        usable = False
        if subject_val is not None and _MIN_MULTIPLE <= subject_val <= cap and median_val > 0:
            premium_pct = round((subject_val / median_val - 1) * 100, 1)
            usable = True

        results.append({
            "name": name,
            "key": key,
            "subject": _round(subject_val) if subject_val and _MIN_MULTIPLE <= subject_val <= cap else None,
            "peer_median": _round(median_val),
            "peer_mean": _round(mean_val),
            "peer_min": _round(min(cleaned)),
            "peer_max": _round(max(cleaned)),
            "peer_count": len(cleaned),
            "outliers_removed": len(peer_vals) - len(cleaned),
            "premium_pct": premium_pct,
            "usable": usable,
        })

    return results


# ═════════════════════════════════════════════════════════════
#  Fair value derivation
# ═════════════════════════════════════════════════════════════

def _derive_fair_values(
    subject_metrics: dict,
    comparison: list[dict],
    current_price: float | None,
) -> dict:
    """Derive implied fair value per multiple and a composite fair value."""
    implied_prices = []
    detail = []

    for comp in comparison:
        if not comp["usable"] or comp["peer_median"] is None:
            continue

        name = comp["name"]
        key = comp["key"]
        peer_med = comp["peer_median"]
        subject_val = comp["subject"]

        # Try to compute implied price = (peer_median / subject_multiple) * current_price
        # Or: implied price = peer_median_multiple × per_share_fundamental
        implied = None

        # Find per-share key for this multiple
        per_share_key = None
        for _n, _k, psk, _inv in MULTIPLES:
            if _k == key:
                per_share_key = psk
                break

        if per_share_key:
            per_share_val = subject_metrics.get(per_share_key)
            if per_share_val and per_share_val > 0:
                implied = peer_med * per_share_val

        # Fallback: ratio method if we have current price and subject multiple
        if implied is None and current_price and current_price > 0 and subject_val and subject_val > 0:
            implied = current_price * (peer_med / subject_val)

        if implied and implied > 0:
            implied_prices.append(implied)
            detail.append({
                "multiple": name,
                "implied_price": round(implied, 2),
                "method": "per_share" if per_share_key and subject_metrics.get(per_share_key, 0) > 0 else "ratio",
            })

    composite_fair_value = None
    if implied_prices:
        composite_fair_value = round(statistics.median(implied_prices), 2)

    upside_pct = None
    if composite_fair_value and current_price and current_price > 0:
        upside_pct = round((composite_fair_value / current_price - 1) * 100, 1)

    return {
        "composite_fair_value": composite_fair_value,
        "current_price": round(current_price, 2) if current_price else None,
        "upside_pct": upside_pct,
        "implied_by_multiple": detail,
        "multiples_used": len(detail),
    }


# ═════════════════════════════════════════════════════════════
#  Verdict
# ═════════════════════════════════════════════════════════════

def _compute_verdict(fair_values: dict, current_price: float | None) -> dict:
    """Classify as UNDERVALUED → OVERVALUED based on composite upside."""
    upside = fair_values.get("upside_pct")
    if upside is None:
        return {"label": "INSUFFICIENT_DATA", "upside_pct": None, "description": "Could not compute fair value"}

    if upside >= 25:
        label, desc = "UNDERVALUED", "Trading significantly below peer-implied fair value"
    elif upside >= 10:
        label, desc = "SLIGHTLY_UNDERVALUED", "Trading modestly below peer-implied fair value"
    elif upside >= -10:
        label, desc = "FAIRLY_VALUED", "Trading near peer-implied fair value"
    elif upside >= -25:
        label, desc = "SLIGHTLY_OVERVALUED", "Trading modestly above peer-implied fair value"
    else:
        label, desc = "OVERVALUED", "Trading significantly above peer-implied fair value"

    return {"label": label, "upside_pct": upside, "description": desc}


# ═════════════════════════════════════════════════════════════
#  Confidence
# ═════════════════════════════════════════════════════════════

def _compute_confidence(
    peer_count: int, comparison: list[dict], sector: str,
) -> dict:
    """Confidence level based on peer count and data coverage."""
    usable = sum(1 for c in comparison if c["usable"])
    total = len(comparison)

    peer_score = min(peer_count / 8.0, 1.0)       # 8+ peers → full confidence
    coverage_score = usable / total if total > 0 else 0  # fraction of usable multiples

    raw = peer_score * 0.6 + coverage_score * 0.4
    pct = round(raw * 100)

    if pct >= 75:
        level = "HIGH"
    elif pct >= 50:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        "level": level,
        "score_pct": pct,
        "peer_count": peer_count,
        "usable_multiples": usable,
        "total_multiples": total,
    }


# ═════════════════════════════════════════════════════════════
#  Peer summary table
# ═════════════════════════════════════════════════════════════

def _build_peer_table(valid_peers: dict[str, dict]) -> list[dict]:
    """Build a summary row per peer with key multiples."""
    rows = []
    for sym, pdata in sorted(valid_peers.items()):
        m = pdata.get("metrics", {})
        rows.append({
            "symbol": sym,
            "pe": _round(m.get("peBasicExclExtraTTM")),
            "ev_ebitda": _round(m.get("evEbitdaTTM")),
            "ps": _round(m.get("psTTM")),
            "pfcf": _round(m.get("pfcfShareTTM")),
            "pb": _round(m.get("pbQuarterly")),
            "ev_revenue": _round(m.get("evRevenueTTM")),
            "peg": _round(m.get("pegTTM")),
            "market_cap_m": _round(m.get("marketCapitalization")),
        })
    return rows


# ═════════════════════════════════════════════════════════════
#  LLM narrative
# ═════════════════════════════════════════════════════════════

async def _llm_comps_narrative(
    symbol, name, sector, mcap, price,
    comparison, fair_values, verdict, peer_table, confidence,
) -> str | None:
    """Ask LLM for a concise comps analysis narrative."""
    system = (
        "You are a senior equity research analyst. Write a concise comparable-company "
        "valuation analysis (3-5 paragraphs). Be direct, specific, and data-driven. "
        "Reference the actual multiples and peer data provided. End with a clear verdict."
    )

    # Build compact user prompt
    comp_lines = []
    for c in comparison:
        if c["usable"]:
            comp_lines.append(
                f"  {c['name']}: subject={c['subject']}x, peer median={c['peer_median']}x, "
                f"premium={c['premium_pct']:+.1f}%"
            )

    peer_names = ", ".join(p["symbol"] for p in peer_table[:8])

    fv = fair_values.get("composite_fair_value")
    upside = fair_values.get("upside_pct")

    user = (
        f"Company: {name} ({symbol})\n"
        f"Sector: {sector} | Market Cap: ${mcap:,.0f}M\n"
        f"Current Price: ${price:,.2f}\n\n"
        f"Peer Group ({len(peer_table)}): {peer_names}\n\n"
        f"Multiples vs Peers:\n" + "\n".join(comp_lines) + "\n\n"
        f"Composite Fair Value: ${fv:,.2f} ({upside:+.1f}% upside)\n"
        f"Verdict: {verdict['label']}\n"
        f"Confidence: {confidence['level']} ({confidence['score_pct']}%)\n\n"
        f"Write a comparable-company valuation analysis for {symbol}."
    ) if fv and price and upside is not None else (
        f"Company: {name} ({symbol}), Sector: {sector}\n"
        f"Limited valuation data available. Summarize what can be inferred from the peer comparison."
    )

    return await call_llm(system, user, max_tokens=1000)


# ── helpers ──────────────────────────────────────────────────

def _iqr_filter(values: list[float]) -> list[float]:
    """Remove outliers using the 1.5×IQR rule. Returns filtered list (≥1 item)."""
    s = sorted(values)
    n = len(s)
    q1 = s[n // 4]
    q3 = s[3 * n // 4]
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    filtered = [v for v in s if lo <= v <= hi]
    return filtered if filtered else s  # fallback to unfiltered


def _apply_fair_value_sanity(fair_values: dict, current_price: float | None):
    """Flag unrealistic fair values and lower confidence when FV is extreme."""
    fv = fair_values.get("composite_fair_value")
    if fv is None or current_price is None or current_price <= 0:
        return

    ratio = fv / current_price
    if ratio > 2.0:
        fair_values["warning"] = (
            f"Fair value (${fv:,.0f}) is >{ratio:.1f}x current price (${current_price:,.0f}). "
            "Peer multiples may be inflated by outliers. Treat with caution."
        )
        fair_values["sanity"] = "extreme_upside"
    elif ratio < 0.3:
        fair_values["warning"] = (
            f"Fair value (${fv:,.0f}) is <30% of current price (${current_price:,.0f}). "
            "Peer multiples may not be representative. Treat with caution."
        )
        fair_values["sanity"] = "extreme_downside"
    else:
        fair_values["warning"] = None
        fair_values["sanity"] = "ok"


def _round(v, n=2):
    return round(v, n) if isinstance(v, (int, float)) and v == v else None

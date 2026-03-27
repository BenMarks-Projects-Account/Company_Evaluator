"""Company analyst — builds LLM prompt from scores and parses the response."""

import json
import logging
from analysis.llm_client import call_llm
from analysis.prompts import COMPANY_ANALYSIS_SYSTEM_PROMPT

_log = logging.getLogger(__name__)

_VALID_RECOMMENDATIONS = {"STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"}


async def analyze_company(
    symbol: str,
    profile: dict,
    scores: dict,
) -> dict | None:
    """Send scored metrics to the LLM and return parsed analysis.

    Returns a dict with recommendation, conviction, summary, thesis, risks,
    catalysts — or None if the LLM is unavailable / response unparseable.
    """
    user_prompt = _build_user_prompt(symbol, profile, scores)
    _log.info("event=llm_analysis_start symbol=%s", symbol)

    raw = await call_llm(COMPANY_ANALYSIS_SYSTEM_PROMPT, user_prompt)
    if raw is None:
        _log.warning("event=llm_no_response symbol=%s", symbol)
        return None

    parsed = _parse_response(raw)
    if parsed is None:
        _log.warning("event=llm_parse_failed symbol=%s raw_len=%d", symbol, len(raw))
        return None

    _log.info(
        "event=llm_analysis_complete symbol=%s rec=%s conviction=%s",
        symbol, parsed.get("recommendation"), parsed.get("conviction"),
    )
    return parsed


def _build_user_prompt(symbol: str, profile: dict, scores: dict) -> str:
    """Construct the user prompt with all quantitative data."""
    ps = scores.get("pillar_scores", {})
    pd = scores.get("pillar_details", {})

    # Format market cap for readability
    mkt = profile.get("market_cap")
    if mkt and mkt >= 1e12:
        mkt_str = f"${mkt / 1e12:.2f}T"
    elif mkt and mkt >= 1e9:
        mkt_str = f"${mkt / 1e9:.1f}B"
    elif mkt:
        mkt_str = f"${mkt / 1e6:.0f}M"
    else:
        mkt_str = "N/A"

    lines = [
        f"=== Company: {profile.get('company_name', symbol)} ({symbol}) ===",
        f"Sector: {profile.get('sector', 'N/A')}",
        f"Market Cap: {mkt_str}",
        "",
        f"COMPOSITE SCORE: {scores.get('composite_score')}/100",
        "",
        "PILLAR SCORES:",
        f"  1. Business Quality:     {ps.get('business_quality')}/100",
        f"  2. Operational Health:    {ps.get('operational_health')}/100",
        f"  3. Capital Allocation:    {ps.get('capital_allocation')}/100",
        f"  4. Growth Quality:        {ps.get('growth_quality')}/100",
        f"  5. Valuation:             {ps.get('valuation')}/100",
    ]

    # Add key metrics from each pillar
    for pillar_name, label in [
        ("business_quality", "BUSINESS QUALITY METRICS"),
        ("operational_health", "OPERATIONAL HEALTH METRICS"),
        ("capital_allocation", "CAPITAL ALLOCATION METRICS"),
        ("growth_quality", "GROWTH QUALITY METRICS"),
        ("valuation", "VALUATION METRICS"),
    ]:
        detail = pd.get(pillar_name, {})
        metrics = detail.get("metrics", {})
        sub_scores = detail.get("scores", {})
        if metrics:
            lines.append(f"\n{label}:")
            for k, v in metrics.items():
                s = sub_scores.get(k)
                score_str = f" (score: {s}/100)" if s is not None else ""
                lines.append(f"  {k}: {v}{score_str}")

    lines.append("\nProvide your analysis as JSON.")
    return "\n".join(lines)


def _parse_response(raw: str) -> dict | None:
    """Parse the LLM's JSON response, tolerating markdown fences."""
    text = raw.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = text.index("\n") if "\n" in text else 3
        text = text[first_newline + 1:]
    if text.endswith("```"):
        text = text[:-3].rstrip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                data = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None

    # Validate required fields
    rec = data.get("recommendation", "").upper()
    if rec not in _VALID_RECOMMENDATIONS:
        data["recommendation"] = "HOLD"  # safe default

    conv = data.get("conviction")
    if not isinstance(conv, (int, float)) or conv < 0 or conv > 100:
        data["conviction"] = 50

    data["conviction"] = int(data["conviction"])

    return data

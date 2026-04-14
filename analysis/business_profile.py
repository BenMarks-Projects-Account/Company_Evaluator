"""Business profile LLM generator.

Generates a qualitative business analysis for a single company using
the local LLM (LM Studio).  Called from the on-demand evaluator pipeline
only — NOT from the crawler.
"""

import json
import logging
from datetime import datetime, timezone

from analysis.llm_client import call_llm

_log = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are a business analyst writing structured qualitative profiles of public companies for an investment research tool.

CRITICAL OUTPUT REQUIREMENT: Your entire response must be a single valid JSON object. Nothing before it, nothing after it. No markdown code fences. No explanations. No "Here is the analysis:" preamble. No trailing commentary. The very first character of your response must be `{` and the very last character must be `}`. Any deviation breaks the downstream parser.

You analyze the business itself: what it does, how it makes money, what sets it apart, who it competes with, and what risks are specific to its business model (not generic market risks). Be direct, concrete, and evidence-based. If you don't have enough information to answer a field confidently, use null for that field rather than guessing.

Avoid:
- Generic statements that could apply to any company in the sector
- Marketing language or boosterism
- Hedging phrases like "could potentially"
- Repeating the company description verbatim

Prioritize:
- Specific examples and numbers where available
- Differentiation from named competitors
- Business-model-specific risks (not "macro conditions")"""


USER_PROMPT_TEMPLATE = """Analyze the following company and return a JSON object matching the schema below.

COMPANY: {symbol} — {name}
SECTOR: {sector}
INDUSTRY: {industry}
MARKET CAP: {market_cap}
EMPLOYEES: {employees}

COMPANY DESCRIPTION:
{description}

KEY METRICS:
- Revenue TTM: {revenue_ttm}
- Gross margin: {gross_margin}
- Operating margin: {operating_margin}
- Net margin: {net_margin}
- FCF margin: {fcf_margin}
- ROIC: {roic}

PEER GROUP: {peers}

Return this exact JSON schema (all fields required, use null for unknown values):

{{
  "elevator_pitch": "string — 2-3 sentences in plain English",
  "business_model": {{
    "revenue_streams": ["list", "of", "named", "streams"],
    "customer_type": "string — who buys from them",
    "pricing_model": "string — how they charge",
    "contract_type": "string — nature of customer relationships"
  }},
  "moat": {{
    "primary": "string — the single most important competitive advantage",
    "strength": "STRONG | MODERATE | WEAK | NONE",
    "signals": ["list", "of", "specific", "evidence"]
  }},
  "competitive_landscape": {{
    "direct_competitors": ["list", "of", "company", "names"],
    "differentiation": "string — what makes this company different",
    "market_position": "LEADER | CHALLENGER | NICHE | FOLLOWER"
  }},
  "key_risks": [
    "list of business-model-specific risks"
  ],
  "confidence": "HIGH | MEDIUM | LOW"
}}

Respond with valid JSON only."""


async def generate_business_profile(
    symbol: str,
    profile: dict,
    evaluation: dict | None = None,
    comps: dict | None = None,
) -> dict:
    """Generate a business profile for a single company.

    Args:
        symbol: Stock ticker
        profile: Company profile dict (name, sector, industry, market_cap, employees, description)
        evaluation: Optional evaluation dict with pillar_breakdowns for metric extraction
        comps: Optional comps dict with peer_group.symbols

    Returns:
        dict with business_profile payload. Never raises — returns
        {"ok": false, ...} on any error.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Build the user prompt with safe fallbacks
    try:
        prompt = _build_user_prompt(symbol, profile, evaluation, comps)
    except Exception as e:
        _log.warning("[business_profile] prompt build failed for %s: %s", symbol, e)
        return {
            "ok": False,
            "llm_available": False,
            "error": f"Failed to build prompt: {e}",
            "generated_at": now,
        }

    # Call the LLM (returns None if LM Studio is unavailable)
    _log.info("event=business_profile_start symbol=%s", symbol)
    raw = await call_llm(SYSTEM_PROMPT, prompt, max_tokens=1500)

    if raw is None:
        _log.warning("[business_profile] LLM unavailable for %s", symbol)
        return {
            "ok": False,
            "llm_available": False,
            "error": "LLM not available (LM Studio may not be running)",
            "generated_at": now,
        }

    # Parse and validate JSON
    try:
        parsed = _parse_llm_response(raw)
    except Exception as e:
        _log.warning("[business_profile] LLM response parse failed for %s: %s", symbol, e)
        _log.warning(
            "[business_profile] raw response (first 2000 chars): %s",
            raw[:2000] if raw else "EMPTY",
        )
        return {
            "ok": False,
            "llm_available": True,
            "error": f"LLM response was not valid JSON: {e}",
            "generated_at": now,
        }

    # Validate required fields (log but don't fail)
    required = ["elevator_pitch", "business_model", "moat", "competitive_landscape", "key_risks", "confidence"]
    missing = [f for f in required if f not in parsed]
    if missing:
        _log.warning("[business_profile] LLM missing fields for %s: %s", symbol, missing)

    _log.info("event=business_profile_complete symbol=%s confidence=%s", symbol, parsed.get("confidence"))

    return {
        "ok": True,
        "llm_available": True,
        **parsed,
        "generated_at": now,
    }


def _build_user_prompt(symbol: str, profile: dict, evaluation: dict | None, comps: dict | None) -> str:
    """Build the user prompt string with safe fallbacks."""

    def fmt_money(v):
        if v is None:
            return "unknown"
        if isinstance(v, str):
            return v
        if v >= 1e12:
            return f"${v / 1e12:.2f}T"
        if v >= 1e9:
            return f"${v / 1e9:.2f}B"
        if v >= 1e6:
            return f"${v / 1e6:.2f}M"
        return f"${v:,.0f}"

    def fmt_pct(v):
        if v is None:
            return "unknown"
        # Values stored as decimals (0.35) or already percentage (35.0)
        if isinstance(v, (int, float)):
            if abs(v) < 1:
                return f"{v * 100:.1f}%"
            return f"{v:.1f}%"
        return str(v)

    # Extract metrics from evaluation pillar breakdowns if available
    metrics: dict = {}
    if evaluation and isinstance(evaluation, dict):
        pillar_breakdowns = evaluation.get("pillar_breakdowns")
        if isinstance(pillar_breakdowns, dict):
            biz = pillar_breakdowns.get("business_quality")
            if isinstance(biz, dict):
                comps_dict = biz.get("components", {})
                if isinstance(comps_dict, dict):
                    for key in ("gross_margin", "operating_margin", "roic", "fcf_margin"):
                        entry = comps_dict.get(key)
                        if isinstance(entry, dict):
                            metrics[key] = entry.get("value")
        # Also try pillar_scores level for revenue
        raw_fin = evaluation.get("raw_financials")
        if isinstance(raw_fin, str):
            try:
                raw_fin = json.loads(raw_fin)
            except Exception:
                raw_fin = None
        if isinstance(raw_fin, dict):
            cd = raw_fin.get("company_data", raw_fin)
            if isinstance(cd, dict):
                computed = cd.get("computed_inputs", {})
                if isinstance(computed, dict):
                    metrics.setdefault("revenue", computed.get("revenue"))

    # Peer symbols
    peers = []
    if comps and isinstance(comps, dict):
        pg = comps.get("peer_group")
        if isinstance(pg, dict):
            peers = pg.get("symbols", [])[:8]
    peers_str = ", ".join(str(p) for p in peers) if peers else "unknown"

    return USER_PROMPT_TEMPLATE.format(
        symbol=symbol,
        name=profile.get("name") or profile.get("company_name") or symbol,
        sector=profile.get("sector") or "unknown",
        industry=profile.get("industry") or "unknown",
        market_cap=fmt_money(profile.get("market_cap")),
        employees=f"{profile.get('employees'):,}" if profile.get("employees") else "unknown",
        description=profile.get("description") or "No description available",
        revenue_ttm=fmt_money(metrics.get("revenue")),
        gross_margin=fmt_pct(metrics.get("gross_margin")),
        operating_margin=fmt_pct(metrics.get("operating_margin")),
        net_margin=fmt_pct(metrics.get("net_margin")),
        fcf_margin=fmt_pct(metrics.get("fcf_margin")),
        roic=fmt_pct(metrics.get("roic")),
        peers=peers_str,
    )


def _parse_llm_response(text: str) -> dict:
    """Parse LLM JSON response, handling common failure modes:

    - Leading/trailing whitespace
    - Markdown code fences (``` or ```json)
    - Trailing explanatory text after the JSON
    - Extra content before the first `{`
    - Multiple JSON objects (takes the first)
    """
    if not text:
        raise ValueError("Empty response")

    text = text.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline > 0:
            text = text[first_newline + 1:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3].rstrip()

    # Strip leading "json" or "JSON" language hint if present
    stripped_lower = text.lstrip().lower()
    if stripped_lower.startswith("json"):
        idx = text.lower().find("json")
        after = text[idx + 4:]
        if after and after[0] in (" ", "\n", "\t", "\r"):
            text = after.lstrip()

    # Try raw_decode first — parses the first valid JSON object
    # and returns (obj, end_index), gracefully ignoring any
    # trailing content.
    try:
        decoder = json.JSONDecoder()
        obj, _end = decoder.raw_decode(text)
        if isinstance(obj, dict):
            return obj
        raise ValueError(f"Top-level JSON is not an object (got {type(obj).__name__})")
    except json.JSONDecodeError:
        pass

    # Find the first `{` and extract a balanced object.
    first_brace = text.find("{")
    if first_brace == -1:
        raise ValueError("No JSON object found in response")

    # Walk forward matching braces, respecting string escapes
    depth = 0
    in_string = False
    escape_next = False
    end_idx = -1

    for i in range(first_brace, len(text)):
        ch = text[i]

        if escape_next:
            escape_next = False
            continue

        if ch == "\\":
            escape_next = True
            continue

        if ch == '"':
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break

    if end_idx == -1:
        raise ValueError("Unbalanced braces in JSON response")

    extracted = text[first_brace:end_idx]

    try:
        obj = json.loads(extracted)
        if not isinstance(obj, dict):
            raise ValueError(f"Top-level JSON is not an object (got {type(obj).__name__})")
        return obj
    except json.JSONDecodeError as e:
        raise ValueError(f"Extracted JSON failed to parse: {e}")

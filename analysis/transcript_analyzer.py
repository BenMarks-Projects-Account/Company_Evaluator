"""On-demand earnings call transcript analysis using the LLM.

This is a research tool, not part of the automated scoring pipeline.
The user triggers it for companies under serious consideration.
"""

import json
import logging
import re
from datetime import datetime, timezone

from analysis.llm_client import call_llm

_log = logging.getLogger(__name__)

TRANSCRIPT_ANALYSIS_SYSTEM_PROMPT = """You are an experienced equity analyst focused on medium-term wealth generation through high-quality businesses. You're analyzing an earnings call transcript to extract signal that quantitative metrics miss.

Your job is to read the transcript and produce a structured analysis focused on what matters for a 3-7 year investment horizon:

1. **Business Quality Signals** — Is the business getting better or worse? Look for: pricing power, customer retention, competitive moats strengthening or eroding, market share changes.

2. **Capital Allocation** — Is management deploying capital wisely? Look for: R&D investment vs buybacks, M&A discipline, debt management, dividend policy changes, capex plans.

3. **Strategic Direction** — Where is management taking this company? Look for: stated strategic priorities, new initiatives, market expansion plans, product roadmap, AI/digital transformation moves.

4. **Risk Acknowledgment** — Is management being honest about challenges? Look for: explicit problem acknowledgment vs hand-waving, regulatory concerns, customer concentration risks, supply chain issues.

5. **Forward Guidance Quality** — How confident is management in the future? Look for: specific guidance vs vague language, raised vs lowered guidance, qualitative language about confidence levels.

6. **Tone & Confidence** — How is management communicating? Look for: defensive vs assertive tone, willingness to take hard questions, specificity of answers vs evasion.

7. **Red Flags** — Anything that should concern a long-term holder? Look for: changing accounting practices, key executive departures, missed targets without explanation, deflection on key questions.

8. **Green Flags** — Anything that should excite a long-term holder? Look for: improving unit economics, new high-quality customers, positive customer feedback cited, operational improvements, capital discipline.

You MUST return ONLY a valid JSON object with this exact structure:

{
  "overall_sentiment": "very_bullish | bullish | neutral | bearish | very_bearish",
  "conviction_score": 0-100,
  "headline": "One sentence summary of the most important takeaway",

  "business_quality": {
    "direction": "improving | stable | declining",
    "evidence": ["specific quote or paraphrase 1", "..."],
    "concerns": ["..."]
  },

  "capital_allocation": {
    "assessment": "excellent | good | adequate | concerning | poor",
    "key_decisions": ["..."],
    "concerns": ["..."]
  },

  "strategic_direction": {
    "clarity": "very_clear | clear | mixed | unclear",
    "priorities": ["..."],
    "concerns": ["..."]
  },

  "risk_acknowledgment": {
    "transparency": "high | medium | low",
    "risks_acknowledged": ["..."],
    "risks_glossed_over": ["..."]
  },

  "forward_guidance": {
    "tone": "raised | maintained | lowered | withdrawn",
    "specificity": "very_specific | specific | vague",
    "key_metrics": ["..."]
  },

  "management_tone": {
    "confidence_level": "very_high | high | medium | low | very_low",
    "communication_quality": "excellent | good | adequate | poor",
    "notable_observations": ["..."]
  },

  "red_flags": [
    {
      "severity": "high | medium | low",
      "issue": "...",
      "context": "..."
    }
  ],

  "green_flags": [
    {
      "significance": "high | medium | low",
      "signal": "...",
      "context": "..."
    }
  ],

  "key_quotes": [
    {
      "quote": "Direct quote from the call",
      "speaker": "Name and title",
      "significance": "Why this quote matters"
    }
  ],

  "investment_thesis_impact": {
    "thesis_strengthened": ["..."],
    "thesis_weakened": ["..."],
    "thesis_unchanged": "Brief summary"
  },

  "medium_term_outlook": {
    "summary": "2-3 sentence summary for a 3-7 year holder",
    "watch_for_next_quarter": ["..."]
  }
}

Be specific. Use direct quotes when possible. Don't hedge or be wishy-washy.
A long-term investor needs your honest read on what management is actually
saying vs what they're trying to say.

Return ONLY the JSON object. No preamble, no explanation, no markdown code fences."""


async def analyze_transcript(
    symbol: str,
    fmp_client,
    year: int | None = None,
    quarter: int | None = None,
) -> dict:
    """Fetch and analyze an earnings call transcript.

    Returns a result dict with transcript_metadata, analysis, and errors.
    """
    symbol = symbol.upper()

    # ── Fetch transcript ─────────────────────────────────────
    transcript = await fmp_client.get_earnings_transcript(symbol, year, quarter)

    if not transcript or not transcript.get("content"):
        return {
            "symbol": symbol,
            "error": f"No transcript available for {symbol}",
            "transcript_metadata": None,
            "analysis": None,
        }

    transcript_text = transcript["content"]
    word_count = len(transcript_text.split())

    metadata = {
        "year": transcript.get("year"),
        "quarter": transcript.get("quarter"),
        "date": transcript.get("date"),
        "word_count": word_count,
    }

    # ── Truncate if necessary ────────────────────────────────
    MAX_WORDS = 10_000
    if word_count > MAX_WORDS:
        words = transcript_text.split()
        first_chunk = " ".join(words[: int(MAX_WORDS * 0.6)])
        last_chunk = " ".join(words[-int(MAX_WORDS * 0.4) :])
        transcript_text = (
            first_chunk
            + "\n\n[... transcript truncated for length ...]\n\n"
            + last_chunk
        )
        metadata["truncated"] = True
        metadata["original_word_count"] = word_count
        metadata["word_count"] = MAX_WORDS
    else:
        metadata["truncated"] = False

    # ── LLM analysis ─────────────────────────────────────────
    user_prompt = (
        f"Analyze this earnings call transcript for {symbol}.\n\n"
        f"Transcript date: {metadata['date']}\n"
        f"Quarter: Q{metadata['quarter']} {metadata['year']}\n\n"
        f"TRANSCRIPT:\n{transcript_text}\n\n"
        "Return your analysis as a JSON object following the exact structure "
        "specified in the system prompt."
    )

    _log.info(
        "event=transcript_analysis_start symbol=%s quarter=Q%s-%s words=%d",
        symbol, metadata["quarter"], metadata["year"], metadata["word_count"],
    )

    try:
        raw = await call_llm(
            TRANSCRIPT_ANALYSIS_SYSTEM_PROMPT,
            user_prompt,
            max_tokens=4000,
        )

        if raw is None:
            _log.warning("event=transcript_llm_no_response symbol=%s", symbol)
            return {
                "symbol": symbol,
                "transcript_metadata": metadata,
                "analysis": None,
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
                "errors": ["LLM returned no response"],
            }

        analysis = _parse_llm_json(raw)

        if analysis is None:
            _log.warning(
                "event=transcript_parse_failed symbol=%s raw_len=%d", symbol, len(raw)
            )
            # Retry once — ask the LLM to fix its JSON
            retry_prompt = (
                "Your previous response was not valid JSON. "
                "Please return ONLY the valid JSON object, no other text:\n\n"
                + raw[:3000]
            )
            raw_retry = await call_llm(
                TRANSCRIPT_ANALYSIS_SYSTEM_PROMPT,
                retry_prompt,
                max_tokens=4000,
            )
            if raw_retry:
                analysis = _parse_llm_json(raw_retry)

            if analysis is None:
                return {
                    "symbol": symbol,
                    "transcript_metadata": metadata,
                    "analysis": None,
                    "analyzed_at": datetime.now(timezone.utc).isoformat(),
                    "errors": ["Failed to parse LLM response as JSON"],
                }

        _log.info(
            "event=transcript_analysis_complete symbol=%s sentiment=%s conviction=%s",
            symbol,
            analysis.get("overall_sentiment"),
            analysis.get("conviction_score"),
        )

        return {
            "symbol": symbol,
            "transcript_metadata": metadata,
            "analysis": analysis,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }

    except Exception as exc:
        _log.error("event=transcript_analysis_failed symbol=%s error=%s", symbol, exc)
        return {
            "symbol": symbol,
            "transcript_metadata": metadata,
            "analysis": None,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
            "errors": [str(exc)],
        }


def _parse_llm_json(response: str) -> dict | None:
    """Parse the LLM's JSON response, handling common formatting issues."""
    if not response:
        return None

    text = response.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl > 0:
            text = text[first_nl + 1 :]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to extract a JSON object from surrounding text
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            _log.debug("event=json_extract_failed")

    return None

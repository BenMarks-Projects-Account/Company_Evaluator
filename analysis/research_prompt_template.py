"""
Deep research prompt generator.

Builds a templated chat prompt that hands an evaluation result
to a chat-based research model (Claude/ChatGPT/Gemini) for
narrative analysis aligned with the user's medium-term value
investing thesis.
"""

from datetime import datetime, timezone


def build_research_prompt(evaluation: dict) -> str:
    """
    Build a deep research prompt from an on-demand evaluation result.

    Args:
        evaluation: The full on-demand evaluation dict, including
                    company, evaluation, dcf, eva, comps, epv,
                    piotroski_f_score, business_profile, etc.

    Returns:
        Plain text prompt ready to paste into a chat conversation.
    """
    company = evaluation.get("company") or {}
    eval_data = evaluation.get("evaluation") or {}
    pillars = eval_data.get("pillar_breakdowns") or {}

    symbol = company.get("symbol", "UNKNOWN")
    name = company.get("name") or symbol
    sector = company.get("sector", "Unknown sector")
    industry = company.get("industry", "Unknown industry")
    market_cap = company.get("market_cap")
    current_price = company.get("price")
    employees = company.get("employees")

    sections = [
        _header_section(symbol, name),
        _investor_context_section(),
        _company_snapshot_section(symbol, name, sector, industry, market_cap, current_price, employees),
        _financial_data_section(eval_data, pillars),
        _valuation_data_section(evaluation),
        _quality_data_section(evaluation, pillars),
        _existing_business_profile_section(evaluation),
        _research_questions_section(),
        _output_format_section(),
        _final_instructions_section(),
    ]

    return "\n\n".join(s for s in sections if s)


# ═══════════════════════════════════════════════════════════════════════
# SECTION BUILDERS
# ═══════════════════════════════════════════════════════════════════════

def _header_section(symbol: str, name: str) -> str:
    return (
        f"# Deep Research Request: {name} ({symbol})\n\n"
        f"I need a thorough investment analysis of {name} ({symbol}). I'm\n"
        f"giving you the structured data my financial evaluator has already\n"
        f"computed — your job is to turn it into a real research narrative\n"
        f"with current context I don't have, then give me a position-sizing\n"
        f"recommendation given my goals."
    )


def _investor_context_section() -> str:
    return """## My Investment Goals

I'm looking for medium-term wealth-building opportunities (1-5 year
holding periods). My specific hunting strategy is:

- Find small-cap or mid-cap companies with strong fundamentals that
  could grow into large-cap or mega-cap names over the next 3-7 years
- Prioritize companies with real earnings (not pre-profit speculation)
  and visible competitive advantages
- I want asymmetric risk/reward — limited downside from a quality
  business floor, substantial upside from successful execution
- I avoid pure value traps (cheap-but-declining businesses) and pure
  growth speculation (no-earnings hype stories)
- Position sizing matters — I want a specific recommendation for how
  much of $10K capital I should allocate to this name given the
  risk/reward profile

Do NOT give me generic "buy / hold / sell" or "consult a financial
advisor" boilerplate. I want your actual reasoning and a specific
position-size recommendation."""


def _company_snapshot_section(symbol, name, sector, industry, market_cap, price, employees) -> str:
    parts = [f"## Company Snapshot", f"- **Symbol**: {symbol}", f"- **Name**: {name}"]
    if sector and sector != "Unknown sector":
        parts.append(f"- **Sector**: {sector}")
    if industry and industry != "Unknown industry":
        parts.append(f"- **Industry**: {industry}")
    if market_cap:
        parts.append(f"- **Market Cap**: ${_fmt_money(market_cap)}")
    if price:
        parts.append(f"- **Current Price** (when evaluated): ${price:.2f}")
    if employees:
        parts.append(f"- **Employees**: {employees:,}")
    return "\n".join(parts)


def _financial_data_section(eval_data: dict, pillars: dict) -> str:
    """Extract key financial metrics from pillar breakdowns."""
    bq = (pillars.get("business_quality") or {}).get("metrics") or {}
    oh = (pillars.get("operational_health") or {}).get("metrics") or {}
    ca = (pillars.get("capital_allocation") or {}).get("metrics") or {}
    growth = (pillars.get("growth_quality") or {}).get("metrics") or {}

    lines = ["## Financial Profile (computed from filings)"]

    # Profitability
    lines.append("\n**Profitability:**")
    if bq.get("gross_margin") is not None:
        lines.append(f"- Gross Margin: {bq['gross_margin'] * 100:.1f}%")
    if bq.get("op_margin") is not None:
        lines.append(f"- Operating Margin: {bq['op_margin'] * 100:.1f}%")
    if bq.get("net_margin") is not None:
        lines.append(f"- Net Margin: {bq['net_margin'] * 100:.1f}%")
    if bq.get("fcf_yield") is not None:
        lines.append(f"- FCF Yield: {bq['fcf_yield'] * 100:.1f}%")
    if bq.get("roic") is not None:
        lines.append(f"- ROIC: {bq['roic'] * 100:.1f}%")

    # Capital quality
    lines.append("\n**Capital Quality:**")
    if ca.get("roic_wacc_spread") is not None:
        lines.append(f"- ROIC vs WACC spread: {ca['roic_wacc_spread'] * 100:+.1f} pts")
    if ca.get("wacc_est") is not None:
        lines.append(f"- WACC estimate: {ca['wacc_est'] * 100:.1f}%")
    if ca.get("share_trend") is not None:
        trend = ca['share_trend'] * 100
        verb = "buybacks" if trend < 0 else "dilution"
        lines.append(f"- Share Count Trend: {trend:+.1f}% YoY ({verb})")
    if ca.get("payout_ratio") is not None:
        lines.append(f"- Payout Ratio: {ca['payout_ratio'] * 100:.1f}%")

    # Growth
    lines.append("\n**Growth:**")
    if growth.get("revenue_cagr_3y") is not None:
        lines.append(f"- Revenue CAGR (3y): {growth['revenue_cagr_3y'] * 100:.1f}%")
    if growth.get("revenue_cagr_5y") is not None:
        lines.append(f"- Revenue CAGR (5y): {growth['revenue_cagr_5y'] * 100:.1f}%")
    if growth.get("fcf_growth") is not None:
        lines.append(f"- FCF Growth: {growth['fcf_growth'] * 100:.1f}%")
    if growth.get("eps_growth_yoy") is not None:
        lines.append(f"- EPS Growth (YoY): {growth['eps_growth_yoy']:.1f}%")

    # Operational health
    lines.append("\n**Operational Health:**")
    if oh.get("current_ratio") is not None:
        lines.append(f"- Current Ratio: {oh['current_ratio']:.2f}")
    if oh.get("debt_to_ebitda") is not None:
        lines.append(f"- Debt/EBITDA: {oh['debt_to_ebitda']:.2f}x")
    if oh.get("interest_coverage") is not None:
        ic = oh["interest_coverage"]
        if ic == "no_debt":
            lines.append("- Interest Coverage: N/A — no debt")
        elif isinstance(ic, (int, float)):
            if ic > 100:
                lines.append(f"- Interest Coverage: >{int(ic)}x (essentially unconstrained)")
            else:
                lines.append(f"- Interest Coverage: {ic:.2f}x")
    if oh.get("altman_z") is not None:
        z = oh['altman_z']
        zone = "Safe" if z >= 2.99 else "Watch" if z >= 1.81 else "Distress"
        lines.append(f"- Altman Z-Score: {z:.2f} ({zone} zone)")

    return "\n".join(lines)


def _valuation_data_section(evaluation: dict) -> str:
    """Format DCF, EVA, Comps, EPV results."""
    dcf = evaluation.get("dcf") or {}
    eva = evaluation.get("eva") or {}
    comps = evaluation.get("comps") or {}
    epv = evaluation.get("epv") or {}

    lines = ["## Valuation Models (already computed)"]

    if dcf.get("ok"):
        fv = dcf.get("fair_value_per_share")
        if fv is not None:
            lines.append(f"\n**DCF Intrinsic Value**: ${fv:.2f}")
            inputs = dcf.get("inputs") or {}
            if inputs.get("wacc"):
                lines.append(f"- WACC: {inputs['wacc'] * 100:.2f}%")
            if inputs.get("terminal_growth"):
                lines.append(f"- Terminal Growth: {inputs['terminal_growth'] * 100:.1f}%")

    if eva.get("ok"):
        eva_val = eva.get("fair_value") or eva.get("eva_value")
        if eva_val is not None:
            lines.append(f"\n**EVA (Economic Value Added)**: ${_fmt_money(eva_val)}")
        eva_wacc = (eva.get("wacc") or {}).get("wacc")
        if eva_wacc:
            lines.append(f"- WACC used: {eva_wacc * 100:.2f}%")

    if comps.get("ok"):
        cv = comps.get("fair_value_per_share")
        if cv is not None:
            lines.append(f"\n**Peer Comparison Fair Value**: ${cv:.2f}")
        peers = comps.get("peers") or []
        if peers:
            peer_names = [p.get("symbol", "?") for p in peers[:5]]
            lines.append(f"- Peers compared: {', '.join(peer_names)}")

    # EPV with dual values + emergence signal
    if epv.get("ok"):
        trailing = epv.get("trailing") or {}
        normalized = epv.get("normalized") or {}
        emergence = epv.get("emergence") or {}

        lines.append(f"\n**EPV (Greenwald Earnings Power Value)**:")
        if trailing.get("fair_value_per_share") is not None:
            lines.append(
                f"- Trailing (1y) EPV: ${trailing['fair_value_per_share']:.2f}/share, "
                f"premium {trailing.get('growth_premium_pct', 0):.1f}% "
                f"[{trailing.get('growth_premium_label', '')}]"
            )
        if normalized.get("fair_value_per_share") is not None:
            lines.append(
                f"- Normalized (5y) EPV: ${normalized['fair_value_per_share']:.2f}/share, "
                f"premium {normalized.get('growth_premium_pct', 0):.1f}% "
                f"[{normalized.get('growth_premium_label', '')}]"
            )
        if emergence.get("signal"):
            lines.append(f"- **Emergence Signal**: {emergence['signal']}")
            if emergence.get("interpretation"):
                lines.append(f"  - {emergence['interpretation']}")
        if emergence.get("ebit_history"):
            hist = emergence['ebit_history']
            hist_str = " → ".join(f"${_fmt_money_short(v)}" for v in hist)
            lines.append(f"- 5y EBIT History (oldest→newest): {hist_str}")
    elif epv.get("error"):
        lines.append(f"\n**EPV**: not available ({epv['error']})")

    return "\n".join(lines)


def _quality_data_section(evaluation: dict, pillars: dict) -> str:
    """Format quality screens — Piotroski, distress, insider activity."""
    lines = ["## Quality Signals"]

    piotroski = evaluation.get("piotroski_f_score") or {}
    if piotroski.get("ok"):
        lines.append(
            f"\n**Piotroski F-Score**: {piotroski.get('score', 0)}/9 "
            f"({piotroski.get('label', '')})"
        )
        if piotroski.get("interpretation"):
            lines.append(f"- {piotroski['interpretation']}")
        # List the failed checks specifically
        failed = []
        for check_name, check_data in (piotroski.get("checks") or {}).items():
            if isinstance(check_data, dict) and not check_data.get("passed"):
                failed.append(check_data.get("label", check_name))
        if failed:
            lines.append(f"- Failed checks: {', '.join(failed)}")
    elif piotroski.get("error"):
        lines.append(f"\n**Piotroski F-Score**: not available ({piotroski['error']})")

    # Insider activity
    ca = (pillars.get("capital_allocation") or {}).get("metrics") or {}
    insider_score = ca.get("insider_score")
    insider_net = ca.get("insider_net")
    if insider_score is not None or insider_net:
        lines.append(f"\n**Insider Activity**: score {insider_score or 'N/A'}/100, direction: {insider_net or 'N/A'}")

    return "\n".join(lines)


def _existing_business_profile_section(evaluation: dict) -> str:
    """Include the existing structured business profile as a starting point."""
    bp = evaluation.get("business_profile") or {}
    if not bp or not bp.get("ok", True):
        return ""

    lines = ["## Existing Structured Profile (from my evaluator — supplement and correct as needed)"]

    if bp.get("elevator_pitch"):
        lines.append(f"\n**Elevator Pitch**: {bp['elevator_pitch']}")
    if bp.get("business_model"):
        lines.append(f"\n**Business Model**: {bp['business_model']}")
    if bp.get("moat"):
        moat = bp["moat"]
        if isinstance(moat, dict):
            lines.append(f"\n**Moat**: {moat.get('description', '')}")
            if moat.get("strength"):
                lines.append(f"- Strength: {moat['strength']}")
        else:
            lines.append(f"\n**Moat**: {moat}")
    if bp.get("competitive_landscape"):
        cl = bp["competitive_landscape"]
        if isinstance(cl, dict):
            if cl.get("market_position"):
                lines.append(f"\n**Market Position**: {cl['market_position']}")
            if cl.get("differentiation"):
                lines.append(f"- Differentiation: {cl['differentiation']}")
            if cl.get("direct_competitors"):
                competitors = ", ".join(str(c) for c in cl["direct_competitors"][:8])
                lines.append(f"- Listed competitors: {competitors}")
                lines.append("  (NOTE: my evaluator's competitor list may be incomplete or "
                             "out of date. Verify with current information and find current "
                             "ticker symbols if needed.)")
    if bp.get("key_risks"):
        risks = bp["key_risks"]
        if isinstance(risks, list):
            lines.append(f"\n**Listed Risks**:")
            for risk in risks[:6]:
                lines.append(f"- {risk}")

    return "\n".join(lines)


def _research_questions_section() -> str:
    return """## What I Need From You

Use web search to answer these questions specifically. Do NOT
make up dates, names, or specific facts — search and verify.

### 1. The Business in One Paragraph
What does this company actually do, how does it make money, and
what percentage of revenue comes from its dominant product or
segment? If revenue concentration is heavy in one product or
customer, say so explicitly.

### 2. The Next 12 Months — What Actually Matters
What are the SPECIFIC catalysts in the next 12 months?
- Earnings dates
- Regulatory decisions (FDA approvals, court rulings, etc.)
- Product launches
- Major contract renewals
- Expected data readouts or trial results
- Anything else with a known date

For each catalyst, tell me:
- The specific date or expected window
- Whether the outcome is binary (single event determines outcome)
  or incremental
- What the upside/downside ranges look like

### 3. The Bridge / Strategic Position
What is protecting current revenue right now? What is the company
building to extend that runway? Where is the gap or cliff?

For pharma/biotech: patent expiration dates and successor pipeline
For tech/SaaS: contract renewal cycles, customer concentration, NDR trends
For consumer: brand strength, market share trends, channel dynamics
For industrial: cyclical exposure, backlog, end-market diversification

Be specific. "Patent expires in 2030" not "patent risks exist."

### 4. Competitive Reality
Who does this company ACTUALLY compete with TODAY in each of its
business lines? Not the generic peer group — the actual products
and companies fighting for the same customers/dollars.

For each major segment:
- Top 2-3 real competitors and what differentiates them
- Where this company is winning share vs losing share
- Whether the competitive position is improving or deteriorating

If my evaluator listed companies that are stale, defunct, or
acquired, call them out and replace with current competitors.

### 5. Management & Capital Allocation
- How long has the current CEO been in the seat?
- What's their track record on capital allocation? (buybacks vs
  dividends vs M&A vs reinvestment)
- Recent insider activity context — buying or selling, and at
  what scale relative to their compensation?
- Any recent strategic shifts or notable management departures?

### 6. The Asymmetric Setup
Given current price and market cap, what's the realistic distribution
of outcomes over a 1-3 year horizon?

- **Downside scenario** (specific events that lead to it, price target)
- **Base case** (most likely outcome, price target, time horizon)
- **Upside scenario** (specific events that lead to it, price target)

Approximate probabilities for each scenario. Be honest about
uncertainty.

### 7. Position Sizing Recommendation
Given my goals (medium-term wealth building, hunting for small/mid
caps before they go large), if I have $10,000 to allocate across
my portfolio, how much should I put into THIS specific name?

Frame your answer as:
- Specific dollar amount or % of portfolio
- Reasoning for that sizing (concentration risk, catalyst timing,
  binary events, etc.)
- Whether to enter all at once, in stages, or wait for a specific
  event/price
- A specific exit/re-evaluation trigger if the thesis breaks

Do NOT just say "depends on your risk tolerance" — give me your
actual recommendation and reasoning."""


def _output_format_section() -> str:
    return """## Output Format

Write this as a NARRATIVE analysis, not a checklist. Use headers
to organize the sections above. Each section should read like a
real research note — full sentences and paragraphs, with specific
facts and reasoning.

Do NOT:
- Use generic risk disclaimers as a substitute for actual analysis
- List 10 bullet-point "considerations" instead of giving a real
  recommendation
- Refuse to give a position-sizing answer because "I'm not a
  financial advisor" — I know you're not, that's understood, give
  me your reasoning anyway
- Hedge every claim with "may" / "might" / "could" — be direct
  when the data supports a direct claim
- Pad with general industry commentary that doesn't relate
  specifically to this company

DO:
- Cite specific facts with sources from your web searches
- Distinguish between what the data shows vs what you're inferring
- Flag where you're uncertain and why
- Make the recommendation actionable"""


def _final_instructions_section() -> str:
    now = datetime.now(timezone.utc).strftime("%B %Y")
    return f"""## Final Instructions

This analysis is being generated in {now}. Use current information
from web searches — prices, market caps, recent news, and pipeline
updates may have changed since my evaluator last ran.

Start with the Business in One Paragraph and work through each
section in order. End with the Position Sizing Recommendation as
the actionable conclusion."""


# ═══════════════════════════════════════════════════════════════════════
# FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _fmt_money(v) -> str:
    if v is None:
        return "—"
    n = abs(float(v))
    sign = "-" if float(v) < 0 else ""
    if n >= 1e12:
        return f"{sign}{n / 1e12:.2f}T"
    if n >= 1e9:
        return f"{sign}{n / 1e9:.2f}B"
    if n >= 1e6:
        return f"{sign}{n / 1e6:.2f}M"
    return f"{sign}{n:,.0f}"


def _fmt_money_short(v) -> str:
    return _fmt_money(v)

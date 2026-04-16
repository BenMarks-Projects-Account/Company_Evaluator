"""SQL queries for the Phase A cross-sectional audit.

All queries are read-only. They are expressed against the schema defined
in ``db/database.py`` and run through the async session factory. No inline
SQL belongs in ``analyses.py``; every query lives here as a named constant.

The ``:version`` bind parameter, where present, filters to a single
evaluation_version (resolved by ``run_audit.py`` to the most common value).
If only one version exists in the DB the filter is a no-op.
"""

# ── Pre-flight: evaluation_version distribution ──────────────────────
# Used to pick the dominant version and to populate analysis #13.
Q_EVALUATION_VERSION_DISTRIBUTION = """
SELECT
    COALESCE(evaluation_version, '<null>') AS evaluation_version,
    COUNT(*)                               AS n
FROM company_evaluations
GROUP BY evaluation_version
ORDER BY n DESC
"""

# ── Primary pull: one row per evaluated symbol, filtered by version ──
# Feeds analyses 1–11 (everything cross-sectional except history-based #12).
# Keeps raw_financials JSON so fundamentals can be parsed lazily per analysis.
Q_COMPANY_EVALUATIONS = """
SELECT
    symbol,
    company_name,
    sector,
    industry,
    market_cap,
    pillar_1_business_quality,
    pillar_2_operational_health,
    pillar_3_capital_allocation,
    pillar_4_growth_quality,
    pillar_5_valuation,
    composite_score,
    rank,
    pillar_1_detail,
    pillar_2_detail,
    pillar_3_detail,
    pillar_4_detail,
    pillar_5_detail,
    llm_recommendation,
    llm_conviction,
    raw_financials,
    evaluated_at,
    data_freshness,
    evaluation_version
FROM company_evaluations
WHERE composite_score IS NOT NULL
  AND (:version IS NULL OR evaluation_version = :version)
"""

# ── Universe metadata join source for tier analyses (#8) ──────────────
# market_cap_tier lives on the evaluation row but legacy rows may be stale;
# universe_symbols is authoritative. Read-only.
Q_UNIVERSE_SYMBOLS = """
SELECT
    symbol,
    market_cap_tier,
    tier,
    sector AS universe_sector,
    industry AS universe_industry
FROM universe_symbols
WHERE active = 1
"""

# ── Evaluation history: consecutive-snapshot score deltas (analysis #12) ──
# Returns every history row for symbols that have ≥2 snapshots. Pandas then
# groups by symbol, sorts by evaluated_at, and computes composite-score and
# per-pillar deltas between adjacent rows. `rank` is NULL in history so we
# use composite_score deltas as the stability signal. `snapshot` holds
# {pillar_scores, market_cap, ...} for per-pillar instability breakdown.
Q_EVALUATION_HISTORY = """
SELECT
    h.symbol,
    h.composite_score,
    h.rank,
    h.evaluated_at,
    h.llm_recommendation,
    h.snapshot
FROM evaluation_history h
WHERE h.composite_score IS NOT NULL
  AND h.symbol IN (
      SELECT symbol
      FROM evaluation_history
      WHERE composite_score IS NOT NULL
      GROUP BY symbol
      HAVING COUNT(*) >= 2
  )
ORDER BY h.symbol, h.evaluated_at
"""

# ── Universe size for normalizing rank deltas in analysis #12 ─────────
Q_UNIVERSE_SIZE = """
SELECT COUNT(*) AS n
FROM company_evaluations
WHERE composite_score IS NOT NULL
  AND (:version IS NULL OR evaluation_version = :version)
"""


ALL_QUERIES = {
    "evaluation_version_distribution": Q_EVALUATION_VERSION_DISTRIBUTION,
    "company_evaluations":             Q_COMPANY_EVALUATIONS,
    "universe_symbols":                Q_UNIVERSE_SYMBOLS,
    "evaluation_history":              Q_EVALUATION_HISTORY,
    "universe_size":                   Q_UNIVERSE_SIZE,
}

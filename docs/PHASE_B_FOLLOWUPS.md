# Phase B Follow-ups

Running list of items surfaced during Phase A audit + P2 cleanup that are out of scope for the immediate cleanup but should be revisited when planning Phase B. Each item includes origin context and rough shape-of-work.

---

## 1. Interest-expense data gap for mega-caps

**Origin:** P2 cleanup STOP 2 verification — after the `interest_coverage` None/zero fix, AAPL, GOOGL, BRK.B, CVX all dropped from score 100 (via false `"no_debt"` sentinel) to score 0 because Polygon's quarterly `interest_expense` field is `None` for those symbols.

These are companies with material interest expense on their real 10-Q statements (AAPL alone reports ~$800M/quarter). The data gap is in the ingestion layer, not the scoring layer. Investigate whether Polygon is genuinely not reporting the line, whether we're pulling the wrong field name (e.g., `interest_expense` vs `interest_expense_operating` vs `non_operating_income_expense`), or whether it's a rollup/aggregation issue in our SEC XBRL normalization path. Fixing the capture is cleaner than adding sentinel workarounds downstream.

**Scope hint:** one or two hours of `data/polygon_client.py` + `data/fmp_normalizer.py` (or wherever the Polygon financials JSON gets mapped to statement rows) plus a raw API probe against a few known-good symbols.

---

## 2. Finnhub `netInterestCoverageTTM` quality audit

**Origin:** same STOP 2 output — when Polygon lacks `interest_expense`, the new fix falls through to Finnhub's `netInterestCoverageTTM`. For the same mega-caps the Finnhub value came back as `0` (GOOGL), `0.57` (BRK.B), or negative (`-5.38` for CVX). Those numbers are not plausible for the actual businesses.

Either Finnhub's computed field is wrong on these symbols, we're reading the wrong field name, or there's a unit mismatch (e.g., Finnhub reporting a different definition like EBIT/TotalDebt or a coverage *ratio* vs a *multiple*). Worth a small standalone probe that compares Finnhub's claimed coverage against a manually computed one from the same company's 10-K, across 15–20 symbols spanning sectors and sizes. If the fallback is systematically unreliable, we need to know before we keep relying on it.

**Scope hint:** ~2-3 hour audit; can be its own `docs/FINNHUB_COVERAGE_AUDIT.md` deliverable. No code changes until the audit concludes.

---

## 3. Corroboration-based `"no_debt"` sentinel for `interest_coverage`

**Origin:** flagged in P2 cleanup STOP 2 surprises. Post-cleanup, honestly-conservative scoring drops several genuinely-low-leverage mega-caps (AAPL, GOOGL) to 0 on `interest_coverage` because Polygon's `interest_expense` is None and Finnhub's fallback returns 0. A rule like "reinstate `no_debt` sentinel when Finnhub `longTermDebt/equityQuarterly` < 0.1" would recover the signal for companies with genuinely trivial leverage without re-introducing the original false-positive bug.

Needs **failure-mode analysis before implementing**: how many companies would it incorrectly flag (e.g., a company with low L/E but substantial short-term debt or lease obligations)? What's the false-positive rate? Does the threshold hold across sectors, or do REITs/utilities need a different bar? Likely blocked by (1) and (2) above — if we can fix the underlying `interest_expense` data gap, the corroboration rule may not be needed at all.

**Scope hint:** 30-minute probe across the universe to estimate true/false-positive rates at a few threshold values (0.05, 0.1, 0.2), then a scoped implementation proposal with the results. Do **not** implement without that probe.

---

## Process notes

- These items should be revisited at Phase B planning. None are urgent enough to warrant a mid-phase patch.
- Items (1) and (2) are pure data-layer work and can be parallelized with framework work.
- Item (3) is P2-scoring work and should wait until (1) is either fixed or confirmed unfixable.

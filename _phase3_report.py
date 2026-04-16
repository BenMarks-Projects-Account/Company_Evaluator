#!/usr/bin/env python3
"""Phase 3 Shadow Validation — Discrepancy Report Generator.

Parses logs/data_source_diff.log (JSON-lines) and produces a structured
report for each call site, including field-level stats, systemic patterns,
error summaries, and GREEN/YELLOW/RED verdicts.

Usage:
    python _phase3_report.py
    python _phase3_report.py --start 2026-04-15T09:30:00 --end 2026-04-18T16:00:00
"""

import argparse
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

DIFF_LOG = os.path.join(os.path.dirname(__file__), "logs", "data_source_diff.log")

# ── Data loading ─────────────────────────────────────────────

def load_entries(path: str, start: str | None = None, end: str | None = None) -> list[dict]:
    """Load JSON-lines from the diff log, optionally filtered by time window."""
    if not os.path.isfile(path):
        print(f"ERROR: Diff log not found at {path}")
        sys.exit(1)

    entries = []
    bad = 0
    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue

            ts = entry.get("timestamp")
            if ts and start and ts < start:
                continue
            if ts and end and ts > end:
                continue
            entries.append(entry)

    if bad > 0:
        print(f"WARNING: {bad} malformed lines skipped")
    return entries


# ── Field-level statistics ───────────────────────────────────

class FieldStats:
    """Accumulates per-field comparison statistics."""

    def __init__(self):
        self.total = 0
        self.exact = 0          # both present and identical (or both None)
        self.within_tol = 0     # numeric fields within threshold
        self.exceeded = 0       # numeric fields exceeding threshold
        self.missing_fmp = 0
        self.missing_polygon = 0
        self.both_none = 0
        self.abs_deltas: list[float] = []
        self.pct_deltas: list[float] = []

    def record(self, field_entry: dict):
        self.total += 1
        status = field_entry.get("status")
        if status == "both_none":
            self.both_none += 1
            self.exact += 1
            return
        if status == "missing_in_fmp":
            self.missing_fmp += 1
            return
        if status == "missing_in_polygon":
            self.missing_polygon += 1
            return

        # Numeric comparison
        if "abs_delta" in field_entry:
            ad = field_entry["abs_delta"]
            pd = field_entry.get("pct_delta")
            self.abs_deltas.append(abs(ad))
            if pd is not None:
                self.pct_deltas.append(pd)
            if field_entry.get("exceeds_threshold"):
                self.exceeded += 1
            else:
                self.within_tol += 1
        # Categorical comparison
        elif "match" in field_entry:
            if field_entry["match"]:
                self.exact += 1
            else:
                self.exceeded += 1

    def summary(self) -> dict:
        out: dict = {
            "total": self.total,
            "exact_match": self.exact,
            "within_tolerance": self.within_tol,
            "exceeded_tolerance": self.exceeded,
            "missing_in_fmp": self.missing_fmp,
            "missing_in_polygon": self.missing_polygon,
            "both_none": self.both_none,
        }
        if self.abs_deltas:
            sorted_abs = sorted(self.abs_deltas)
            out["abs_delta_median"] = round(statistics.median(sorted_abs), 6)
            out["abs_delta_p95"] = round(_percentile(sorted_abs, 95), 6)
            out["abs_delta_max"] = round(max(sorted_abs), 6)
        if self.pct_deltas:
            sorted_pct = sorted(self.pct_deltas)
            out["pct_delta_median"] = round(statistics.median(sorted_pct), 4)
            out["pct_delta_p95"] = round(_percentile(sorted_pct, 95), 4)
            out["pct_delta_max"] = round(max(sorted_pct), 4)
        return out


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * pct / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


# ── Per-call-site analysis ───────────────────────────────────

class CallSiteReport:
    """Aggregates all entries for one call site."""

    def __init__(self, key: str):
        self.key = key
        self.total_calls = 0
        self.calls_with_discrepancy = 0
        self.polygon_errors = 0
        self.fmp_errors = 0
        self.fmp_none_responses = 0
        self.polygon_none_responses = 0
        self.both_none = 0
        self.field_stats: dict[str, FieldStats] = defaultdict(FieldStats)
        self.symbols_seen: set[str] = set()
        self.error_details: list[dict] = []
        self.macd_notes: list[str] = []

        # Bar-level stats (for get_raw_bars / get_price_history)
        self.bar_count_polygon: list[int] = []
        self.bar_count_fmp: list[int] = []
        self.dates_only_polygon: list[int] = []
        self.dates_only_fmp: list[int] = []

    def add(self, entry: dict):
        self.total_calls += 1
        symbol = entry.get("symbol")
        if symbol:
            self.symbols_seen.add(symbol)

        pe = entry.get("polygon_error")
        fe = entry.get("fmp_error")
        if pe:
            self.polygon_errors += 1
            self.error_details.append({"type": "polygon", "symbol": symbol, "error": pe})
        if fe:
            self.fmp_errors += 1
            self.error_details.append({"type": "fmp", "symbol": symbol, "error": fe})

        diff = entry.get("diff", {})
        status = diff.get("status")

        if status == "both_none":
            self.both_none += 1
            return
        if status == "fmp_none":
            self.fmp_none_responses += 1
            return
        if status == "polygon_none":
            self.polygon_none_responses += 1
            return

        # MACD notes
        note = diff.get("note")
        if note and note not in self.macd_notes:
            self.macd_notes.append(note)

        # Dict-level field comparisons
        fields = diff.get("fields", [])
        has_discrepancy = False
        for f in fields:
            fname = f.get("field", "unknown")
            self.field_stats[fname].record(f)
            if f.get("exceeds_threshold") or ("match" in f and not f["match"]):
                has_discrepancy = True

        # Bar-level comparisons (list responses)
        if "polygon_bar_count" in diff:
            self.bar_count_polygon.append(diff["polygon_bar_count"])
            self.bar_count_fmp.append(diff.get("fmp_bar_count", 0))
            self.dates_only_polygon.append(diff.get("dates_only_polygon", 0))
            self.dates_only_fmp.append(diff.get("dates_only_fmp", 0))
            # Check bar diffs for discrepancies
            for bar_diff in diff.get("bar_diffs_sample", []):
                if bar_diff.get("status") in ("missing_in_fmp", "missing_in_polygon"):
                    has_discrepancy = True
                for bf in bar_diff.get("fields", []):
                    fname = f"bar.{bf.get('field', 'unknown')}"
                    self.field_stats[fname].record(bf)
                    if bf.get("exceeds_threshold"):
                        has_discrepancy = True

        if has_discrepancy:
            self.calls_with_discrepancy += 1

    @property
    def discrepancy_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return self.calls_with_discrepancy / self.total_calls * 100.0

    def verdict(self) -> str:
        rate = self.discrepancy_rate
        has_systemic = self._detect_systemic_issues()
        persistent_errors = (self.fmp_errors / max(self.total_calls, 1)) > 0.05

        if rate > 5.0 or has_systemic == "major" or persistent_errors:
            return "RED"
        if rate > 1.0 or has_systemic == "minor" or self.fmp_errors > 0:
            return "YELLOW"
        return "GREEN"

    def _detect_systemic_issues(self) -> str | None:
        """Returns 'major', 'minor', or None."""
        for fname, fs in self.field_stats.items():
            if fs.total == 0:
                continue
            # >20% of comparisons have missing fields = systemic
            missing_rate = (fs.missing_fmp + fs.missing_polygon) / fs.total
            if missing_rate > 0.20:
                return "major"
            # >10% exceed tolerance = systemic minor
            exceed_rate = fs.exceeded / fs.total if fs.total > 0 else 0
            if exceed_rate > 0.10:
                return "minor"
        return None

    def report(self) -> dict:
        r: dict = {
            "call_site": self.key,
            "total_calls": self.total_calls,
            "symbols_seen": len(self.symbols_seen),
            "discrepancy_rate_pct": round(self.discrepancy_rate, 2),
            "verdict": self.verdict(),
            "polygon_errors": self.polygon_errors,
            "fmp_errors": self.fmp_errors,
            "fmp_none_responses": self.fmp_none_responses,
            "polygon_none_responses": self.polygon_none_responses,
            "both_none": self.both_none,
        }
        if self.macd_notes:
            r["macd_notes"] = self.macd_notes

        # Field-level breakdown
        r["field_breakdown"] = {
            fname: fs.summary()
            for fname, fs in sorted(self.field_stats.items())
        }

        # Bar-level stats
        if self.bar_count_polygon:
            r["bar_stats"] = {
                "avg_polygon_bars": round(statistics.mean(self.bar_count_polygon), 1),
                "avg_fmp_bars": round(statistics.mean(self.bar_count_fmp), 1),
                "avg_dates_only_polygon": round(statistics.mean(self.dates_only_polygon), 1),
                "avg_dates_only_fmp": round(statistics.mean(self.dates_only_fmp), 1),
            }

        # Error samples (first 5)
        if self.error_details:
            r["error_samples"] = self.error_details[:5]

        return r


# ── Systemic pattern detection ───────────────────────────────

def detect_systemic_patterns(entries: list[dict]) -> list[dict]:
    """Look for cross-call-site systemic patterns."""
    patterns = []

    # 1. Check for consistent field-level rounding differences
    field_exceed_counts: dict[str, int] = defaultdict(int)
    field_total_counts: dict[str, int] = defaultdict(int)
    for entry in entries:
        diff = entry.get("diff", {})
        for f in diff.get("fields", []):
            fname = f.get("field", "")
            field_total_counts[fname] += 1
            if f.get("exceeds_threshold"):
                field_exceed_counts[fname] += 1

    for fname, count in field_exceed_counts.items():
        total = field_total_counts.get(fname, 1)
        rate = count / total * 100
        if rate > 5 and count >= 3:
            patterns.append({
                "type": "persistent_field_divergence",
                "field": fname,
                "exceed_count": count,
                "total": total,
                "rate_pct": round(rate, 1),
                "severity": "major" if rate > 20 else "minor",
            })

    # 2. Check for MACD-specific signal line divergence
    macd_entries = [e for e in entries if "macd" in e.get("call_site", "").lower()]
    if macd_entries:
        signal_deltas = []
        macd_deltas = []
        for e in macd_entries:
            for f in e.get("diff", {}).get("fields", []):
                if f.get("field") == "signal" and "pct_delta" in f and f["pct_delta"] is not None:
                    signal_deltas.append(f["pct_delta"])
                elif f.get("field") == "value" and "pct_delta" in f and f["pct_delta"] is not None:
                    macd_deltas.append(f["pct_delta"])

        if signal_deltas:
            patterns.append({
                "type": "macd_signal_divergence",
                "description": "MACD signal line: FMP uses SMA, Polygon uses EMA",
                "signal_pct_delta_median": round(statistics.median(signal_deltas), 2) if signal_deltas else None,
                "signal_pct_delta_max": round(max(signal_deltas), 2) if signal_deltas else None,
                "macd_value_pct_delta_median": round(statistics.median(macd_deltas), 2) if macd_deltas else None,
                "macd_value_pct_delta_max": round(max(macd_deltas), 2) if macd_deltas else None,
                "sample_count": len(signal_deltas),
                "severity": "minor" if (statistics.median(signal_deltas) < 10) else "major",
                "recommendation": "Switch FMP signal to EMA-based if delta >5% median",
            })

    # 3. Check for FMP missing symbols
    fmp_missing = defaultdict(int)
    for e in entries:
        if e.get("diff", {}).get("status") == "fmp_none" and e.get("symbol"):
            fmp_missing[e["symbol"]] += 1
    if fmp_missing:
        patterns.append({
            "type": "fmp_missing_symbols",
            "symbols": dict(sorted(fmp_missing.items(), key=lambda x: -x[1])[:10]),
            "total_missing_calls": sum(fmp_missing.values()),
            "severity": "major" if sum(fmp_missing.values()) > 10 else "minor",
        })

    # 4. Check for date alignment issues in bar data
    date_mismatches = 0
    for e in entries:
        diff = e.get("diff", {})
        dop = diff.get("dates_only_polygon", 0)
        dof = diff.get("dates_only_fmp", 0)
        if dop > 2 or dof > 2:
            date_mismatches += 1
    if date_mismatches > 0:
        patterns.append({
            "type": "bar_date_alignment",
            "calls_with_date_gaps": date_mismatches,
            "description": "Some bar comparisons have >2 dates present in one source but not the other",
            "severity": "minor" if date_mismatches < 5 else "major",
        })

    return patterns


# ── VIX-specific analysis ────────────────────────────────────

def analyze_vix(entries: list[dict]) -> dict | None:
    """Check VIX (I:VIX) handling in FMP vs Polygon."""
    vix_entries = [e for e in entries if e.get("symbol") in ("I:VIX", "^VIX", "VIX")]
    if not vix_entries:
        return {"status": "no_vix_calls_in_log", "note": "VIX may not have been evaluated during the window"}

    fmp_ok = sum(1 for e in vix_entries if e.get("diff", {}).get("status") != "fmp_none" and not e.get("fmp_error"))
    fmp_fail = sum(1 for e in vix_entries if e.get("diff", {}).get("status") == "fmp_none" or e.get("fmp_error"))

    return {
        "total_vix_calls": len(vix_entries),
        "fmp_success": fmp_ok,
        "fmp_failure": fmp_fail,
        "proxy_likely_used": fmp_fail > 0,
        "verdict": "FMP lacks I:VIX — proxy fallback expected" if fmp_fail > 0 else "FMP handles VIX OK",
    }


# ── Fiscal edge case analysis ────────────────────────────────

def analyze_fiscal_edge_cases(entries: list[dict]) -> dict:
    """Check specific non-calendar fiscal year companies."""
    target_symbols = {"AAPL", "MSFT", "ORCL"}  # Sep, Jun, May fiscal years
    financials_entries = [
        e for e in entries
        if e.get("call_site") == "company_data_service.get_financials"
        and e.get("symbol") in target_symbols
    ]

    results = {}
    for sym in target_symbols:
        sym_entries = [e for e in financials_entries if e.get("symbol") == sym]
        if not sym_entries:
            results[sym] = {"status": "not_evaluated", "note": "Not in validation window"}
            continue

        # Check for period/date alignment issues
        has_discrepancy = False
        period_mismatches = 0
        for entry in sym_entries:
            diff = entry.get("diff", {})
            for f in diff.get("fields", []):
                fname = f.get("field", "")
                if fname in ("period", "fiscal_period", "fiscal_year", "filing_date"):
                    if "match" in f and not f["match"]:
                        period_mismatches += 1
                        has_discrepancy = True

        results[sym] = {
            "calls": len(sym_entries),
            "period_mismatches": period_mismatches,
            "has_fiscal_alignment_issue": has_discrepancy,
        }

    return results


# ── Rate limiter analysis ────────────────────────────────────

def analyze_rate_limiter(entries: list[dict]) -> dict:
    """Estimate FMP call volume and check for saturation signals."""
    # Count FMP calls per minute bucket
    minute_buckets: dict[str, int] = defaultdict(int)
    total_fmp_calls = 0

    for e in entries:
        ts = e.get("timestamp", "")
        if not ts:
            continue
        # Each shadow entry = 1 FMP call (plus 1 Polygon call)
        total_fmp_calls += 1
        minute_key = ts[:16]  # "2026-04-15T14:23"
        minute_buckets[minute_key] += 1

    peak_rpm = max(minute_buckets.values()) if minute_buckets else 0
    avg_rpm = statistics.mean(minute_buckets.values()) if minute_buckets else 0

    # MACD uses 2 FMP API calls internally (2 EMA fetches)
    macd_calls = sum(1 for e in entries if "macd" in e.get("call_site", "").lower())
    estimated_true_fmp_calls = total_fmp_calls + macd_calls  # MACD double-counts

    # Also count get_full_financials which uses 3 internal calls
    financials_calls = sum(1 for e in entries if "get_financials" in e.get("call_site", ""))
    estimated_true_fmp_calls += financials_calls * 2  # 3 total - 1 already counted

    effective_limit = 240  # 300 * 0.80

    return {
        "total_shadow_entries": total_fmp_calls,
        "estimated_true_fmp_api_calls": estimated_true_fmp_calls,
        "peak_rpm_in_log": peak_rpm,
        "avg_rpm": round(avg_rpm, 1),
        "effective_limit_rpm": effective_limit,
        "saturation_risk": peak_rpm > effective_limit * 0.8,
        "note": (
            f"Peak {peak_rpm} RPM vs {effective_limit} limit"
            + (" — SATURATED" if peak_rpm >= effective_limit else " — OK")
        ),
    }


# ── Main report ──────────────────────────────────────────────

def generate_report(entries: list[dict]) -> dict:
    """Build the full Phase 3 discrepancy report."""

    # Group by call site
    by_site: dict[str, CallSiteReport] = {}
    for entry in entries:
        key = entry.get("call_site", "unknown")
        if key not in by_site:
            by_site[key] = CallSiteReport(key)
        by_site[key].add(entry)

    # Build per-site reports
    site_reports = {}
    for key in sorted(by_site.keys()):
        site_reports[key] = by_site[key].report()

    # Timestamp analysis
    timestamps = [e.get("timestamp", "") for e in entries if e.get("timestamp")]
    ts_min = min(timestamps) if timestamps else None
    ts_max = max(timestamps) if timestamps else None

    report = {
        "report_generated": datetime.now(timezone.utc).isoformat(),
        "validation_window": {
            "first_entry": ts_min,
            "last_entry": ts_max,
            "total_entries": len(entries),
        },
        "per_call_site": site_reports,
        "systemic_patterns": detect_systemic_patterns(entries),
        "vix_analysis": analyze_vix(entries),
        "fiscal_edge_cases": analyze_fiscal_edge_cases(entries),
        "rate_limiter_analysis": analyze_rate_limiter(entries),
    }

    # Summary verdicts
    verdicts = {}
    for key, sr in site_reports.items():
        verdicts[key] = sr["verdict"]
    report["verdict_summary"] = verdicts

    # Cutover recommendation
    green = [k for k, v in verdicts.items() if v == "GREEN"]
    yellow = [k for k, v in verdicts.items() if v == "YELLOW"]
    red = [k for k, v in verdicts.items() if v == "RED"]
    report["cutover_recommendation"] = {
        "safe_for_phase4": green,
        "cutover_with_monitoring": yellow,
        "do_not_cutover": red,
    }

    return report


def print_report(report: dict):
    """Pretty-print the report to stdout."""
    print("=" * 70)
    print("  PHASE 3 SHADOW VALIDATION — DISCREPANCY REPORT")
    print("=" * 70)

    vw = report["validation_window"]
    print(f"\nValidation Window:")
    print(f"  First entry:   {vw['first_entry']}")
    print(f"  Last entry:    {vw['last_entry']}")
    print(f"  Total entries: {vw['total_entries']}")

    print(f"\n{'─' * 70}")
    print("  VERDICT SUMMARY")
    print(f"{'─' * 70}")
    for key, verdict in report["verdict_summary"].items():
        color = {"GREEN": "✅", "YELLOW": "⚠️", "RED": "❌"}.get(verdict, "❓")
        print(f"  {color} {verdict:6s}  {key}")

    print(f"\n{'─' * 70}")
    print("  PER-CALL-SITE DETAILS")
    print(f"{'─' * 70}")
    for key, sr in report["per_call_site"].items():
        print(f"\n  ┌── {key}")
        print(f"  │ Calls: {sr['total_calls']}  Symbols: {sr['symbols_seen']}  "
              f"Discrepancy: {sr['discrepancy_rate_pct']}%  Verdict: {sr['verdict']}")
        print(f"  │ Polygon errors: {sr['polygon_errors']}  FMP errors: {sr['fmp_errors']}  "
              f"FMP none: {sr['fmp_none_responses']}")

        if sr.get("macd_notes"):
            for note in sr["macd_notes"]:
                print(f"  │ ⚠ {note}")

        fb = sr.get("field_breakdown", {})
        if fb:
            print(f"  │ Field breakdown:")
            for fname, stats in fb.items():
                exc = stats.get("exceeded_tolerance", 0)
                total = stats.get("total", 0)
                mfmp = stats.get("missing_in_fmp", 0)
                line = f"  │   {fname:30s}  total={total}  exact={stats.get('exact_match',0)}  " \
                       f"within_tol={stats.get('within_tolerance',0)}  exceeded={exc}  " \
                       f"missing_fmp={mfmp}"
                if stats.get("pct_delta_median") is not None:
                    line += f"  pct_med={stats['pct_delta_median']}  pct_p95={stats.get('pct_delta_p95')}  pct_max={stats.get('pct_delta_max')}"
                print(line)

        if sr.get("bar_stats"):
            bs = sr["bar_stats"]
            print(f"  │ Bar stats: polygon_avg={bs['avg_polygon_bars']:.0f}  "
                  f"fmp_avg={bs['avg_fmp_bars']:.0f}  "
                  f"dates_only_poly={bs['avg_dates_only_polygon']:.1f}  "
                  f"dates_only_fmp={bs['avg_dates_only_fmp']:.1f}")

        if sr.get("error_samples"):
            print(f"  │ Error samples:")
            for err in sr["error_samples"]:
                print(f"  │   [{err['type']}] {err['symbol']}: {err['error'][:80]}")

        print(f"  └──")

    # Systemic patterns
    patterns = report.get("systemic_patterns", [])
    if patterns:
        print(f"\n{'─' * 70}")
        print("  SYSTEMIC PATTERNS")
        print(f"{'─' * 70}")
        for p in patterns:
            sev = "🔴" if p.get("severity") == "major" else "🟡"
            print(f"\n  {sev} {p['type']}")
            for k, v in p.items():
                if k not in ("type", "severity"):
                    print(f"     {k}: {v}")

    # VIX
    vix = report.get("vix_analysis")
    if vix:
        print(f"\n{'─' * 70}")
        print("  VIX ANALYSIS")
        print(f"{'─' * 70}")
        for k, v in vix.items():
            print(f"  {k}: {v}")

    # Fiscal edge cases
    fec = report.get("fiscal_edge_cases", {})
    if fec:
        print(f"\n{'─' * 70}")
        print("  FISCAL EDGE CASES (non-calendar FY)")
        print(f"{'─' * 70}")
        for sym, data in fec.items():
            print(f"  {sym}: {data}")

    # Rate limiter
    rl = report.get("rate_limiter_analysis", {})
    if rl:
        print(f"\n{'─' * 70}")
        print("  RATE LIMITER")
        print(f"{'─' * 70}")
        for k, v in rl.items():
            print(f"  {k}: {v}")

    # Cutover recommendation
    rec = report.get("cutover_recommendation", {})
    print(f"\n{'=' * 70}")
    print("  CUTOVER RECOMMENDATION")
    print(f"{'=' * 70}")
    green = rec.get("safe_for_phase4", [])
    yellow = rec.get("cutover_with_monitoring", [])
    red = rec.get("do_not_cutover", [])
    if green:
        print(f"\n  ✅ Safe for Phase 4 cutover ({len(green)}):")
        for s in green:
            print(f"     {s}")
    if yellow:
        print(f"\n  ⚠️ Cutover with monitoring ({len(yellow)}):")
        for s in yellow:
            print(f"     {s}")
    if red:
        print(f"\n  ❌ Do NOT cutover ({len(red)}):")
        for s in red:
            print(f"     {s}")

    print(f"\n{'=' * 70}")


def main():
    parser = argparse.ArgumentParser(description="Phase 3 Shadow Validation Report")
    parser.add_argument("--start", help="Start timestamp filter (ISO format)")
    parser.add_argument("--end", help="End timestamp filter (ISO format)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON instead of formatted text")
    parser.add_argument("--log", default=DIFF_LOG, help=f"Path to diff log (default: {DIFF_LOG})")
    args = parser.parse_args()

    entries = load_entries(args.log, start=args.start, end=args.end)
    if not entries:
        print("No entries found in the validation window. Run evaluations in shadow mode first.")
        sys.exit(1)

    report = generate_report(entries)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print_report(report)

    # Also write JSON to file
    out_path = os.path.join(os.path.dirname(__file__), "logs", "phase3_report.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nJSON report saved to: {out_path}")


if __name__ == "__main__":
    main()

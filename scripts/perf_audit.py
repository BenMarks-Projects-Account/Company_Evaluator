"""Performance audit — per-symbol evaluation timing breakdown.

Runs `evaluate_company` against 10 representative symbols with the
crawler stopped. Uses zero-change monkey patches on `httpx.AsyncClient`
to time every outbound HTTP call (Polygon / Finnhub / FMP / LM Studio)
and on `asyncio.sleep` to measure total rate-limit backoff per symbol.

After Tier 2 (LLM routing) the audit processes symbols in concurrent
batches of N=settings.llm_concurrent_symbols so the two LM Studio
endpoints are actually exercised.

Outputs a per-symbol breakdown + aggregate host/phase report +
LLM router stats.

Usage:
    .\\.venv\\Scripts\\python.exe scripts\\perf_audit.py
"""
from __future__ import annotations

import asyncio
import contextvars
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Monkey-patch httpx.AsyncClient to time every request ─────────────
import httpx  # noqa: E402

_HTTP_CALLS: list[dict] = []
# Per-task symbol binding so concurrent evaluate_company() calls under
# asyncio.gather attribute their HTTP/LLM/sleep samples to the right symbol.
_CURRENT_SYMBOL_CV: contextvars.ContextVar[str] = contextvars.ContextVar(
    "perf_audit_current_symbol", default="?"
)


def _cur_sym() -> str:
    return _CURRENT_SYMBOL_CV.get()


_orig_send = httpx.AsyncClient.send


async def _timed_send(self, request, *args, **kwargs):
    t = time.time()
    try:
        resp = await _orig_send(self, request, *args, **kwargs)
        status = resp.status_code
        size = int(resp.headers.get("content-length") or 0)
    except Exception as exc:  # pragma: no cover
        _HTTP_CALLS.append({
            "symbol": _cur_sym(),
            "host": urlparse(str(request.url)).hostname or "?",
            "path": urlparse(str(request.url)).path,
            "method": request.method,
            "status": -1,
            "size": 0,
            "elapsed": time.time() - t,
            "error": str(exc)[:80],
        })
        raise
    _HTTP_CALLS.append({
        "symbol": _cur_sym(),
        "host": urlparse(str(request.url)).hostname or "?",
        "path": urlparse(str(request.url)).path,
        "method": request.method,
        "status": status,
        "size": size,
        "elapsed": time.time() - t,
        "error": None,
    })
    return resp


httpx.AsyncClient.send = _timed_send  # type: ignore[assignment]


# ── Monkey-patch asyncio.sleep to measure rate-limit waits ──────────
_SLEEP_TOTAL: defaultdict[str, float] = defaultdict(float)
_orig_sleep = asyncio.sleep


async def _timed_sleep(delay, *args, **kwargs):
    if delay and delay > 0:
        _SLEEP_TOTAL[_cur_sym()] += float(delay)
    return await _orig_sleep(delay, *args, **kwargs)


asyncio.sleep = _timed_sleep  # type: ignore[assignment]

# ── Logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("perf_audit")

for noisy in ("httpx", "bulk.bulk_fetcher", "bulk.bulk_parser"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ── Imports that depend on the monkey-patches being in place ─────────
from config import get_settings  # noqa: E402
from db.database import init_db  # noqa: E402
from pipeline.evaluator import evaluate_company  # noqa: E402
from analysis.llm_client import call_llm as _orig_call_llm  # noqa: E402
import analysis.llm_client as _llm_module  # noqa: E402

# Instrument the LLM wrapper to capture prompt/response sizes + exact time.
_LLM_CALLS: list[dict] = []


async def _timed_call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 2000):
    t = time.time()
    result = await _orig_call_llm(system_prompt, user_prompt, max_tokens)
    elapsed = time.time() - t
    _LLM_CALLS.append({
        "symbol": _cur_sym(),
        "prompt_chars": len(system_prompt) + len(user_prompt),
        "response_chars": len(result) if result else 0,
        "elapsed": elapsed,
    })
    return result


_llm_module.call_llm = _timed_call_llm
# Re-bind inside company_analyst where it was imported by name
import analysis.company_analyst as _ca  # noqa: E402
_ca.call_llm = _timed_call_llm


TEST_SYMBOLS = [
    "MSFT", "AAPL", "WMS", "EXEL", "PLTR",
    "KO", "JPM", "XOM", "AMT", "DDOG",
]


def _classify_host(host: str) -> str:
    h = (host or "").lower()
    if "polygon" in h:
        return "polygon"
    if "finnhub" in h:
        return "finnhub"
    if "financialmodelingprep" in h or "fmp" in h:
        return "fmp"
    if "localhost" in h or "127.0.0.1" in h or "192.168.1.143" in h:
        return "lmstudio"
    return h or "unknown"


async def _evaluate_one(sym: str) -> dict:
    """Evaluate a single symbol with its own contextvar binding."""
    _CURRENT_SYMBOL_CV.set(sym)
    logger.info("\n%s\n>>> %s\n%s", "=" * 60, sym, "=" * 60)
    t0 = time.time()
    ok = True
    try:
        result = await evaluate_company(sym, skip_rankings=True)
        status = result.get("status")
        if status != "complete":
            ok = False
            logger.warning("%s: status=%s", sym, status)
    except Exception as exc:
        ok = False
        logger.error("%s: EXCEPTION %s", sym, exc, exc_info=True)
    return {"symbol": sym, "elapsed": time.time() - t0, "ok": ok}


async def main():
    settings = get_settings()
    await init_db(settings.database_url)
    concurrent_n = max(1, getattr(settings, "llm_concurrent_symbols", 2))
    logger.info(
        "Perf audit starting — %d symbols, concurrent=%d",
        len(TEST_SYMBOLS), concurrent_n,
    )

    per_symbol: list[dict] = []
    wall_start = time.time()
    for i in range(0, len(TEST_SYMBOLS), concurrent_n):
        batch = TEST_SYMBOLS[i : i + concurrent_n]
        results = await asyncio.gather(
            *(_evaluate_one(s) for s in batch),
            return_exceptions=False,
        )
        per_symbol.extend(results)
    wall_elapsed = time.time() - wall_start

    # End-of-batch rankings flush (matches crawler behavior)
    try:
        from pipeline.evaluator import _update_rankings
        rk_t0 = time.time()
        await _update_rankings()
        logger.info("End-of-batch rankings update: %.1fs", time.time() - rk_t0)
    except Exception as exc:
        logger.warning("End-of-batch rankings update failed: %s", exc)

    _CURRENT_SYMBOL_CV.set("(done)")

    # ── Aggregate ────────────────────────────────────────────────
    print("\n" + "=" * 78)
    print("PER-SYMBOL TOTALS")
    print("=" * 78)
    print(f"{'symbol':<8} {'status':<5} {'total_s':>8} "
          f"{'http_s':>8} {'llm_s':>8} {'sleep_s':>8} {'http_n':>7}")
    total_all = 0.0
    for r in per_symbol:
        sym = r["symbol"]
        http_time = sum(c["elapsed"] for c in _HTTP_CALLS if c["symbol"] == sym)
        http_count = sum(1 for c in _HTTP_CALLS if c["symbol"] == sym)
        llm_time = sum(c["elapsed"] for c in _LLM_CALLS if c["symbol"] == sym)
        sleep_time = _SLEEP_TOTAL.get(sym, 0.0)
        total_all += r["elapsed"]
        print(f"{sym:<8} {'OK' if r['ok'] else 'FAIL':<5} {r['elapsed']:>8.1f} "
              f"{http_time:>8.1f} {llm_time:>8.1f} {sleep_time:>8.1f} "
              f"{http_count:>7d}")

    avg = total_all / len(per_symbol) if per_symbol else 0
    print(f"\nAverage total/symbol: {avg:.1f}s (sum of per-symbol wall times)")
    print(f"WALL CLOCK total: {wall_elapsed:.1f}s (concurrent={concurrent_n})")
    if per_symbol:
        print(f"Effective per-symbol (wall / N): {wall_elapsed / len(per_symbol):.1f}s")

    # ── LLM routing stats ────────────────────────────────────────
    try:
        from analysis.llm_router import get_router
        router_stats = get_router().get_stats()
        print("\n" + "=" * 78)
        print("LLM ROUTING STATS")
        print("=" * 78)
        for e in router_stats["endpoints"]:
            print(
                f"  {e['name']:<15} calls={e['total_calls']:>3} "
                f"errs={e['total_errors']:>2} "
                f"avg={e['avg_latency_s']:>5.1f}s healthy={e['healthy']}"
            )
        print(f"  total_calls={router_stats['total_calls']} "
              f"healthy_endpoints={router_stats['healthy_endpoints']}")
    except Exception as exc:
        logger.warning("Could not fetch router stats: %s", exc)

    # ── Host breakdown ───────────────────────────────────────────
    host_stats: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "time": 0.0, "bytes": 0, "errors": 0}
    )
    for c in _HTTP_CALLS:
        key = _classify_host(c["host"])
        s = host_stats[key]
        s["count"] += 1
        s["time"] += c["elapsed"]
        s["bytes"] += c["size"]
        if c["status"] != 200:
            s["errors"] += 1

    print("\n" + "=" * 78)
    print("HOST BREAKDOWN (aggregate across all symbols)")
    print("=" * 78)
    print(f"{'host':<12} {'calls':>6} {'total_s':>8} {'avg_ms':>8} "
          f"{'MB':>6} {'errs':>5}")
    for key, s in sorted(host_stats.items(), key=lambda x: -x[1]["time"]):
        avg_ms = (s["time"] / s["count"] * 1000) if s["count"] else 0
        print(f"{key:<12} {s['count']:>6d} {s['time']:>8.1f} "
              f"{avg_ms:>8.0f} {s['bytes'] / 1e6:>6.1f} {s['errors']:>5d}")

    # ── LLM detail ───────────────────────────────────────────────
    print("\n" + "=" * 78)
    print("LLM CALLS (one per symbol)")
    print("=" * 78)
    if _LLM_CALLS:
        print(f"{'symbol':<8} {'prompt_chars':>12} {'resp_chars':>10} "
              f"{'elapsed_s':>10}")
        for c in _LLM_CALLS:
            print(f"{c['symbol']:<8} {c['prompt_chars']:>12d} "
                  f"{c['response_chars']:>10d} {c['elapsed']:>10.1f}")
        total_llm = sum(c["elapsed"] for c in _LLM_CALLS)
        avg_llm = total_llm / len(_LLM_CALLS)
        print(f"\nLLM avg: {avg_llm:.1f}s/symbol  ({total_llm:.1f}s total)")

    # ── Top 15 slowest HTTP calls ────────────────────────────────
    print("\n" + "=" * 78)
    print("TOP 15 SLOWEST HTTP CALLS")
    print("=" * 78)
    slowest = sorted(_HTTP_CALLS, key=lambda c: -c["elapsed"])[:15]
    for c in slowest:
        print(f"  {c['elapsed']:>6.2f}s  [{c['status']}] "
              f"{c['symbol']:<6} {c['host']}{c['path'][:80]}")

    # ── Sleep breakdown ──────────────────────────────────────────
    print("\n" + "=" * 78)
    print("ASYNC SLEEP TOTALS (rate-limit + backoff)")
    print("=" * 78)
    total_sleep = sum(_SLEEP_TOTAL.values())
    for sym, sec in sorted(_SLEEP_TOTAL.items(), key=lambda x: -x[1]):
        if sec > 0.01:
            print(f"  {sym:<10} {sec:>6.2f}s")
    print(f"  TOTAL sleep across all symbols: {total_sleep:.1f}s")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()) or 0)

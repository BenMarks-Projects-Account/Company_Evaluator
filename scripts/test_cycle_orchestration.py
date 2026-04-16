"""Phase 2c smoke test — exercise CycleOrchestrator end-to-end.

Simulates a crawler cycle:
  - before_cycle() checks staleness + optionally refreshes
  - process a few symbols through the cached client
  - after_cycle() logs stats
  - prints refresh log and cache stats

Usage:
    python scripts/test_cycle_orchestration.py
"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_settings
from bulk.cycle_orchestrator import CycleOrchestrator
from pipeline.evaluator import _get_fmp_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    settings = get_settings()

    cached_client = _get_fmp_client()
    if cached_client is None:
        print("FMP is not enabled or missing API key — cannot run smoke test")
        return 1

    orch = CycleOrchestrator(settings=settings, cached_fmp_client=cached_client)
    orch.log_startup_state()

    print("\n=== BEFORE CYCLE ===")
    before = orch.before_cycle()
    print("before_cycle summary:", before)

    print("\n=== SIMULATING SYMBOL PROCESSING ===")
    test_symbols = ["MSFT", "AAPL", "NVDA", "GOOGL", "AMZN"]
    for sym in test_symbols:
        try:
            profile = await cached_client.get_company_profile(sym)
            ratios = await cached_client.get_ratios_ttm(sym)
            metrics = await cached_client.get_key_metrics_ttm(sym)
            n_keys = 0
            if isinstance(profile, list) and profile:
                n_keys = len(profile[0].keys())
            elif isinstance(profile, dict):
                n_keys = len(profile.keys())
            print(
                f"  {sym}: profile_keys={n_keys} "
                f"ratios={'ok' if ratios else 'none'} "
                f"metrics={'ok' if metrics else 'none'}"
            )
        except Exception as exc:
            print(f"  {sym}: ERROR {exc}")
        orch.record_symbol_processed()

    print("\n=== AFTER CYCLE ===")
    after = orch.after_cycle()
    print("after_cycle summary:", after)

    print("\n=== REFRESH LOG ===")
    print(orch.get_refresh_log())
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

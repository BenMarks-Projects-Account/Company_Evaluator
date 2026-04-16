"""Isolation test for the LLM router: verifies both endpoints respond and
that two concurrent calls actually run in parallel (not serialized).
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.llm_router import get_router  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main():
    router = get_router()
    model = await router.resolve_model()
    print(f"\nModel: {model}")
    print(f"Endpoints: {[(e.name, e.base_url, e.healthy) for e in router.endpoints]}\n")

    sys_prompt = "You are a terse financial analyst. One sentence only."

    # Test 1: single call
    print("=== TEST 1: single call ===")
    t0 = time.time()
    r1 = await router.call_llm(sys_prompt, "Describe Apple Inc. in one sentence.", max_tokens=120)
    t1 = time.time() - t0
    print(f"Single call: {t1:.1f}s")
    print(f"Response: {(r1 or 'NONE')[:160]}\n")

    # Test 2: two concurrent calls (should route to different endpoints)
    print("=== TEST 2: two concurrent calls ===")
    t0 = time.time()
    r2a, r2b = await asyncio.gather(
        router.call_llm(sys_prompt, "Describe Microsoft in one sentence.", max_tokens=120),
        router.call_llm(sys_prompt, "Describe Amazon in one sentence.", max_tokens=120),
    )
    t2 = time.time() - t0
    print(f"Two concurrent: {t2:.1f}s (single was {t1:.1f}s)")
    print(f"Speedup vs 2x serial: {(2 * t1) / t2:.2f}x")
    print(f"MSFT: {(r2a or 'NONE')[:120]}")
    print(f"AMZN: {(r2b or 'NONE')[:120]}\n")

    print("=== stats ===")
    stats = router.get_stats()
    for e in stats["endpoints"]:
        print(f"  {e['name']:<15} calls={e['total_calls']} errs={e['total_errors']} "
              f"avg={e['avg_latency_s']}s healthy={e['healthy']}")


if __name__ == "__main__":
    asyncio.run(main())

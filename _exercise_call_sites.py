"""Exercise the 6 untested call sites + fiscal edge case evals.

Run with: python _exercise_call_sites.py
"""

import asyncio
import httpx
import json
import sys

BASE = "http://localhost:8100"

SYMBOLS = ["AAPL", "MSFT", "ORCL", "GOOGL", "NVDA"]


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=120) as c:
        # ── 1. chart_service.get_raw_bars ────────────────────────
        print("\n=== 1. chart_service.get_raw_bars (GET /api/charts/{symbol}) ===")
        for sym in SYMBOLS:
            try:
                r = await c.get(f"/api/charts/{sym}")
                print(f"  {sym}: {r.status_code} ({len(r.content)} bytes)")
            except Exception as e:
                print(f"  {sym}: ERROR {e}")

        # ── 2. routes_quote.get_snapshot (GET /api/quote/{symbol}) ──
        print("\n=== 2. routes_quote.get_snapshot (GET /api/quote/{symbol}) ===")
        for sym in SYMBOLS:
            try:
                r = await c.get(f"/api/quote/{sym}")
                print(f"  {sym}: {r.status_code} ({len(r.content)} bytes)")
            except Exception as e:
                print(f"  {sym}: ERROR {e}")

        # ── 3. routes_admin.get_company_details (POST /api/universe/add) ──
        print("\n=== 3. routes_admin.get_company_details (POST /api/universe/add) ===")
        for sym in SYMBOLS:
            try:
                r = await c.post("/api/universe/add", json={"symbol": sym})
                print(f"  {sym}: {r.status_code}")
            except Exception as e:
                print(f"  {sym}: ERROR {e}")

        # ── 4 & 5. on_demand.get_company_details + on_demand.get_snapshot ──
        # (POST /api/on-demand/analyze triggers both)
        print("\n=== 4&5. on_demand (POST /api/on-demand/analyze) ===")
        for sym in SYMBOLS:
            try:
                r = await c.post("/api/on-demand/analyze", json={"symbol": sym})
                data = r.json() if r.status_code == 200 else r.text
                job_id = data.get("job_id", "?") if isinstance(data, dict) else "?"
                print(f"  {sym}: {r.status_code} job_id={job_id}")
            except Exception as e:
                print(f"  {sym}: ERROR {e}")

        # ── 6. universe_builder.get_tickers (POST /api/universe/refresh) ──
        print("\n=== 6. universe_builder.get_tickers (POST /api/universe/refresh) ===")
        try:
            r = await c.post("/api/universe/refresh", json={"tier": "large_cap"})
            print(f"  large_cap: {r.status_code} ({len(r.content)} bytes)")
        except Exception as e:
            print(f"  large_cap: ERROR {e}")

        # ── 7. Fiscal edge case evals (AAPL Sep FY, MSFT Jun FY, ORCL May FY) ──
        print("\n=== 7. Fiscal edge case evals (entry-point) ===")
        for sym in ["AAPL", "MSFT", "ORCL"]:
            try:
                r = await c.post("/api/entry-point/analyze", json={"symbol": sym, "skip_llm": True})
                status = r.status_code
                data = r.json() if status == 200 else r.text[:200]
                print(f"  {sym}: {status}")
            except Exception as e:
                print(f"  {sym}: ERROR {e}")

        # Wait a bit for background on-demand jobs to complete
        print("\n=== Waiting 30s for on-demand background jobs to complete... ===")
        await asyncio.sleep(30)

        # Check diff log counts
        print("\n=== Diff log summary ===")
        try:
            with open("logs/data_source_diff.log") as f:
                lines = f.readlines()
            entries = [json.loads(l) for l in lines]
            from collections import Counter
            counts = Counter(e["call_site"] for e in entries)
            for cs, count in sorted(counts.items()):
                print(f"  {cs}: {count}")
            print(f"  TOTAL: {len(entries)}")
        except Exception as e:
            print(f"  ERROR reading diff log: {e}")


if __name__ == "__main__":
    asyncio.run(main())

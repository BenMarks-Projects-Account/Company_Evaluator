"""Test EPV endpoint via on-demand pipeline."""
import sys, time, json, urllib.request

BASE = "http://localhost:8100/api/on-demand"
SYMBOLS = sys.argv[1:] or ["MSFT", "AAPL", "ERIE", "SBUX", "SNOW", "KO", "WMT", "CRWV"]


def post_json(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


for sym in SYMBOLS:
    print(f"\n{'='*60}")
    print(f"Testing {sym}...")
    try:
        resp = post_json(f"{BASE}/analyze", {"symbol": sym})
    except Exception as e:
        print(f"  SUBMIT FAILED: {e}")
        continue

    job_id = resp.get("job_id")
    if not job_id:
        print(f"  No job_id returned: {resp}")
        continue

    print(f"  Job: {job_id}")

    # Poll until complete
    for _ in range(120):
        status = get_json(f"{BASE}/jobs/{job_id}")
        prog = status.get("progress", {})
        pct = prog.get("percent", 0)
        step = prog.get("current_step", "")
        st = status.get("status", "")
        print(f"  [{pct}%] {st}: {step}")
        if st in ("complete", "failed"):
            break
        time.sleep(5)
    else:
        print("  TIMEOUT after 10 min")
        continue

    if st == "failed":
        print(f"  FAILED: {status.get('error')}")
        continue

    # Fetch result
    result = get_json(f"{BASE}/jobs/{job_id}/result")
    epv = result.get("epv")

    if not epv:
        print(f"  EPV key missing from result!")
        continue

    if epv.get("ok"):
        print(f"\n  {sym}: EPV/share=${epv['fair_value_per_share']} vs price=${epv.get('current_price')}")
        print(f"  Growth premium: {epv['growth_premium_pct']}% [{epv['growth_premium_label']}]")
        print(f"  WACC: {epv['inputs']['wacc']}, Tax: {epv['inputs']['tax_rate']} ({epv['inputs']['tax_rate_source']})")
        print(f"  Normalized EBIT: ${epv['inputs']['normalized_ebit']:,.0f} over {epv['inputs']['normalization_period_years']}y")
        print(f"  NOPAT: ${epv['inputs']['nopat']:,.0f}")
        print(f"  EPV total: ${epv['epv_total']:,.0f}")
        print(f"  Interpretation: {epv['interpretation']}")
    else:
        print(f"\n  {sym}: ok=false — {epv.get('error')}")

    # Also print piotroski to confirm it still works
    pio = result.get("piotroski_f_score")
    if pio and pio.get("ok"):
        print(f"  Piotroski: {pio['score']}/9 ({pio['label']})")
    elif pio:
        print(f"  Piotroski: ok=false — {pio.get('error')}")

print(f"\n{'='*60}")
print("Done.")

"""Test Piotroski via on-demand endpoint. Submit job, poll, print result."""
import httpx
import json
import time
import sys

BASE = "http://192.168.1.143:8100"
symbols = sys.argv[1:] if len(sys.argv) > 1 else ["MSFT"]

for symbol in symbols:
    print(f"\n{'='*60}")
    print(f"Testing {symbol}...")
    
    # Submit job
    r = httpx.post(f"{BASE}/api/on-demand/analyze", json={"symbol": symbol}, timeout=30)
    if r.status_code != 200:
        print(f"  Submit failed: {r.status_code} {r.text[:200]}")
        continue
    
    job_id = r.json().get("job_id")
    print(f"  Job: {job_id}")
    
    # Poll until complete
    for _ in range(120):  # max 6 minutes
        time.sleep(3)
        sr = httpx.get(f"{BASE}/api/on-demand/jobs/{job_id}", timeout=10)
        status = sr.json()
        state = status.get("status")
        progress = status.get("progress", {})
        step = progress.get("current_step", "")
        pct = progress.get("percent", 0)
        print(f"  [{pct}%] {state}: {step}")
        if state in ("complete", "failed"):
            break
    
    if state == "failed":
        print(f"  FAILED: {status.get('error')}")
        continue
    
    # Get result
    rr = httpx.get(f"{BASE}/api/on-demand/jobs/{job_id}/result", timeout=30)
    result = rr.json()
    
    p = result.get("piotroski_f_score")
    if p is None:
        print(f"  ERROR: piotroski_f_score missing from result!")
        print(f"  Keys: {list(result.keys())}")
        continue
    
    if p.get("ok"):
        print(f"\n  {symbol}: F-Score {p['score']}/9 ({p['label']})")
        for name, check in p['checks'].items():
            status_str = "PASS" if check['passed'] else "FAIL"
            print(f"    [{status_str}] {check['label']}: {check['details']}")
        print(f"  Interpretation: {p['interpretation']}")
    else:
        print(f"\n  {symbol}: ok=false — {p.get('error')}")
    
    # For MSFT, dump full JSON
    if symbol == "MSFT":
        print(f"\n  Full piotroski_f_score JSON:")
        print(json.dumps(p, indent=2))

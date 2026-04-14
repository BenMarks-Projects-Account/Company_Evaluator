import re
with open("pipeline/on_demand.py", encoding="utf-8") as f:
    src = f.read()
m = re.search(r'PIPELINE_STEPS\s*=\s*\[(.*?)\]', src, re.DOTALL)
lines = [l.strip().strip(",").strip('"') for l in m.group(1).splitlines() if l.strip().strip(",").strip('"')]
print(f"Step count: {len(lines)}")
for i, s in enumerate(lines):
    print(f"  [{i}] {s}")

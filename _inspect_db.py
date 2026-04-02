"""Quick DB inspection script — delete after use."""
import sqlite3

conn = sqlite3.connect("db/company_eval.db")

print("=== TABLES ===")
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
for t in tables:
    print(t)

print("\n=== ROW COUNTS ===")
for t in tables:
    c = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
    print(f"  {t}: {c} rows")

print("\n=== PRAGMA journal_mode ===")
print(" ", conn.execute("PRAGMA journal_mode").fetchone()[0])

print("\n=== SAMPLE COMPANIES (top 5 by rank) ===")
for row in conn.execute(
    "SELECT symbol, company_name, composite_score, rank, evaluated_at "
    "FROM company_evaluations ORDER BY rank LIMIT 5"
):
    print(" ", row)

print("\n=== HISTORY SAMPLE ===")
if "evaluation_history" in tables:
    c = conn.execute("SELECT COUNT(*) FROM evaluation_history").fetchone()[0]
    print(f"  evaluation_history: {c} rows")
    for row in conn.execute(
        "SELECT symbol, composite_score, evaluated_at FROM evaluation_history ORDER BY evaluated_at DESC LIMIT 5"
    ):
        print(" ", row)

print("\n=== CRAWLER STATE JSON ===")
import json, pathlib
sf = pathlib.Path("db/crawler_state.json")
if sf.exists():
    state = json.loads(sf.read_text())
    for k, v in state.items():
        if k == "symbols":
            print(f"  symbols: [{len(v)} items]")
        elif k in ("completed_symbols", "failed_symbols"):
            print(f"  {k}: [{len(v)} items]")
        else:
            print(f"  {k}: {v}")
else:
    print("  (not found)")

conn.close()

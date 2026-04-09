"""Quick check of universe DB state."""
import sqlite3
from config import get_settings

conn = sqlite3.connect(get_settings().database_path)
c = conn.cursor()

print("=== By source (TIER_DEFINITIONS key) ===")
c.execute("""
    SELECT source, COUNT(*) as total, 
           SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) as active 
    FROM universe_symbols 
    GROUP BY source 
    ORDER BY total DESC
""")
for row in c.fetchall():
    src = row[0] or "(null)"
    print(f"  {src:20s} total={row[1]:4d} active={row[2]:4d}")

print("\n=== By market_cap_tier (classification) ===")
c.execute("""
    SELECT market_cap_tier, COUNT(*) as total, 
           SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) as active 
    FROM universe_symbols 
    GROUP BY market_cap_tier 
    ORDER BY total DESC
""")
for row in c.fetchall():
    tier = row[0] or "(null)"
    print(f"  {tier:20s} total={row[1]:4d} active={row[2]:4d}")

print("\n=== Last screened dates (source=large_cap) ===")
c.execute("""
    SELECT last_screened_at FROM universe_symbols 
    WHERE source='large_cap' AND last_screened_at IS NOT NULL 
    ORDER BY last_screened_at DESC LIMIT 3
""")
for row in c.fetchall():
    print(f"  {row[0]}")

print("\n=== last_screened_at for source=small_cap ===")
c.execute("""
    SELECT last_screened_at FROM universe_symbols 
    WHERE source='small_cap' AND last_screened_at IS NOT NULL 
    ORDER BY last_screened_at DESC LIMIT 3
""")
for row in c.fetchall():
    print(f"  {row[0]}")

c.execute("SELECT COUNT(*) FROM universe_symbols WHERE active=1")
print(f"\nTOTAL ACTIVE: {c.fetchone()[0]}")

conn.close()

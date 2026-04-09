import sqlite3
from config import get_settings

conn = sqlite3.connect(get_settings().database_path)
c = conn.cursor()
c.execute("UPDATE universe_symbols SET last_screened_at = NULL WHERE source = 'small_cap'")
conn.commit()
print(f"Cleared last_screened_at for {c.rowcount} small_cap records")
conn.close()

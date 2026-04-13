import sqlite3
import sys
import json

db_path = r"\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# Check if on_demand_jobs table exists
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", [t["name"] for t in tables])

rows = conn.execute('''
    SELECT job_id, symbol, status, created_at, started_at, completed_at, 
           current_step, current_step_index, total_steps, percent, error
    FROM on_demand_jobs
    ORDER BY created_at DESC
    LIMIT 15
''').fetchall()

print(f'\nFound {len(rows)} recent jobs:')
for r in rows:
    print(f'  {r["job_id"]}')
    print(f'    symbol={r["symbol"]} status={r["status"]}')
    print(f'    created={r["created_at"]} started={r["started_at"]} completed={r["completed_at"]}')
    print(f'    step={r["current_step"]} ({r["current_step_index"]}/{r["total_steps"]}) {r["percent"]}%')
    if r['error']:
        print(f'    ERROR: {r["error"]}')
    print()

# Find the most recent COMPLETED job for SNOW
snow_complete = conn.execute('''
    SELECT job_id, result_json FROM on_demand_jobs
    WHERE symbol = 'SNOW' AND status = 'complete'
    ORDER BY completed_at DESC LIMIT 1
''').fetchone()

if snow_complete:
    print(f'=== Successful SNOW job_id: {snow_complete["job_id"]} ===')
    rj = snow_complete["result_json"]
    print(f'result_json length: {len(rj) if rj else 0} bytes')
else:
    print('No completed SNOW runs found')

# Find the most recent FAILED job for SNOW
snow_failed = conn.execute('''
    SELECT job_id, error, created_at, completed_at FROM on_demand_jobs
    WHERE symbol = 'SNOW' AND status = 'failed'
    ORDER BY created_at DESC LIMIT 1
''').fetchone()

if snow_failed:
    print(f'\n=== Failed SNOW job_id: {snow_failed["job_id"]} ===')
    print(f'    created={snow_failed["created_at"]} completed={snow_failed["completed_at"]}')
    print(f'    ERROR: {snow_failed["error"]}')
else:
    print('\nNo failed SNOW runs found')

conn.close()

import sqlite3
import json

db_path = r"\\192.168.1.149\CompanyEvaluatorData\company_evaluator\db\company_eval.db"
job_id = "ondemand_2026-04-13T02:13:52_SNOW_775e"
output_path = "_snow_result_dump.json"

conn = sqlite3.connect(db_path)
row = conn.execute('SELECT result_json FROM on_demand_jobs WHERE job_id = ?', (job_id,)).fetchone()
conn.close()

if not row or not row[0]:
    print('No result found')
    exit(1)

data = json.loads(row[0])
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)

print(f'Wrote {len(row[0])} bytes to {output_path}')
print(f'Top-level keys: {list(data.keys())}')
for k, v in data.items():
    if v is None:
        print(f'  {k}: None')
    elif isinstance(v, dict):
        print(f'  {k}: dict with keys {list(v.keys())}')
    elif isinstance(v, list):
        print(f'  {k}: list[{len(v)}]')
    elif isinstance(v, str):
        print(f'  {k}: str ({len(v)} chars)')
    else:
        print(f'  {k}: {type(v).__name__} = {v}')

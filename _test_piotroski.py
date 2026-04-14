"""Quick test of Piotroski F-Score using saved raw data from DB."""
import asyncio, json, logging
logging.disable(logging.CRITICAL)

async def test():
    from db.database import init_db, get_session, CompanyEvaluation
    from config import get_settings
    from sqlalchemy import select
    from analysis.piotroski import compute_piotroski_score
    from metrics.helpers import get_statements

    settings = get_settings()
    await init_db(settings.database_url)

    for sym in ['MSFT', 'AAPL', 'ERIE', 'SBUX', 'SNOW']:
        async with get_session() as session:
            result = await session.execute(
                select(CompanyEvaluation).where(CompanyEvaluation.symbol == sym)
            )
            row = result.scalar_one_or_none()
            if not row:
                print(f"{sym}: NOT FOUND in DB")
                continue

            raw = row.raw_financials
            if isinstance(raw, str):
                raw = json.loads(raw)
            cd = raw.get("company_data", raw)
            annual = get_statements(cd, "annual")

            p = compute_piotroski_score(annual)
            if p.get("ok"):
                print(f"{sym}: F-Score {p['score']}/9 ({p['label']})")
                for name, check in p['checks'].items():
                    status = "PASS" if check['passed'] else "FAIL"
                    print(f"  [{status}] {check['label']}: {check['details']}")
                print(f"  Interpretation: {p['interpretation']}")
            else:
                print(f"{sym}: {p.get('error')}")
            print()

asyncio.run(test())

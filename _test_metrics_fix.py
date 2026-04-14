"""Quick test of metric fixes using saved raw data from DB."""
import asyncio, json, sys, logging
logging.disable(logging.CRITICAL)

async def test():
    from db.database import init_db, get_session, CompanyEvaluation
    from config import get_settings
    from sqlalchemy import select
    settings = get_settings()
    await init_db(settings.database_url)

    for sym in ['ERIE', 'SBUX', 'MSFT', 'AAPL']:
        async with get_session() as session:
            result = await session.execute(
                select(CompanyEvaluation).where(CompanyEvaluation.symbol == sym)
            )
            row = result.scalar_one_or_none()
            if not row:
                print(f"{sym}: NOT FOUND")
                continue
            raw = row.raw_financials
            if isinstance(raw, str):
                raw = json.loads(raw)
            cd = raw.get("company_data", raw)

            from metrics.business_quality import compute as bq_compute
            from metrics.growth_quality import compute as gq_compute
            from metrics.capital_allocation import compute as ca_compute

            bq = bq_compute(cd)
            bq_m = bq.get("metrics", {})
            gq = gq_compute(cd)
            gq_m = gq.get("metrics", {})
            gq_r = gq.get("raw_metrics", {})
            ca = ca_compute(cd)
            ca_m = ca.get("metrics", {})

            print(f"{sym}:")
            print(f"  BQ={bq['pillar_score']:.1f}  gm={bq_m.get('gross_margin')}  om={bq_m.get('op_margin')}  nm={bq_m.get('net_margin')}")
            print(f"  GQ={gq['pillar_score']:.1f}  eps={gq_m.get('eps_growth_yoy')} (raw={gq_r.get('eps_growth_yoy')})")
            print(f"  CA={ca['pillar_score']:.1f}  share={ca_m.get('share_trend')}")
            flags = gq.get("data_quality_flags", [])
            if flags:
                print(f"  GQ flags: {flags}")

asyncio.run(test())

"""Capture the real expansion traceback by initialising the DB first."""
import asyncio
import traceback
import sys
import os

async def main():
    # Replicate FastAPI startup: init_db so _session_factory is set
    sys.path.insert(0, os.path.dirname(__file__))
    from config import get_settings
    from db.database import init_db

    settings = get_settings()
    await init_db(settings.database_url)
    print(f"DB initialised: {settings.database_url}")

    try:
        from data.universe_expansion import expand_universe
        result = await expand_universe(dry_run=True)
        print("SUCCESS")
        import json
        print(json.dumps(result, indent=2, default=str))
    except Exception:
        print("FAILED")
        traceback.print_exc()
        sys.exit(1)

asyncio.run(main())

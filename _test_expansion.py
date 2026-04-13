import asyncio
import traceback
import sys

async def main():
    try:
        from data.universe_expansion import expand_universe
        result = await expand_universe(dry_run=True)
        print("SUCCESS")
        print(result)
    except Exception as e:
        print("FAILED")
        traceback.print_exc()
        sys.exit(1)

asyncio.run(main())

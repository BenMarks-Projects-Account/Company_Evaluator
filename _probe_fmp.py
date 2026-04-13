import asyncio
import os
import sys

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.fmp_client import FMPClient

async def main():
    # Load the API key the same way the service does
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("FMP_API_KEY")
    
    if not api_key:
        print("ERROR: FMP_API_KEY not set")
        return
    
    print(f"API key: {api_key[:8]}...")
    
    client = FMPClient(api_key=api_key)
    print(f"Base URL: {client._base_url}")
    
    # Test 1: Plain breakout zone
    print("\n=== Test 1: Breakout zone 500M-2B ===")
    result = await client.stock_screener(
        market_cap_min=500_000_000,
        market_cap_max=2_000_000_000,
        price_min=5.0,
        volume_min=200_000,
        country="US",
        exchange="nyse,nasdaq",
        is_actively_trading=True,
        limit=1000,
    )
    print(f"Count: {len(result) if result else 0}")
    
    # Test 2: Technology sector
    print("\n=== Test 2: Technology sector 500M-20B ===")
    result = await client.stock_screener(
        sector="Technology",
        market_cap_min=500_000_000,
        market_cap_max=20_000_000_000,
        price_min=5.0,
        volume_min=200_000,
        country="US",
        exchange="nyse,nasdaq",
        is_actively_trading=True,
        limit=500,
    )
    print(f"Count: {len(result) if result else 0}")
    if result:
        sectors = set(c.get("sector") for c in result[:20])
        print(f"First 20 sectors: {sectors}")

asyncio.run(main())

import logging

from fastapi import APIRouter, HTTPException, Query

from analysis.chart_service import get_chart_data, TIMEFRAMES

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/charts/{symbol}")
async def get_chart(
    symbol: str,
    timeframe: str = Query("1Y", description="Chart timeframe"),
):
    """Return price history + SMAs + key levels for a symbol."""
    try:
        return await get_chart_data(symbol, timeframe)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        msg = str(e)
        if "No price data" in msg:
            raise HTTPException(404, msg)
        raise HTTPException(503, msg)
    except Exception as e:
        logger.exception(f"Chart fetch failed for {symbol} {timeframe}")
        raise HTTPException(500, f"Internal error: {e}")

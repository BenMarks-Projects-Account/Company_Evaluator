import logging
from fastapi import APIRouter, HTTPException, Query

from analysis.search_service import search_symbols

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/search/symbols")
async def search_symbols_endpoint(
    query: str = Query(..., description="Search query (company name or partial ticker)"),
    limit: int = Query(10, ge=1, le=20, description="Max results"),
):
    """Search for stock symbols by company name or partial ticker."""
    try:
        return await search_symbols(query, limit)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    except Exception as e:
        logger.exception("Symbol search failed for query=%r", query)
        raise HTTPException(500, f"Internal error: {e}")

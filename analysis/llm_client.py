"""LM Studio client for company analysis."""

import logging
import httpx
from config import get_settings

_log = logging.getLogger(__name__)


async def call_llm(system_prompt: str, user_prompt: str) -> str | None:
    """Call LM Studio for analysis. Returns raw response text or None on failure."""
    settings = get_settings()

    payload = {
        "model": "local-model",  # LM Studio ignores this, uses loaded model
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": settings.llm_temperature,
        "max_tokens": 2000,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.llm_timeout) as client:
            resp = await client.post(settings.llm_endpoint, json=payload)

            if resp.status_code != 200:
                _log.error("event=llm_error status=%d body=%s", resp.status_code, resp.text[:200])
                return None

            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content.strip() if content else None
    except httpx.ConnectError:
        _log.warning("event=llm_unavailable endpoint=%s", settings.llm_endpoint)
        return None
    except Exception as exc:
        _log.error("event=llm_call_failed error=%s", exc)
        return None

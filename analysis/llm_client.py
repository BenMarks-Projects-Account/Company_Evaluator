"""LM Studio client for company analysis."""

import logging
import httpx
from config import get_settings

_log = logging.getLogger(__name__)

# Cached model name — resolved once per process
_resolved_model: str | None = None


async def _resolve_model(settings) -> str:
    """Return the model name to use.

    If ``settings.llm_model`` is set, use it directly.  Otherwise query
    the LM Studio ``/v1/models`` endpoint and pick the first chat model
    (skip embedding models).  The result is cached for the process lifetime.
    """
    global _resolved_model
    if _resolved_model:
        return _resolved_model

    if settings.llm_model:
        _resolved_model = settings.llm_model
        _log.info("event=llm_model_configured model=%s", _resolved_model)
        return _resolved_model

    # Auto-detect from LM Studio
    base = settings.llm_endpoint.rsplit("/v1/", 1)[0] + "/v1/models"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(base)
            if resp.status_code == 200:
                models = resp.json().get("data", [])
                for m in models:
                    mid = m.get("id", "")
                    # Skip embedding / vision models
                    if "embed" in mid.lower():
                        continue
                    if "vision" in mid.lower() or "glm-4" in mid.lower():
                        continue
                    _resolved_model = mid
                    _log.info("event=llm_model_auto model=%s", _resolved_model)
                    return _resolved_model
    except Exception as exc:
        _log.warning("event=llm_model_detect_failed error=%s", exc)

    _resolved_model = "local-model"
    _log.warning("event=llm_model_fallback model=local-model")
    return _resolved_model


async def call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> str | None:
    """Call LM Studio for analysis. Returns raw response text or None on failure."""
    settings = get_settings()
    model = await _resolve_model(settings)

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": settings.llm_temperature,
        "max_tokens": max_tokens,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.llm_timeout) as client:
            resp = await client.post(settings.llm_endpoint, json=payload)

            if resp.status_code != 200:
                _log.error("event=llm_error status=%d model=%s body=%s", resp.status_code, model, resp.text[:300])
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

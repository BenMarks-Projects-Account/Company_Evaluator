"""LM Studio client — thin wrapper over :mod:`analysis.llm_router`.

Historical ``call_llm(system_prompt, user_prompt, max_tokens)`` signature is
preserved so existing call sites keep working. All routing / endpoint
selection / health tracking lives in the router.
"""

import logging

from analysis.llm_router import get_router

_log = logging.getLogger(__name__)


async def _resolve_model(settings=None) -> str:  # backwards-compat shim
    return await get_router().resolve_model()


async def call_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
) -> str | None:
    """Call LM Studio for analysis. Returns raw response text or None on failure."""
    return await get_router().call_llm(system_prompt, user_prompt, max_tokens)

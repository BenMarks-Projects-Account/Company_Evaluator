"""LLM Router — distributes inference calls across multiple LM Studio endpoints.

Design:
- Each endpoint owns an asyncio.Semaphore(1). LM Studio processes one chat
  completion at a time per instance; running more than one concurrent call
  per endpoint just queues inside LM Studio with worse latency.
- The router picks the least-busy healthy endpoint (tiebreaker: lower
  ``priority`` number = preferred, faster machine first).
- On failure, the router retries on the next endpoint so a flaky remote
  never blocks a symbol from completing.
- After ``CONSECUTIVE_ERROR_THRESHOLD`` consecutive failures an endpoint is
  marked unhealthy and skipped for ``HEALTH_CHECK_INTERVAL`` minutes.
  ``health_check()`` rechecks unhealthy endpoints and auto-reinstates them.

The public surface mirrors the pre-existing ``analysis.llm_client.call_llm``
contract: a raw ``str | None`` response. All existing call sites keep working
unchanged.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import httpx

_log = logging.getLogger(__name__)


@dataclass
class LLMEndpoint:
    name: str
    base_url: str  # e.g. "http://localhost:1234" (no trailing slash, no /v1)
    priority: int = 0  # lower = preferred on tiebreak

    healthy: bool = True
    in_flight: int = 0
    total_calls: int = 0
    total_errors: int = 0
    total_latency: float = 0.0
    last_error: str | None = None
    last_error_at: datetime | None = None
    last_success_at: datetime | None = None
    last_health_check: datetime | None = None

    semaphore: asyncio.Semaphore = field(default=None, repr=False)  # type: ignore[assignment]

    def __post_init__(self):
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(1)

    @property
    def avg_latency(self) -> float:
        return self.total_latency / self.total_calls if self.total_calls else 0.0

    @property
    def error_rate(self) -> float:
        return self.total_errors / self.total_calls if self.total_calls else 0.0

    @property
    def completions_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/v1/chat/completions"

    @property
    def models_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/v1/models"


class LLMRouter:
    """Routes chat-completion calls across multiple LM Studio endpoints."""

    HEALTH_CHECK_INTERVAL = timedelta(minutes=5)
    CONSECUTIVE_ERROR_THRESHOLD = 3

    def __init__(self, settings):
        self.settings = settings
        self.endpoints: list[LLMEndpoint] = []
        self._consecutive_errors: dict[str, int] = {}
        self._resolved_model: str | None = None
        self._model_lock = asyncio.Lock()
        self._setup_endpoints()

    def _setup_endpoints(self):
        routing_enabled = getattr(self.settings, "llm_routing_enabled", True)
        local_url = getattr(self.settings, "llm_local_url", "http://localhost:1234")
        remote_url = getattr(
            self.settings, "llm_model_machine_url", "http://192.168.1.89:1234"
        )

        if routing_enabled and remote_url and remote_url != local_url:
            # Remote is the faster machine → higher priority (lower number).
            self.endpoints.append(
                LLMEndpoint(name="model_machine", base_url=remote_url, priority=0)
            )

        self.endpoints.append(LLMEndpoint(name="local", base_url=local_url, priority=1))

        _log.info(
            "event=llm_router_init endpoints=%d %s",
            len(self.endpoints),
            ", ".join(f"{e.name}({e.base_url})" for e in self.endpoints),
        )

    def _select_endpoint(self, exclude: set[str]) -> LLMEndpoint | None:
        candidates = [e for e in self.endpoints if e.healthy and e.name not in exclude]
        if not candidates:
            # Last-resort: try local even if flagged unhealthy (LM Studio
            # might have just come back). Skip if already tried this call.
            fallback = [
                e for e in self.endpoints if e.name == "local" and e.name not in exclude
            ]
            if fallback:
                _log.warning("event=llm_router_fallback reason=all_unhealthy endpoint=local")
                return fallback[0]
            return None
        candidates.sort(key=lambda e: (e.in_flight, e.priority))
        return candidates[0]

    async def resolve_model(self) -> str:
        """Return model name to use. Uses settings.llm_model if set, else auto-detect."""
        if self._resolved_model:
            return self._resolved_model
        async with self._model_lock:
            if self._resolved_model:
                return self._resolved_model
            if getattr(self.settings, "llm_model", ""):
                self._resolved_model = self.settings.llm_model
                _log.info("event=llm_model_configured model=%s", self._resolved_model)
                return self._resolved_model
            # Auto-detect from the first healthy endpoint.
            for ep in self.endpoints:
                if not ep.healthy:
                    continue
                try:
                    async with httpx.AsyncClient(timeout=10) as client:
                        resp = await client.get(ep.models_url)
                        if resp.status_code == 200:
                            for m in resp.json().get("data", []):
                                mid = m.get("id", "")
                                low = mid.lower()
                                if "embed" in low or "vision" in low or "glm-4" in low:
                                    continue
                                self._resolved_model = mid
                                _log.info(
                                    "event=llm_model_auto model=%s via=%s",
                                    mid,
                                    ep.name,
                                )
                                return self._resolved_model
                except Exception as exc:
                    _log.warning(
                        "event=llm_model_detect_failed endpoint=%s error=%s",
                        ep.name,
                        exc,
                    )
            self._resolved_model = "local-model"
            _log.warning("event=llm_model_fallback model=local-model")
            return self._resolved_model

    async def call_llm(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
    ) -> str | None:
        """Route a chat-completion call. Returns response text or None."""
        model = await self.resolve_model()
        timeout = getattr(self.settings, "llm_timeout", 120)
        temperature = getattr(self.settings, "llm_temperature", 0.0)

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        tried: set[str] = set()
        last_exc: Exception | None = None

        for _ in range(len(self.endpoints) + 1):  # +1 allows local-fallback retry
            ep = self._select_endpoint(exclude=tried)
            if ep is None:
                break
            tried.add(ep.name)
            try:
                result = await self._call_endpoint(ep, payload, timeout)
                if result is not None:
                    return result
            except Exception as exc:
                last_exc = exc
                _log.warning(
                    "event=llm_call_failed endpoint=%s error=%s — trying next",
                    ep.name,
                    exc,
                )

        _log.error(
            "event=llm_all_endpoints_failed attempts=%d last_error=%s",
            len(tried),
            last_exc,
        )
        return None

    async def _call_endpoint(
        self,
        ep: LLMEndpoint,
        payload: dict[str, Any],
        timeout: int,
    ) -> str | None:
        async with ep.semaphore:
            ep.in_flight += 1
            start = time.time()
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(ep.completions_url, json=payload)

                elapsed = time.time() - start
                ep.total_calls += 1
                ep.total_latency += elapsed

                if resp.status_code != 200:
                    ep.total_errors += 1
                    ep.last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    ep.last_error_at = datetime.now()
                    self._bump_error(ep)
                    _log.error(
                        "event=llm_http_error endpoint=%s status=%d body=%s",
                        ep.name,
                        resp.status_code,
                        resp.text[:300],
                    )
                    return None

                data = resp.json()
                content = (
                    data.get("choices", [{}])[0].get("message", {}).get("content", "")
                )
                ep.last_success_at = datetime.now()
                self._consecutive_errors[ep.name] = 0
                _log.info(
                    "event=llm_call_ok endpoint=%s elapsed=%.1fs chars=%d in_flight=%d",
                    ep.name,
                    elapsed,
                    len(content or ""),
                    ep.in_flight - 1,
                )
                return content.strip() if content else None

            except httpx.ConnectError as exc:
                ep.total_calls += 1
                ep.total_errors += 1
                ep.last_error = f"ConnectError: {exc}"
                ep.last_error_at = datetime.now()
                self._bump_error(ep)
                _log.warning(
                    "event=llm_connect_error endpoint=%s url=%s",
                    ep.name,
                    ep.base_url,
                )
                raise
            except Exception as exc:
                ep.total_calls += 1
                ep.total_errors += 1
                ep.last_error = str(exc)
                ep.last_error_at = datetime.now()
                self._bump_error(ep)
                raise
            finally:
                ep.in_flight -= 1

    def _bump_error(self, ep: LLMEndpoint):
        self._consecutive_errors[ep.name] = self._consecutive_errors.get(ep.name, 0) + 1
        if self._consecutive_errors[ep.name] >= self.CONSECUTIVE_ERROR_THRESHOLD:
            if ep.healthy:
                _log.error(
                    "event=llm_endpoint_unhealthy endpoint=%s consecutive_errors=%d "
                    "retry_after=%s",
                    ep.name,
                    self._consecutive_errors[ep.name],
                    self.HEALTH_CHECK_INTERVAL,
                )
            ep.healthy = False

    async def health_check(self):
        """Recheck unhealthy endpoints; re-enable if /v1/models responds."""
        now = datetime.now()
        for ep in self.endpoints:
            if ep.healthy:
                continue
            if (
                ep.last_health_check is not None
                and now - ep.last_health_check < self.HEALTH_CHECK_INTERVAL
            ):
                continue
            ep.last_health_check = now
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.get(ep.models_url)
                if resp.status_code == 200 and resp.json().get("data"):
                    ep.healthy = True
                    self._consecutive_errors[ep.name] = 0
                    _log.info(
                        "event=llm_endpoint_recovered endpoint=%s", ep.name
                    )
            except Exception as exc:
                _log.debug(
                    "event=llm_health_check_failed endpoint=%s error=%s",
                    ep.name,
                    exc,
                )

    def get_stats(self) -> dict[str, Any]:
        return {
            "endpoints": [
                {
                    "name": e.name,
                    "base_url": e.base_url,
                    "healthy": e.healthy,
                    "in_flight": e.in_flight,
                    "total_calls": e.total_calls,
                    "total_errors": e.total_errors,
                    "error_rate": round(e.error_rate, 3),
                    "avg_latency_s": round(e.avg_latency, 2),
                    "last_error": e.last_error,
                }
                for e in self.endpoints
            ],
            "total_calls": sum(e.total_calls for e in self.endpoints),
            "healthy_endpoints": sum(1 for e in self.endpoints if e.healthy),
        }


# Process-wide singleton (lazy).
_router: LLMRouter | None = None


def get_router() -> LLMRouter:
    global _router
    if _router is None:
        from config import get_settings

        _router = LLMRouter(get_settings())
    return _router

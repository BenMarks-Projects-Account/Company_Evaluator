"""Overnight batch crawler — evaluates all companies in the universe."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from data.universe import get_universe
from pipeline.evaluator import evaluate_company
from config import get_settings

_log = logging.getLogger(__name__)


class Crawler:
    def __init__(self):
        self._running = False
        self._progress = {}
        self._current_symbol = None
        self._start_time = None

    @property
    def status(self):
        elapsed = time.time() - self._start_time if self._start_time and self._running else 0
        return {
            "running": self._running,
            "current_symbol": self._current_symbol,
            "progress": self._progress,
            "elapsed_seconds": round(elapsed, 1),
        }

    async def run(self, symbols: list = None):
        if self._running:
            return {"error": "Crawler already running"}

        self._running = True
        self._start_time = time.time()
        settings = get_settings()

        if symbols is None:
            symbols = get_universe(settings.universe)

        total = len(symbols)
        completed = 0
        failed = 0
        self._progress = {"total": total, "completed": 0, "failed": 0, "remaining": total}

        _log.info("event=crawler_start total_symbols=%d", total)

        for i, symbol in enumerate(symbols):
            if not self._running:
                _log.info("event=crawler_stopped_early completed=%d", completed)
                break

            self._current_symbol = symbol

            try:
                result = await evaluate_company(symbol)

                if result.get("status") == "complete":
                    completed += 1
                    _log.info(
                        "event=crawler_symbol_complete symbol=%s score=%s (%d/%d)",
                        symbol, result.get("composite_score"), i + 1, total,
                    )
                else:
                    failed += 1
                    _log.warning(
                        "event=crawler_symbol_failed symbol=%s status=%s",
                        symbol, result.get("status"),
                    )
            except Exception as exc:
                failed += 1
                _log.error("event=crawler_symbol_error symbol=%s error=%s", symbol, exc)

            self._progress = {
                "total": total,
                "completed": completed,
                "failed": failed,
                "remaining": total - (completed + failed),
                "pct": round(((completed + failed) / total) * 100, 1),
            }

            # Brief pause between companies to avoid rate limit pressure
            await asyncio.sleep(1)

        elapsed = time.time() - self._start_time
        _log.info(
            "event=crawler_complete completed=%d failed=%d elapsed_min=%.1f",
            completed, failed, elapsed / 60,
        )

        self._running = False
        self._current_symbol = None

        return {
            "status": "complete",
            "completed": completed,
            "failed": failed,
            "elapsed_minutes": round(elapsed / 60, 1),
        }

    def stop(self):
        """Signal the crawler to stop after the current symbol."""
        if self._running:
            _log.info("event=crawler_stop_requested")
            self._running = False


_crawler = None


def get_crawler():
    global _crawler
    if _crawler is None:
        _crawler = Crawler()
    return _crawler

_crawler = None

def get_crawler():
    global _crawler
    if _crawler is None:
        _crawler = Crawler()
    return _crawler

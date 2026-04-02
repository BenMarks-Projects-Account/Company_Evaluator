"""Continuous crawler — evaluates all companies in the universe, oldest-first, no skipping."""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from pipeline.evaluator import evaluate_company
from config import get_settings

_log = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent.parent / "db" / "crawler_state.json"


def _load_state() -> dict | None:
    """Load persistent crawler state from disk."""
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        _log.warning("Could not load crawler state: %s", exc)
        return None


def _save_state(state: dict):
    """Persist crawler state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


async def _get_ordered_symbols() -> list[str]:
    """Get all active symbols ordered by staleness (never-evaluated first, oldest next)."""
    from db.database import get_session, UniverseSymbol, CompanyEvaluation
    from sqlalchemy import select, outerjoin, case

    async with get_session() as session:
        # Left join universe_symbols → company_evaluations to get evaluated_at
        j = outerjoin(
            UniverseSymbol, CompanyEvaluation,
            UniverseSymbol.symbol == CompanyEvaluation.symbol,
        )
        stmt = (
            select(UniverseSymbol.symbol, CompanyEvaluation.evaluated_at)
            .select_from(j)
            .where(UniverseSymbol.active == True)
            .order_by(
                # Never-evaluated first (NULL → 0, else 1)
                case(
                    (CompanyEvaluation.evaluated_at == None, 0),
                    else_=1,
                ),
                # Then oldest evaluated_at first
                CompanyEvaluation.evaluated_at.asc(),
                # Tie-break by priority desc, then symbol
                UniverseSymbol.priority.desc(),
                UniverseSymbol.symbol.asc(),
            )
        )
        rows = (await session.execute(stmt)).all()
        return [row[0] for row in rows]


class Crawler:
    MAX_RECENT = 20

    def __init__(self):
        self._running = False
        self._paused = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        self._progress = {}
        self._current_symbol = None
        self._start_time = None
        self._cycle_number = 0
        self._cycle_started_at = None
        self._last_cycle_completed_at = None
        self._recent_activity: list[dict] = []
        self._last_error: dict | None = None
        self._eval_times: list[float] = []

    def _record_activity(self, symbol: str, status: str, score=None, recommendation=None, error=None):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "score": score,
            "recommendation": recommendation,
            "status": status,
        }
        if error:
            entry["error"] = str(error)[:200]
            self._last_error = entry
        self._recent_activity.insert(0, entry)
        self._recent_activity = self._recent_activity[:self.MAX_RECENT]

    @property
    def status(self):
        elapsed = time.time() - self._start_time if self._start_time and self._running else 0
        progress = dict(self._progress)
        total = progress.get("total", 0)
        evaluated = progress.get("evaluated", 0)
        failed = progress.get("failed", 0)
        current_index = progress.get("current_index", 0)
        remaining = progress.get("remaining", 0)

        if not self._running:
            desc_status = "idle"
        elif self._paused:
            desc_status = "paused"
        elif self._current_symbol:
            desc_status = "evaluating"
        else:
            desc_status = "running"

        avg_sec = round(sum(self._eval_times) / len(self._eval_times), 1) if self._eval_times else None
        eta_sec = round(remaining * avg_sec) if avg_sec and remaining else None

        return {
            "running": self._running,
            "paused": self._paused,
            "status": desc_status,
            "current_symbol": self._current_symbol,
            "progress": progress,
            "elapsed_seconds": round(elapsed, 1),
            "cycle_number": self._cycle_number,
            "cycle_started_at": self._cycle_started_at,
            "last_cycle_completed_at": self._last_cycle_completed_at,
            "avg_seconds_per_symbol": avg_sec,
            "eta_seconds": eta_sec,
            "recent_activity": self._recent_activity[:10],
            "last_error": self._last_error,
        }

    def _build_state(self, symbols, index, status, cycle, evaluated, failed):
        """Build a state dict for saving."""
        return {
            "symbols": symbols,
            "last_completed_index": index,
            "last_completed_symbol": symbols[index] if 0 <= index < len(symbols) else None,
            "status": status,
            "cycle_number": cycle,
            "cycle_started_at": self._cycle_started_at,
            "symbols_evaluated": evaluated,
            "symbols_failed": failed,
        }

    async def run(self, symbols: list = None, start_index: int = 0, cycle_number: int = 1):
        """Run the crawler continuously — evaluate everything, oldest-first, no skipping."""
        if self._running:
            return {"error": "Crawler already running"}

        self._running = True
        self._paused = False
        self._resume_event.set()
        self._start_time = time.time()
        settings = get_settings()
        pause_sec = settings.pause_between_symbols_sec

        # Load symbols from DB (ordered by staleness) if not provided
        if symbols is None:
            try:
                symbols = await _get_ordered_symbols()
                _log.info("Crawler loaded %d symbols from DB (oldest-first)", len(symbols))
            except Exception as exc:
                _log.warning("Failed to load universe from DB: %s — falling back to hardcoded list", exc)
                from data.universe import get_universe
                symbols = get_universe(settings.universe)

        if not symbols:
            _log.warning("No symbols to evaluate — crawler stopping")
            self._running = False
            return {"error": "No symbols in universe"}

        current_cycle = cycle_number
        current_start = start_index

        # ── Outer loop: one iteration per cycle ──────────────────
        while self._running:
            total = len(symbols)
            evaluated = 0
            failed = 0

            self._cycle_number = current_cycle
            self._cycle_started_at = datetime.now(timezone.utc).isoformat()

            self._progress = {
                "total": total,
                "evaluated": 0,
                "failed": 0,
                "current_index": current_start,
                "remaining": total - current_start,
                "pct": round((current_start / total) * 100, 1) if total else 0,
            }

            if current_start > 0:
                _log.info("=" * 60)
                _log.info(
                    "CRAWLER RESUME: Cycle %d, index %d/%d — %d remaining",
                    current_cycle, current_start, total, total - current_start,
                )
                _log.info("=" * 60)
            else:
                _log.info("=" * 60)
                _log.info(
                    "CRAWLER START: Cycle %d — %d symbols",
                    current_cycle, total,
                )
                _log.info("=" * 60)

            _save_state(self._build_state(
                symbols, current_start - 1, "running",
                current_cycle, 0, 0,
            ))

            # ── Inner loop: one symbol at a time ─────────────────
            for i in range(current_start, total):
                if not self._running:
                    _log.info("CRAWLER STOPPED at %d/%d (user requested)", i, total)
                    _save_state(self._build_state(
                        symbols, i - 1, "stopped",
                        current_cycle, evaluated, failed,
                    ))
                    break

                if self._paused:
                    _log.info("CRAWLER PAUSED at index %d (%s)", i, symbols[i])
                    _save_state(self._build_state(
                        symbols, i - 1, "paused",
                        current_cycle, evaluated, failed,
                    ))
                    await self._resume_event.wait()
                    if not self._running:
                        break
                    _log.info("CRAWLER RESUMED at index %d (%s)", i, symbols[i])

                symbol = symbols[i]
                self._current_symbol = symbol

                _log.info("-" * 40)
                _log.info("CRAWLER [%d/%d]: Starting %s", i + 1, total, symbol)

                eval_start = time.time()
                try:
                    result = await evaluate_company(symbol)

                    if result.get("status") == "complete":
                        evaluated += 1
                        eval_sec = time.time() - eval_start
                        self._eval_times.append(eval_sec)
                        if len(self._eval_times) > 50:
                            self._eval_times = self._eval_times[-50:]
                        score = result.get("composite_score") or 0
                        rec = result.get("llm_recommendation")
                        _log.info(
                            "CRAWLER [%d/%d]: %s COMPLETE — score=%.1f quality=%s (%.0fs)",
                            i + 1, total, symbol, score,
                            result.get("data_quality", "?"), eval_sec,
                        )
                        self._record_activity(symbol, "success", score=score, recommendation=rec)
                    else:
                        failed += 1
                        err_msg = result.get("error", f"status={result.get('status')}")
                        _log.warning(
                            "CRAWLER [%d/%d]: %s FAILED — status=%s",
                            i + 1, total, symbol, result.get("status"),
                        )
                        self._record_activity(symbol, "error", error=err_msg)
                except Exception as exc:
                    failed += 1
                    _log.error(
                        "CRAWLER [%d/%d]: %s EXCEPTION — %s",
                        i + 1, total, symbol, exc, exc_info=True,
                    )
                    self._record_activity(symbol, "error", error=str(exc))

                self._progress = {
                    "total": total,
                    "evaluated": evaluated,
                    "failed": failed,
                    "current_index": i + 1,
                    "remaining": total - i - 1,
                    "pct": round(((i + 1) / total) * 100, 1),
                }
                _log.info(
                    "CRAWLER PROGRESS: Cycle %d — %.1f%% — %d evaluated, %d failed, %d remaining",
                    current_cycle, self._progress["pct"],
                    evaluated, failed, self._progress["remaining"],
                )

                _save_state(self._build_state(
                    symbols, i, "running",
                    current_cycle, evaluated, failed,
                ))

                await asyncio.sleep(pause_sec)

            # ── End of inner loop ────────────────────────────────
            if not self._running:
                break

            # Cycle complete
            _log.info("=" * 60)
            _log.info(
                "CYCLE %d COMPLETE: %d symbols, %d evaluated, %d failed",
                current_cycle, total, evaluated, failed,
            )
            _log.info("=" * 60)

            self._last_cycle_completed_at = datetime.now(timezone.utc).isoformat()

            # Prepare next cycle — reload from DB with fresh ordering
            current_cycle += 1
            current_start = 0

            try:
                new_symbols = await _get_ordered_symbols()
                if new_symbols:
                    symbols = new_symbols
                    _log.info("Reloaded universe for cycle %d: %d symbols (oldest-first)", current_cycle, len(symbols))
                else:
                    _log.warning("No active symbols in DB for cycle %d — reusing previous list", current_cycle)
            except Exception as exc:
                _log.warning("Could not reload universe from DB: %s — reusing previous list", exc)

            _save_state(self._build_state(
                symbols, -1, "running",
                current_cycle, 0, 0,
            ))

            # Brief pause between cycles (5s) to let DB settle
            await asyncio.sleep(5)

        # ── Crawler stopped ──────────────────────────────────────
        self._running = False
        self._paused = False
        self._current_symbol = None

        elapsed_total = time.time() - self._start_time
        return {
            "status": "stopped",
            "cycle_number": current_cycle,
            "elapsed_minutes": round(elapsed_total / 60, 1),
        }

    def stop(self):
        """Signal the crawler to stop after the current symbol."""
        if self._running:
            _log.info("event=crawler_stop_requested")
            self._running = False
            # Unblock if paused so the loop can exit
            self._resume_event.set()

    def pause(self):
        """Pause the crawler after the current symbol finishes."""
        if self._running and not self._paused:
            _log.info("event=crawler_pause_requested")
            self._paused = True
            self._resume_event.clear()
            return True
        return False

    def resume(self):
        """Resume a paused crawler."""
        if self._paused:
            _log.info("event=crawler_resume_requested")
            self._paused = False
            self._resume_event.set()
            return True
        return False


_crawler = None


def get_crawler():
    global _crawler
    if _crawler is None:
        _crawler = Crawler()
    return _crawler

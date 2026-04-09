"""Market-hours scheduler for crawler auto start/stop behavior."""

import asyncio
import logging
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

from pipeline.crawler import get_crawler, _load_state

_log = logging.getLogger(__name__)

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dt_time(hour=9, minute=30)
MARKET_CLOSE = dt_time(hour=16, minute=0)
POLL_INTERVAL_SECONDS = 60

_scheduler = None


def _now_et() -> datetime:
    return datetime.now(MARKET_TZ)


def is_market_hours(now: datetime | None = None) -> bool:
    """Check if current time is during US market trading hours."""
    now_et = now.astimezone(MARKET_TZ) if now else _now_et()

    if now_et.weekday() >= 5:
        return False

    current_time = now_et.timetz().replace(tzinfo=None)
    return MARKET_OPEN <= current_time < MARKET_CLOSE


def is_crawler_scheduled(now: datetime | None = None) -> bool:
    """Crawler should run when the market is closed."""
    return not is_market_hours(now)


def get_next_market_transition(now: datetime | None = None) -> datetime:
    """Return the next ET timestamp when market state changes."""
    now_et = now.astimezone(MARKET_TZ) if now else _now_et()
    current_date = now_et.date()

    if now_et.weekday() >= 5:
        days_until_monday = 7 - now_et.weekday()
        next_date = current_date + timedelta(days=days_until_monday)
        return datetime.combine(next_date, MARKET_OPEN, tzinfo=MARKET_TZ)

    market_open_dt = datetime.combine(current_date, MARKET_OPEN, tzinfo=MARKET_TZ)
    market_close_dt = datetime.combine(current_date, MARKET_CLOSE, tzinfo=MARKET_TZ)

    if now_et < market_open_dt:
        return market_open_dt
    if now_et < market_close_dt:
        return market_close_dt

    next_date = current_date + timedelta(days=1)
    while next_date.weekday() >= 5:
        next_date += timedelta(days=1)
    return datetime.combine(next_date, MARKET_OPEN, tzinfo=MARKET_TZ)


class CrawlerScheduler:
    """Monitors market hours and auto-starts/stops the crawler."""

    def __init__(self, crawler=None):
        self.crawler = crawler or get_crawler()
        self._manual_override = False
        self._last_market_state: bool | None = None
        self._running = False
        self._monitor_task: asyncio.Task | None = None

    @property
    def status(self) -> dict:
        market_open = is_market_hours()
        next_transition = get_next_market_transition()
        return {
            "enabled": self._running,
            "mode": "manual" if self._manual_override else "auto",
            "manual_override": self._manual_override,
            "schedule_state": "market_open" if market_open else "market_closed",
            "market_hours": market_open,
            "crawler_should_run": not market_open,
            "next_transition": next_transition.isoformat(),
            "next_transition_label": next_transition.strftime("%a %I:%M %p ET"),
            "timezone": "US/Eastern",
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        }

    async def start(self):
        """Start the schedule monitor and enforce the initial schedule."""
        if self._running:
            return

        self._running = True
        self._last_market_state = is_market_hours()
        await self._apply_current_schedule(reason="startup")
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        _log.info(
            "event=market_scheduler_started schedule_state=%s next_transition=%s",
            self.status["schedule_state"],
            self.status["next_transition"],
        )

    async def stop(self):
        """Stop the schedule monitor."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

    async def _apply_current_schedule(self, reason: str):
        if is_crawler_scheduled():
            await self._start_crawler_for_schedule(reason)
        elif self.crawler._running:
            _log.info("Scheduler: market is open (%s), requesting crawler stop", reason)
            self.crawler.stop()
        else:
            _log.info("Scheduler: market is open (%s), crawler remains stopped", reason)

    async def _start_crawler_for_schedule(self, reason: str):
        if self.crawler._running:
            _log.info("Scheduler: crawler already running during market-closed window (%s)", reason)
            return

        state = _load_state() or {}
        prev_status = state.get("status")
        resume_idx = state.get("last_completed_index", -1) + 1
        cycle_number = state.get("cycle_number", 1)
        last_sym = state.get("last_completed_symbol")

        if prev_status == "running":
            _log.info(
                "Scheduler: market closed (%s), resuming crawler at index %d (last=%s)",
                reason,
                resume_idx,
                last_sym,
            )
            asyncio.create_task(self.crawler.run(start_index=resume_idx, cycle_number=cycle_number))
            return

        _log.info("Scheduler: market closed (%s), auto-starting crawler", reason)
        asyncio.create_task(self.crawler.run())

    async def _monitor_loop(self):
        """Check market state every 60 seconds and react to transitions only."""
        try:
            while self._running:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

                current_market_state = is_market_hours()
                if current_market_state == self._last_market_state:
                    continue

                self._last_market_state = current_market_state
                self._manual_override = False

                if current_market_state:
                    _log.info("Scheduler: market opened, clearing manual override and stopping crawler")
                    if self.crawler._running:
                        self.crawler.stop()
                else:
                    _log.info("Scheduler: market closed, clearing manual override and starting crawler")
                    if not self.crawler._running:
                        await self._start_crawler_for_schedule(reason="market_close_transition")
        except asyncio.CancelledError:
            _log.debug("Scheduler monitor task cancelled")
            raise

    def set_manual_override(self, started: bool):
        """Respect manual start/stop until the next market transition."""
        self._manual_override = True
        _log.info(
            "Scheduler: manual override set (crawler %s)",
            "started" if started else "stopped",
        )


def start_scheduler(settings=None):
    """Create and start the scheduler singleton."""
    global _scheduler
    if _scheduler is None:
        _scheduler = CrawlerScheduler(get_crawler())
    return _scheduler


def get_scheduler() -> CrawlerScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = CrawlerScheduler(get_crawler())
    return _scheduler

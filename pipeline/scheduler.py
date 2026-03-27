"""APScheduler integration for nightly crawler runs."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pipeline.crawler import get_crawler

_log = logging.getLogger(__name__)

_scheduler = None


def start_scheduler(settings):
    global _scheduler
    hour, minute = settings.crawler_schedule.split(":")

    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _run_nightly,
        trigger="cron",
        hour=int(hour),
        minute=int(minute),
        id="nightly_crawler",
    )
    _scheduler.start()
    _log.info("event=scheduler_started schedule=%s", settings.crawler_schedule)


async def _run_nightly():
    _log.info("event=nightly_crawl_triggered")
    crawler = get_crawler()
    await crawler.run()

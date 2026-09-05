from __future__ import annotations

import asyncio
import logging
import signal
from typing import Optional

from betterer_ratings.api_server import start_api_server
from betterer_ratings.config.schema import AppConfig
from betterer_ratings.core.clock import now_epoch, to_log_time
from betterer_ratings.wiring.container import AppContainer, build_container

LOGGER = logging.getLogger("betterer-ratings")


async def run_app(config: AppConfig) -> None:
    container: Optional[AppContainer] = None
    api_runner = None

    try:
        container = build_container(config=config)
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _signal_stop() -> None:
            if not stop_event.is_set():
                LOGGER.info("Stop signal received. Beginning graceful shutdown.")
                stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_stop)
            except NotImplementedError:
                pass

        # Start dashboard API server
        try:
            api_runner = await start_api_server(container.db)
        except Exception:
            LOGGER.warning("Dashboard API server failed to start; continuing without it.", exc_info=True)

        LOGGER.info(
            "Starting betterer-ratings worker database=%s source_scan_interval_hours=%s "
            "title_refresh_days=%s episode_refresh_days=%s failed_retry_days=%s "
            "enabled_sources=%s submitter_workers=%s",
            container.db_path,
            container.config.runtime.source_scan_interval_hours,
            container.config.runtime.title_refresh_days,
            container.config.runtime.episode_refresh_days,
            container.config.runtime.failed_retry_days,
            len(container.config.tmdb.sources),
            container.config.runtime.submitter_workers,
        )
        now_ts = now_epoch()
        mdblist_pause_active = int(container.mdblist_gate.paused_until or 0) > now_ts
        mdblist_reset_at = (
            to_log_time(int(container.mdblist_gate.rate_reset))
            if container.mdblist_gate.rate_reset
            else "unknown"
        )
        mdblist_pause_text = "inactive"
        mdblist_quota_extra = {
            "event": "quota.state",
            "mdblist_rate_limit": container.mdblist_gate.rate_limit,
            "mdblist_rate_remaining": container.mdblist_gate.rate_remaining,
            "mdblist_rate_reset_at": mdblist_reset_at if mdblist_reset_at != "unknown" else "",
            "pause_active": mdblist_pause_active,
            "pause_reason": container.mdblist_gate.pause_reason if mdblist_pause_active else "",
        }
        if mdblist_pause_active:
            pause_until_at = to_log_time(int(container.mdblist_gate.paused_until))
            mdblist_pause_text = (
                f"active until {pause_until_at} "
                f"({container.mdblist_gate.pause_reason or 'rate limit pause'})"
            )
            mdblist_quota_extra["pause_until_at"] = pause_until_at
        LOGGER.info(
            "[MDBList] Quota state: remaining=%s/%s reset_at=%s pause=%s.",
            container.mdblist_gate.rate_remaining
            if container.mdblist_gate.rate_remaining is not None
            else "unknown",
            container.mdblist_gate.rate_limit
            if container.mdblist_gate.rate_limit is not None
            else "unknown",
            mdblist_reset_at,
            mdblist_pause_text,
            extra=mdblist_quota_extra,
        )

        tasks = [
            asyncio.create_task(container.harvester.run(stop_event), name="harvester"),
            asyncio.create_task(container.submitter.run(stop_event), name="submitter"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for finished in done:
            exc = finished.exception()
            if exc is not None:
                LOGGER.error("Task %s failed: %s. Initiating shutdown.", finished.get_name(), exc)
                stop_event.set()
                for pending_task in pending:
                    pending_task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise exc

        for pending_task in pending:
            pending_task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        LOGGER.info("Shutdown complete.")

    finally:
        if api_runner is not None:
            await api_runner.cleanup()
        if container is not None:
            container.harvester.close()
            await container.tmdb_client.aclose()
            await container.mdblist_client.aclose()
            if container.mal_client is not None:
                await container.mal_client.aclose()
            await container.pmdb_client.aclose()
            container.db.close()


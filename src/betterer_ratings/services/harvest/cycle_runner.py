from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable


async def run_harvester(
    *,
    harvester: Any,
    stop_event: asyncio.Event,
    logger: Any,
    now_epoch_fn: Callable[[], int],
) -> None:
    cycle_attempted = 0
    while not stop_event.is_set():
        cycle_start = now_epoch_fn()
        started = time.monotonic()
        cycle_attempted += 1
        try:
            result = await harvester._run_cycle(stop_event)
        except Exception:
            logger.exception(
                "[Harvester] Unexpected cycle failure",
                extra={"event": "harvest.cycle.failed", "cycle": cycle_attempted,
                       "elapsed_ms": round((time.monotonic() - started) * 1000, 1)},
            )
        else:
            errors = result.tmdb_list_request_errors + result.mdblist_request_failures
            outcome = "interrupted" if result.interrupted else "partial" if errors else "success"
            level = logging.WARNING if errors else logging.INFO if result.selected_candidates else logging.DEBUG
            logger.log(
                level,
                "[Harvester] Cycle complete: outcome=%s candidates=%s TMDB_errors=%s MDBList_errors=%s",
                outcome, result.selected_candidates, result.tmdb_list_request_errors, result.mdblist_request_failures,
                extra={
                    "event": "harvest.cycle.completed", "cycle": cycle_attempted,
                    "outcome": outcome, "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                    "selected_candidates": result.selected_candidates,
                    "tmdb_list_request_errors": result.tmdb_list_request_errors,
                    "mdblist_request_failures": result.mdblist_request_failures,
                    "interrupted": result.interrupted,
                },
            )

        if stop_event.is_set():
            logger.info(
                "[Harvester] Stop requested; exiting after cycle %s.",
                cycle_attempted,
            )
            break

        elapsed = now_epoch_fn() - cycle_start
        sleep_for = max(1, harvester.cycle_sleep_seconds - elapsed)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
        except asyncio.TimeoutError:
            pass

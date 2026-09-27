from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List

from betterer_ratings.services.harvest import cycle_discovery_phase as harvest_cycle_discovery_phase
from betterer_ratings.services.harvest import cycle_mdblist_phase as harvest_cycle_mdblist_phase
from betterer_ratings.services.harvest import cycle_tmdb_phase as harvest_cycle_tmdb_phase
from betterer_ratings.services.harvest.discovery_tmdb_exports import commit_export_cursors


async def run_cycle(
    *,
    harvester: Any,
    stop_event: asyncio.Event,
    logger: Any,
    now_epoch_fn: Callable[[], int],
    local_day_key_fn: Callable[..., str],
    to_iso_fn: Callable[[int], str],
    harvest_cycle_result_cls: Any,
) -> Any:
    self = harvester
    if hasattr(self, "_pending_tmdb_export_cursors"):
        self._pending_tmdb_export_cursors = {}

    await self._run_imdb_episode_cycle(stop_event)
    if stop_event.is_set():
        logger.info("[Harvester] Stop requested during IMDb episode cycle.")
        return harvest_cycle_result_cls(
            selected_candidates=0,
            tmdb_list_request_errors=0,
            mdblist_request_failures=0,
            interrupted=True,
        )

    local_stats: Dict[str, int]
    source_stats: Dict[str, Dict[str, int]]
    candidates: List[Any]
    (
        candidates,
        source_stats,
        tmdb_list_request_errors,
        local_stats,
        day_key,
        discovery_result,
    ) = await harvest_cycle_discovery_phase.run_discovery_phase(
        harvester=self,
        stop_event=stop_event,
        logger=logger,
        now_epoch_fn=now_epoch_fn,
        local_day_key_fn=local_day_key_fn,
        harvest_cycle_result_cls=harvest_cycle_result_cls,
    )
    del day_key
    if discovery_result is not None:
        return discovery_result

    if candidates:
        logger.info("[Harvester] Cycle collected %s title candidate(s).", len(candidates))
    else:
        logger.debug("[Harvester] Cycle collected 0 title candidate(s).")
    if not candidates:
        if getattr(self, "_pending_tmdb_export_cursors", None):
            commit_export_cursors(self.db, self.tmdb_export_directory, self._pending_tmdb_export_cursors)
            self._pending_tmdb_export_cursors = {}
        logger.debug("[Harvester] No title candidates eligible this cycle.")
        return harvest_cycle_result_cls(
            selected_candidates=0,
            tmdb_list_request_errors=tmdb_list_request_errors,
            mdblist_request_failures=0,
            interrupted=False,
        )

    tmdb_details, tmdb_phase_result = await harvest_cycle_tmdb_phase.run_tmdb_details_phase(
        harvester=self,
        stop_event=stop_event,
        logger=logger,
        candidates=candidates,
        source_stats=source_stats,
        tmdb_list_request_errors=tmdb_list_request_errors,
        local_stats=local_stats,
        harvest_cycle_result_cls=harvest_cycle_result_cls,
    )
    if tmdb_phase_result is not None:
        return tmdb_phase_result
    assert tmdb_details is not None

    result = await harvest_cycle_mdblist_phase.run_mdblist_enrichment_phase(
        harvester=self,
        logger=logger,
        now_epoch_fn=now_epoch_fn,
        stop_event=stop_event,
        candidates=candidates,
        tmdb_details=tmdb_details,
        source_stats=source_stats,
        tmdb_list_request_errors=tmdb_list_request_errors,
        local_stats=local_stats,
        harvest_cycle_result_cls=harvest_cycle_result_cls,
    )
    if not result.interrupted and getattr(self, "_pending_tmdb_export_cursors", None):
        commit_export_cursors(self.db, self.tmdb_export_directory, self._pending_tmdb_export_cursors)
        self._pending_tmdb_export_cursors = {}
    return result

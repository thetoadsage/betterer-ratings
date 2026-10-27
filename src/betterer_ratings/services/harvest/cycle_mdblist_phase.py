from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional, Sequence, Set, Tuple


async def _run_standard_mdblist_enrichment_phase(
    *,
    harvester: Any,
    logger: Any,
    now_epoch_fn: Callable[[], int],
    stop_event: asyncio.Event,
    candidates: Sequence[Any],
    tmdb_details: Dict[Tuple[str, int], Optional[Dict[str, Any]]],
    source_stats: Dict[str, Dict[str, int]],
    tmdb_list_request_errors: int,
    local_stats: Dict[str, int],
    harvest_cycle_result_cls: Any,
) -> Any:
    self = harvester
    del source_stats, local_stats

    candidate_lookup: Dict[Tuple[str, int], Any] = {
        (candidate.media_type, candidate.tmdb_id): candidate for candidate in candidates
    }
    enriched = 0
    enriched_mdblist_ok = 0
    enriched_mdblist_miss = 0
    enriched_tmdb_only = 0
    queue_added_ratings = 0
    queue_added_mappings = 0
    persisted_keys: Set[Tuple[str, int]] = set()
    queue_log_every = max(100, min(2000, len(candidates) // 10 or 100))

    def on_mdblist_chunk(
        media_type: str,
        chunk_ids: Sequence[int],
        chunk_results: Dict[int, Dict[str, Any]],
        chunk_attempted: Set[int],
        chunk_index: int,
        total_chunks: int,
    ) -> None:
        del chunk_attempted
        nonlocal enriched
        nonlocal enriched_mdblist_ok
        nonlocal enriched_mdblist_miss
        nonlocal enriched_tmdb_only
        nonlocal queue_added_ratings
        nonlocal queue_added_mappings

        now_ts = now_epoch_fn()
        for tmdb_id in chunk_ids:
            key = (media_type, tmdb_id)
            if key in persisted_keys:
                continue
            candidate = candidate_lookup.get(key)
            if candidate is None:
                continue
            (
                mdblist_ok_inc,
                mdblist_miss_inc,
                tmdb_only_inc,
                queue_r_inc,
                queue_m_inc,
            ) = self._save_candidate_enrichment(
                candidate=candidate,
                details=tmdb_details.get(key),
                md_item=chunk_results.get(tmdb_id),
                now_ts=now_ts,
            )
            persisted_keys.add(key)
            enriched += 1
            enriched_mdblist_ok += mdblist_ok_inc
            enriched_mdblist_miss += mdblist_miss_inc
            enriched_tmdb_only += tmdb_only_inc
            queue_added_ratings += queue_r_inc
            queue_added_mappings += queue_m_inc

        if chunk_index == 1 or chunk_index == total_chunks or enriched % queue_log_every == 0:
            queue_counts = self.db.queue_counts()
            logger.info(
                "[Harvester] MDBList persist progress: batch=%s/%s enriched=%s/%s mdblist_found=%s mdblist_missing=%s queued(ratings=%s mappings=%s) pending(ratings=%s mappings=%s).",
                chunk_index,
                total_chunks,
                enriched,
                len(candidates),
                enriched_mdblist_ok,
                enriched_mdblist_miss,
                queue_added_ratings,
                queue_added_mappings,
                queue_counts["ratings_pending"],
                queue_counts["mappings_pending"],
            )
    (
        mdblist_data,
        _,
        mdblist_all_success,
        mdblist_halted,
        mdblist_halt_reason,
        mdblist_failed_batches,
    ) = await self.mdblist_client.fetch_for_candidates(
        candidates,
        on_chunk=on_mdblist_chunk,
        stop_event=stop_event,
    )
    del mdblist_failed_batches

    if mdblist_halted or stop_event.is_set():
        remaining_candidates = [
            candidate
            for candidate in candidates
            if (candidate.media_type, candidate.tmdb_id) not in persisted_keys
        ]
        logger.info(
            "[Harvester] MDBList enrichment paused (%s). %s candidate(s) will be reconsidered later.",
            mdblist_halt_reason or "interrupted",
            len(remaining_candidates),
        )
        return harvest_cycle_result_cls(
            selected_candidates=len(candidates),
            tmdb_list_request_errors=tmdb_list_request_errors,
            mdblist_request_failures=1,
            interrupted=bool(stop_event.is_set()),
        )

    # Safety net: ensure every candidate is persisted, even if a chunk callback
    # did not fire (for example due to unexpected mid-cycle failures).
    if len(persisted_keys) < len(candidates):
        logger.warning(
            "[Harvester] Missing persisted items after chunking (%s/%s). Backfilling.",
            len(candidates) - len(persisted_keys),
            len(candidates),
        )
        now_ts = now_epoch_fn()
        for candidate in candidates:
            key = (candidate.media_type, candidate.tmdb_id)
            if key in persisted_keys:
                continue
            (
                mdblist_ok_inc,
                mdblist_miss_inc,
                tmdb_only_inc,
                queue_r_inc,
                queue_m_inc,
            ) = self._save_candidate_enrichment(
                candidate=candidate,
                details=tmdb_details.get(key),
                md_item=mdblist_data.get(key),
                now_ts=now_ts,
            )
            enriched += 1
            enriched_mdblist_ok += mdblist_ok_inc
            enriched_mdblist_miss += mdblist_miss_inc
            enriched_tmdb_only += tmdb_only_inc
            queue_added_ratings += queue_r_inc
            queue_added_mappings += queue_m_inc
            persisted_keys.add(key)

    queue_counts = self.db.queue_counts()
    logger.info(
        "[Harvester] Cycle completed: candidates=%s enriched=%s mdblist_found=%s mdblist_missing=%s tmdb_only=%s queued_ratings=%s queued_mappings=%s pending(ratings=%s mappings=%s episode_ratings=%s).",
        len(candidates),
        enriched,
        enriched_mdblist_ok,
        enriched_mdblist_miss,
        enriched_tmdb_only,
        queue_added_ratings,
        queue_added_mappings,
        queue_counts["ratings_pending"],
        queue_counts["mappings_pending"],
        queue_counts["episode_ratings_pending"],
    )
    return harvest_cycle_result_cls(
        selected_candidates=len(candidates),
        tmdb_list_request_errors=tmdb_list_request_errors,
        mdblist_request_failures=0 if mdblist_all_success else 1,
        interrupted=False,
    )


async def run_mdblist_enrichment_phase(*, harvester: Any, logger: Any, now_epoch_fn: Callable[[], int], stop_event: asyncio.Event, candidates: Sequence[Any], tmdb_details: Dict[Tuple[str, int], Optional[Dict[str, Any]]], source_stats: Dict[str, Dict[str, int]], tmdb_list_request_errors: int, local_stats: Dict[str, int], harvest_cycle_result_cls: Any) -> Any:
    if getattr(harvester, "mal_client", None) is None:
        phase = _run_standard_mdblist_enrichment_phase
    else:
        from betterer_ratings.services.harvest.anime_enrichment_phase import (
            run_anime_enrichment_phase,
        )
        phase = run_anime_enrichment_phase
    return await phase(harvester=harvester, logger=logger, now_epoch_fn=now_epoch_fn, stop_event=stop_event, candidates=candidates, tmdb_details=tmdb_details, source_stats=source_stats, tmdb_list_request_errors=tmdb_list_request_errors, local_stats=local_stats, harvest_cycle_result_cls=harvest_cycle_result_cls)

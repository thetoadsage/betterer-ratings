from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Callable, Dict, Optional, Sequence, Set, Tuple

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.services.harvest.discovery_local import mdblist_daily_quota_pause_until
from betterer_ratings.services.harvest.mal import MALResult, fetch_candidate_mal_score


async def run_mdblist_enrichment_phase(
    *, harvester: Any, logger: Any, now_epoch_fn: Callable[[], int],
    stop_event: asyncio.Event, candidates: Sequence[Any],
    tmdb_details: Dict[Tuple[str, int], Optional[Dict[str, Any]]],
    source_stats: Dict[str, Dict[str, int]], tmdb_list_request_errors: int,
    local_stats: Dict[str, int], harvest_cycle_result_cls: Any,
) -> Any:
    del source_stats, local_stats
    self = harvester
    ttl = max(1, int(getattr(self, "ratings_ttl_seconds", 7 * 86400)))
    missing_retry = max(60, int(getattr(self, "failed_retry_seconds", 7 * 86400)))
    lookup = {(c.media_type, c.tmdb_id): c for c in candidates}
    persisted: set[tuple[str, int]] = set()
    queue_ratings = queue_mappings = 0
    pause_until = mdblist_daily_quota_pause_until(db=self.db, now_ts=now_epoch_fn())
    md_due = [c for c in candidates if self.db.get_enrichment_state(
        c.tmdb_id, c.media_type, "mdblist"
    )["next_due"] <= now_epoch_fn()]

    async def save_candidate(candidate: Any, md_item: Any = None, *, attempted: bool = False) -> None:
        nonlocal queue_ratings, queue_mappings
        if stop_event.is_set():
            return
        key = (candidate.media_type, candidate.tmdb_id)
        if key in persisted:
            return
        now_ts = now_epoch_fn()
        details = tmdb_details.get(key)
        md_state = self.db.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "mdblist")
        # Retain IDs for MAL discovery, but do not replay stale MDBList ratings.
        mal_item = md_item if md_item is not None else md_state["payload"]
        mal_result = MALResult(None, None, False, "not_attempted")
        mal_update = None
        if getattr(self, "mal_client", None) is not None:
            state = self.db.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "mal")
            context = hashlib.sha256(json.dumps(
                [details, extract_mappings(candidate.media_type, details, mal_item), self.db.get_title_mapping(
                    tmdb_id=candidate.tmdb_id, media_type=candidate.media_type, id_type="mal"
                )], sort_keys=True,
            ).encode()).hexdigest()
            cached = state["payload"] or {}
            if state["next_due"] <= now_ts or cached.get("context") != context:
                mal_result = await fetch_candidate_mal_score(
                    client=self.mal_client, db=self.db, candidate=candidate,
                    details=details, md_item=mal_item, stop_event=stop_event,
                    anime_mapping_cache=getattr(self, "anime_mapping_cache", None),
                    jikan=getattr(self, "jikan", None),
                )
                if stop_event.is_set():
                    return
                if mal_result.validated and mal_result.mal_id is not None:
                    context = hashlib.sha256(json.dumps(
                        [details, extract_mappings(candidate.media_type, details, mal_item),
                         str(mal_result.mal_id)], sort_keys=True,
                    ).encode()).hexdigest()
                mal_update = {"result": vars(mal_result), "context": context}
            else:
                data = cached.get("result")
                if isinstance(data, dict):
                    mal_result = MALResult(**data)
                elif cached.get("score") is not None:
                    mal_result = MALResult(None, None, False, "legacy_score", cached["score"])
        _, _, _, queued_r, queued_m = self._save_candidate_enrichment(
            candidate=candidate, details=details, md_item=md_item, now_ts=now_ts,
            mal_result=mal_result, mdblist_attempted=attempted,
        )
        queue_ratings += queued_r
        queue_mappings += queued_m
        tm_state = self.db.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "tmdb")
        if details is not None and tm_state["payload"] == details:
            self.db.save_enrichment_state(
                candidate.tmdb_id, candidate.media_type, "tmdb", payload=details,
                now_ts=tm_state["fetched_at"], next_due=tm_state["fetched_at"] + ttl,
            )
        if mal_update is not None:
            self.db.save_enrichment_state(
                candidate.tmdb_id, candidate.media_type, "mal", payload=mal_update, now_ts=now_ts,
                next_due=now_ts + (ttl if mal_result.validated else missing_retry),
            )
        if attempted:
            self.db.save_enrichment_state(
                candidate.tmdb_id, candidate.media_type, "mdblist",
                payload=md_item, now_ts=now_ts,
                next_due=now_ts + (ttl if md_item is not None else missing_retry),
            )
        persisted.add(key)

    async def on_chunk(
        media_type: str, chunk_ids: Sequence[int], chunk_results: Dict[int, Dict[str, Any]],
        chunk_attempted: Set[int], chunk_index: int, total_chunks: int,
    ) -> None:
        del chunk_index, total_chunks
        for tmdb_id in chunk_ids:
            if stop_event.is_set():
                return
            candidate = lookup[(media_type, tmdb_id)]
            await save_candidate(candidate, chunk_results.get(tmdb_id), attempted=tmdb_id in chunk_attempted)
            if tmdb_id not in chunk_attempted:
                self.db.save_enrichment_state(
                    tmdb_id, media_type, "mdblist", payload=None, now_ts=now_epoch_fn(),
                    next_due=now_epoch_fn() + 3600,
                )

    all_success = True
    halted = False
    if md_due and pause_until <= now_epoch_fn() and not stop_event.is_set():
        data, attempted_keys, all_success, halted, reason, _ = await self.mdblist_client.fetch_for_candidates(
            md_due, on_chunk=on_chunk, stop_event=stop_event,
        )
        # Include clients which returned results without a callback.
        for c in md_due:
            key = (c.media_type, c.tmdb_id)
            if key not in persisted and key in attempted_keys:
                await save_candidate(c, data.get(key), attempted=True)
        if halted:
            logger.info("[Harvester] MDBList deferred (%s); continuing independent enrichment.", reason)
    elif md_due:
        logger.info("[Harvester] MDBList quota pause until %s; continuing independent enrichment.", pause_until)

    # This also persists TMDB/MAL work for unattempted batches after a mid-cycle pause.
    for candidate in candidates:
        if stop_event.is_set():
            break
        key = (candidate.media_type, candidate.tmdb_id)
        if key in persisted:
            continue
        await save_candidate(candidate)
        state = self.db.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "mdblist")
        if state["next_due"] <= now_epoch_fn():
            paused = mdblist_daily_quota_pause_until(db=self.db, now_ts=now_epoch_fn())
            self.db.save_enrichment_state(
                candidate.tmdb_id, candidate.media_type, "mdblist", payload=None,
                now_ts=now_epoch_fn(), next_due=max(paused, now_epoch_fn() + 300),
            )
    logger.info(
        "[Harvester] Cycle completed: candidates=%s enriched=%s queued_ratings=%s queued_mappings=%s.",
        len(candidates), len(persisted), queue_ratings, queue_mappings,
    )
    return harvest_cycle_result_cls(
        selected_candidates=len(candidates), tmdb_list_request_errors=tmdb_list_request_errors,
        mdblist_request_failures=0 if all_success else 1, interrupted=stop_event.is_set(),
    )

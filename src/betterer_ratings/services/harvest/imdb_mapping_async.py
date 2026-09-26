from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, Callable, Dict, Optional, Tuple

from betterer_ratings.domain.models import Candidate


async def map_episode_parents_to_tmdb(
    *,
    parent_ids: Any,
    stop_event: asyncio.Event,
    details_concurrency: int,
    resolve_imdb_to_tmdb_local_fn: Callable[[str, str], Optional[Candidate]],
    fetch_find_by_imdb_fn: Callable[[str], Any],
    extract_tmdb_from_find_payload_fn: Callable[
        [Dict[str, Any], str], Tuple[Optional[int], str, float]
    ],
    imdb_cache: Any,
    normalize_imdb_title_id_fn: Callable[[Any], Optional[str]],
    now_epoch_fn: Callable[[], int],
    candidate_cls: Any = Candidate,
) -> Tuple[Dict[str, Candidate], int, int]:
    mapped: Dict[str, Candidate] = {}
    lookup_errors = 0
    missing_mappings = 0
    queue: asyncio.Queue[str] = asyncio.Queue()
    for parent_id in parent_ids:
        normalized_parent = normalize_imdb_title_id_fn(parent_id)
        if normalized_parent:
            queue.put_nowait(normalized_parent)

    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal lookup_errors
        nonlocal missing_mappings
        cache_updates: list[tuple[str, str, int, str, float, int]] = []
        try:
            while True:
                if stop_event.is_set():
                    return
                try:
                    parent_id = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    local_candidate = resolve_imdb_to_tmdb_local_fn(parent_id, "tv")
                    if local_candidate is not None:
                        async with lock:
                            mapped[parent_id] = local_candidate
                        continue

                    response = await fetch_find_by_imdb_fn(parent_id)
                    if not response.ok or not isinstance(response.data, dict):
                        async with lock:
                            lookup_errors += 1
                        continue
                    tmdb_id, title, popularity = extract_tmdb_from_find_payload_fn(
                        response.data,
                        "tv",
                    )
                    if tmdb_id is None:
                        async with lock:
                            missing_mappings += 1
                        continue
                    candidate = candidate_cls(
                        tmdb_id=tmdb_id,
                        media_type="tv",
                        title=title or f"TMDB-{tmdb_id}",
                        popularity=popularity,
                        harvest_reason="source",
                    )
                    async with lock:
                        mapped[parent_id] = candidate
                    cache_updates.append(
                        (
                            parent_id,
                            "tv",
                            int(tmdb_id),
                            candidate.title,
                            candidate.popularity,
                            now_epoch_fn(),
                        )
                    )
                finally:
                    queue.task_done()
        finally:
            if cache_updates:
                imdb_cache.upsert_many(cache_updates)

    worker_count = max(1, min(details_concurrency, len(parent_ids)))
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    await asyncio.gather(*workers)
    return mapped, lookup_errors, missing_mappings


async def map_imdb_candidates_to_tmdb(
    *,
    candidates: Any,
    stop_event: asyncio.Event,
    details_concurrency: int,
    resolve_imdb_to_tmdb_local_fn: Callable[[str, str], Optional[Candidate]],
    fetch_find_by_imdb_fn: Callable[[str], Any],
    extract_tmdb_from_find_payload_fn: Callable[
        [Dict[str, Any], str], Tuple[Optional[int], str, float]
    ],
    imdb_cache: Any,
    now_epoch_fn: Callable[[], int],
    candidate_cls: Any = Candidate,
) -> Tuple[Any, int, int]:
    if not candidates:
        return [], 0, 0

    mapped = []
    lookup_errors = 0
    missing_mappings = 0
    completed = 0
    mapped_count = 0
    lock = asyncio.Lock()
    queue: asyncio.Queue[Any] = asyncio.Queue()
    for candidate in candidates:
        queue.put_nowait(candidate)

    async def worker() -> None:
        nonlocal lookup_errors
        nonlocal missing_mappings
        nonlocal completed
        nonlocal mapped_count
        cache_updates: list[tuple[str, str, int, str, float, int]] = []
        try:
            while True:
                if stop_event.is_set():
                    return
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    local_candidate = resolve_imdb_to_tmdb_local_fn(
                        item.imdb_id,
                        item.media_type,
                    )
                    if local_candidate is not None:
                        async with lock:
                            mapped.append(replace(
                                local_candidate, archive_imdb_id=item.imdb_id,
                                archive_rating=item.average_rating, archive_votes=item.num_votes,
                            ))
                            mapped_count += 1
                            completed += 1
                        continue

                    response = await fetch_find_by_imdb_fn(item.imdb_id)
                    if not response.ok or not isinstance(response.data, dict):
                        async with lock:
                            lookup_errors += 1
                            completed += 1
                        continue
                    tmdb_id, title, popularity = extract_tmdb_from_find_payload_fn(
                        response.data,
                        item.media_type,
                    )
                    if tmdb_id is None:
                        async with lock:
                            missing_mappings += 1
                            completed += 1
                        continue
                    mapped_candidate = candidate_cls(
                        tmdb_id=tmdb_id,
                        media_type=item.media_type,
                        title=title or f"TMDB-{tmdb_id}",
                        popularity=popularity,
                        harvest_reason="source",
                        archive_imdb_id=item.imdb_id,
                        archive_rating=item.average_rating,
                        archive_votes=item.num_votes,
                    )
                    cache_updates.append(
                        (
                            item.imdb_id,
                            item.media_type,
                            int(tmdb_id),
                            mapped_candidate.title,
                            mapped_candidate.popularity,
                            now_epoch_fn(),
                        )
                    )
                    async with lock:
                        mapped.append(mapped_candidate)
                        mapped_count += 1
                        completed += 1
                finally:
                    queue.task_done()
        finally:
            if cache_updates:
                imdb_cache.upsert_many(cache_updates)

    worker_count = max(
        1,
        min(details_concurrency, len(candidates)),
    )
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    await asyncio.gather(*workers)
    return mapped, lookup_errors, missing_mappings

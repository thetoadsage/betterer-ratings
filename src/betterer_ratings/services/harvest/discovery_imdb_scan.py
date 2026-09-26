from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Set, Tuple

from betterer_ratings.core.clock import now_epoch


async def scan_imdb_archive_source(
    *,
    stop_event: Any,
    db: Any,
    imdb_archive_source: Any,
    source_stats: Dict[str, Dict[str, int]],
    candidates: List[Any],
    seen: Set[Tuple[str, int]],
    ensure_imdb_index_fn: Callable[[Any], int],
    read_imdb_index_batch_fn: Callable[[Any], Tuple[List[Any], int, int, bool]],
    commit_imdb_cursor_fn: Callable[..., None],
    map_imdb_candidates_to_tmdb_fn: Callable[..., Any],
    imdb_cursor_line_key: str,
    imdb_total_key: str,
    publish_scan_progress_fn: Callable[..., None],
    logger: Any,
    pages_scanned: int,
    raw_seen_total: int,
) -> Tuple[bool, int, int]:
    interrupted = False
    existing_title_keys: Set[Tuple[str, int]] = (
        db.title_key_set() if hasattr(db, "title_key_set") else set()
    )

    source = imdb_archive_source
    title_batch_size = max(1, int(source.title_batch_size))
    stat = source_stats[source.name]
    pages_scanned += 1
    cursor_line_before = max(0, db.get_state_int(imdb_cursor_line_key, 0))
    total_indexed_hint = max(0, db.get_state_int(imdb_total_key, 0))
    total_batches_hint = (
        max(1, math.ceil(total_indexed_hint / title_batch_size))
        if total_indexed_hint > 0
        else 1
    )
    completed_batches_hint = (
        min(
            total_batches_hint,
            math.ceil(cursor_line_before / title_batch_size),
        )
        if total_indexed_hint > 0
        else 0
    )
    stat["pages_discovered"] = total_batches_hint
    stat["pages_effective"] = total_batches_hint
    stat["pages_fetched"] = completed_batches_hint
    stat["first_page"] = 1
    stat["last_page"] = 1
    publish_scan_progress_fn(source, current_page=1)
    try:
        total_indexed = ensure_imdb_index_fn(source)
        total_batches = (
            max(1, math.ceil(total_indexed / title_batch_size))
            if total_indexed > 0
            else 1
        )
        stat["pages_discovered"] = total_batches
        stat["pages_effective"] = total_batches
        if total_indexed == 0:
            publish_scan_progress_fn(source, current_page=1)
        (
            imdb_batch,
            next_cursor_line,
            next_cursor_byte,
            batch_exhausted,
        ) = read_imdb_index_batch_fn(source)
        stat["raw_seen"] += len(imdb_batch)
        raw_seen_total += len(imdb_batch)
        if total_indexed > 0 and imdb_batch:
            current_batch = min(
                total_batches,
                (cursor_line_before // title_batch_size) + 1,
            )
            stat["pages_fetched"] = max(int(stat["pages_fetched"]), current_batch)
        publish_scan_progress_fn(source, current_page=1)
        if imdb_batch and not stop_event.is_set():
            if logger is not None:
                logger.info(
                    "[IMDbArchive] Mapping %s IMDb candidate(s) to TMDB IDs.",
                    len(imdb_batch),
                )
            (
                mapped_candidates,
                lookup_errors,
                missing_mappings,
            ) = await map_imdb_candidates_to_tmdb_fn(
                candidates=imdb_batch,
                stop_event=stop_event,
            )
            stat["errors"] += lookup_errors
            stat["skipped"] += missing_mappings
            unique_candidates = []
            source_at = int((source.path / "title.ratings.tsv").stat().st_mtime)
            for candidate in mapped_candidates:
                db.save_archive_title_rating(
                    candidate, source_at=source_at,
                    now_ts=now_epoch(),
                )
                key = (candidate.media_type, candidate.tmdb_id)
                if key in seen:
                    stat["duplicates"] += 1
                    continue
                seen.add(key)
                if key in existing_title_keys:
                    stat["skipped"] += 1
                    continue
                unique_candidates.append(candidate)
            for candidate in unique_candidates:
                candidates.append(candidate)
                existing_title_keys.add((candidate.media_type, candidate.tmdb_id))
                stat["added"] += 1
            if logger is not None:
                logger.info(
                    "[IMDbArchive] TMDB mapping complete: mapped=%s missing=%s errors=%s.",
                    len(mapped_candidates),
                    missing_mappings,
                    lookup_errors,
                )
        if not stop_event.is_set():
            commit_imdb_cursor_fn(
                cursor_line=next_cursor_line,
                cursor_byte=next_cursor_byte,
                exhausted=batch_exhausted,
            )
            if total_indexed > 0:
                completed_batches = min(
                    total_batches,
                    math.ceil(next_cursor_line / title_batch_size),
                )
                stat["pages_fetched"] = max(int(stat["pages_fetched"]), completed_batches)
        publish_scan_progress_fn(source, current_page=1)
        if stop_event.is_set():
            interrupted = True
    except Exception as exc:
        stat["errors"] += 1
        if logger is not None:
            logger.warning("[IMDbArchive] candidate scan failed: %s", exc)
        publish_scan_progress_fn(source, current_page=1)

    return interrupted, pages_scanned, raw_seen_total

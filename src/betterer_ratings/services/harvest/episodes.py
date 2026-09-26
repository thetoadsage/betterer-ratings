from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable, Dict, List

from betterer_ratings.core.clock import format_duration
from betterer_ratings.services.harvest.episode_identity import verify_episode_identities


async def run_imdb_episode_cycle(
    *,
    harvester: Any,
    stop_event: asyncio.Event,
    logger: Any,
    now_epoch_fn: Callable[[], int],
) -> Dict[str, int]:
    self = harvester
    stats: Dict[str, int] = {
        "enabled": 1
        if (self.imdb_archive_source is not None and self.imdb_episodes_enabled)
        else 0,
        "rows_selected": 0,
        "titles_selected": 0,
        "titles_mapped": 0,
        "titles_missing": 0,
        "lookup_errors": 0,
        "rows_queued": 0,
        "queue_total": 0,
        "exhausted": 0,
    }
    if self.imdb_archive_source is None or not self.imdb_episodes_enabled:
        return stats

    source = self.imdb_archive_source
    now_ts = now_epoch_fn()
    if self.db.get_state_int(self._imdb_episode_exhausted_key, 0) == 1:
        last_full_scan = self.db.get_state_int(self._imdb_episode_last_full_scan_key, 0)
        ttl_seconds = max(1, int(self.episode_ratings_ttl_seconds))
        if last_full_scan <= 0 or now_ts - last_full_scan >= ttl_seconds:
            logger.info("[IMDbArchive] Episode TTL expired; restarting episode ratings scan.")
            self._reset_imdb_episode_cursor(exhausted=False)
        else:
            last_idle_log = max(0, int(getattr(self, "_episode_idle_last_log_ts", 0) or 0))
            if last_idle_log <= 0 or now_ts - last_idle_log >= 3600:
                queue_counts = self.db.queue_counts()
                next_full_scan_in = max(0, int(last_full_scan) + ttl_seconds - now_ts)
                logger.info(
                    "[IMDbArchive] Episode ratings current: scan complete; next full scan in %s; queue pending=%s in_flight=%s.",
                    format_duration(next_full_scan_in),
                    queue_counts["episode_ratings_pending"],
                    queue_counts["episode_ratings_in_flight"],
                )
                self._episode_idle_last_log_ts = now_ts

    try:
        self._ensure_imdb_episode_index(source)
    except Exception as exc:
        logger.warning("[IMDbArchive] episode index unavailable: %s", exc)
        stats["lookup_errors"] = 1
        queue_counts = self.db.queue_counts()
        stats["queue_total"] = int(queue_counts["episode_ratings_pending"])
        return stats

    (
        batch,
        next_cursor_line,
        next_cursor_byte,
        exhausted,
    ) = self._read_imdb_episode_index_batch(
        source=source,
        day_key="",
    )
    stats["rows_selected"] = len(batch)
    stats["titles_selected"] = len({entry.parent_imdb_id for entry in batch})
    stats["exhausted"] = 1 if exhausted else 0

    if stop_event.is_set():
        return stats

    rows_queued = 0
    retry_pending = False
    if batch:
        parent_ids = sorted({entry.parent_imdb_id for entry in batch})
        (
            mapped_parents,
            lookup_errors,
            missing_mappings,
        ) = await self._map_imdb_episode_parents_to_tmdb(
            parent_ids=parent_ids,
            stop_event=stop_event,
        )
        stats["titles_mapped"] = len(mapped_parents)
        stats["titles_missing"] = int(missing_mappings)
        stats["lookup_errors"] = int(lookup_errors)
        retry_pending = lookup_errors > 0

        grouped_entries: Dict[str, List[Any]] = defaultdict(list)
        for entry in batch:
            grouped_entries[entry.parent_imdb_id].append(entry)

        for parent_id, entries in grouped_entries.items():
            if stop_event.is_set():
                break
            mapped_candidate = mapped_parents.get(parent_id)
            if mapped_candidate is None:
                continue
            verified, errors, skipped = await verify_episode_identities(
                entries=entries, tmdb_id=mapped_candidate.tmdb_id, db=self.db,
                tmdb_client=self.tmdb_client, stop_event=stop_event,
                now_epoch_fn=now_epoch_fn, concurrency=self.details_concurrency,
            )
            stats["lookup_errors"] += errors
            stats["identity_skipped"] = stats.get("identity_skipped", 0) + skipped
            retry_pending = retry_pending or errors > 0
            if stop_event.is_set():
                break
            rows_queued += self.db.save_imdb_episode_ratings(
                tmdb_id=mapped_candidate.tmdb_id,
                media_type=mapped_candidate.media_type,
                imdb_parent_id=parent_id,
                entries=verified,
                now_ts=now_ts,
                default_label="IM",
            )

    stats["rows_queued"] = int(rows_queued)

    if not stop_event.is_set() and not retry_pending:
        self._commit_imdb_episode_cursor(
            cursor_line=next_cursor_line,
            cursor_byte=next_cursor_byte,
            exhausted=exhausted,
        )
        if exhausted:
            self.db.set_state(self._imdb_episode_last_full_scan_key, now_epoch_fn())

    queue_counts = self.db.queue_counts()
    stats["queue_total"] = int(queue_counts["episode_ratings_pending"])

    if batch:
        logger.info(
            "[IMDbArchive] Episode cycle: rows=%s titles=%s mapped=%s queued=%s missing=%s errors=%s scan_complete=%s queue_pending=%s",
            len(batch),
            len({entry.parent_imdb_id for entry in batch}),
            int(stats["titles_mapped"]),
            int(stats["rows_queued"]),
            int(stats["titles_missing"]),
            int(stats["lookup_errors"]),
            int(stats["exhausted"]),
            int(stats["queue_total"]),
        )
    return stats

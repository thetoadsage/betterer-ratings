from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional, Sequence, Tuple


async def fetch_tmdb_details(
    *,
    candidates: Sequence[Any],
    stop_event: asyncio.Event,
    tmdb_client: Any,
    db: Any,
    details_concurrency: int,
    now_epoch_fn: Callable[[], int],
    logger: Any,
    refresh_seconds: int = 7 * 86400,
    retry_seconds: int = 3600,
) -> Tuple[Dict[Tuple[str, int], Optional[Dict[str, Any]]], bool]:
    details: Dict[Tuple[str, int], Optional[Dict[str, Any]]] = {}
    progress_lock = asyncio.Lock()
    queue: asyncio.Queue[Any] = asyncio.Queue()
    for candidate in candidates:
        queue.put_nowait(candidate)
    completed = 0
    succeeded = 0
    failed = 0
    total = len(candidates)
    started_ts = now_epoch_fn()
    progress_every = max(250, min(2000, total // 20 if total > 0 else 250))

    async def worker() -> None:
        nonlocal completed, succeeded, failed
        while True:
            if stop_event.is_set():
                return
            try:
                candidate = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            success = False
            try:
                key = (candidate.media_type, candidate.tmdb_id)
                state = db.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "tmdb")
                fresh = state["fetched_at"] is not None and now_epoch_fn() - state["fetched_at"] < refresh_seconds
                if fresh:
                    details[key] = state["payload"]
                    success = isinstance(details[key], dict)
                elif state["next_due"] > now_epoch_fn():
                    details[key] = None
                else:
                    if candidate.harvest_reason == "daily_export":
                        response = await tmdb_client.fetch_details(
                            candidate.media_type, candidate.tmdb_id, single_attempt=True
                        )
                    else:
                        response = await tmdb_client.fetch_details(candidate.media_type, candidate.tmdb_id)
                    success = response.ok and isinstance(response.data, dict) and bool(response.data)
                    payload = response.data if success else None
                    details[key] = payload
                    now_ts = now_epoch_fn()
                    db.save_enrichment_state(
                        candidate.tmdb_id, candidate.media_type, "tmdb", payload=payload,
                        now_ts=now_ts,
                        # Keep successful metadata due until its ratings/mappings
                        # are persisted. A stop between phases must not lose work.
                        next_due=now_ts if success else now_ts + retry_seconds,
                    )
                    if not success and logger is not None:
                        logger.warning(
                            "[TMDB] details failed for %s %s (%s): %s",
                            candidate.media_type, candidate.tmdb_id, response.status, response.text[:240],
                        )
            except Exception as exc:
                details[(candidate.media_type, candidate.tmdb_id)] = None
                db.save_enrichment_state(
                    candidate.tmdb_id, candidate.media_type, "tmdb", payload=None,
                    now_ts=now_epoch_fn(), next_due=now_epoch_fn() + retry_seconds,
                )
                if logger is not None:
                    logger.warning(
                        "[TMDB] details failed for %s %s: %s",
                        candidate.media_type,
                        candidate.tmdb_id,
                        exc,
                    )
            finally:
                queue.task_done()

            async with progress_lock:
                completed += 1
                if success:
                    succeeded += 1
                else:
                    failed += 1

                if completed == 1 or completed == total or completed % progress_every == 0:
                    elapsed = max(1, now_epoch_fn() - started_ts)
                    rps = completed / elapsed
                    if logger is not None:
                        logger.debug(
                            "[Harvester] TMDB details progress: %s/%s (ok=%s, failed=%s, ~%.2f items/s)",
                            completed,
                            total,
                            succeeded,
                            failed,
                            rps,
                        )

    worker_count = max(1, min(details_concurrency, total if total > 0 else 1))
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    await asyncio.gather(*workers)
    interrupted = stop_event.is_set() and completed < total
    return details, interrupted

from __future__ import annotations

import asyncio
from typing import Any, Sequence

from betterer_ratings.infra.db.queue_claims import claim_token_for

QUEUE_KINDS = ("mapping", "rating", "episode_ratings")


def queue_order_for_worker(worker_id: int, worker_count: int) -> tuple[str, ...]:
    """Reserve roughly half the workers for mappings and 3/8 for ratings."""
    safe_count = max(1, int(worker_count))
    safe_id = max(1, min(safe_count, int(worker_id)))
    if safe_count == 1:
        return ("mapping", "rating", "episode_ratings")

    mapping_workers = max(1, safe_count // 2)
    rating_workers = max(1, (safe_count * 3) // 8)
    if mapping_workers + rating_workers > safe_count:
        rating_workers = safe_count - mapping_workers

    if safe_id <= mapping_workers:
        return ("mapping", "rating", "episode_ratings")
    if safe_id <= mapping_workers + rating_workers:
        return ("rating", "mapping", "episode_ratings")
    return ("episode_ratings", "rating", "mapping")


async def worker_loop(
    *,
    stop_event: asyncio.Event,
    db: Any,
    poll_seconds: float,
    now_epoch_fn: Any,
    submit_mapping_group_fn: Any,
    submit_rating_fn: Any,
    submit_episode_ratings_batch_fn: Any,
    active_claims: set[str],
    episode_batch_size: int = 50,
    queue_order: Sequence[str] = QUEUE_KINDS,
) -> None:
    while not stop_event.is_set():
        did_work = False

        now_ts = now_epoch_fn()
        for kind in queue_order:
            if kind == "mapping":
                rows = db.claim_next_pending_mapping_group(now_ts)
                if not rows:
                    continue
                claim = rows[0]
                submission = submit_mapping_group_fn(rows)
            elif kind == "rating":
                claim = db.claim_next_pending_rating(now_ts)
                if claim is None:
                    continue
                submission = submit_rating_fn(claim)
            elif kind == "episode_ratings":
                rows = db.claim_next_pending_episode_ratings_batch(
                    now_ts=now_ts,
                    batch_size=episode_batch_size,
                )
                if not rows:
                    continue
                claim = rows[0]
                submission = submit_episode_ratings_batch_fn(rows)
            else:
                continue

            did_work = True
            token = claim_token_for(claim)
            if token is not None:
                active_claims.add(token)
            try:
                await submission
            finally:
                if token is not None:
                    active_claims.discard(token)
            break

        if not did_work:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                pass

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import Any, Callable, Sequence

from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.domain.models import IMDbEpisodeArchiveCandidate


def episode_coordinates(payload: Any) -> dict[str, int] | None:
    """Require one unambiguous episode identity; never infer from IMDb numbering."""
    rows = payload.get("tv_episode_results") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        return None
    row = rows[0]
    names = ("show_id", "season_number", "episode_number")
    if any(type(row.get(name)) is not int for name in names):
        return None
    values = {name: int(row[name]) for name in names}
    if values["show_id"] <= 0 or values["season_number"] < 1 or values["episode_number"] < 1:
        return None
    return values


async def verify_episode_identities(
    *, entries: Sequence[IMDbEpisodeArchiveCandidate], tmdb_id: int, db: Any,
    tmdb_client: Any, stop_event: asyncio.Event, now_epoch_fn: Callable[[], int],
    concurrency: int = 8,
) -> tuple[list[IMDbEpisodeArchiveCandidate], int, int]:
    verified: list[IMDbEpisodeArchiveCandidate] = []
    failures = skipped = 0
    queue: asyncio.Queue[IMDbEpisodeArchiveCandidate] = asyncio.Queue()
    for entry in entries:
        queue.put_nowait(entry)

    async def worker() -> None:
        nonlocal failures, skipped
        while not stop_event.is_set():
            try:
                entry = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            imdb_id = normalize_imdb_title_id(entry.episode_imdb_id)
            if not imdb_id:
                skipped += 1
                continue
            now_ts = now_epoch_fn()
            cached = db.conn.execute(
                "SELECT * FROM imdb_episode_identity WHERE imdb_id=?", (imdb_id,)
            ).fetchone()
            if cached and cached["next_due"] > now_ts:
                coordinates = json.loads(cached["payload"]) if cached["payload"] else None
                retryable = bool(cached["retryable"])
            else:
                coordinates = None
                retryable = True
                try:
                    response = await tmdb_client.fetch_find_by_imdb(imdb_id)
                    if response.ok and isinstance(response.data, dict) and isinstance(
                        response.data.get("tv_episode_results"), list
                    ):
                        coordinates = episode_coordinates(response.data)
                        retryable = False
                    elif response.status == 404:
                        retryable = False
                except (TimeoutError, OSError):
                    pass
                with db.conn:
                    db.conn.execute("""
                        INSERT INTO imdb_episode_identity VALUES (?, ?, ?, ?)
                        ON CONFLICT(imdb_id) DO UPDATE SET payload=excluded.payload,
                            retryable=excluded.retryable, next_due=excluded.next_due
                    """, (imdb_id, json.dumps(coordinates) if coordinates else None,
                          retryable, now_ts + (300 if retryable else (7 * 86400 if coordinates else 86400))))
            if retryable:
                failures += 1
            elif coordinates and coordinates["show_id"] == tmdb_id:
                verified.append(replace(
                    entry, season=coordinates["season_number"], episode=coordinates["episode_number"],
                ))
            else:
                skipped += 1

    workers = [asyncio.create_task(worker()) for _ in range(min(max(1, concurrency), len(entries)))]
    await asyncio.gather(*workers)
    # A conflicting pair must not race to overwrite a single PMDB episode slot.
    by_slot: dict[tuple[int, int], list[IMDbEpisodeArchiveCandidate]] = {}
    for entry in verified:
        by_slot.setdefault((entry.season, entry.episode), []).append(entry)
    unique = [rows[0] for rows in by_slot.values() if len({r.episode_imdb_id for r in rows}) == 1]
    skipped += len(verified) - len(unique)
    return unique, failures, skipped

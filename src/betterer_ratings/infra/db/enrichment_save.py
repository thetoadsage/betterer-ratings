from __future__ import annotations

import sqlite3
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from betterer_ratings.core.clock import local_day_key


def _record_new_fields(conn: sqlite3.Connection, kind: str, count: int, now_ts: int) -> None:
    if count <= 0:
        return
    for suffix in ("total", f"day:{local_day_key(now_ts)}"):
        conn.execute("""
            INSERT INTO state(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=CAST(state.value AS INTEGER)+excluded.value
        """, (f"metrics:harvest_contributed:{kind}:{suffix}", str(count)))


def save_enriched_item(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    title: str,
    imdb_id: Optional[str],
    popularity: float,
    tmdb_vote_average: Optional[float],
    enrichment_error: Optional[str],
    ratings: Dict[str, float],
    mappings: Dict[str, str],
    now_ts: int,
    supported_pmdb_mapping_types: Sequence[str],
    clamp_0_100_fn: Callable[[Any], Optional[float]],
    mdblist_attempted: bool = True,
    upsert_title_fn: Callable[..., None],
    upsert_rating_fn: Callable[..., bool],
    upsert_mapping_fn: Callable[..., bool],
) -> Tuple[int, int]:
    queued_ratings = 0
    queued_mappings = 0
    new_ratings = 0
    new_mappings = 0
    with conn:
        upsert_title_fn(
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title,
            imdb_id=imdb_id,
            popularity=popularity,
            tmdb_vote_average=tmdb_vote_average,
            now_ts=now_ts,
            error_message=enrichment_error,
            mdblist_attempted=mdblist_attempted,
        )

        for label, score in ratings.items():
            safe_score = clamp_0_100_fn(score)
            if safe_score is None:
                continue
            first = conn.execute(
                "SELECT 1 FROM ratings WHERE tmdb_id=? AND media_type=? AND label=?",
                (tmdb_id, media_type, label.upper()),
            ).fetchone() is None
            queued = upsert_rating_fn(
                tmdb_id=tmdb_id,
                media_type=media_type,
                label=label.upper(),
                score=safe_score,
                fetched_at=now_ts,
            )
            if queued:
                queued_ratings += 1
                new_ratings += int(first)

        for id_type, id_value in mappings.items():
            if not id_value:
                continue
            normalized_type = id_type.lower()
            if normalized_type not in supported_pmdb_mapping_types:
                continue
            first = conn.execute(
                "SELECT 1 FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type=?",
                (tmdb_id, media_type, normalized_type),
            ).fetchone() is None
            queued = upsert_mapping_fn(
                tmdb_id=tmdb_id,
                media_type=media_type,
                id_type=normalized_type,
                id_value=str(id_value),
                fetched_at=now_ts,
            )
            if queued:
                queued_mappings += 1
                new_mappings += int(first)
        _record_new_fields(conn, "ratings", new_ratings, now_ts)
        _record_new_fields(conn, "mappings", new_mappings, now_ts)
    return queued_ratings, queued_mappings


def save_imdb_episode_ratings(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    imdb_parent_id: str,
    entries: Sequence[Any],
    now_ts: int,
    default_label: str = "IM",
    parse_int_fn: Callable[[Any], Optional[int]],
    clamp_0_100_fn: Callable[[Any], Optional[float]],
    normalize_imdb_title_id_fn: Callable[[Any], Optional[str]],
    upsert_episode_rating_fn: Callable[..., bool],
) -> int:
    queued = 0
    safe_media = str(media_type or "").strip().lower()
    if parse_int_fn(tmdb_id) is None or safe_media not in {"movie", "tv"}:
        return 0
    normalized_parent = normalize_imdb_title_id_fn(imdb_parent_id)
    if not normalized_parent:
        return 0
    safe_label = str(default_label or "IM").strip().upper() or "IM"
    with conn:
        for entry in entries:
            safe_season = parse_int_fn(entry.season)
            safe_episode = parse_int_fn(entry.episode)
            safe_score = clamp_0_100_fn(entry.score)
            if (
                safe_season is None
                or safe_episode is None
                or safe_season < 1
                or safe_episode < 1
                or safe_score is None
            ):
                continue
            safe_episode_imdb = normalize_imdb_title_id_fn(entry.episode_imdb_id)
            safe_votes = parse_int_fn(entry.votes)
            queued_now = upsert_episode_rating_fn(
                tmdb_id=int(tmdb_id),
                media_type=safe_media,
                season=int(safe_season),
                episode=int(safe_episode),
                label=safe_label,
                score=float(safe_score),
                fetched_at=int(now_ts),
                imdb_parent_id=normalized_parent,
                imdb_episode_id=safe_episode_imdb,
                votes=int(safe_votes or 0),
            )
            if queued_now:
                queued += 1
    return queued

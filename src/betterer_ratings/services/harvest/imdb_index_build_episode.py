from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple, cast

from betterer_ratings.services.harvest.imdb_archive_refresh_utils import safe_unlink
from betterer_ratings.services.harvest.imdb_index_io import atomic_index_writer


def build_imdb_episode_fingerprint(
    *,
    source: Any,
) -> Tuple[str, Path, Path, Path]:
    ratings_path = source.path / "title.ratings.tsv"
    basics_path = source.path / "title.basics.tsv"
    episode_path = source.path / "title.episode.tsv"
    if not ratings_path.exists():
        raise FileNotFoundError(f"IMDb ratings file missing: {ratings_path}")
    if not basics_path.exists():
        raise FileNotFoundError(f"IMDb basics file missing: {basics_path}")
    if not episode_path.exists():
        raise FileNotFoundError(f"IMDb episode file missing: {episode_path}")

    ratings_stat = ratings_path.stat()
    basics_stat = basics_path.stat()
    episode_stat = episode_path.stat()
    payload = {
        "ratings": {
            "path": str(ratings_path.resolve()),
            "size": int(ratings_stat.st_size),
            "mtime_ns": int(ratings_stat.st_mtime_ns),
        },
        "basics": {
            "path": str(basics_path.resolve()),
            "size": int(basics_stat.st_size),
            "mtime_ns": int(basics_stat.st_mtime_ns),
        },
        "episode": {
            "path": str(episode_path.resolve()),
            "size": int(episode_stat.st_size),
            "mtime_ns": int(episode_stat.st_mtime_ns),
        },
        "index": {
            "version": 2,
            "order": "parent_votes_desc,season_asc,episode_asc",
        },
        "source": {
            "min_votes": int(source.min_votes),
            "title_batch_size": int(source.title_batch_size),
        },
    }
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        episode_path,
        ratings_path,
        basics_path,
    )


def rebuild_imdb_episode_index(
    *,
    index_path: Path,
    source: Any,
    episode_path: Path,
    ratings_path: Path,
    basics_path: Path,
    parse_int_fn: Callable[[Any], Optional[int]],
    is_valid_imdb_title_id_fn: Callable[[str], bool],
    clamp_0_100_fn: Callable[[Any], Optional[float]],
    now_epoch_fn: Callable[[], int],
    logger: Any,
) -> int:
    started_at = now_epoch_fn()
    allowed_types = {str(x or "").strip().lower() for x in source.types}
    allowed_parent_ids: Set[str] = set()
    with basics_path.open("r", encoding="utf-8", errors="replace") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        header_map = {name: idx for idx, name in enumerate(header)}
        tconst_idx = header_map.get("tconst")
        type_idx = header_map.get("titleType")
        start_year_idx = header_map.get("startYear")
        if tconst_idx is None or type_idx is None or start_year_idx is None:
            raise ValueError("IMDb basics header is missing required columns")

        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if max(tconst_idx, type_idx, start_year_idx) >= len(parts):
                continue
            imdb_id = str(parts[tconst_idx]).strip()
            if not is_valid_imdb_title_id_fn(imdb_id):
                continue
            title_type = str(parts[type_idx]).strip().lower()
            if title_type not in allowed_types:
                continue
            if source.exclude_unknown_year and str(parts[start_year_idx]).strip() == "\\N":
                continue
            allowed_parent_ids.add(imdb_id)

    ratings_lookup: Dict[str, Tuple[int, float]] = {}
    with ratings_path.open("r", encoding="utf-8", errors="replace") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        header_map = {name: idx for idx, name in enumerate(header)}
        tconst_idx = header_map.get("tconst")
        avg_idx = header_map.get("averageRating")
        votes_idx = header_map.get("numVotes")
        if tconst_idx is None or avg_idx is None or votes_idx is None:
            raise ValueError("IMDb ratings header is missing required columns")

        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if max(tconst_idx, avg_idx, votes_idx) >= len(parts):
                continue
            imdb_id = str(parts[tconst_idx]).strip()
            if not is_valid_imdb_title_id_fn(imdb_id):
                continue
            votes = parse_int_fn(parts[votes_idx])
            if votes is None or votes < source.min_votes:
                continue
            try:
                average_rating = float(str(parts[avg_idx]).strip())
            except (TypeError, ValueError):
                continue
            safe_score = clamp_0_100_fn(average_rating * 10.0)
            if safe_score is None:
                continue
            ratings_lookup[imdb_id] = (int(votes), float(safe_score))

    builder_path = index_path.with_suffix(".build.sqlite3")
    builder_wal_path = Path(f"{builder_path}-wal")
    builder_shm_path = Path(f"{builder_path}-shm")
    safe_unlink(builder_path)
    safe_unlink(builder_wal_path)
    safe_unlink(builder_shm_path)

    total = 0
    builder_conn = sqlite3.connect(builder_path)
    try:
        with builder_conn:
            builder_conn.execute("PRAGMA journal_mode=OFF")
            builder_conn.execute("PRAGMA synchronous=OFF")
            builder_conn.execute("PRAGMA temp_store=MEMORY")
            builder_conn.execute(
                """
                CREATE TABLE episode_candidates (
                    parent_imdb_id TEXT NOT NULL,
                    episode_imdb_id TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    episode INTEGER NOT NULL,
                    episode_votes INTEGER NOT NULL,
                    score REAL NOT NULL,
                    parent_votes INTEGER NOT NULL
                )
                """
            )

        insert_buffer: List[Tuple[str, str, int, int, int, float, int]] = []
        insert_buffer_max = 5000
        with episode_path.open("r", encoding="utf-8", errors="replace") as in_handle:
            header = in_handle.readline().rstrip("\n").split("\t")
            header_map = {name: idx for idx, name in enumerate(header)}
            tconst_idx = header_map.get("tconst")
            parent_idx = header_map.get("parentTconst")
            season_idx = header_map.get("seasonNumber")
            episode_idx = header_map.get("episodeNumber")
            if (
                tconst_idx is None
                or parent_idx is None
                or season_idx is None
                or episode_idx is None
            ):
                raise ValueError("IMDb episode header is missing required columns")

            for raw_line in in_handle:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if max(tconst_idx, parent_idx, season_idx, episode_idx) >= len(parts):
                    continue

                episode_imdb_id = str(parts[tconst_idx]).strip()
                parent_imdb_id = str(parts[parent_idx]).strip()
                season = parse_int_fn(parts[season_idx])
                episode = parse_int_fn(parts[episode_idx])
                if (
                    not is_valid_imdb_title_id_fn(episode_imdb_id)
                    or not is_valid_imdb_title_id_fn(parent_imdb_id)
                    or season is None
                    or episode is None
                    or season < 1
                    or episode < 1
                    or parent_imdb_id not in allowed_parent_ids
                ):
                    continue

                episode_rating = ratings_lookup.get(episode_imdb_id)
                parent_rating = ratings_lookup.get(parent_imdb_id)
                if episode_rating is None or parent_rating is None:
                    continue

                episode_votes, episode_score = episode_rating
                parent_votes, _parent_score = parent_rating
                insert_buffer.append(
                    (
                        parent_imdb_id,
                        episode_imdb_id,
                        int(season),
                        int(episode),
                        int(episode_votes),
                        float(episode_score),
                        int(parent_votes),
                    )
                )

                if len(insert_buffer) >= insert_buffer_max:
                    with builder_conn:
                        builder_conn.executemany(
                            """
                            INSERT INTO episode_candidates(
                                parent_imdb_id,
                                episode_imdb_id,
                                season,
                                episode,
                                episode_votes,
                                score,
                                parent_votes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            insert_buffer,
                        )
                    insert_buffer.clear()

        if insert_buffer:
            with builder_conn:
                builder_conn.executemany(
                    """
                    INSERT INTO episode_candidates(
                        parent_imdb_id,
                        episode_imdb_id,
                        season,
                        episode,
                        episode_votes,
                        score,
                        parent_votes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_buffer,
                )

        with atomic_index_writer(index_path) as out_handle:
            cursor = builder_conn.execute(
                """
                SELECT
                    parent_imdb_id,
                    episode_imdb_id,
                    season,
                    episode,
                    episode_votes,
                    score
                FROM episode_candidates
                ORDER BY
                    parent_votes DESC,
                    parent_imdb_id ASC,
                    season ASC,
                    episode ASC,
                    episode_imdb_id ASC
                """
            )
            for row in cursor:
                out_handle.write(
                    (
                        f"{str(row[0]).strip()}\t{str(row[1]).strip()}\t"
                        f"{int(row[2])}\t{int(row[3])}\t{int(row[4])}\t"
                        f"{float(row[5]):.1f}\n"
                    )
                )
                total += 1
    finally:
        builder_conn.close()
        safe_unlink(builder_path)
        safe_unlink(builder_wal_path)
        safe_unlink(builder_shm_path)

    elapsed = max(1, now_epoch_fn() - started_at)
    logger.info(
        "[IMDbArchive] Rebuilt episode index: rows=%s path=%s (%.1f rows/s)",
        total,
        index_path,
        total / elapsed if elapsed > 0 else 0.0,
    )
    return total


async def ensure_imdb_episode_index(
    *,
    harvester: Any,
    source: Any,
    refresh_imdb_archives_if_due_fn: Callable[[Any], Awaitable[None]],
    build_imdb_episode_fingerprint_fn: Callable[[Any], Tuple[str, Path, Path, Path]],
    rebuild_imdb_episode_index_fn: Callable[..., int],
) -> int:
    self = harvester
    await refresh_imdb_archives_if_due_fn(source)
    fingerprint, episode_path, ratings_path, basics_path = build_imdb_episode_fingerprint_fn(source)
    existing_fingerprint = self.db.get_state(self._imdb_episode_fingerprint_key)
    existing_total = cast(int, max(0, self.db.get_state_int(self._imdb_episode_total_key, 0)))
    if (
        existing_fingerprint == fingerprint
        and self._imdb_episode_index_path.exists()
        and existing_total >= 0
    ):
        return existing_total
    total = await asyncio.to_thread(
        rebuild_imdb_episode_index_fn,
        index_path=self._imdb_episode_index_path,
        source=source,
        episode_path=episode_path,
        ratings_path=ratings_path,
        basics_path=basics_path,
    )
    self._reset_imdb_episode_cursor(exhausted=(total == 0))
    self.db.set_state(self._imdb_episode_total_key, total)
    self.db.set_state(self._imdb_episode_fingerprint_key, fingerprint)
    return total

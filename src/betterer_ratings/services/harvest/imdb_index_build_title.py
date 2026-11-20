from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, cast

from betterer_ratings.services.harvest.imdb_index_io import atomic_index_writer


def build_imdb_fingerprint(
    *,
    source: Any,
) -> Tuple[str, Path, Path]:
    ratings_path = source.path / "title.ratings.tsv"
    basics_path = source.path / "title.basics.tsv"
    if not ratings_path.exists():
        raise FileNotFoundError(f"IMDb ratings file missing: {ratings_path}")
    if not basics_path.exists():
        raise FileNotFoundError(f"IMDb basics file missing: {basics_path}")

    ratings_stat = ratings_path.stat()
    basics_stat = basics_path.stat()
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
        "source": {
            "min_votes": int(source.min_votes),
            "types": list(source.types),
            "exclude_unknown_year": bool(source.exclude_unknown_year),
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")), ratings_path, basics_path


def rebuild_imdb_index(
    *,
    index_path: Path,
    source: Any,
    ratings_path: Path,
    basics_path: Path,
    parse_int_fn: Callable[[Any], Optional[int]],
    is_valid_imdb_title_id_fn: Callable[[str], bool],
    imdb_title_type_to_media_type_fn: Callable[[str], Optional[str]],
    now_epoch_fn: Callable[[], int],
    logger: Any,
) -> int:
    started_at = now_epoch_fn()
    allowed_types = {str(x or "").strip().lower() for x in source.types}
    allowed_media_by_imdb: Dict[str, str] = {}

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
            media_type = imdb_title_type_to_media_type_fn(title_type)
            if media_type is None:
                continue
            allowed_media_by_imdb[imdb_id] = media_type

    ranked: List[Tuple[int, str, str, Optional[float]]] = []
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
            media_type = allowed_media_by_imdb.get(imdb_id)
            if media_type is None:
                continue
            votes = parse_int_fn(parts[votes_idx])
            if votes is None or votes < source.min_votes:
                continue
            average_rating: Optional[float] = None
            raw_avg = str(parts[avg_idx]).strip()
            if raw_avg and raw_avg != "\\N":
                try:
                    average_rating = float(raw_avg)
                except (TypeError, ValueError):
                    average_rating = None
            ranked.append((votes, imdb_id, media_type, average_rating))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    with atomic_index_writer(index_path) as handle:
        for votes, imdb_id, media_type, average_rating in ranked:
            avg_text = "" if average_rating is None else f"{average_rating:.1f}"
            handle.write(f"{imdb_id}\t{media_type}\t{votes}\t{avg_text}\n")

    total = len(ranked)
    elapsed = max(1, now_epoch_fn() - started_at)
    logger.info(
        "[IMDbArchive] Rebuilt ranked index: total=%s path=%s (%.1f items/s)",
        total,
        index_path,
        total / elapsed if elapsed > 0 else 0.0,
    )
    return total


async def ensure_imdb_index(
    *,
    harvester: Any,
    source: Any,
    refresh_imdb_archives_if_due_fn: Callable[[Any], Awaitable[None]],
    build_imdb_fingerprint_fn: Callable[[Any], Tuple[str, Path, Path]],
    rebuild_imdb_index_fn: Callable[..., int],
) -> int:
    self = harvester
    await refresh_imdb_archives_if_due_fn(source)
    fingerprint, ratings_path, basics_path = build_imdb_fingerprint_fn(source)
    existing_fingerprint = self.db.get_state(self._imdb_fingerprint_key)
    existing_total = cast(int, max(0, self.db.get_state_int(self._imdb_total_key, 0)))
    if (
        existing_fingerprint == fingerprint
        and self._imdb_index_path.exists()
        and existing_total >= 0
    ):
        return existing_total
    total = await asyncio.to_thread(
        rebuild_imdb_index_fn,
        index_path=self._imdb_index_path,
        source=source,
        ratings_path=ratings_path,
        basics_path=basics_path,
    )
    self._reset_imdb_cursor(exhausted=(total == 0))
    self.db.set_state(self._imdb_total_key, total)
    self.db.set_state(self._imdb_fingerprint_key, fingerprint)
    return total

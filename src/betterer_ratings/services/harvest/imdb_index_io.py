from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, List, Optional, Set, Tuple


def reset_cursor(
    *,
    db: Any,
    cursor_line_key: str,
    cursor_byte_key: str,
    exhausted_key: str,
    exhausted: bool = False,
) -> None:
    db.set_state(cursor_line_key, 0)
    db.set_state(cursor_byte_key, 0)
    db.set_state(exhausted_key, 1 if exhausted else 0)


def commit_cursor(
    *,
    db: Any,
    cursor_line_key: str,
    cursor_byte_key: str,
    exhausted_key: str,
    cursor_line: int,
    cursor_byte: int,
    exhausted: bool,
) -> None:
    db.set_state(cursor_line_key, max(0, int(cursor_line)))
    db.set_state(cursor_byte_key, max(0, int(cursor_byte)))
    db.set_state(exhausted_key, 1 if exhausted else 0)


def read_imdb_index_batch(
    *,
    db: Any,
    source: Any,
    imdb_exhausted_key: str,
    imdb_cursor_line_key: str,
    imdb_cursor_byte_key: str,
    imdb_total_key: str,
    imdb_index_path: Path,
    parse_int_fn: Callable[[Any], Optional[int]],
    is_valid_imdb_title_id_fn: Callable[[str], bool],
    imdb_archive_candidate_cls: Any,
) -> Tuple[List[Any], int, int, bool]:
    if db.get_state_int(imdb_exhausted_key, 0) == 1:
        current_line = max(0, db.get_state_int(imdb_cursor_line_key, 0))
        current_byte = max(0, db.get_state_int(imdb_cursor_byte_key, 0))
        return [], current_line, current_byte, True
    if not imdb_index_path.exists():
        current_line = max(0, db.get_state_int(imdb_cursor_line_key, 0))
        current_byte = max(0, db.get_state_int(imdb_cursor_byte_key, 0))
        return [], current_line, current_byte, True

    max_items = max(1, int(source.title_batch_size))
    cursor_line = max(0, db.get_state_int(imdb_cursor_line_key, 0))
    cursor_byte = max(0, db.get_state_int(imdb_cursor_byte_key, 0))
    total_lines = max(0, db.get_state_int(imdb_total_key, 0))

    batch: List[Any] = []
    lines_read = 0
    eof_reached = False
    file_size = imdb_index_path.stat().st_size
    if cursor_byte > file_size:
        cursor_byte = 0
        cursor_line = 0

    with imdb_index_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(cursor_byte)
        while len(batch) < max_items:
            line = handle.readline()
            if not line:
                eof_reached = True
                break
            lines_read += 1
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            parts = stripped.split("\t")
            if len(parts) < 3:
                continue
            imdb_id = str(parts[0]).strip()
            media_type = str(parts[1]).strip().lower()
            votes = parse_int_fn(parts[2])
            if (
                not is_valid_imdb_title_id_fn(imdb_id)
                or media_type not in {"movie", "tv"}
                or votes is None
            ):
                continue
            average_rating: Optional[float] = None
            if len(parts) >= 4 and str(parts[3]).strip():
                try:
                    average_rating = float(parts[3])
                except (TypeError, ValueError):
                    average_rating = None
            batch.append(
                imdb_archive_candidate_cls(
                    imdb_id=imdb_id,
                    media_type=media_type,
                    num_votes=votes,
                    average_rating=average_rating,
                )
            )
        next_byte = handle.tell()

    new_cursor_line = cursor_line + lines_read
    exhausted = eof_reached or (total_lines > 0 and new_cursor_line >= total_lines)
    return batch, new_cursor_line, next_byte, exhausted


def read_imdb_episode_index_batch(
    *,
    db: Any,
    source: Any,
    day_key: str,
    imdb_episode_exhausted_key: str,
    imdb_episode_cursor_line_key: str,
    imdb_episode_cursor_byte_key: str,
    imdb_episode_total_key: str,
    imdb_episode_index_path: Path,
    parse_int_fn: Callable[[Any], Optional[int]],
    is_valid_imdb_title_id_fn: Callable[[str], bool],
    clamp_0_100_fn: Callable[[Any], Optional[float]],
    imdb_episode_archive_candidate_cls: Any,
) -> Tuple[List[Any], int, int, bool]:
    if db.get_state_int(imdb_episode_exhausted_key, 0) == 1:
        current_line = max(
            0,
            db.get_state_int(imdb_episode_cursor_line_key, 0),
        )
        current_byte = max(
            0,
            db.get_state_int(imdb_episode_cursor_byte_key, 0),
        )
        return [], current_line, current_byte, True
    if not imdb_episode_index_path.exists():
        current_line = max(
            0,
            db.get_state_int(imdb_episode_cursor_line_key, 0),
        )
        current_byte = max(
            0,
            db.get_state_int(imdb_episode_cursor_byte_key, 0),
        )
        return [], current_line, current_byte, True

    title_batch_size = max(1, int(source.title_batch_size))
    cursor_line = max(0, db.get_state_int(imdb_episode_cursor_line_key, 0))
    cursor_byte = max(0, db.get_state_int(imdb_episode_cursor_byte_key, 0))
    total_lines = max(0, db.get_state_int(imdb_episode_total_key, 0))
    file_size = imdb_episode_index_path.stat().st_size
    if cursor_byte > file_size:
        cursor_byte = 0
        cursor_line = 0

    del day_key

    batch: List[Any] = []
    lines_read = 0
    eof_reached = False
    cycle_titles_seen: Set[str] = set()
    # Episode identity lookups must not monopolize a whole harvest cycle.
    max_rows = max(1, min(1000, title_batch_size * 250))

    with imdb_episode_index_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(cursor_byte)
        while len(batch) < max_rows:
            line_start = handle.tell()
            line = handle.readline()
            if not line:
                eof_reached = True
                break
            lines_read += 1
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            parts = stripped.split("\t")
            if len(parts) < 6:
                continue

            parent_imdb_id = str(parts[0]).strip()
            episode_imdb_id = str(parts[1]).strip()
            season = parse_int_fn(parts[2])
            episode = parse_int_fn(parts[3])
            votes = parse_int_fn(parts[4])
            try:
                score = float(parts[5])
            except (TypeError, ValueError):
                score = -1.0

            if (
                not is_valid_imdb_title_id_fn(parent_imdb_id)
                or not is_valid_imdb_title_id_fn(episode_imdb_id)
                or season is None
                or episode is None
                or season < 1
                or episode < 1
                or votes is None
                or votes < 0
                or clamp_0_100_fn(score) is None
            ):
                continue

            if parent_imdb_id not in cycle_titles_seen:
                if len(cycle_titles_seen) >= title_batch_size:
                    lines_read = max(0, lines_read - 1)
                    handle.seek(line_start)
                    break
                cycle_titles_seen.add(parent_imdb_id)

            batch.append(
                imdb_episode_archive_candidate_cls(
                    parent_imdb_id=parent_imdb_id,
                    episode_imdb_id=episode_imdb_id,
                    season=int(season),
                    episode=int(episode),
                    score=float(score),
                    votes=int(votes),
                )
            )

        next_byte = handle.tell()

    new_cursor_line = cursor_line + lines_read
    exhausted = eof_reached or (total_lines > 0 and new_cursor_line >= total_lines)
    return batch, new_cursor_line, next_byte, exhausted

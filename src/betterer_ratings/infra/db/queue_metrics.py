from __future__ import annotations

import sqlite3
from typing import Callable, Dict, Tuple

from betterer_ratings.core.clock import local_day_bounds
from betterer_ratings.infra.db.anime_mapping_holds import MAPPING_ELIGIBLE_SQL


def queue_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM ratings WHERE pmdb_status IN ('pending', 'retry')) AS ratings_pending,
            (SELECT COUNT(*) FROM episode_ratings WHERE pmdb_status IN ('pending', 'retry')) AS episode_ratings_pending,
            (SELECT COUNT(*) FROM mappings WHERE pmdb_status IN ('pending', 'retry')) AS mappings_pending,
            (SELECT COUNT(*) FROM ratings WHERE pmdb_status = 'in_flight') AS ratings_in_flight,
            (SELECT COUNT(*) FROM episode_ratings WHERE pmdb_status = 'in_flight') AS episode_ratings_in_flight,
            (SELECT COUNT(*) FROM mappings WHERE pmdb_status = 'in_flight') AS mappings_in_flight,
            (SELECT COUNT(*) FROM ratings WHERE pmdb_status = 'failed') AS ratings_failed,
            (SELECT COUNT(*) FROM episode_ratings WHERE pmdb_status = 'failed') AS episode_ratings_failed,
            (SELECT COUNT(*) FROM mappings WHERE pmdb_status = 'failed') AS mappings_failed
        """
    ).fetchone()
    return {
        "ratings_pending": int(row["ratings_pending"]),
        "episode_ratings_pending": int(row["episode_ratings_pending"]),
        "mappings_pending": int(row["mappings_pending"]),
        "ratings_in_flight": int(row["ratings_in_flight"]),
        "episode_ratings_in_flight": int(row["episode_ratings_in_flight"]),
        "mappings_in_flight": int(row["mappings_in_flight"]),
        "ratings_failed": int(row["ratings_failed"]),
        "episode_ratings_failed": int(row["episode_ratings_failed"]),
        "mappings_failed": int(row["mappings_failed"]),
    }


def count_due_queue(conn: sqlite3.Connection, *, kind: str, now_ts: int) -> int:
    normalized_kind = str(kind).strip().lower()
    if normalized_kind in {"ratings", "rating"}:
        table = "ratings"
    elif normalized_kind in {"episode_ratings", "episode_rating", "episodes"}:
        table = "episode_ratings"
    else:
        table = "mappings"
    eligibility = f"AND {MAPPING_ELIGIBLE_SQL}" if table == "mappings" else ""
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM {table}
        WHERE pmdb_status IN ('pending', 'retry')
          AND pmdb_retry_after <= ?
          {eligibility}
        """,
        (int(now_ts),),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def submission_summary(
    conn: sqlite3.Connection,
    *,
    now_ts: int,
    local_day_bounds_fn: Callable[[int], Tuple[str, int, int]] = local_day_bounds,
) -> Dict[str, int | str]:
    day_key, start_ts, end_ts = local_day_bounds_fn(int(now_ts))
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM ratings WHERE pmdb_submitted_at >= ? AND pmdb_submitted_at < ?) AS ratings_today,
            (SELECT COUNT(*) FROM mappings WHERE pmdb_submitted_at >= ? AND pmdb_submitted_at < ?) AS mappings_today,
            (SELECT COUNT(*) FROM episode_ratings WHERE pmdb_submitted_at >= ? AND pmdb_submitted_at < ?) AS episode_ratings_today,
            (SELECT COUNT(*) FROM ratings) AS ratings_total,
            (SELECT COUNT(*) FROM mappings) AS mappings_total,
            (SELECT COUNT(*) FROM episode_ratings) AS episode_ratings_total,
            (SELECT COUNT(*) FROM titles) AS titles_total
        """,
        (
            start_ts,
            end_ts,
            start_ts,
            end_ts,
            start_ts,
            end_ts,
        ),
    ).fetchone()
    return {
        "day": day_key,
        "day_start_ts": start_ts,
        "day_end_ts": end_ts,
        "ratings_today": int(row["ratings_today"]),
        "mappings_today": int(row["mappings_today"]),
        "episode_ratings_today": int(row["episode_ratings_today"]),
        "ratings_total": int(row["ratings_total"]),
        "mappings_total": int(row["mappings_total"]),
        "episode_ratings_total": int(row["episode_ratings_total"]),
        "titles_total": int(row["titles_total"]),
    }

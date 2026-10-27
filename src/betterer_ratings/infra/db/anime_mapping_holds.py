from __future__ import annotations

import sqlite3
from typing import Any

# Correlated with the mappings table, shared by both claim paths and scheduling.
MAPPING_ELIGIBLE_SQL = """
    mappings.id_type NOT IN ('anilist', 'anidb')
    AND NOT EXISTS (
        SELECT 1 FROM anime_mapping_holds h
        WHERE h.tmdb_id=mappings.tmdb_id AND h.media_type=mappings.media_type
          AND h.id_type=mappings.id_type
    )
"""


def update_mal_hold(
    conn: sqlite3.Connection, *, tmdb_id: int, media_type: str, result: Any, now_ts: int,
) -> None:
    if result is None:
        return
    with conn:
        if result.validated and result.mal_id is not None:
            conn.execute("DELETE FROM anime_mapping_holds WHERE tmdb_id=? AND media_type=? AND id_type='mal'",
                         (tmdb_id, media_type))
        elif result.outcome in {
            'mapping_conflict', 'ambiguous_identity', 'correction_available',
            'invalid_mapping', 'invalid_response', 'not_found',
        } or result.outcome.startswith('skipped_'):
            conn.execute("""
                INSERT INTO anime_mapping_holds VALUES (?, ?, 'mal', ?, ?)
                ON CONFLICT(tmdb_id, media_type, id_type) DO UPDATE SET
                    reason=excluded.reason, updated_at=excluded.updated_at
            """, (tmdb_id, media_type, result.outcome, now_ts))
        # Outages/missing evidence never release a previous identity hold.


def defer_held_mapping(conn: sqlite3.Connection, row: Any) -> bool:
    """Final synchronous check for a hold added after a row was claimed."""
    allowed = conn.execute(f"""
        SELECT 1 FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type=?
          AND id_value=? AND {MAPPING_ELIGIBLE_SQL}
    """, (row['tmdb_id'], row['media_type'], row['id_type'], row['id_value'])).fetchone()
    if allowed:
        return False
    with conn:
        conn.execute("""
            UPDATE mappings SET pmdb_status='pending', pmdb_claimed_at=NULL
            WHERE tmdb_id=? AND media_type=? AND id_type=? AND pmdb_status='in_flight'
        """, (row['tmdb_id'], row['media_type'], row['id_type']))
    return True

from __future__ import annotations

import sqlite3
from typing import Mapping


def update_coverage_gaps(
    conn: sqlite3.Connection, *, tmdb_id: int, media_type: str,
    expected: Mapping[tuple[str, str], str], now_ts: int,
) -> None:
    """Record only fields with a known eligible source and no local contribution."""
    with conn:
        for (kind, field), outcome in expected.items():
            table, column = ("ratings", "label") if kind == "rating" else ("mappings", "id_type")
            present = conn.execute(
                f"SELECT 1 FROM {table} WHERE tmdb_id=? AND media_type=? AND {column}=?",
                (tmdb_id, media_type, field),
            ).fetchone()
            if present:
                conn.execute(
                    "DELETE FROM coverage_gaps WHERE tmdb_id=? AND media_type=? AND kind=? AND field=?",
                    (tmdb_id, media_type, kind, field),
                )
            else:
                conn.execute("""
                    INSERT INTO coverage_gaps VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tmdb_id, media_type, kind, field) DO UPDATE SET
                        outcome=excluded.outcome, updated_at=excluded.updated_at
                """, (tmdb_id, media_type, kind, field, outcome, now_ts))


def coverage_summary(conn: sqlite3.Connection) -> list[dict[str, object]]:
    return [dict(row) for row in conn.execute("""
        SELECT kind, field, outcome, COUNT(*) AS titles
        FROM coverage_gaps g
        WHERE (g.kind='rating' AND NOT EXISTS (
            SELECT 1 FROM ratings r WHERE r.tmdb_id=g.tmdb_id
                AND r.media_type=g.media_type AND r.label=g.field
        )) OR (g.kind='mapping' AND NOT EXISTS (
            SELECT 1 FROM mappings m WHERE m.tmdb_id=g.tmdb_id
                AND m.media_type=g.media_type AND m.id_type=g.field
        ))
        GROUP BY kind, field, outcome
        ORDER BY kind, field, outcome
    """)]

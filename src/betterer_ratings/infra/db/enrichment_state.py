from __future__ import annotations

import json
import sqlite3
from typing import Any


class EnrichmentStateMixin:
    conn: sqlite3.Connection

    def get_enrichment_state(self, tmdb_id: int, media_type: str, provider: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT payload, fetched_at, next_due FROM enrichment_state "
            "WHERE tmdb_id=? AND media_type=? AND provider=?",
            (tmdb_id, media_type, provider),
        ).fetchone()
        if row is None:
            return {"payload": None, "fetched_at": None, "next_due": 0}
        return {**dict(row), "payload": json.loads(row["payload"]) if row["payload"] else None}

    def save_enrichment_state(
        self, tmdb_id: int, media_type: str, provider: str, *,
        payload: dict[str, Any] | None, now_ts: int, next_due: int,
    ) -> None:
        # A deferred/failed request keeps its last successful data and timestamp.
        with self.conn:
            self.conn.execute("""
                INSERT INTO enrichment_state VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(tmdb_id, media_type, provider) DO UPDATE SET
                    payload=COALESCE(excluded.payload, enrichment_state.payload),
                    fetched_at=COALESCE(excluded.fetched_at, enrichment_state.fetched_at),
                    next_due=excluded.next_due
            """, (tmdb_id, media_type, provider,
                  json.dumps(payload) if payload is not None else None,
                  now_ts if payload is not None else None, next_due))

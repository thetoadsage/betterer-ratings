from __future__ import annotations

import json
import math
import sqlite3
from typing import Any

from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.domain.models import Candidate


class ProviderStateMixin:
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

    def select_provider_due_titles(
        self, *, now_ts: int, mdblist_pause_until: int, mal_enabled: bool, limit: int = 1000,
    ) -> list[sqlite3.Row]:
        safe_limit = max(0, int(limit))
        if safe_limit == 0:
            return []
        rows = self.conn.execute("""
            SELECT t.*, CASE WHEN t.last_harvested_at IS NULL THEN 'new' ELSE 'ttl' END
                AS harvest_reason,
                (SELECT COUNT(*) FROM coverage_gaps g
                 WHERE g.tmdb_id=t.tmdb_id AND g.media_type=t.media_type
                   AND g.outcome='no_data'
                   AND ((g.kind='rating' AND NOT EXISTS (
                       SELECT 1 FROM ratings r WHERE r.tmdb_id=g.tmdb_id
                           AND r.media_type=g.media_type AND r.label=g.field
                   )) OR (g.kind='mapping' AND NOT EXISTS (
                       SELECT 1 FROM mappings m WHERE m.tmdb_id=g.tmdb_id
                           AND m.media_type=g.media_type AND m.id_type=g.field
                   )))) AS missing_fields,
                (SELECT COUNT(*) FROM coverage_gaps g
                 WHERE g.tmdb_id=t.tmdb_id AND g.media_type=t.media_type
                   AND g.outcome='provider_unavailable'
                   AND ((g.kind='rating' AND NOT EXISTS (
                       SELECT 1 FROM ratings r WHERE r.tmdb_id=g.tmdb_id
                           AND r.media_type=g.media_type AND r.label=g.field
                   )) OR (g.kind='mapping' AND NOT EXISTS (
                       SELECT 1 FROM mappings m WHERE m.tmdb_id=g.tmdb_id
                           AND m.media_type=g.media_type AND m.id_type=g.field
                   )))) AS unavailable_fields,
                (SELECT COUNT(*) FROM coverage_gaps g
                 WHERE g.tmdb_id=t.tmdb_id AND g.media_type=t.media_type
                   AND g.outcome='ambiguous_identity'
                   AND ((g.kind='rating' AND NOT EXISTS (
                       SELECT 1 FROM ratings r WHERE r.tmdb_id=g.tmdb_id
                           AND r.media_type=g.media_type AND r.label=g.field
                   )) OR (g.kind='mapping' AND NOT EXISTS (
                       SELECT 1 FROM mappings m WHERE m.tmdb_id=g.tmdb_id
                           AND m.media_type=g.media_type AND m.id_type=g.field
                   )))) AS ambiguous_fields
            FROM titles t
            LEFT JOIN enrichment_state tm ON tm.tmdb_id=t.tmdb_id
                AND tm.media_type=t.media_type AND tm.provider='tmdb'
            LEFT JOIN enrichment_state md ON md.tmdb_id=t.tmdb_id
                AND md.media_type=t.media_type AND md.provider='mdblist'
            LEFT JOIN enrichment_state ml ON ml.tmdb_id=t.tmdb_id
                AND ml.media_type=t.media_type AND ml.provider='mal'
            WHERE COALESCE(tm.next_due, 0)<=?
                OR (?<=? AND COALESCE(md.next_due, 0)<=?)
                OR (? AND COALESCE(ml.next_due, 0)<=?)
            ORDER BY (missing_fields + unavailable_fields + ambiguous_fields) DESC,
                COALESCE(t.last_harvested_at, 0), t.media_type, t.tmdb_id
        """, (now_ts, mdblist_pause_until, now_ts, now_ts, mal_enabled, now_ts)).fetchall()
        first = [row for row in rows if row["last_harvested_at"] is None]
        refresh = [row for row in rows if row["last_harvested_at"] is not None]
        reserved = max(1, (safe_limit + 3) // 4) if first else 0
        selected = first[:reserved] + refresh[:safe_limit - reserved]
        if len(selected) < safe_limit:
            selected += first[reserved:reserved + safe_limit - len(selected)]
        if len(selected) < safe_limit:
            selected += refresh[safe_limit - reserved:safe_limit - reserved + safe_limit - len(selected)]
        return selected

    def save_archive_title_rating(self, candidate: Candidate, *, source_at: int, now_ts: int) -> bool:
        from betterer_ratings.core.scoring import score_to_tenths
        from betterer_ratings.infra.db.enrichment_upserts import upsert_rating

        imdb_id = normalize_imdb_title_id(candidate.archive_imdb_id)
        score = candidate.archive_rating
        if (not imdb_id or score is None or not math.isfinite(score)
                or not 0 < score <= 10 or candidate.archive_votes <= 0
                or source_at > now_ts or now_ts - source_at >= 2 * 86400):
            return False
        # Never attach an archive score to a title with a conflicting known identity.
        identities = self.conn.execute("""
            SELECT imdb_id AS value FROM titles WHERE tmdb_id=? AND media_type=?
            UNION ALL SELECT id_value FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type='imdb'
        """, (candidate.tmdb_id, candidate.media_type, candidate.tmdb_id, candidate.media_type)).fetchall()
        if any(row["value"] and normalize_imdb_title_id(row["value"]) != imdb_id for row in identities):
            return False
        state = self.get_enrichment_state(candidate.tmdb_id, candidate.media_type, "imdb")
        if state["fetched_at"] is not None and source_at < state["fetched_at"]:
            return False
        with self.conn:
            # Archive discovery must not mark other providers as harvested.
            self.conn.execute("""
                INSERT INTO titles (tmdb_id, media_type, title, imdb_id, popularity, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(tmdb_id, media_type) DO NOTHING
            """, (candidate.tmdb_id, candidate.media_type, candidate.title,
                  imdb_id, candidate.popularity, now_ts))
            queued = upsert_rating(
                self.conn, tmdb_id=candidate.tmdb_id, media_type=candidate.media_type,
                label="IM", score=score * 10, fetched_at=source_at,
                score_to_tenths_fn=score_to_tenths,
            )
            self.save_enrichment_state(
                candidate.tmdb_id, candidate.media_type, "imdb",
                payload={"imdb_id": imdb_id, "score": score * 10, "votes": candidate.archive_votes},
                now_ts=source_at, next_due=source_at + 2 * 86400,
            )
        return queued

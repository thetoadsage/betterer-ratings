from __future__ import annotations

import sqlite3


def mark_rating_retry(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    label: str,
    retry_after: int,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE ratings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'retry' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0,
                pmdb_retry_after = CASE WHEN pmdb_claim_changed THEN 0 ELSE ? END
            WHERE tmdb_id = ? AND media_type = ? AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (error_text, retry_after, tmdb_id, media_type, label, claim_token, claim_token),
        )


def mark_mapping_retry(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    id_type: str,
    retry_after: int,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE mappings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'retry' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0,
                pmdb_retry_after = CASE WHEN pmdb_claim_changed THEN 0 ELSE ? END
            WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (error_text, retry_after, tmdb_id, media_type, id_type, claim_token, claim_token),
        )


def mark_episode_rating_retry(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    season: int,
    episode: int,
    label: str,
    retry_after: int,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE episode_ratings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'retry' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0,
                pmdb_retry_after = CASE WHEN pmdb_claim_changed THEN 0 ELSE ? END
            WHERE tmdb_id = ?
              AND media_type = ?
              AND season = ?
              AND episode = ?
              AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (
                str(error_text),
                int(retry_after),
                int(tmdb_id),
                str(media_type),
                int(season),
                int(episode),
                str(label),
                claim_token,
                claim_token,
            ),
        )


def clear_rating_pmdb_item_id(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    label: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE ratings
            SET pmdb_item_id = NULL
            WHERE tmdb_id = ? AND media_type = ? AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (tmdb_id, media_type, label, claim_token, claim_token),
        )


def clear_mapping_pmdb_item_id(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    id_type: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE mappings
            SET pmdb_item_id = NULL,
                pmdb_item_value = NULL
            WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (tmdb_id, media_type, id_type, claim_token, claim_token),
        )


def clear_episode_rating_pmdb_item_id(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    season: int,
    episode: int,
    label: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE episode_ratings
            SET pmdb_item_id = NULL
            WHERE tmdb_id = ?
              AND media_type = ?
              AND season = ?
              AND episode = ?
              AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (tmdb_id, media_type, season, episode, label, claim_token, claim_token),
        )


def mark_rating_failed(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    label: str,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE ratings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'failed' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0
            WHERE tmdb_id = ? AND media_type = ? AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (error_text, tmdb_id, media_type, label, claim_token, claim_token),
        )


def mark_mapping_failed(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    id_type: str,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE mappings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'failed' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0
            WHERE tmdb_id = ? AND media_type = ? AND id_type = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (error_text, tmdb_id, media_type, id_type, claim_token, claim_token),
        )


def mark_episode_rating_failed(
    conn: sqlite3.Connection,
    *,
    tmdb_id: int,
    media_type: str,
    season: int,
    episode: int,
    label: str,
    error_text: str,
    claim_token: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE episode_ratings
            SET pmdb_status = CASE WHEN pmdb_claim_changed THEN 'pending' ELSE 'failed' END,
                pmdb_attempts = pmdb_attempts + CASE WHEN pmdb_claim_changed THEN 0 ELSE 1 END,
                pmdb_last_error = CASE WHEN pmdb_claim_changed THEN NULL ELSE ? END,
                pmdb_claimed_at = NULL,
                pmdb_claim_token = NULL,
                pmdb_claim_changed = 0
            WHERE tmdb_id = ?
              AND media_type = ?
              AND season = ?
              AND episode = ?
              AND label = ?
              AND (? IS NULL OR (pmdb_status = 'in_flight' AND pmdb_claim_token = ?))
            """,
            (
                str(error_text),
                int(tmdb_id),
                str(media_type),
                int(season),
                int(episode),
                str(label),
                claim_token,
                claim_token,
            ),
        )

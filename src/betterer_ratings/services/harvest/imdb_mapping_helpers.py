from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple, cast

from betterer_ratings.domain.models import Candidate


def extract_tmdb_from_find_payload(
    *,
    payload: Dict[str, Any],
    media_type: str,
    parse_int_fn: Callable[[Any], Optional[int]],
) -> Tuple[Optional[int], str, float]:
    result_key = "movie_results" if media_type == "movie" else "tv_results"
    result_items = payload.get(result_key)
    if not isinstance(result_items, list) or len(result_items) != 1:
        return None, "", 0.0
    first_item = result_items[0] if result_items else None
    if not isinstance(first_item, dict):
        return None, "", 0.0
    tmdb_id = parse_int_fn(first_item.get("id"))
    if tmdb_id is None:
        return None, "", 0.0
    if media_type == "movie":
        title = str(first_item.get("title") or "").strip()
    else:
        title = str(first_item.get("name") or "").strip()
    popularity = 0.0
    try:
        popularity = float(first_item.get("popularity") or 0.0)
    except (TypeError, ValueError):
        popularity = 0.0
    return tmdb_id, title, popularity


def resolve_imdb_to_tmdb_local(
    *,
    db: Any,
    imdb_cache: Any,
    imdb_id: str,
    media_type: str,
    normalize_imdb_title_id_fn: Callable[[Any], Optional[str]],
    parse_int_fn: Callable[[Any], Optional[int]],
    candidate_cls: Any = Candidate,
) -> Optional[Candidate]:
    normalized_imdb = normalize_imdb_title_id_fn(imdb_id)
    normalized_media = str(media_type or "").strip().lower()
    if not normalized_imdb or normalized_media not in {"movie", "tv"}:
        return None

    local_targets = db.conn.execute(
        """
        SELECT tmdb_id FROM mappings WHERE id_type='imdb' AND id_value=? AND media_type=?
        UNION SELECT tmdb_id FROM titles WHERE imdb_id=? AND media_type=?
        LIMIT 2
        """,
        (normalized_imdb, normalized_media, normalized_imdb, normalized_media),
    ).fetchall()
    if len(local_targets) > 1:
        # Let the caller perform an exact remote lookup rather than selecting
        # whichever conflicting local mapping happened to be refreshed last.
        return None

    mapping_row = db.conn.execute(
        """
        SELECT t.tmdb_id, t.title, t.popularity
        FROM mappings m
        JOIN titles t
          ON t.tmdb_id = m.tmdb_id
         AND t.media_type = m.media_type
        WHERE m.id_type = 'imdb'
          AND m.id_value = ?
          AND m.media_type = ?
        ORDER BY COALESCE(t.last_harvested_at, 0) DESC, t.tmdb_id ASC
        LIMIT 1
        """,
        (normalized_imdb, normalized_media),
    ).fetchone()
    if mapping_row is not None:
        tmdb_id = parse_int_fn(mapping_row["tmdb_id"])
        if tmdb_id is not None:
            title = str(mapping_row["title"] or "").strip() or f"TMDB-{tmdb_id}"
            try:
                popularity = float(mapping_row["popularity"] or 0.0)
            except (TypeError, ValueError):
                popularity = 0.0
            return cast(Candidate, candidate_cls(
                tmdb_id=tmdb_id,
                media_type=normalized_media,
                title=title,
                popularity=popularity,
            ))

    title_row = db.conn.execute(
        """
        SELECT tmdb_id, title, popularity
        FROM titles
        WHERE imdb_id = ? AND media_type = ?
        ORDER BY COALESCE(last_harvested_at, 0) DESC, tmdb_id ASC
        LIMIT 1
        """,
        (normalized_imdb, normalized_media),
    ).fetchone()
    if title_row is not None:
        tmdb_id = parse_int_fn(title_row["tmdb_id"])
        if tmdb_id is not None:
            title = str(title_row["title"] or "").strip() or f"TMDB-{tmdb_id}"
            try:
                popularity = float(title_row["popularity"] or 0.0)
            except (TypeError, ValueError):
                popularity = 0.0
            return cast(Candidate, candidate_cls(
                tmdb_id=tmdb_id,
                media_type=normalized_media,
                title=title,
                popularity=popularity,
            ))

    cached = imdb_cache.get(normalized_imdb, normalized_media)
    if cached is None:
        return None
    cached_tmdb_id, cached_title, cached_popularity = cached
    return cast(Candidate, candidate_cls(
        tmdb_id=int(cached_tmdb_id),
        media_type=normalized_media,
        title=str(cached_title or "").strip() or f"TMDB-{cached_tmdb_id}",
        popularity=float(cached_popularity or 0.0),
    ))

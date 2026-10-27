from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from betterer_ratings.services.harvest.dins_mappings import DinsMappings


def save_candidate_enrichment(
    *,
    db: Any,
    candidate: Any,
    details: Optional[Dict[str, Any]],
    md_item: Optional[Dict[str, Any]],
    now_ts: int,
    dins_mapping_cache: DinsMappings | None = None,
    parse_mdblist_ratings_fn: Callable[[Optional[Dict[str, Any]]], Dict[str, float]],
    parse_tmdb_vote_average_fn: Callable[[Optional[Dict[str, Any]]], Optional[float]],
    extract_mappings_fn: Callable[
        [str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]], Dict[str, str]
    ],
) -> Tuple[int, int, int, int, int]:
    ratings = parse_mdblist_ratings_fn(md_item)
    tm_score = parse_tmdb_vote_average_fn(details)
    if tm_score is not None:
        ratings["TM"] = tm_score

    mappings = extract_mappings_fn(candidate.media_type, details, md_item)
    if dins_mapping_cache is not None:
        mappings.update(dins_mapping_cache.supplements(
            db=db, media_type=candidate.media_type, tmdb_id=candidate.tmdb_id, mappings=mappings,
        ))
    imdb_id = mappings.get("imdb")

    error_reasons = []
    if details is None:
        error_reasons.append("TMDB details failed or unavailable")
    if md_item is None:
        error_reasons.append("MDBList item missing or unavailable")
    enrichment_error = "; ".join(error_reasons) if error_reasons else None

    queued_ratings, queued_mappings = db.save_enriched_item(
        tmdb_id=candidate.tmdb_id,
        media_type=candidate.media_type,
        title=(
            str((details or {}).get("title" if candidate.media_type == "movie" else "name") or candidate.title)
            if candidate.title == f"TMDB-{candidate.tmdb_id}" else candidate.title
        ),
        imdb_id=imdb_id,
        popularity=candidate.popularity,
        tmdb_vote_average=(details or {}).get("vote_average")
        if isinstance(details, dict)
        else None,
        enrichment_error=enrichment_error,
        ratings=ratings,
        mappings=mappings,
        now_ts=now_ts,
    )
    mdblist_ok = 1 if md_item is not None else 0
    mdblist_miss = 1 if md_item is None else 0
    tmdb_only = 1 if (md_item is None and details is not None) else 0
    return mdblist_ok, mdblist_miss, tmdb_only, queued_ratings, queued_mappings

from __future__ import annotations

import asyncio
import logging
import math
import unicodedata
from datetime import date as date_type
from typing import Any

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.scoring import clamp_0_100

LOGGER = logging.getLogger("betterer-ratings")


def _title(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "".join(c for c in unicodedata.normalize("NFKC", value).casefold() if c.isalnum())


def _date(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    date = value[:10]
    # Missing/partial dates cannot establish matching scope.
    try:
        date_type.fromisoformat(date)
    except ValueError:
        return ""
    return date


def eligibility_reason(media_type: str, details: dict[str, Any], anime: dict[str, Any]) -> str:
    tmdb_titles = {_title(details.get(k)) for k in ("title", "original_title", "name", "original_name")}
    mal_titles = {_title(anime.get(k)) for k in ("title", "title_english", "title_japanese")}
    titles = anime.get("titles")
    if isinstance(titles, list):
        mal_titles.update(_title(t.get("title")) for t in titles if isinstance(t, dict))
    if not (tmdb_titles & mal_titles) - {""}:
        return "title_mismatch"
    aired = anime.get("aired")
    if not isinstance(aired, dict):
        return "missing_air_dates"
    start = _date(aired.get("from"))
    if media_type == "movie":
        release = _date(details.get("release_date"))
        if anime.get("type") != "Movie":
            return "format_mismatch"
        return "" if start and release and start[:4] == release[:4] else "release_year_mismatch"
    if media_type != "tv" or anime.get("type") not in {"TV", "ONA"}:
        return "format_mismatch"
    if details.get("status") != "Ended" or anime.get("status") != "Finished Airing":
        return "incomplete_series"
    if details.get("number_of_seasons") != 1:
        return "multiple_or_unknown_seasons"
    episodes = anime.get("episodes")
    if type(episodes) is not int or episodes <= 0 or episodes != details.get("number_of_episodes"):
        return "episode_count_mismatch"
    end = _date(aired.get("to"))
    if not start or not end or start != _date(details.get("first_air_date")) or end != _date(details.get("last_air_date")):
        return "air_dates_mismatch"
    return ""


def mal_score(anime: dict[str, Any]) -> float | None:
    score = anime.get("score")
    votes = anime.get("scored_by")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return None
    if not math.isfinite(score) or not 0 < score <= 10:
        return None
    if type(votes) is not int or votes <= 0:
        return None
    return clamp_0_100(float(score) * 10)


async def fetch_candidate_mal_score(
    *, client: Any, db: Any, candidate: Any, details: Any, md_item: Any,
    stop_event: asyncio.Event,
) -> float | None:
    def outcome(reason: str, score: float | None = None) -> float | None:
        LOGGER.info(
            "[MAL] %s tmdb=%s/%s score=%s", reason, candidate.media_type,
            candidate.tmdb_id, score,
            extra={"event": "mal.enrichment", "outcome": reason},
        )
        return score

    if stop_event.is_set():
        return None
    mappings = extract_mappings(candidate.media_type, details, md_item)
    raw_id = mappings.get("mal")
    if raw_id is None:
        raw_id = db.get_title_mapping(
            tmdb_id=candidate.tmdb_id, media_type=candidate.media_type, id_type="mal"
        )
    # Most titles are not anime: avoid per-title noise and requests for these.
    if raw_id is None:
        return None
    if not str(raw_id).isascii() or not str(raw_id).isdigit() or int(raw_id) <= 0:
        return outcome("invalid_mapping")
    if not isinstance(details, dict):
        return outcome("missing_tmdb_details")
    if candidate.media_type == "tv" and (
        details.get("status") != "Ended" or details.get("number_of_seasons") != 1
    ):
        return outcome("skipped_series_scope")

    # Bound optional enrichment, including Retry-After sleeps, so failures cannot
    # hold a whole MDBList chunk indefinitely. Cancellation remains propagated.
    try:
        async with asyncio.timeout(35):
            response = await client.fetch_anime(int(raw_id))
    except TimeoutError:
        return outcome("timeout")
    if response.status != 200:
        return outcome("not_found" if response.status == 404 else "provider_failure")
    anime = response.data.get("data") if isinstance(response.data, dict) else None
    if not isinstance(anime, dict) or anime.get("mal_id") != int(raw_id):
        return outcome("invalid_response")
    reason = eligibility_reason(candidate.media_type, details, anime)
    if reason:
        return outcome("skipped_" + reason)
    score = mal_score(anime)
    return outcome("success" if score is not None else "missing_score", score)

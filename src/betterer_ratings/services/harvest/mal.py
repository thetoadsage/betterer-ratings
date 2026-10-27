from __future__ import annotations

import asyncio
import logging
import math
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import date as date_type
from typing import Any

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.scoring import clamp_0_100

LOGGER = logging.getLogger("betterer-ratings")


@dataclass(frozen=True)
class MALResult:
    mal_id: int | None
    provenance: str | None
    validated: bool
    outcome: str
    score: float | None = None
    candidates: list[dict[str, str]] = field(default_factory=list)
    proposed_mal_id: int | None = None
    conflicts: list[dict[str, str]] = field(default_factory=list)
    mapping_decisions: list[dict[str, Any]] = field(default_factory=list)


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
    stop_event: asyncio.Event, anime_mapping_cache: Any | None = None,
    jikan: Any | None = None,
    fribb_mapping_cache: Any | None = None,
) -> MALResult:
    source = "official_mal"
    identity_source: str | None = None
    raw_id: Any = None
    candidates: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []
    proposed_mal_id: int | None = None
    mapping_decisions: list[dict[str, Any]] = []

    def canonical_id(value: Any) -> str:
        text = str(value)
        return str(int(text)) if text.isascii() and text.isdigit() and int(text) > 0 else text

    def remember(value: Any, provenance: str) -> None:
        if value is not None:
            candidates.append({"mal_id": canonical_id(value), "source": provenance})

    def outcome(reason: str, score: float | None = None) -> MALResult:
        if reason in {"stopped", "missing_mapping", "skipped_series_scope", "skipped_dataset_scope"}:
            level = logging.DEBUG
        elif reason in {"mapping_conflict", "correction_available"}:
            level = logging.WARNING
        else:
            level = logging.INFO
        LOGGER.log(
            level,
            "[MAL] %s tmdb=%s/%s score=%s", reason, candidate.media_type,
            candidate.tmdb_id, score,
            extra={
                "event": "mal.enrichment", "outcome": reason, "source": source,
                "tmdb_id": candidate.tmdb_id, "media_type": candidate.media_type,
                "mal_id": raw_id, "score": score, "identity_source": identity_source,
                "candidates": candidates, "conflicts": conflicts,
                "proposed_mal_id": proposed_mal_id, "mapping_decisions": mapping_decisions,
            },
        )
        valid = reason in {"success", "missing_score"}
        return MALResult(
            int(raw_id) if valid else None, identity_source, valid, reason, score,
            candidates, proposed_mal_id, conflicts, mapping_decisions,
        )

    if stop_event.is_set():
        return outcome("stopped")
    if not isinstance(details, dict):
        return outcome("missing_tmdb_details")
    if candidate.media_type == "tv" and (
        details.get("status") != "Ended" or details.get("number_of_seasons") != 1
    ):
        return outcome("skipped_series_scope")
    mappings = extract_mappings(candidate.media_type, details, md_item)
    remember(mappings.get("mal"), "source_mapping")
    stored_mappings = {
        provider: db.get_title_mapping(
            tmdb_id=candidate.tmdb_id, media_type=candidate.media_type, id_type=provider
        ) for provider in ("mal", "anilist", "anidb")
    }
    remember(stored_mappings["mal"], "stored_mapping")
    # A conflicting bridge must not become evidence for either side.
    for provider, stored in stored_mappings.items():
        supplied = mappings.get(provider)
        if supplied is not None and stored is not None and canonical_id(supplied) != canonical_id(stored):
            conflicts.append({"provider": provider, "source_id": str(supplied), "stored_id": str(stored)})
    if conflicts:
        identity_source = "mapping_conflict"
        return outcome("mapping_conflict")
    known_mappings = {**mappings, **{
        k: v for k, v in stored_mappings.items() if v is not None
    }}
    if anime_mapping_cache is not None and any(
        known_mappings.get(p) is not None for p in ("anilist", "anidb")
    ):
        if hasattr(anime_mapping_cache, "resolve_decision"):
            decision = await anime_mapping_cache.resolve_decision(known_mappings)
            mapping_decisions.append({"source": "offline_bridge", **asdict(decision)})
            for value in decision.mal_ids:
                remember(value, "offline_bridge")
            if decision.outcome == "conflict":
                identity_source = "offline_bridge"
                return outcome("mapping_conflict")
        else:
            remember(await anime_mapping_cache.resolve(known_mappings), "offline_bridge")
    if fribb_mapping_cache is not None and not stop_event.is_set():
        # Compare the dataset candidate with the selected MAL ID, rather
        # than filtering away disagreement before we can report it.
        anchors = {k: v for k, v in known_mappings.items() if k != "mal"}
        if hasattr(fribb_mapping_cache, "resolve_decision"):
            decision = await fribb_mapping_cache.resolve_decision(
                media_type=candidate.media_type, tmdb_id=candidate.tmdb_id, mappings=anchors,
            )
            mapping_decisions.append({"source": "fribb_mapping", **asdict(decision)})
            if decision.outcome == "conflict":
                for value in decision.mal_ids:
                    remember(value, "fribb_mapping")
                identity_source = "fribb_mapping"
                return outcome("mapping_conflict")
            if decision.outcome == "unsafe_scope":
                identity_source = "fribb_mapping"
                return outcome("skipped_dataset_scope")
            remember(decision.mal_id, "fribb_mapping")
        else:
            fribb = await fribb_mapping_cache.resolve(
                media_type=candidate.media_type, tmdb_id=candidate.tmdb_id, mappings=anchors,
            )
            remember(fribb.get("mal"), "fribb_mapping")
    if len({c["mal_id"] for c in candidates}) > 1:
        identity_source = "mapping_conflict"
        return outcome("mapping_conflict")
    if candidates:
        raw_id = candidates[0]["mal_id"]
        identity_source = candidates[0]["source"]
    if raw_id is None and jikan is not None:
        if hasattr(jikan, "resolve_decision"):
            raw_id, discovery_outcome = await jikan.resolve_decision(candidate.media_type, details)
        else:
            raw_id = await jikan.resolve(candidate.media_type, details)
            discovery_outcome = "no_match"
        if discovery_outcome != "ineligible":
            identity_source = "jikan_discovery"
            remember(raw_id, identity_source)
    if raw_id is None or stop_event.is_set():
        if stop_event.is_set():
            return outcome("stopped")
        if jikan is not None and discovery_outcome == "ambiguous":
            return outcome("ambiguous_identity")
        if jikan is not None and discovery_outcome in {
            "provider_failure", "timeout", "incomplete_search", "invalid_response",
            "invalid_pagination", "repeated_page",
        }:
            return outcome("provider_unavailable")
        return outcome("missing_mapping")

    official_not_found = False

    async def validate(value: int) -> tuple[str, float | None]:
        nonlocal source, official_not_found
        # Bound provider waits and propagate cancellation.
        try:
            async with asyncio.timeout(35):
                response = await client.fetch_anime(value)
        except TimeoutError:
            response = None
        official_not_found = response is not None and response.status == 404
        if jikan is not None and jikan.config.fallback and not stop_event.is_set() and (
            response is None or response.status in {0, 404, 429} or response.status >= 500
        ):
            source = "jikan"
            try:
                async with asyncio.timeout(10):
                    response = await jikan.client.fetch_anime(value)
            except TimeoutError:
                return "timeout", None
        if response is None:
            return "timeout", None
        if response.status != 200:
            return ("not_found" if response.status == 404 else "provider_failure"), None
        anime = response.data.get("data") if isinstance(response.data, dict) else None
        if not isinstance(anime, dict) or anime.get("mal_id") != value:
            return "invalid_response", None
        reason = eligibility_reason(candidate.media_type, details, anime)
        if reason:
            return "skipped_" + reason, None
        score = mal_score(anime)
        return ("success" if score is not None else "missing_score"), score

    if not str(raw_id).isascii() or not str(raw_id).isdigit() or int(raw_id) <= 0:
        reason, score = "invalid_mapping", None
    else:
        reason, score = await validate(int(raw_id))
    # Only definitive identity rejection can trigger a correction proposal.
    # Provider outages are not evidence that an existing identity is wrong.
    if (reason in {"not_found", "invalid_mapping"} or reason.startswith("skipped_")) and (
        jikan is not None and (source == "official_mal" or (official_not_found and reason == "not_found"))
        and identity_source != "jikan_discovery" and not stop_event.is_set()
    ):
        alternative, decision = await jikan.resolve_decision(candidate.media_type, details)
        if decision == "matched" and type(alternative) is int and alternative > 0 and str(alternative) != str(raw_id):
            remember(alternative, "jikan_correction")
            alternative_reason, _ = await validate(alternative)
            if alternative_reason in {"success", "missing_score"} and not stop_event.is_set():
                proposed_mal_id = alternative
                return outcome("correction_available")
    if stop_event.is_set():
        return outcome("stopped")
    return outcome(reason, score)

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from betterer_ratings.config.schema import JikanConfig
from betterer_ratings.providers.jikan_client import JikanClient
from betterer_ratings.services.harvest.mal import eligibility_reason

LOGGER = logging.getLogger("betterer-ratings")


def likely_anime(details: dict[str, Any]) -> bool:
    genres = details.get("genres")
    return details.get("original_language") == "ja" and isinstance(genres, list) and any(
        isinstance(g, dict) and g.get("id") == 16 for g in genres
    )


class JikanEnrichment:
    """Cache discovery decisions separately from submitted mappings and ratings."""

    def __init__(self, *, client: JikanClient, config: JikanConfig, cache_dir: Path) -> None:
        self.client = client
        self.config = config
        self.cache_dir = cache_dir
        self._lock = asyncio.Lock()

    async def resolve(self, media_type: str, details: dict[str, Any]) -> int | None:
        if not self.config.discover_missing or not likely_anime(details):
            return None
        # Include matching metadata, endpoint and policy version to invalidate stale decisions.
        key = hashlib.sha256(json.dumps(
            [1, self.client.base_url, self.config.max_search_pages, media_type, {
                k: details.get(k) for k in (
                    "title", "original_title", "name", "original_name", "release_date",
                    "status", "number_of_seasons", "number_of_episodes", "first_air_date",
                    "last_air_date",
                )
            }],
            sort_keys=True, ensure_ascii=False,
        ).encode()).hexdigest()
        path = self.cache_dir / f"{key}.json"
        async with self._lock:
            try:
                cached = json.loads(path.read_text())
                if isinstance(cached, dict) and cached["expires"] > time.time():
                    value = cached.get("mal_id")
                    if value is None or (type(value) is int and value > 0):
                        self._log("cache_" + str(cached.get("outcome", "hit")), value)
                        return value
            except (OSError, ValueError, KeyError, TypeError):
                pass
            try:
                async with asyncio.timeout(25):
                    value, outcome = await self._search(media_type, details)
            except TimeoutError:
                value, outcome = None, "timeout"
            self._log(outcome, value)
            ttl = 86400 if value else (3600 if outcome in {"no_match", "ambiguous"} else 300)
            try:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix(".tmp")
                temporary.write_text(json.dumps({
                    "mal_id": value, "outcome": outcome, "expires": time.time() + ttl,
                }))
                os.replace(temporary, path)
            except OSError:
                LOGGER.warning("Jikan discovery cache could not be written")
            return value

    @staticmethod
    def _log(outcome: str, mal_id: int | None) -> None:
        LOGGER.info("[Jikan] discovery=%s mal_id=%s", outcome, mal_id,
                    extra={"event": "jikan.discovery", "outcome": outcome, "mal_id": mal_id})

    async def _search(self, media_type: str, details: dict[str, Any]) -> tuple[int | None, str]:
        names = ("title", "original_title") if media_type == "movie" else ("name", "original_name")
        queries = list(dict.fromkeys(
            details[k].strip() for k in names
            if isinstance(details.get(k), str) and details[k].strip()
        ))
        matches: set[int] = set()
        for query in queries:
            seen: set[int] = set()
            for page in range(1, self.config.max_search_pages + 1):
                response = await self.client.search(query=query, page=page, media_type=media_type)
                if response.status != 200:
                    return None, "provider_failure"
                body = response.data
                if not isinstance(body, dict) or not isinstance(body.get("data"), list):
                    return None, "invalid_response"
                pagination = body.get("pagination")
                if not isinstance(pagination, dict) or type(pagination.get("has_next_page")) is not bool:
                    return None, "invalid_pagination"
                if type(pagination.get("current_page")) is not int or pagination["current_page"] != page:
                    return None, "invalid_pagination"
                ids: set[int] = set()
                for anime in body["data"]:
                    if not isinstance(anime, dict) or type(anime.get("mal_id")) is not int or anime["mal_id"] <= 0:
                        return None, "invalid_response"
                    ids.add(anime["mal_id"])
                    # Missing scores must not hide a second otherwise eligible identity.
                    if not eligibility_reason(media_type, details, anime):
                        matches.add(anime["mal_id"])
                if len(matches) > 1:
                    return None, "ambiguous"
                if page > 1 and (not ids or ids <= seen):
                    return None, "repeated_page"
                seen.update(ids)
                if not pagination["has_next_page"]:
                    break
                if not ids or page == self.config.max_search_pages:
                    return None, "incomplete_search"
        return (next(iter(matches)), "matched") if matches else (None, "no_match")

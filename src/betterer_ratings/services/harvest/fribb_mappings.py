from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.services.harvest.anime_mapping_decision import AnimeMappingDecision

LOGGER = logging.getLogger("betterer-ratings")
DATASET_URL = (
    "https://raw.githubusercontent.com/Fribb/anime-lists/refs/heads/master/anime-list-full.json"
)
REFRESH_SECONDS = 7 * 24 * 60 * 60
RETRY_SECONDS = 60 * 60
ANIME_PROVIDERS = ("mal", "anilist", "anidb")


def _positive_id(value: object) -> str | None:
    if type(value) is int and value > 0:
        return str(value)
    if isinstance(value, str) and value.isascii() and value.isdigit() and int(value) > 0:
        return str(int(value))
    return None


def _values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [] if value is None else [value]


@dataclass(frozen=True)
class FribbEntry:
    media_type: str | None
    tmdb_ids: frozenset[tuple[str, str]]
    imdb_ids: frozenset[str]
    anime_ids: tuple[tuple[str, str], ...]
    whole_title: bool


class FribbIndex:
    """Resolve MAL candidates; retain all anime owners to detect ambiguity."""

    def __init__(self, rows: list[Any]) -> None:
        self.anchors: dict[tuple[str, str, str], set[FribbEntry]] = {}
        self.anime_owners: dict[tuple[str, str], set[FribbEntry]] = {}
        self.usable_entries = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            media_type = {"MOVIE": "movie", "TV": "tv", "ONA": "tv"}.get(
                str(row.get("type", "")).upper()
            )
            safe = media_type is not None
            raw_tmdb = row.get("themoviedb_id", {})
            if raw_tmdb is None:
                raw_tmdb = {}
            tmdb_ids: set[tuple[str, str]] = set()
            if not isinstance(raw_tmdb, dict):
                safe = False  # A scalar has lost the movie/TV namespace.
                raw_tmdb = {}
            for namespace, raw_ids in raw_tmdb.items():
                if namespace not in {"movie", "tv"}:
                    safe = False
                    continue
                for raw_id in _values(raw_ids):
                    value = _positive_id(raw_id)
                    if value is None:
                        safe = False
                    else:
                        tmdb_ids.add((namespace, value))
            imdb_ids: set[str] = set()
            for raw_id in _values(row.get("imdb_id")):
                value = normalize_imdb_title_id(raw_id)
                if value is None:
                    safe = False
                else:
                    imdb_ids.add(value)
            anime_ids: list[tuple[str, str]] = []
            for provider in ANIME_PROVIDERS:
                raw_id = row.get(f"{provider}_id")
                value = _positive_id(raw_id)
                if raw_id is not None and value is None:
                    safe = False
                if value is not None:
                    anime_ids.append((provider, value))
            # Arrays can represent several distinct films under one anime ID.
            safe = safe and len(tmdb_ids) <= 1 and len(imdb_ids) <= 1
            safe = safe and all(namespace == media_type for namespace, _ in tmdb_ids)
            if media_type == "tv":
                seasons = row.get("season")
                offsets = row.get("episode_offset", {})
                safe = safe and isinstance(seasons, dict) and bool(seasons)
                if isinstance(seasons, dict):
                    safe = safe and all(type(v) is int and v == 1 for v in seasons.values())
                    if tmdb_ids:
                        safe = safe and seasons.get("tmdb") == 1
                safe = safe and isinstance(offsets, dict)
                if isinstance(offsets, dict):
                    safe = safe and all(type(v) is int and v == 0 for v in offsets.values())
            safe = safe and "mal" in dict(anime_ids)
            entry = FribbEntry(
                media_type, frozenset(tmdb_ids), frozenset(imdb_ids), tuple(anime_ids), safe
            )
            for namespace, value in tmdb_ids:
                self.anchors.setdefault(("tmdb", namespace, value), set()).add(entry)
            for value in imdb_ids:
                # Unknown formats still block otherwise plausible IMDb matches.
                for namespace in ({media_type} if media_type else {"movie", "tv"}):
                    self.anchors.setdefault(("imdb", namespace, value), set()).add(entry)
            for identity in anime_ids:
                self.anime_owners.setdefault(identity, set()).add(entry)
            self.usable_entries += int(safe and bool(tmdb_ids or imdb_ids))

    def resolve(
        self, *, media_type: str, tmdb_id: int, mappings: Mapping[str, object]
    ) -> dict[str, str]:
        decision = self.resolve_decision(media_type=media_type, tmdb_id=tmdb_id, mappings=mappings)
        return {"mal": decision.mal_id} if decision.mal_id else {}

    def resolve_decision(
        self, *, media_type: str, tmdb_id: int, mappings: Mapping[str, object]
    ) -> AnimeMappingDecision:
        matches = set(self.anchors.get(("tmdb", media_type, str(tmdb_id)), set()))
        imdb_id = normalize_imdb_title_id(mappings.get("imdb"))
        if imdb_id:
            matches.update(self.anchors.get(("imdb", media_type, imdb_id), set()))
        mal_ids = tuple(sorted({v for e in matches for k, v in e.anime_ids if k == "mal"}))
        evidence = [{"provider": k, "id_value": v} for k, v in sorted({
            pair for e in matches for pair in e.anime_ids
        })]
        def decision(outcome: str) -> AnimeMappingDecision:
            return AnimeMappingDecision(outcome, mal_ids, evidence)
        if not matches:
            return decision("missing")
        if len(matches) != 1:
            return decision("conflict")
        entry = matches.pop()
        if not entry.whole_title or entry.media_type != media_type:
            return decision("unsafe_scope")
        if entry.tmdb_ids and (media_type, str(tmdb_id)) not in entry.tmdb_ids:
            return decision("conflict")
        if imdb_id and entry.imdb_ids and imdb_id not in entry.imdb_ids:
            return decision("conflict")
        for provider, value in entry.anime_ids:
            if self.anime_owners[(provider, value)] != {entry}:
                return decision("conflict")
            supplied = mappings.get(provider)
            if supplied is not None and _positive_id(supplied) != value:
                return decision("conflict")
        return decision("matched")


def load_fribb_index(path: Path) -> FribbIndex:
    with path.open(encoding="utf-8") as dataset:
        rows = json.load(dataset)
    if not isinstance(rows, list):
        raise ValueError("Fribb dataset must be a JSON array")
    index = FribbIndex(rows)
    if not index.usable_entries:
        raise ValueError("Fribb dataset contains no usable whole-title mappings")
    return index


class FribbAnimeMappings:
    """Weekly local cache; failed or invalid downloads retain the last good copy."""

    def __init__(self, cache_path: Path, *, url: str = DATASET_URL) -> None:
        self.cache_path = cache_path
        self.url = url
        self._index: FribbIndex | None = None
        self._loaded_mtime_ns: int | None = None
        self._refresh_attempted_at: float | None = None
        self._lock = asyncio.Lock()

    async def resolve(
        self, *, media_type: str, tmdb_id: int, mappings: Mapping[str, object]
    ) -> dict[str, str]:
        decision = await self.resolve_decision(media_type=media_type, tmdb_id=tmdb_id, mappings=mappings)
        return {"mal": decision.mal_id} if decision.mal_id else {}

    async def resolve_decision(
        self, *, media_type: str, tmdb_id: int, mappings: Mapping[str, object]
    ) -> AnimeMappingDecision:
        await self._ensure_loaded()
        if self._index is None:
            return AnimeMappingDecision("unavailable")
        return self._index.resolve_decision(media_type=media_type, tmdb_id=tmdb_id, mappings=mappings)

    def _is_stale(self) -> bool:
        try:
            return time.time() - self.cache_path.stat().st_mtime >= REFRESH_SECONDS
        except OSError:
            return True

    async def _ensure_loaded(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._is_stale() and (
                self._refresh_attempted_at is None or now - self._refresh_attempted_at >= RETRY_SECONDS
            ):
                self._refresh_attempted_at = now
                try:
                    await self._download_refresh()
                except (httpx.HTTPError, OSError, ValueError) as exc:
                    LOGGER.warning("[Fribb] Refresh failed; retaining cached copy: %s", exc)
            try:
                stat = self.cache_path.stat()
                if self._index is None or self._loaded_mtime_ns != stat.st_mtime_ns:
                    index = await asyncio.to_thread(load_fribb_index, self.cache_path)
                    self._index = index
                    self._loaded_mtime_ns = stat.st_mtime_ns
            except (OSError, ValueError) as exc:
                LOGGER.warning("[Fribb] Could not load cached dataset: %s", exc)

    async def _download_refresh(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.cache_path.with_name(f".{self.cache_path.name}.download.tmp")
        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", self.url) as response:
                    response.raise_for_status()
                    with temporary_path.open("wb") as output:
                        async for chunk in response.aiter_bytes():
                            output.write(chunk)
            index = await asyncio.to_thread(load_fribb_index, temporary_path)
            os.replace(temporary_path, self.cache_path)
            self._index = index
            self._loaded_mtime_ns = self.cache_path.stat().st_mtime_ns
            LOGGER.info("[Fribb] Refreshed %s whole-title candidates.", index.usable_entries)
        finally:
            temporary_path.unlink(missing_ok=True)

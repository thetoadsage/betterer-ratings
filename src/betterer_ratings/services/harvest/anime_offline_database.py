from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from urllib.parse import urlparse

import httpx

LOGGER = logging.getLogger("betterer-ratings")

DATASET_URL = (
    "https://github.com/cedya77/anime-offline-database/releases/download/latest/"
    "anime-offline-database.jsonl"
)
REFRESH_SECONDS = 7 * 24 * 60 * 60
RETRY_SECONDS = 60 * 60
_PROVIDERS = {"anilist", "anidb", "mal"}


def _source_id(source: object) -> tuple[str, str] | None:
    if not isinstance(source, str):
        return None
    parsed = urlparse(source)
    host = parsed.hostname or ""
    provider = next((name for name in _PROVIDERS if host == f"{name}.net"), None)
    if provider is None and host == "anilist.co":
        provider = "anilist"
    if provider is None and host == "myanimelist.net":
        provider = "mal"
    if provider is None:
        return None
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 2 or path_parts[0] != "anime" or not path_parts[1].isdigit():
        return None
    return provider, str(int(path_parts[1]))


def build_mal_lookup(lines: Iterable[str]) -> dict[tuple[str, str], frozenset[str]]:
    """Build a conservative AniList/AniDB-to-MAL index from JSONL records."""
    mutable_lookup: dict[tuple[str, str], set[str]] = {}
    for line in lines:
        try:
            entry = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue  # The leading metadata line is intentionally ignored.
        if not isinstance(entry, dict) or not isinstance(entry.get("sources"), list):
            continue
        identities = {
            identity
            for source in entry["sources"]
            if (identity := _source_id(source)) is not None
        }
        mal_ids = {value for provider, value in identities if provider == "mal"}
        if len(mal_ids) != 1:
            continue
        mal_id = mal_ids.pop()
        for provider, value in identities:
            if provider in {"anilist", "anidb"}:
                mutable_lookup.setdefault((provider, value), set()).add(mal_id)
    return {key: frozenset(values) for key, values in mutable_lookup.items()}


def resolve_mal_id(
    mappings: Mapping[str, object], lookup: Mapping[tuple[str, str], frozenset[str]],
) -> str | None:
    """Return a MAL ID only when every supplied provider mapping agrees."""
    resolved: set[str] = set()
    for provider in ("anilist", "anidb"):
        raw_id = mappings.get(provider)
        value = str(raw_id) if raw_id is not None else ""
        if not value.isascii() or not value.isdigit() or int(value) <= 0:
            continue
        mapped = lookup.get((provider, str(int(value))))
        if mapped is not None:
            resolved.update(mapped)
    return resolved.pop() if len(resolved) == 1 else None


class AnimeOfflineDatabase:
    """Weekly, on-disk cache of the anime-offline-database ID relationships."""

    def __init__(
        self,
        cache_path: Path,
        *,
        url: str = DATASET_URL,
        refresh_seconds: int = REFRESH_SECONDS,
    ) -> None:
        self.cache_path = cache_path
        self.url = url
        self.refresh_seconds = refresh_seconds
        self._lookup: dict[tuple[str, str], frozenset[str]] | None = None
        self._loaded_mtime_ns: int | None = None
        self._refresh_attempted_at = 0.0
        self._lock = asyncio.Lock()

    async def resolve(self, mappings: Mapping[str, object]) -> str | None:
        await self._ensure_loaded()
        return resolve_mal_id(mappings, self._lookup or {})

    def _is_stale(self) -> bool:
        try:
            return time.time() - self.cache_path.stat().st_mtime >= self.refresh_seconds
        except OSError:
            return True

    async def _ensure_loaded(self) -> None:
        async with self._lock:
            if self._is_stale() and time.monotonic() - self._refresh_attempted_at >= RETRY_SECONDS:
                self._refresh_attempted_at = time.monotonic()
                try:
                    await self._download_refresh()
                except (httpx.HTTPError, OSError, ValueError) as exc:
                    LOGGER.warning("[AnimeOfflineDatabase] Refresh failed; retaining cached copy: %s", exc)
            try:
                stat = self.cache_path.stat()
            except OSError:
                return
            if self._lookup is None or self._loaded_mtime_ns != stat.st_mtime_ns:
                try:
                    with self.cache_path.open(encoding="utf-8") as dataset:
                        self._lookup = build_mal_lookup(dataset)
                    self._loaded_mtime_ns = stat.st_mtime_ns
                    LOGGER.info(
                        "[AnimeOfflineDatabase] Loaded %s AniList/AniDB mapping keys.",
                        len(self._lookup),
                    )
                except (OSError, UnicodeDecodeError) as exc:
                    LOGGER.warning("[AnimeOfflineDatabase] Could not read cached dataset: %s", exc)

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
            with temporary_path.open(encoding="utf-8") as dataset:
                lookup = build_mal_lookup(dataset)
            if not lookup:
                raise ValueError("downloaded dataset contains no usable AniList/AniDB-to-MAL mappings")
            os.replace(temporary_path, self.cache_path)
            self._lookup = lookup
            self._loaded_mtime_ns = self.cache_path.stat().st_mtime_ns
            LOGGER.info("[AnimeOfflineDatabase] Refreshed %s mapping keys.", len(lookup))
        finally:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass

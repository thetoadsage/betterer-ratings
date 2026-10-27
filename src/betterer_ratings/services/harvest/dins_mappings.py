from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from betterer_ratings.infra.db.local_database import LocalDatabase

LOGGER = logging.getLogger("betterer-ratings")
DATASET_BASE_URL = "https://raw.githubusercontent.com/0xConstant1/dins-mappings/main"
REFRESH_SECONDS = 86400
RETRY_SECONDS = 3600
MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
PROVIDERS = {"imdb": "imdb_id", "tmdb": "tmdb_id", "tvdb": "tvdb_id"}


@dataclass(frozen=True)
class DinsEntry:
    imdb_id: str
    tmdb_id: str
    tvdb_id: str

    def mappings(self) -> dict[str, str]:
        return {
            key: value for key, value in (("imdb", self.imdb_id), ("tvdb", self.tvdb_id)) if value
        }


def build_index(path: Path, datasets: dict[str, Path]) -> int:
    """Build one complete generation, retaining ambiguous rows for lookup rejection."""
    total = 0
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE entries (media_type TEXT, imdb_id TEXT, tmdb_id TEXT, tvdb_id TEXT, PRIMARY KEY(media_type, imdb_id, tmdb_id, tvdb_id))"
        )
        for media_type in ("movie", "tv"):
            count = tmdb_count = 0
            batch: list[tuple[str, str, str, str]] = []
            with datasets[media_type].open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle, strict=True)
                expected = {"imdb_id", "tmdb_id", "tvdb_id"}
                if media_type == "tv":
                    expected.add("tvmaze_id")
                if (
                    reader.fieldnames is None
                    or set(reader.fieldnames) != expected
                    or len(reader.fieldnames) != len(expected)
                ):
                    raise ValueError(f"Unexpected {media_type} CSV header")
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"Incomplete {media_type} CSV row")
                    values = {key: value.strip() for key, value in row.items()}
                    if re.fullmatch(r"tt[0-9]+", values["imdb_id"]) is None:
                        raise ValueError(f"Invalid {media_type} IMDb ID")
                    for key, value in values.items():
                        if (
                            key != "imdb_id"
                            and value
                            and re.fullmatch(r"[1-9][0-9]*", value) is None
                        ):
                            raise ValueError(f"Invalid {media_type} {key}")
                    batch.append(
                        (media_type, values["imdb_id"], values["tmdb_id"], values["tvdb_id"])
                    )
                    count += 1
                    tmdb_count += bool(values["tmdb_id"])
                    if len(batch) >= 5000:
                        conn.executemany("INSERT OR IGNORE INTO entries VALUES (?, ?, ?, ?)", batch)
                        batch.clear()
                conn.executemany("INSERT OR IGNORE INTO entries VALUES (?, ?, ?, ?)", batch)
            if not count or not tmdb_count:
                raise ValueError(f"Empty {media_type} mapping dataset")
            total += count
        for column in PROVIDERS.values():
            conn.execute(f"CREATE INDEX by_{column} ON entries(media_type, {column})")
        conn.execute("PRAGMA user_version=1")
    return total


class DinsMappings:
    """Daily atomic SQLite cache of the trusted movie/TV mapping exports."""

    def __init__(self, cache_path: Path, *, base_url: str = DATASET_BASE_URL) -> None:
        self.cache_path = cache_path
        self.base_url = base_url.rstrip("/")
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        self._attempted_at: float | None = None
        self._next_check = 0.0

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _open(self) -> None:
        conn = sqlite3.connect(self.cache_path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            if conn.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise ValueError("Unsupported Dins cache version")
            conn.execute("SELECT media_type, imdb_id, tmdb_id, tvdb_id FROM entries LIMIT 1")
        except Exception:
            conn.close()
            raise
        self._conn = conn

    async def prepare(self) -> None:
        if time.monotonic() < self._next_check:
            return
        async with self._lock:
            now = time.monotonic()
            if now < self._next_check:
                return
            self._next_check = now + 60
            if self._conn is None and self.cache_path.exists():
                try:
                    self._open()
                except (OSError, ValueError, sqlite3.Error) as exc:
                    LOGGER.warning("[Dins] Cannot load cache: %s", exc)
            try:
                stale = time.time() - self.cache_path.stat().st_mtime >= REFRESH_SECONDS
            except OSError:
                stale = True
            if (stale or self._conn is None) and (
                self._attempted_at is None or now - self._attempted_at >= RETRY_SECONDS
            ):
                self._attempted_at = now
                try:
                    await self._refresh()
                except (httpx.HTTPError, OSError, ValueError, csv.Error, sqlite3.Error) as exc:
                    LOGGER.warning("[Dins] Refresh failed; retaining last good cache: %s", exc)

    async def _refresh(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.cache_path.with_name(f".{self.cache_path.name}.download.tmp")
        datasets = {kind: temporary.with_suffix(f".{kind}.csv") for kind in ("movie", "tv")}
        try:
            temporary.unlink(missing_ok=True)
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                for kind, path in datasets.items():
                    async with client.stream(
                        "GET", f"{self.base_url}/{kind}_mappings.csv"
                    ) as response:
                        response.raise_for_status()
                        size = 0
                        with path.open("wb") as output:
                            async for chunk in response.aiter_bytes():
                                size += len(chunk)
                                if size > MAX_DOWNLOAD_BYTES:
                                    raise ValueError("Dins download exceeds size limit")
                                output.write(chunk)
            # Keep the event loop responsive while parsing and indexing the exports.
            # Await completion even during cancellation before cleaning its files.
            task = asyncio.create_task(asyncio.to_thread(build_index, temporary, datasets))
            try:
                count = await asyncio.shield(task)
            except asyncio.CancelledError:
                try:
                    await task
                finally:
                    raise
            self.close()
            os.replace(temporary, self.cache_path)
            self._open()
            LOGGER.info("[Dins] Refreshed %s movie/TV mapping rows.", count)
        finally:
            for path in (temporary, *datasets.values()):
                path.unlink(missing_ok=True)

    def lookup(self, media_type: str, provider: str, value: str) -> DinsEntry | None:
        if (
            self._conn is None
            or media_type not in {"movie", "tv"}
            or provider not in PROVIDERS
            or not value
        ):
            return None
        try:
            rows = self._conn.execute(
                f"SELECT imdb_id, tmdb_id, tvdb_id FROM entries WHERE media_type=? AND {PROVIDERS[provider]}=? LIMIT 2",
                (media_type, value),
            ).fetchall()
            if len(rows) != 1:
                return None
            entry = DinsEntry(*rows[0])
            # A unique forward match must also have unique reverse ownership.
            for column, identity in zip(PROVIDERS.values(), rows[0], strict=True):
                if (
                    identity
                    and len(
                        self._conn.execute(
                            f"SELECT 1 FROM entries WHERE media_type=? AND {column}=? LIMIT 2",
                            (media_type, identity),
                        ).fetchall()
                    )
                    != 1
                ):
                    return None
            return entry
        except sqlite3.Error as exc:
            LOGGER.warning("[Dins] Cache lookup failed: %s", exc)
            self.close()
            return None

    def supplements(
        self,
        *,
        db: LocalDatabase,
        media_type: str,
        tmdb_id: int,
        mappings: dict[str, str],
    ) -> dict[str, str]:
        database = db
        title = database.conn.execute(
            "SELECT imdb_id FROM titles WHERE media_type=? AND tmdb_id=?", (media_type, tmdb_id)
        ).fetchone()
        lookup_state = getattr(database, "get_enrichment_state", None)
        archive = (lookup_state(tmdb_id, media_type, "imdb")["payload"] or {}) if callable(lookup_state) else {}
        known_imdb = (
            mappings.get("imdb")
            or database.get_title_mapping(tmdb_id=tmdb_id, media_type=media_type, id_type="imdb")
            or (title[0] if title else None)
            or archive.get("imdb_id")
        )
        entry = self.lookup(media_type, "tmdb", str(tmdb_id))
        if entry is None and known_imdb:
            entry = self.lookup(media_type, "imdb", known_imdb)
        if entry is not None and entry.tmdb_id and entry.tmdb_id != str(tmdb_id):
            return {}
        if entry is None:
            return {}
        proposed = entry.mappings()
        for provider, value in proposed.items():
            stored = database.get_title_mapping(
                tmdb_id=tmdb_id, media_type=media_type, id_type=provider
            )
            if any(known and str(known) != value for known in (mappings.get(provider), stored)):
                return {}
            owners = database.conn.execute(
                "SELECT tmdb_id FROM mappings WHERE media_type=? AND id_type=? AND id_value=? AND tmdb_id<>? LIMIT 1",
                (media_type, provider, value, tmdb_id),
            ).fetchone()
            if owners is not None:
                return {}
        # Title and archive state can also hold an IMDb identity.
        if any(
            known and known != entry.imdb_id
            for known in (title[0] if title else None, archive.get("imdb_id"))
        ):
            return {}
        owner = database.conn.execute(
            "SELECT 1 FROM titles WHERE media_type=? AND imdb_id=? AND tmdb_id<>? LIMIT 1",
            (media_type, entry.imdb_id, tmdb_id),
        ).fetchone()
        if owner is not None:
            return {}
        return {key: value for key, value in proposed.items() if key not in mappings}

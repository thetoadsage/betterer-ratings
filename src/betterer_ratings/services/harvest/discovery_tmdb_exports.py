from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import tempfile
import zlib
from pathlib import Path
from typing import Any

import httpx

from betterer_ratings.config.schema import TMDBDailyExportsConfig
from betterer_ratings.domain.models import Candidate

_EXPORTS = {"movie": "movie_ids", "tv": "tv_series_ids"}


def _download_export(path: Path, media_type: str, date: str) -> None:
    stamp = dt.date.fromisoformat(date).strftime("%m_%d_%Y")
    url = f"https://files.tmdb.org/p/exports/{_EXPORTS[media_type]}_{stamp}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="tmdb-export-", dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        with httpx.stream("GET", url, timeout=120, follow_redirects=True) as response:
            response.raise_for_status()
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            with temporary.open("wb") as target:
                for chunk in response.iter_raw():
                    target.write(decoder.decompress(chunk))
                target.write(decoder.flush())
            if not decoder.eof:
                raise ValueError("truncated gzip export")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def scan_daily_exports(
    *,
    db: Any,
    config: TMDBDailyExportsConfig,
    directory: Path,
    candidates: list[Candidate],
    seen: set[tuple[str, int]],
    stop_event: asyncio.Event,
    logger: Any,
    now_ts: int,
) -> dict[str, tuple[str, int]]:
    """Select a bounded batch, returning cursors to commit after enrichment."""
    if not config.enabled or stop_event.is_set():
        return {}
    day = dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).date().isoformat()
    spent_day = db.get_state("tmdb_export_budget_day")
    spent = db.get_state_int("tmdb_export_budget_spent", 0) if spent_day == day else 0
    remaining = min(config.max_new_titles_per_scan, max(0, config.daily_detail_budget - spent))
    if remaining == 0:
        return {}
    existing = db.title_key_set()
    pending: dict[str, tuple[str, int]] = {}
    selected = 0
    # Pin a snapshot until exhausted. New daily exports otherwise reset a large backlog.
    latest = (dt.date.fromisoformat(day) - dt.timedelta(days=1)).isoformat()
    for media_type in ("movie", "tv"):
        if stop_event.is_set():
            break
        state_prefix = f"tmdb_export_{media_type}"
        media_limit = (remaining + 1) // 2 if media_type == "movie" else remaining
        export_day = db.get_state(f"{state_prefix}_day") or latest
        cursor = db.get_state_int(f"{state_prefix}_cursor", 0)
        path = directory / f"{_EXPORTS[media_type]}_{export_day}.jsonl"
        if db.get_state_int(f"{state_prefix}_exhausted", 0) and export_day < latest:
            export_day, cursor = latest, 0
            path = directory / f"{_EXPORTS[media_type]}_{export_day}.jsonl"
        try:
            if not path.is_file():
                await asyncio.to_thread(_download_export, path, media_type, export_day)
            with path.open("rb") as handle:
                handle.seek(cursor)
                while selected < media_limit and not stop_event.is_set():
                    line = handle.readline()
                    if not line:
                        break
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        continue
                    identifier = item.get("id")
                    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
                        continue
                    if item.get("adult") is True or (media_type == "movie" and item.get("video") is True):
                        continue
                    key = (media_type, identifier)
                    if key in seen or key in existing:
                        continue
                    title = str(item.get("original_title") or item.get("original_name") or f"TMDB-{identifier}")
                    try:
                        popularity = float(item.get("popularity") or 0)
                    except (TypeError, ValueError):
                        popularity = 0.0
                    candidates.append(Candidate(identifier, media_type, title, popularity, "daily_export"))
                    seen.add(key)
                    selected += 1
                pending[media_type] = (export_day, handle.tell())
        except (OSError, ValueError, httpx.HTTPError, zlib.error) as exc:
            logger.warning("[TMDB] Daily %s export unavailable: %s", media_type, exc)
    if selected:
        db.set_state("tmdb_export_budget_day", day)
        db.set_state("tmdb_export_budget_spent", spent + selected)
    logger.info("[TMDB] Daily exports selected %s new title(s); UTC budget %s/%s", selected, spent + selected, config.daily_detail_budget)
    return pending


def commit_export_cursors(db: Any, directory: Path, pending: dict[str, tuple[str, int]]) -> None:
    for media_type, (day, offset) in pending.items():
        prefix = f"tmdb_export_{media_type}"
        old_day = db.get_state(f"{prefix}_day")
        db.set_state(f"{prefix}_day", day)
        db.set_state(f"{prefix}_cursor", offset)
        path = directory / f"{_EXPORTS[media_type]}_{day}.jsonl"
        db.set_state(f"{prefix}_exhausted", int(path.is_file() and offset >= path.stat().st_size))
        if old_day and old_day != day:
            (directory / f"{_EXPORTS[media_type]}_{old_day}.jsonl").unlink(missing_ok=True)

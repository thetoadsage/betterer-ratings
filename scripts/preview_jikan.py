"""GET-only Jikan preview; no submitter, production DB writes or persistent cache.

Run with PYTHONPATH=src python scripts/preview_jikan.py --help.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import tempfile
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace

import httpx

from betterer_ratings.config.schema import JikanConfig
from betterer_ratings.infra.rate_limit.limiter import AsyncWindowLimiter
from betterer_ratings.providers.jikan_client import JikanClient
from betterer_ratings.providers.mal_client import MALClient
from betterer_ratings.services.harvest.jikan import JikanEnrichment
from betterer_ratings.services.harvest.mal import fetch_candidate_mal_score


class PreviewGate:
    def __init__(self, name):
        self.name = name
        self.limiter = AsyncWindowLimiter(1, 1.1, name)
        self.paused_until = 0

    async def acquire(self, **_):
        if time.monotonic() < self.paused_until:
            return False
        await self.limiter.acquire()
        return True

    def pause_for(self, seconds, reason):
        self.paused_until = time.monotonic() + seconds

    def observe_headers(self, headers, status):
        pass


class ReadOnlyMappings:
    def __init__(self, path):
        self.conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
        self.conn.execute("PRAGMA query_only=ON")

    def get_title_mapping(self, *, tmdb_id, media_type, id_type):
        row = self.conn.execute(
            "SELECT id_value FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type=?",
            (tmdb_id, media_type, id_type),
        ).fetchone()
        return row[0] if row else None


class OutcomeCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.events = []

    def emit(self, record):
        if getattr(record, "event", "") in {"mal.enrichment", "jikan.discovery"}:
            self.events.append({k: getattr(record, k) for k in
                                ("event", "outcome", "source", "mal_id") if hasattr(record, k)})


async def main(args):
    with open(args.config, "rb") as f:
        config = tomllib.load(f)
    settings = JikanConfig.from_mapping({
        "enabled": True, "base_url": args.jikan_url, "max_search_pages": args.max_search_pages,
    })
    db = ReadOnlyMappings(args.database)
    official = MALClient(client_id=config["mal"]["client_id"], gate=PreviewGate("mal"))
    jikan_client = JikanClient(base_url=settings.base_url, gate=PreviewGate("jikan"))
    capture = OutcomeCapture()
    logger = logging.getLogger("betterer-ratings")
    logger.addHandler(capture)
    logger.setLevel(logging.INFO)
    try:
        with tempfile.TemporaryDirectory(prefix="jikan-preview-") as cache:
            service = JikanEnrichment(client=jikan_client, config=settings, cache_dir=Path(cache))
            async with httpx.AsyncClient(timeout=20) as http:
                for title in args.title:
                    media, raw_id = title.split(":")
                    tid = int(raw_id)
                    if media not in {"movie", "tv"} or tid <= 0:
                        raise ValueError("Titles must be movie:ID or tv:ID")
                    capture.events.clear()
                    result = {"tmdb": title}
                    try:
                        response = await http.get(f"https://api.themoviedb.org/3/{media}/{tid}",
                                                  params={"api_key": config["api_keys"]["tmdb"], "language": "en-US"})
                        if response.status_code != 200:
                            result["tmdb_status"] = response.status_code
                        else:
                            details = response.json()
                            result["title"] = details.get("title") or details.get("name")
                            result["score"] = await fetch_candidate_mal_score(
                                client=official, db=db,
                                candidate=SimpleNamespace(media_type=media, tmdb_id=tid),
                                details=details, md_item={}, stop_event=asyncio.Event(), jikan=service,
                            )
                            result["events"] = capture.events.copy()
                    except (httpx.HTTPError, ValueError) as exc:
                        result["error"] = type(exc).__name__  # Never print credential-bearing URLs.
                    print(json.dumps(result), flush=True)
    finally:
        logger.removeHandler(capture)
        await official.aclose()
        await jikan_client.aclose()
        db.conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--jikan-url", required=True)
    parser.add_argument("--max-search-pages", type=int, default=4)
    parser.add_argument("--title", action="append", required=True, help="movie:ID or tv:ID; repeatable")
    asyncio.run(main(parser.parse_args()))

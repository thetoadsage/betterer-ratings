from __future__ import annotations

import asyncio
import logging

from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.domain.models import Candidate, TMDBSource
from betterer_ratings.services.harvest.details import fetch_tmdb_details
from betterer_ratings.services.harvest.discovery_tmdb_scan import scan_tmdb_sources
from betterer_ratings.services.harvest.imdb_mapping_helpers import resolve_imdb_to_tmdb_local


class _FakeTMDB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def fetch_details(self, media_type: str, tmdb_id: int):
        self.calls.append((media_type, tmdb_id))

        class Response:
            ok = True
            status = 200
            text = ""
            data = {"external_ids": {"imdb_id": "tt1234567"}}

        return Response()


class _FakeSourceResponse:
    ok = True
    status = 200
    text = ""
    data = {
        "total_pages": 1,
        "results": [
            {"id": 1, "title": "Existing", "popularity": 1.0},
            {"id": 2, "title": "New", "popularity": 2.0},
        ],
    }


class _FakeSourceDB:
    def title_key_set(self):
        return {("movie", 1)}

    def should_harvest_many(self, *_args, **_kwargs):
        raise AssertionError("source scan should not do per-title TTL queries")


class _FakeSourceTMDB:
    source_scan_concurrency = 1

    async def fetch_source_page(self, _source, _page):
        return _FakeSourceResponse()


def test_local_refresh_fetches_details_even_when_imdb_mapping_is_cached(local_db):
    local_db._upsert_mapping(1, "movie", "imdb", "tt1234567", 100)
    tmdb = _FakeTMDB()
    candidates = [
        Candidate(1, "movie", "Cached", 0.0, harvest_reason="ttl"),
        Candidate(2, "movie", "Needs Details", 0.0, harvest_reason="ttl"),
    ]

    details, interrupted = asyncio.run(
        fetch_tmdb_details(
            candidates=candidates,
            stop_event=asyncio.Event(),
            tmdb_client=tmdb,
            db=local_db,
            details_concurrency=2,
            now_epoch_fn=lambda: 1000,
            logger=None,
        )
    )

    assert interrupted is False
    assert details[("movie", 1)] == {"external_ids": {"imdb_id": "tt1234567"}}
    assert details[("movie", 2)] == {"external_ids": {"imdb_id": "tt1234567"}}
    assert tmdb.calls == [("movie", 1), ("movie", 2)]


def test_source_scan_skips_existing_titles_from_memory_without_ttl_queries():
    source = TMDBSource("movie/popular", "/movie/popular", "movie", 1)
    stats = {
        source.name: {
            "pages_configured": 1,
            "pages_effective": 1,
            "pages_discovered": 0,
            "pages_fetched": 0,
            "raw_seen": 0,
            "added": 0,
            "duplicates": 0,
            "skipped": 0,
            "unsupported": 0,
            "errors": 0,
            "first_page": 0,
            "last_page": 0,
        }
    }
    candidates: list[Candidate] = []

    interrupted, *_ = asyncio.run(
        scan_tmdb_sources(
            stop_event=asyncio.Event(),
            db=_FakeSourceDB(),
            tmdb_client=_FakeSourceTMDB(),
            tmdb_sources=[source],
            source_stats=stats,
            candidates=candidates,
            seen=set(),
            parse_int_fn=parse_int,
            candidate_cls=Candidate,
            publish_scan_progress_fn=lambda *_args, **_kwargs: None,
            logger=None,
            page_batch_concurrency=1,
            pages_scanned=0,
            effective_target_pages=1,
            raw_seen_total=0,
        )
    )

    assert interrupted is False
    assert [(candidate.tmdb_id, candidate.harvest_reason) for candidate in candidates] == [
        (2, "source")
    ]
    assert stats[source.name]["skipped"] == 1


def test_source_scan_logs_per_source_summary(caplog):
    source = TMDBSource("movie/popular", "/movie/popular", "movie", 1)
    stats = {
        source.name: {
            "pages_configured": 1,
            "pages_effective": 1,
            "pages_discovered": 0,
            "pages_fetched": 0,
            "raw_seen": 0,
            "added": 0,
            "duplicates": 0,
            "skipped": 0,
            "unsupported": 0,
            "errors": 0,
            "first_page": 0,
            "last_page": 0,
        }
    }
    logger = logging.getLogger("betterer-ratings")

    with caplog.at_level(logging.INFO, logger="betterer-ratings"):
        asyncio.run(
            scan_tmdb_sources(
                stop_event=asyncio.Event(),
                db=_FakeSourceDB(),
                tmdb_client=_FakeSourceTMDB(),
                tmdb_sources=[source],
                source_stats=stats,
                candidates=[],
                seen=set(),
                parse_int_fn=parse_int,
                candidate_cls=Candidate,
                publish_scan_progress_fn=lambda *_args, **_kwargs: None,
                logger=logger,
                page_batch_concurrency=1,
                pages_scanned=0,
                effective_target_pages=1,
                raw_seen_total=0,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any("[TMDB] Source scan complete" in message for message in messages)
    assert any("source=movie/popular" in message for message in messages)


class _FakeIMDbCache:
    def __init__(self, value=None) -> None:
        self.value = value

    def get(self, _imdb_id: str, _media_type: str):
        return self.value


def test_imdb_mapping_uses_mappings_then_titles_then_cache(local_db):
    db = local_db
    db._upsert_title(
        tmdb_id=10,
        media_type="movie",
        title="Mapped",
        imdb_id=None,
        popularity=1.0,
        tmdb_vote_average=None,
        now_ts=100,
        error_message=None,
    )
    db._upsert_mapping(10, "movie", "imdb", "tt1111111", 100)
    db._upsert_title(
        tmdb_id=11,
        media_type="movie",
        title="Title IMDb",
        imdb_id="tt2222222",
        popularity=2.0,
        tmdb_vote_average=None,
        now_ts=100,
        error_message=None,
    )

    from_mapping = resolve_imdb_to_tmdb_local(
        db=db,
        imdb_cache=_FakeIMDbCache((99, "Cache", 9.0)),
        imdb_id="tt1111111",
        media_type="movie",
        normalize_imdb_title_id_fn=normalize_imdb_title_id,
        parse_int_fn=parse_int,
    )
    from_title = resolve_imdb_to_tmdb_local(
        db=db,
        imdb_cache=_FakeIMDbCache((99, "Cache", 9.0)),
        imdb_id="tt2222222",
        media_type="movie",
        normalize_imdb_title_id_fn=normalize_imdb_title_id,
        parse_int_fn=parse_int,
    )
    from_cache = resolve_imdb_to_tmdb_local(
        db=db,
        imdb_cache=_FakeIMDbCache((99, "Cache", 9.0)),
        imdb_id="tt3333333",
        media_type="movie",
        normalize_imdb_title_id_fn=normalize_imdb_title_id,
        parse_int_fn=parse_int,
    )

    assert from_mapping and from_mapping.tmdb_id == 10
    assert from_title and from_title.tmdb_id == 11
    assert from_cache and from_cache.tmdb_id == 99

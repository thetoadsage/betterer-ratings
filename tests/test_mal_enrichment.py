import asyncio
import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from betterer_ratings.config.schema import AppConfig, ConfigValidationError, MALConfig
from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.scoring import parse_mdblist_ratings, parse_tmdb_vote_average
from betterer_ratings.domain.models import APIResponse, Candidate, HarvestCycleResult
from betterer_ratings.infra.rate_limit.limiter import AsyncWindowLimiter
from betterer_ratings.infra.rate_limit.service_gate import ServiceGate
from betterer_ratings.providers.mal_client import MALClient
from betterer_ratings.providers.mdblist_client import MDBListClient
from betterer_ratings.services.harvest.anime_offline_database import (
    AnimeOfflineDatabase,
    build_mal_lookup,
    resolve_mal_id,
)
from betterer_ratings.services.harvest.cycle_mdblist_phase import run_mdblist_enrichment_phase
from betterer_ratings.services.harvest.enrichment import save_candidate_enrichment
from betterer_ratings.services.harvest.mal import (
    eligibility_reason,
    fetch_candidate_mal_score,
    mal_score,
)

MOVIE = {"title": "Princess Mononoke", "release_date": "1997-07-12"}
ANIME = {"mal_id": 164, "title": "Mononoke Hime", "title_english": "Princess Mononoke",
         "type": "Movie", "aired": {"from": "1997-07-12T00:00:00+00:00"},
         "score": 8.67, "scored_by": 1000}
CANDIDATE = Candidate(128, "movie", "Princess Mononoke", 1)


def test_anime_offline_database_lookup_requires_an_unambiguous_mal_id():
    lookup = build_mal_lookup([
        '{"license": "metadata line"}\n',
        '{"sources": ["https://anilist.co/anime/10", "https://anidb.net/anime/20", '
        '"https://myanimelist.net/anime/30"]}\n',
        '{"sources": ["https://anidb.net/anime/99", "https://myanimelist.net/anime/31"]}\n',
        '{"sources": ["https://anilist.co/anime/11", "https://myanimelist.net/anime/31"]}\n',
        '{"sources": ["https://anilist.co/anime/11", "https://myanimelist.net/anime/32"]}\n',
    ])
    assert resolve_mal_id({"anilist": "10"}, lookup) == "30"
    assert resolve_mal_id({"anidb": 20}, lookup) == "30"
    assert resolve_mal_id({"anilist": "10", "anidb": "20"}, lookup) == "30"
    assert resolve_mal_id({"anilist": "11"}, lookup) is None
    assert resolve_mal_id({"anilist": "10", "anidb": "999"}, lookup) == "30"
    assert resolve_mal_id({"anilist": "10", "anidb": "99"}, lookup) is None


def test_anime_offline_database_uses_fresh_local_cache_without_downloading(tmp_path):
    cache_path = tmp_path / "anime-offline-database.jsonl"
    cache_path.write_text(
        '{"sources": ["https://anilist.co/anime/10", '
        '"https://myanimelist.net/anime/30"]}\n', encoding="utf-8"
    )
    now = time.time()
    os.utime(cache_path, (now, now))
    cache = AnimeOfflineDatabase(cache_path, url="https://invalid.example/dataset")
    assert asyncio.run(cache.resolve({"anilist": "10"})) == "30"


def test_anime_offline_database_retains_cached_copy_after_failed_refresh(tmp_path):
    cache_path = tmp_path / "anime-offline-database.jsonl"
    cache_path.write_text(
        '{"sources": ["https://anilist.co/anime/10", '
        '"https://myanimelist.net/anime/30"]}\n', encoding="utf-8"
    )
    old = time.time() - 8 * 24 * 60 * 60
    os.utime(cache_path, (old, old))
    cache = AnimeOfflineDatabase(cache_path, url="https://example.test/dataset")

    async def run():
        with respx.mock() as mock:
            mock.get("https://example.test/dataset").mock(return_value=httpx.Response(503))
            return await cache.resolve({"anilist": "10"})

    assert asyncio.run(run()) == "30"


def test_config_opt_in_and_validation(base_valid_config):
    assert not AppConfig.from_mapping(base_valid_config).mal.enabled
    base_valid_config["mal"] = {"enabled": True, "client_id": "test-client"}
    assert AppConfig.from_mapping(base_valid_config).mal.enabled
    for value in ({"enabled": True}, {"enabled": "true"}, {"enabled": 1}, {"key": "unexpected"}):
        with pytest.raises(ConfigValidationError):
            MALConfig.from_mapping(value)


@pytest.mark.parametrize("score,votes,expected", [
    (8.67, 100, 86.7), (10, 1, 100), (None, 1, None), (0, 100, None),
    (11, 1, None), (float("nan"), 1, None), (float("inf"), 1, None),
    (True, 1, None), (8, 0, None), (8, None, None), ("8", 2, None),
])
def test_scores(score, votes, expected):
    assert mal_score({"score": score, "scored_by": votes}) == expected


def test_movie_scope():
    assert eligibility_reason("movie", MOVIE, ANIME) == ""
    assert eligibility_reason("movie", {**MOVIE, "release_date": "2000-01-01"}, ANIME)
    assert eligibility_reason("movie", MOVIE, {**ANIME, "type": "Special"})
    assert eligibility_reason("movie", {**MOVIE, "title": "Princess Mononoke 2"}, ANIME)
    assert eligibility_reason("movie", MOVIE, {**ANIME, "aired": None})


def test_tv_requires_complete_matching_scope():
    details = {"name": "Example", "status": "Ended", "number_of_seasons": 1,
               "number_of_episodes": 12, "first_air_date": "2020-01-01",
               "last_air_date": "2020-03-18"}
    anime = {"title": "Example", "type": "TV", "status": "Finished Airing", "episodes": 12,
             "aired": {"from": "2020-01-01", "to": "2020-03-18"}}
    assert eligibility_reason("tv", details, anime) == ""
    for change in ({"number_of_seasons": 2}, {"number_of_episodes": 24},
                   {"status": "Returning Series"}, {"last_air_date": "2021-03-18"},
                   {"first_air_date": None}):
        assert eligibility_reason("tv", {**details, **change}, anime)
    assert eligibility_reason("tv", details, {**anime, "episodes": None})


@pytest.mark.parametrize("status,data,expected", [
    (200, {"data": ANIME}, 86.7), (404, {}, None), (503, {}, None), (429, {}, None),
    (200, None, None), (200, {"data": {**ANIME, "mal_id": 999}}, None),
    (200, {"data": {**ANIME, "score": None}}, None),
])
def test_fetch_outcomes_and_stored_mapping(local_db, status, data, expected):
    local_db.conn.execute(
        "INSERT INTO mappings (tmdb_id, media_type, id_type, id_value, fetched_at) VALUES (128, 'movie', 'mal', '164', 1)"
    )
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(status, {}, data, "")))
    assert asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE,
        md_item=None, stop_event=asyncio.Event(),
    )) == expected
    client.fetch_anime.assert_awaited_once_with(164)


def test_missing_invalid_mapping_and_stop_do_not_fetch(local_db):
    client = SimpleNamespace(fetch_anime=AsyncMock())
    for item in ({}, {"ids": {"mal": "bad"}}, {"ids": {"mal": "-1"}}):
        assert asyncio.run(fetch_candidate_mal_score(
            client=client, db=local_db, candidate=CANDIDATE, details=MOVIE,
            md_item=item, stop_event=asyncio.Event(),
        )) is None
    stop = asyncio.Event()
    stop.set()
    asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE,
        md_item={"ids": {"mal": 164}}, stop_event=stop,
    ))
    client.fetch_anime.assert_not_awaited()


def test_anilist_mapping_fallback_fetches_official_mal_score(local_db):
    cache = SimpleNamespace(resolve=AsyncMock(return_value="164"))
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": ANIME}, "")))
    assert asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE,
        md_item={"ids": {"anilist": 5114}}, stop_event=asyncio.Event(),
        anime_mapping_cache=cache,
    )) == 86.7
    cache.resolve.assert_awaited_once_with({"anilist": "5114"})
    client.fetch_anime.assert_awaited_once_with(164)


@pytest.mark.parametrize("error", [TimeoutError, asyncio.CancelledError])
def test_timeout_falls_back_but_cancellation_propagates(local_db, error):
    client = SimpleNamespace(fetch_anime=AsyncMock(side_effect=error))
    async def run():
        return await fetch_candidate_mal_score(
            client=client, db=local_db, candidate=CANDIDATE, details=MOVIE,
            md_item={"ids": {"mal": 164}}, stop_event=asyncio.Event(),
        )
    if error is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(run())
    else:
        assert asyncio.run(run()) is None


@pytest.mark.parametrize("direct,expected", [(True, 86.7), (False, 70.0)])
def test_chunk_integration_prefers_direct_and_deduplicates(local_db, base_valid_config, direct, expected):
    import logging

    md_client = MDBListClient(
        api_key="test", config=AppConfig.from_mapping(base_valid_config).mdblist, gate=None,
    )
    item = {"ids": {"mal": 164}, "ratings": [{"source": "myanimelist", "value": 7}]}
    md_client._fetch_batch = AsyncMock(return_value=({128: item}, {128}, True, False, ""))
    mal_client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(
        200 if direct else 503, {}, {"data": ANIME}, ""
    )))
    def save(**kwargs):
        return save_candidate_enrichment(
            db=local_db, parse_mdblist_ratings_fn=parse_mdblist_ratings,
            parse_tmdb_vote_average_fn=parse_tmdb_vote_average,
            extract_mappings_fn=extract_mappings, **kwargs,
        )
    harvester = SimpleNamespace(db=local_db, mdblist_client=md_client, mal_client=mal_client,
                                _save_candidate_enrichment=save)
    async def run():
        for _ in range(2):
            await run_mdblist_enrichment_phase(
                harvester=harvester, logger=logging.getLogger("test"), now_epoch_fn=lambda: 100,
                stop_event=asyncio.Event(), candidates=[CANDIDATE],
                tmdb_details={("movie", 128): MOVIE}, source_stats={}, local_stats={},
                tmdb_list_request_errors=0, harvest_cycle_result_cls=HarvestCycleResult,
            )
    asyncio.run(run())
    rows = local_db.conn.execute("SELECT * FROM ratings WHERE label='ML'").fetchall()
    assert len(rows) == 1
    assert rows[0]["score"] == expected
    assert local_db.queue_counts()["ratings_pending"] == 1


def test_provider_429_persists_pause_and_uses_get(local_db):
    gate = ServiceGate("mal", local_db, AsyncWindowLimiter(1, 1.1, "mal"))
    client = MALClient(client_id="test-client", gate=gate)
    client.http.max_retries = 1
    async def run():
        with respx.mock() as mock:
            route = mock.get("https://api.myanimelist.net/v2/anime/164").mock(
                return_value=httpx.Response(429, headers={"Retry-After": "60"})
            )
            assert (await client.fetch_anime(164)).status == 429
            assert (await client.fetch_anime(164)).status == 429
            assert route.call_count == 1
        await client.aclose()
    asyncio.run(run())
    assert gate.pause_remaining() > 0
    assert local_db.get_service_state("mal")["last_status"] == 429


def test_official_api_authentication_and_normalization(local_db):
    gate = ServiceGate("mal", local_db, AsyncWindowLimiter(1, 1.1, "mal"))
    client = MALClient(client_id="test-client", gate=gate)
    async def run():
        with respx.mock() as mock:
            route = mock.get("https://api.myanimelist.net/v2/anime/164").mock(
                return_value=httpx.Response(200, json={
                    "id": 164, "title": "Mononoke Hime",
                    "alternative_titles": {"en": "Princess Mononoke", "synonyms": []},
                    "media_type": "movie", "status": "finished_airing",
                    "start_date": "1997-07-12", "num_episodes": 1,
                    "mean": 8.67, "num_scoring_users": 1000,
                })
            )
            result = await client.fetch_anime(164)
            assert eligibility_reason("movie", MOVIE, result.data["data"]) == ""
            assert mal_score(result.data["data"]) == 86.7
            request = route.calls[0].request
            assert request.headers["X-MAL-CLIENT-ID"] == "test-client"
            assert "mean" in request.url.params["fields"]
            assert "authorization" not in request.headers
            assert request.content == b""
        await client.aclose()
    asyncio.run(run())


@pytest.mark.parametrize("status", [401, 403])
def test_official_api_invalid_client_pauses(local_db, status):
    gate = ServiceGate("mal", local_db, AsyncWindowLimiter(1, 1.1, "mal"))
    client = MALClient(client_id="test-client", gate=gate)
    async def run():
        with respx.mock() as mock:
            route = mock.get("https://api.myanimelist.net/v2/anime/164").mock(
                return_value=httpx.Response(status, json={"error": "invalid_client"})
            )
            assert (await client.fetch_anime(164)).status == status
            assert (await client.fetch_anime(164)).status == 429
            assert route.call_count == 1
        await client.aclose()
    asyncio.run(run())
    assert gate.pause_remaining() > 0

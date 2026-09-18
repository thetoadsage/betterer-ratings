import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import respx

from betterer_ratings.config.schema import AppConfig, ConfigValidationError, JikanConfig
from betterer_ratings.domain.models import APIResponse, Candidate
from betterer_ratings.providers.jikan_client import JikanClient
from betterer_ratings.services.harvest.jikan import JikanEnrichment
from betterer_ratings.services.harvest.mal import fetch_candidate_mal_score

DETAILS = {"title": "Example", "original_title": "Example", "release_date": "2000-01-01",
           "original_language": "ja", "genres": [{"id": 16}]}
ANIME = {"mal_id": 123, "title": "Example", "type": "Movie",
         "aired": {"from": "2000-01-01"}, "score": 8, "scored_by": 20}


def response(data=None, status=200):
    return APIResponse(status, {}, data, "")


def page(items, number=1, more=False):
    return response({"data": items, "pagination": {"current_page": number, "has_next_page": more}})


def enrichment(tmp_path, pages, **config):
    client = SimpleNamespace(base_url="http://jikan:8080/v4", search=AsyncMock(side_effect=pages),
                             fetch_anime=AsyncMock(return_value=response({"data": ANIME})))
    return JikanEnrichment(client=client, config=JikanConfig(**config), cache_dir=tmp_path)


def test_config_defaults_and_requires_official_mal(base_valid_config):
    assert not AppConfig.from_mapping(base_valid_config).jikan.enabled
    base_valid_config["jikan"] = {"enabled": True, "base_url": "http://jikan:8080/v4"}
    with pytest.raises(ConfigValidationError, match="requires mal.enabled"):
        AppConfig.from_mapping(base_valid_config)
    base_valid_config["mal"] = {"enabled": True, "client_id": "test"}
    assert AppConfig.from_mapping(base_valid_config).jikan.enabled


@pytest.mark.parametrize("config", [
    {"enabled": 1}, {"enabled": True}, {"base_url": "file:///tmp/jikan"},
    {"base_url": "http://user:password@jikan/v4"}, {"base_url": "http://jikan/v4?key=secret"},
    {"fallback": "true"}, {"discover_missing": 0}, {"max_search_pages": 0},
    {"max_search_pages": 11}, {"max_search_pages": True}, {"unexpected": True},
])
def test_invalid_config(config):
    with pytest.raises(ConfigValidationError):
        JikanConfig.from_mapping(config)


def test_complete_pagination_and_persistent_cache(tmp_path):
    service = enrichment(tmp_path, [page([ANIME], more=True), page([{**ANIME, "mal_id": 456, "title": "Other"}], 2)])
    assert asyncio.run(service.resolve("movie", DETAILS)) == 123
    assert service.client.search.await_count == 2
    # Reopening the cache must not search again; volatile TMDB fields do not invalidate it.
    other = enrichment(tmp_path, [])
    assert asyncio.run(other.resolve("movie", {**DETAILS, "popularity": 99})) == 123
    other.client.search.assert_not_awaited()


@pytest.mark.parametrize("pages,config", [
    ([page([ANIME, {**ANIME, "mal_id": 456, "score": None}])], {}),
    ([page([ANIME], more=True)], {"max_search_pages": 1}),
    ([page([ANIME], more=True), page([ANIME], 2)], {}),
    ([page([ANIME], more=True), response(status=503)], {}),
    ([response({"data": [ANIME]})], {}),
    ([page([ANIME], 2)], {}),
    ([page([{"mal_id": "123"}])], {}),
    ([page([], more=True)], {}),
])
def test_incomplete_or_ambiguous_search_never_accepts(tmp_path, pages, config):
    service = enrichment(tmp_path, pages, **config)
    assert asyncio.run(service.resolve("movie", DETAILS)) is None


def test_ambiguity_across_title_variants(tmp_path):
    service = enrichment(tmp_path, [page([ANIME]), page([{**ANIME, "mal_id": 456}])])
    assert asyncio.run(service.resolve("movie", {**DETAILS, "original_title": "Japanese"})) is None


@pytest.mark.parametrize("details", [{}, {**DETAILS, "genres": []}, {**DETAILS, "original_language": "en"}])
def test_non_anime_never_searched(tmp_path, details):
    service = enrichment(tmp_path, [])
    assert asyncio.run(service.resolve("movie", details)) is None
    service.client.search.assert_not_awaited()


def test_expired_or_corrupt_cache_is_refreshed(tmp_path):
    service = enrichment(tmp_path, [page([ANIME]), page([]), page([ANIME])])
    assert asyncio.run(service.resolve("movie", DETAILS)) == 123
    path = next(tmp_path.glob("*.json"))
    path.write_text(json.dumps({"mal_id": 123, "expires": 0}))
    assert asyncio.run(service.resolve("movie", DETAILS)) is None
    path.write_text("broken")
    assert asyncio.run(service.resolve("movie", DETAILS)) == 123


@pytest.mark.parametrize("status,expected,fallback_calls", [
    (200, 80, 0), (503, 80, 1), (404, 80, 1), (429, 80, 1), (0, 80, 1),
    (401, None, 0), (403, None, 0),
])
def test_discovery_and_score_precedence(local_db, tmp_path, status, expected, fallback_calls, caplog):
    service = enrichment(tmp_path, [page([ANIME])])
    official = SimpleNamespace(fetch_anime=AsyncMock(return_value=response({"data": ANIME}, status)))
    with caplog.at_level("INFO", logger="betterer-ratings"):
        score = asyncio.run(fetch_candidate_mal_score(
            client=official, db=local_db, candidate=Candidate(1, "movie", "Example", 1),
            details=DETAILS, md_item={}, stop_event=asyncio.Event(), jikan=service,
        ))
    assert score == expected
    official.fetch_anime.assert_awaited_once_with(123)
    assert service.client.fetch_anime.await_count == fallback_calls
    assert any(getattr(r, "source", None) == ("jikan" if fallback_calls else "official_mal") for r in caplog.records)
    assert local_db.conn.execute("SELECT count(*) FROM mappings").fetchone()[0] == 0
    assert local_db.conn.execute("SELECT count(*) FROM ratings").fetchone()[0] == 0


@pytest.mark.parametrize("anime", [{**ANIME, "mal_id": 999}, {**ANIME, "title": "Other"}, {**ANIME, "score": None}])
def test_fallback_revalidates_details(local_db, tmp_path, anime):
    service = enrichment(tmp_path, [])
    service.client.fetch_anime.return_value = response({"data": anime})
    official = SimpleNamespace(fetch_anime=AsyncMock(return_value=response(status=503)))
    assert asyncio.run(fetch_candidate_mal_score(
        client=official, db=local_db, candidate=Candidate(1, "movie", "Example", 1),
        details=DETAILS, md_item={"ids": {"mal": 123}}, stop_event=asyncio.Event(), jikan=service,
    )) is None
    service.client.search.assert_not_awaited()


def test_valid_official_mismatch_does_not_use_jikan(local_db, tmp_path):
    service = enrichment(tmp_path, [])
    official = SimpleNamespace(fetch_anime=AsyncMock(return_value=response({"data": {**ANIME, "title": "Other"}})))
    assert asyncio.run(fetch_candidate_mal_score(
        client=official, db=local_db, candidate=Candidate(1, "movie", "Example", 1),
        details=DETAILS, md_item={"ids": {"mal": 123}}, stop_event=asyncio.Event(), jikan=service,
    )) is None
    service.client.fetch_anime.assert_not_awaited()


def test_cancellation_propagates(tmp_path):
    service = enrichment(tmp_path, [asyncio.CancelledError()])
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service.resolve("movie", DETAILS))
    assert not list(tmp_path.glob("*.json"))


def test_discovery_timeout_is_cached_as_failure(tmp_path):
    service = enrichment(tmp_path, [TimeoutError()])
    assert asyncio.run(service.resolve("movie", DETAILS)) is None
    assert asyncio.run(service.resolve("movie", DETAILS)) is None
    assert service.client.search.await_count == 1


@pytest.mark.parametrize("change", [{"status": "Returning Series", "number_of_seasons": 1},
                                    {"status": "Ended", "number_of_seasons": 2}])
def test_tv_scope_checked_before_discovery(local_db, tmp_path, change):
    service = enrichment(tmp_path, [])
    official = SimpleNamespace(fetch_anime=AsyncMock())
    assert asyncio.run(fetch_candidate_mal_score(
        client=official, db=local_db, candidate=Candidate(1, "tv", "Example", 1),
        details={**DETAILS, **change}, md_item={}, stop_event=asyncio.Event(), jikan=service,
    )) is None
    service.client.search.assert_not_awaited()
    official.fetch_anime.assert_not_awaited()


def test_discovery_can_be_disabled_independently(tmp_path):
    service = enrichment(tmp_path, [], discover_missing=False)
    assert asyncio.run(service.resolve("movie", DETAILS)) is None
    service.client.search.assert_not_awaited()


def test_fallback_can_be_disabled_independently(local_db, tmp_path):
    service = enrichment(tmp_path, [], fallback=False)
    official = SimpleNamespace(fetch_anime=AsyncMock(return_value=response(status=503)))
    assert asyncio.run(fetch_candidate_mal_score(
        client=official, db=local_db, candidate=Candidate(1, "movie", "Example", 1),
        details=DETAILS, md_item={"ids": {"mal": 123}}, stop_event=asyncio.Event(), jikan=service,
    )) is None
    service.client.fetch_anime.assert_not_awaited()


def test_official_timeout_uses_fallback(local_db, tmp_path):
    service = enrichment(tmp_path, [])
    official = SimpleNamespace(fetch_anime=AsyncMock(side_effect=TimeoutError))
    assert asyncio.run(fetch_candidate_mal_score(
        client=official, db=local_db, candidate=Candidate(1, "movie", "Example", 1),
        details=DETAILS, md_item={"ids": {"mal": 123}}, stop_event=asyncio.Event(), jikan=service,
    )) == 80


def test_dashboard_includes_jikan_state(local_db):
    from betterer_ratings.api_server import handle_services
    local_db.update_service_state(service="jikan", paused_until=0, pause_reason="",
                                  rate_limit=None, rate_remaining=None, rate_reset=None,
                                  last_status=200)
    result = asyncio.run(handle_services(SimpleNamespace(app={"db": local_db})))
    services = json.loads(result.text)["services"]
    assert next(s for s in services if s["service"] == "jikan")["last_status"] == 200


def test_get_only_provider_requests():
    client = JikanClient(base_url="http://jikan:8080/v4/", gate=None)
    async def run():
        with respx.mock() as mock:
            search = mock.get("http://jikan:8080/v4/anime").respond(200, json={})
            detail = mock.get("http://jikan:8080/v4/anime/123").respond(200, json={"data": ANIME})
            await client.search(query="Example", page=2, media_type="movie")
            assert (await client.fetch_anime(123)).data["data"] == ANIME
            assert dict(search.calls[0].request.url.params) == {"q": "Example", "page": "2", "limit": "25", "type": "movie"}
            assert detail.calls[0].request.content == b""
        await client.aclose()
    asyncio.run(run())

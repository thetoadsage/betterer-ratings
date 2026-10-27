import asyncio
import copy
import hashlib
import json
import logging
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
from betterer_ratings.services.harvest.cycle_mdblist_phase import run_mdblist_enrichment_phase
from betterer_ratings.services.harvest.enrichment import save_candidate_enrichment
from betterer_ratings.services.harvest.fribb_mappings import (
    FribbAnimeMappings,
    FribbIndex,
    load_fribb_index,
)
from betterer_ratings.services.harvest.mal import fetch_candidate_mal_score
from scripts.preview_fribb_mappings import preview

ROW = {"type": "MOVIE", "themoviedb_id": {"movie": [128]}, "imdb_id": ["tt0119698"],
       "mal_id": 164, "anilist_id": 164, "anidb_id": 7}
MOVIE = {"title": "Princess Mononoke", "release_date": "1997-07-12",
         "external_ids": {"imdb_id": "tt0119698"}}
ANIME = {"mal_id": 164, "title": "Mononoke Hime", "title_english": "Princess Mononoke",
         "type": "Movie", "aired": {"from": "1997-07-12"}, "score": 8.67, "scored_by": 100}
CANDIDATE = Candidate(128, "movie", "Princess Mononoke", 1)
EXPECTED = {"mal": "164"}


def resolve(rows, *, media="movie", tmdb=128, mappings=None):
    return FribbIndex(rows).resolve(media_type=media, tmdb_id=tmdb, mappings=mappings or {})


def test_typed_tmdb_and_imdb_bridges():
    assert resolve([ROW]) == EXPECTED
    assert resolve([{**ROW, "themoviedb_id": {}}], mappings={"imdb": "tt0119698"}) == EXPECTED
    assert resolve([ROW], media="tv") == {}
    assert resolve([ROW], tmdb=999, mappings={"imdb": "tt0119698"}) == {}
    assert resolve([ROW], mappings={"imdb": "tt9999999"}) == {}
    assert resolve([ROW], mappings={"mal": "164", "anidb": "7"}) == EXPECTED


def test_unverified_supplemental_ids_cannot_be_contributed(local_db, tmp_path):
    path = cache_file(tmp_path)
    path.write_text(json.dumps([{**ROW, "anilist_id": 999999999, "anidb_id": 999999998}]))
    cache = FribbAnimeMappings(path)
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": ANIME}, "")))
    with respx.mock():
        result = asyncio.run(fetch_candidate_mal_score(
            client=client, db=local_db, candidate=CANDIDATE, details=MOVIE, md_item={},
            stop_event=asyncio.Event(), fribb_mapping_cache=cache,
        ))
    assert result.validated and result.mal_id == 164
    save(local_db, candidate=CANDIDATE, details=MOVIE, md_item={}, now_ts=100, mal_result=result)
    assert dict(local_db.conn.execute("SELECT id_type, id_value FROM mappings")) == {
        "imdb": "tt0119698", "mal": "164",
    }
    assert local_db.queue_counts()["mappings_pending"] == 2


def test_anidb_from_other_sources_cannot_enter_pmdb_queue(local_db):
    save(local_db, candidate=CANDIDATE, details=MOVIE,
         md_item={"ids": {"anidb": 15517}}, now_ts=100)
    assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type="anidb") is None
    assert local_db.queue_counts()["mappings_pending"] == 1  # IMDb only.
    # The source ID is still available for the local AniDB-to-MAL bridge.
    assert extract_mappings("movie", MOVIE, {"ids": {"anidb": 15517}})["anidb"] == "15517"


@pytest.mark.parametrize("change", [
    {"mal_id": 999}, {"anilist_id": 999}, {"anidb_id": 999},
    {"type": "OVA"}, {"type": None}, {"mal_id": None}, {"mal_id": True},
    {"themoviedb_id": {"movie": [128, 129]}}, {"imdb_id": ["tt0119698", "tt9999999"]},
    {"themoviedb_id": 128}, {"themoviedb_id": {"movie": [128], "tv": 128}},
])
def test_conflicting_and_unsafe_rows_are_not_hidden_by_filtering(change):
    assert resolve([ROW, {**ROW, **change}]) == {}


@pytest.mark.parametrize("provider,value", [("mal", 999), ("anilist", 999), ("anidb", "bad")])
def test_existing_provider_mapping_conflicts_are_rejected(provider, value):
    assert resolve([ROW], mappings={provider: value}) == {}


def test_cross_anchor_conflict_and_shared_anime_ownership():
    other = {**ROW, "themoviedb_id": {"movie": [999]}, "imdb_id": ["tt9999999"],
             "mal_id": 999, "anilist_id": 999, "anidb_id": 999}
    assert resolve([ROW, other], mappings={"imdb": "tt9999999"}) == {}
    # Reusing an anime ID under another title must not queue that association.
    assert resolve([ROW, {**other, "anilist_id": 164}]) == {}
    # Exact duplicate records do not introduce a different identity.
    assert resolve([ROW, copy.deepcopy(ROW)]) == EXPECTED


def tv_row(**changes):
    return {**ROW, "type": "TV", "themoviedb_id": {"tv": 128},
            "season": {"tmdb": 1, "tvdb": 1}, **changes}


@pytest.mark.parametrize("change", [
    {"season": {"tmdb": 2}}, {"season": {"tmdb": 0}}, {"season": {"tmdb": "1"}},
    {"season": {"tvdb": 1}}, {"season": {}}, {"season": None},
    {"episode_offset": {"tmdb": 12}}, {"episode_offset": {"tvdb": 1}},
    {"episode_offset": {"tmdb": "0"}}, {"episode_offset": None},
])
def test_series_season_and_episode_scope(change):
    assert resolve([tv_row()], media="tv") == EXPECTED
    assert resolve([tv_row(**change)], media="tv") == {}
    assert resolve([tv_row(), tv_row(**change)], media="tv") == {}


def test_movie_and_tv_namespace_collision_keeps_distinct_owners():
    show = tv_row(mal_id=999, anilist_id=999, anidb_id=999, imdb_id=["tt9999999"])
    assert resolve([ROW, show]) == EXPECTED
    assert resolve([ROW, show], media="tv") == {"mal": "999"}


def cache_file(tmp_path):
    path = tmp_path / "fribb.json"
    path.write_text(json.dumps([ROW]), encoding="utf-8")
    return path


def test_fresh_cache_never_downloads(tmp_path):
    cache = FribbAnimeMappings(cache_file(tmp_path), url="https://example.test/fribb")
    with respx.mock():
        assert asyncio.run(cache.resolve(media_type="movie", tmdb_id=128, mappings={})) == EXPECTED


@pytest.mark.parametrize("status,body", [(503, "unavailable"), (200, "broken"),
                                        (200, "{}"), (200, "[]"), (200, '[{"type":"TV"}]')])
def test_refresh_failure_preserves_disk_and_memory_and_backs_off(tmp_path, status, body):
    path = cache_file(tmp_path)
    original = path.read_bytes()
    old = time.time() - 8 * 86400
    os.utime(path, (old, old))
    cache = FribbAnimeMappings(path, url="https://example.test/fribb")
    async def run():
        with respx.mock() as mock:
            route = mock.get(cache.url).mock(return_value=httpx.Response(status, text=body))
            for _ in range(2):
                assert await cache.resolve(media_type="movie", tmdb_id=128, mappings={}) == EXPECTED
            assert route.call_count == 1
    asyncio.run(run())
    assert path.read_bytes() == original
    assert not path.with_name(".fribb.json.download.tmp").exists()


def test_successful_refresh_and_invalid_local_schema(tmp_path):
    path = tmp_path / "new.json"
    cache = FribbAnimeMappings(path, url="https://example.test/fribb")
    with respx.mock() as mock:
        mock.get(cache.url).mock(return_value=httpx.Response(200, json=[ROW]))
        assert asyncio.run(cache.resolve(media_type="movie", tmdb_id=128, mappings={})) == EXPECTED
    assert json.loads(path.read_text()) == [ROW]
    path.write_text("{}")
    with pytest.raises(ValueError, match="JSON array"):
        load_fribb_index(path)


def test_config_requires_explicit_opt_in_and_mal(base_valid_config):
    assert not AppConfig.from_mapping(base_valid_config).mal.fribb_mappings
    assert not MALConfig.from_mapping({"enabled": True, "client_id": "test"}).fribb_mappings
    assert MALConfig.from_mapping({"enabled": True, "client_id": "test", "fribb_mappings": True}).fribb_mappings
    for value in ("true", 1, None):
        with pytest.raises(ConfigValidationError, match="must be a boolean"):
            MALConfig.from_mapping({"fribb_mappings": value})
    with pytest.raises(ConfigValidationError, match="requires mal.enabled"):
        MALConfig.from_mapping({"fribb_mappings": True})


def save(db, **kwargs):
    return save_candidate_enrichment(
        db=db, parse_mdblist_ratings_fn=parse_mdblist_ratings,
        parse_tmdb_vote_average_fn=parse_tmdb_vote_average, extract_mappings_fn=extract_mappings,
        **kwargs,
    )


@pytest.mark.parametrize("anime,valid,score", [
    (ANIME, True, 86.7), ({**ANIME, "score": None}, True, None),
    ({**ANIME, "type": "Special"}, False, None),
    ({**ANIME, "title": "Other", "title_english": "Other"}, False, None),
    ({**ANIME, "aired": {"from": "2000-07-12"}}, False, None),
])
def test_only_validated_fribb_mal_enters_the_queue(local_db, tmp_path, anime, valid, score):
    cache = FribbAnimeMappings(cache_file(tmp_path))
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": anime}, "")))
    with respx.mock():
        result = asyncio.run(fetch_candidate_mal_score(
            client=client, db=local_db, candidate=CANDIDATE, details=MOVIE, md_item={},
            stop_event=asyncio.Event(), fribb_mapping_cache=cache,
        ))
    assert result.provenance == "fribb_mapping"
    assert (result.validated, result.score) == (valid, score)
    client.fetch_anime.assert_awaited_once_with(164)
    save(local_db, candidate=CANDIDATE, details=MOVIE, md_item={}, now_ts=100, mal_result=result)
    for provider, value in EXPECTED.items():
        assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type=provider) == (
            value if valid else None
        )
    for provider in ("anilist", "anidb"):
        assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type=provider) is None
    assert local_db.queue_counts()["mappings_pending"] == (2 if valid else 1)  # Also IMDb.


def test_existing_mappings_and_provider_failures_are_preserved(local_db, tmp_path):
    local_db.conn.execute(
        "INSERT INTO mappings (tmdb_id, media_type, id_type, id_value, fetched_at) VALUES (128, 'movie', 'anilist', '999', 1)"
    )
    cache = FribbAnimeMappings(cache_file(tmp_path))
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(503, {}, {}, "")))
    result = asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE, md_item={},
        stop_event=asyncio.Event(), fribb_mapping_cache=cache,
    ))
    assert not result.validated
    client.fetch_anime.assert_not_awaited()  # Fribb disagrees with the stored AniList mapping.
    local_db.conn.execute("DELETE FROM mappings")
    result = asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE, md_item={},
        stop_event=asyncio.Event(), fribb_mapping_cache=cache,
    ))
    assert result.outcome == "provider_failure"
    save(local_db, candidate=CANDIDATE, details=MOVIE, md_item={}, now_ts=100, mal_result=result)
    assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type="mal") is None


def test_existing_supplemental_mapping_is_preserved(local_db, tmp_path):
    cache = FribbAnimeMappings(cache_file(tmp_path))
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": ANIME}, "")))
    result = asyncio.run(fetch_candidate_mal_score(
        client=client, db=local_db, candidate=CANDIDATE, details=MOVIE, md_item={},
        stop_event=asyncio.Event(), fribb_mapping_cache=cache,
    ))
    local_db.conn.execute(
        "INSERT INTO mappings (tmdb_id, media_type, id_type, id_value, fetched_at) VALUES (128, 'movie', 'anilist', '999', 1)"
    )
    save(local_db, candidate=CANDIDATE, details=MOVIE, md_item={}, now_ts=100, mal_result=result)
    assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type="anilist") == "999"


def test_fribb_series_still_requires_full_mal_scope_validation(local_db, tmp_path):
    path = cache_file(tmp_path)
    path.write_text(json.dumps([tv_row()]))
    cache = FribbAnimeMappings(path)
    details = {"name": "Example", "status": "Ended", "number_of_seasons": 1,
               "number_of_episodes": 12, "first_air_date": "2020-01-01", "last_air_date": "2020-03-18"}
    anime = {"mal_id": 164, "title": "Example", "type": "TV", "status": "Finished Airing",
             "episodes": 12, "aired": {"from": "2020-01-01", "to": "2020-03-18"}}
    async def run():
        for episodes in (12, 24):
            client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(
                200, {}, {"data": {**anime, "episodes": episodes}}, ""
            )))
            result = await fetch_candidate_mal_score(
                client=client, db=local_db, candidate=Candidate(128, "tv", "Example", 1),
                details=details, md_item={}, stop_event=asyncio.Event(), fribb_mapping_cache=cache,
            )
            assert result.validated == (episodes == 12)
            save(local_db, candidate=Candidate(128, "tv", "Example", 1), details=details,
                 md_item={}, now_ts=100, mal_result=result)
            for provider in ("anilist", "anidb"):
                assert local_db.get_title_mapping(tmdb_id=128, media_type="tv", id_type=provider) is None
    asyncio.run(run())


@pytest.mark.parametrize("enabled,fribb", [(False, False), (True, False), (True, True)])
def test_container_wires_cache_only_when_enabled(base_valid_config, tmp_path, monkeypatch, enabled, fribb):
    import betterer_ratings.constants as constants
    from betterer_ratings.wiring.container import build_container

    monkeypatch.setattr(constants, "CONTAINER_DATABASE_PATH", str(tmp_path / "container.sqlite3"))
    monkeypatch.setattr(constants, "CONTAINER_IMDB_ARCHIVE_PATH", str(tmp_path / "imdb"))
    monkeypatch.setattr(constants, "CONTAINER_TEMP_PATH", str(tmp_path / "temp"))
    base_valid_config["mal"] = {"enabled": enabled, "client_id": "test", "fribb_mappings": fribb}
    with respx.mock():
        container = build_container(config=AppConfig.from_mapping(base_valid_config))
        assert (container.fribb_mapping_cache is not None) == fribb
        assert container.harvester.fribb_mapping_cache is container.fribb_mapping_cache
        if fribb:
            assert container.fribb_mapping_cache.cache_path == tmp_path / "temp/anime-mappings/fribb-anime-list-full.json"
        async def close():
            for client in (container.tmdb_client, container.mdblist_client, container.pmdb_client,
                           container.mal_client):
                if client is not None:
                    await client.aclose()
        asyncio.run(close())
        container.db.close()


def test_offline_preview_counts_potential_ids_without_writes_or_requests(local_db, tmp_path):
    save(local_db, candidate=CANDIDATE, details=MOVIE, md_item={}, now_ts=100)
    local_db.save_enrichment_state(128, "movie", "tmdb", payload=MOVIE, now_ts=100, next_due=200)
    database = tmp_path / "test.sqlite3"
    before = local_db.conn.iterdump()
    snapshot = list(before)
    with respx.mock():
        report = preview(database, cache_file(tmp_path), limit=1)
    assert report["potential_new_local_mappings"] == {"mal": 1}
    assert report["scanned_titles"] == report["matched_candidates"] == 1
    assert report["requires_mal_validation"] and not report["pmdb_coverage_checked"]
    assert list(local_db.conn.iterdump()) == snapshot


def test_scope_and_cancellation_prevent_optional_activity(local_db):
    cache = SimpleNamespace(resolve=AsyncMock(return_value=EXPECTED))
    client = SimpleNamespace(fetch_anime=AsyncMock())
    stop = asyncio.Event()
    stop.set()
    for candidate, details, event in (
        (CANDIDATE, MOVIE, stop), (CANDIDATE, None, asyncio.Event()),
        (Candidate(128, "tv", "Example", 1), {"status": "Ended", "number_of_seasons": 2}, asyncio.Event()),
    ):
        asyncio.run(fetch_candidate_mal_score(
            client=client, db=local_db, candidate=candidate, details=details, md_item={},
            stop_event=event, fribb_mapping_cache=cache,
        ))
    cache.resolve.assert_not_awaited()
    client.fetch_anime.assert_not_awaited()


def test_cycle_caches_results_and_invalidates_on_enable_disable(local_db, tmp_path):
    cache = FribbAnimeMappings(cache_file(tmp_path))
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": ANIME}, "")))
    # MDBList is not due; optional enrichment works from cached provider state.
    local_db.save_enrichment_state(128, "movie", "mdblist", payload={}, now_ts=1, next_due=10000)
    harvester = SimpleNamespace(
        db=local_db, mal_client=client, fribb_mapping_cache=None,
        mdblist_client=SimpleNamespace(fetch_for_candidates=AsyncMock()),
        _save_candidate_enrichment=lambda **kw: save(local_db, **kw),
    )
    async def cycle():
        await run_mdblist_enrichment_phase(
            harvester=harvester, logger=logging.getLogger("test"), now_epoch_fn=lambda: 100,
            stop_event=asyncio.Event(), candidates=[CANDIDATE],
            tmdb_details={("movie", 128): MOVIE}, source_stats={}, local_stats={},
            tmdb_list_request_errors=0, harvest_cycle_result_cls=HarvestCycleResult,
        )
    async def run():
        with respx.mock():
            await cycle()  # Missing ID is cached without Fribb.
            harvester.fribb_mapping_cache = cache
            await cycle()  # Enablement invalidates that cached miss.
            await cycle()  # Reuses validated result.
            assert client.fetch_anime.await_count == 1
            harvester.fribb_mapping_cache = None
            await cycle()  # Stored MAL remains useful, Fribb fields no longer replay.
            assert client.fetch_anime.await_count == 2
    asyncio.run(run())
    state = local_db.get_enrichment_state(128, "movie", "mal")["payload"]
    assert "anime_mappings" not in state["result"]
    assert local_db.get_title_mapping(tmdb_id=128, media_type="movie", id_type="anidb") is None
    assert local_db.queue_counts()["mappings_pending"] == 2
    harvester.mdblist_client.fetch_for_candidates.assert_not_awaited()


@pytest.mark.parametrize("fribb_enabled", [False, True])
def test_legacy_cached_supplements_are_ignored(local_db, tmp_path, fribb_enabled):
    cache = FribbAnimeMappings(cache_file(tmp_path)) if fribb_enabled else None
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": ANIME}, "")))
    # Seed a known MAL identity so either enablement state can populate the cache.
    local_db.conn.execute(
        "INSERT INTO mappings (tmdb_id, media_type, id_type, id_value, fetched_at) VALUES (128, 'movie', 'mal', '164', 1)"
    )
    local_db.save_enrichment_state(128, "movie", "mdblist", payload={}, now_ts=1, next_due=10000)
    harvester = SimpleNamespace(
        db=local_db, mal_client=client, fribb_mapping_cache=cache,
        mdblist_client=SimpleNamespace(fetch_for_candidates=AsyncMock()),
        _save_candidate_enrichment=lambda **kw: save(local_db, **kw),
    )
    async def cycle():
        await run_mdblist_enrichment_phase(
            harvester=harvester, logger=logging.getLogger("test"), now_epoch_fn=lambda: 100,
            stop_event=asyncio.Event(), candidates=[CANDIDATE],
            tmdb_details={("movie", 128): MOVIE}, source_stats={}, local_stats={},
            tmdb_list_request_errors=0, harvest_cycle_result_cls=HarvestCycleResult,
        )
    async def run():
        with respx.mock():
            await cycle()
            state = local_db.get_enrichment_state(128, "movie", "mal")["payload"]
            state["result"]["anime_mappings"] = {
                "mal": "164", "anilist": "999999999", "anidb": "999999998",
            }
            local_db.save_enrichment_state(128, "movie", "mal", payload=state, now_ts=100, next_due=10000)
            await cycle()  # Same context: old serialized result must still be safe.
            assert client.fetch_anime.await_count == 1
            if fribb_enabled:
                state["context"] = hashlib.sha256(json.dumps(
                    [MOVIE, {"imdb": "tt0119698"}, "164", "fribb_mappings_v1"], sort_keys=True,
                ).encode()).hexdigest()
                local_db.save_enrichment_state(128, "movie", "mal", payload=state, now_ts=100, next_due=10000)
                await cycle()  # The old policy marker forces fresh MAL validation.
                assert client.fetch_anime.await_count == 2
                refreshed = local_db.get_enrichment_state(128, "movie", "mal")["payload"]
                assert "anime_mappings" not in refreshed["result"]
    asyncio.run(run())
    assert dict(local_db.conn.execute("SELECT id_type, id_value FROM mappings")) == {
        "imdb": "tt0119698", "mal": "164",
    }

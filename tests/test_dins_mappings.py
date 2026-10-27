from __future__ import annotations

import asyncio
import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from betterer_ratings.config.schema import AppConfig, ConfigValidationError, DinsConfig
from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.core.scoring import parse_mdblist_ratings, parse_tmdb_vote_average
from betterer_ratings.domain.models import APIResponse, Candidate, IMDbArchiveCandidate
from betterer_ratings.providers.tmdb_client import TMDBClient
from betterer_ratings.services.harvest.dins_mappings import (
    DATASET_BASE_URL,
    REFRESH_SECONDS,
    DinsMappings,
    build_index,
)
from betterer_ratings.services.harvest.enrichment import save_candidate_enrichment
from betterer_ratings.services.harvest.harvester import Harvester
from betterer_ratings.services.harvest.imdb_mapping_helpers import resolve_imdb_to_tmdb_local

MOVIE = "imdb_id,tmdb_id,tvdb_id\ntt0010,10,55\ntt0011,,56\n"
TV = "imdb_id,tmdb_id,tvdb_id,tvmaze_id\ntt0010,10,65,75\n"


def files(tmp_path, movie=MOVIE, tv=TV):
    result = {"movie": tmp_path / "movie.csv", "tv": tmp_path / "tv.csv"}
    result["movie"].write_text(movie)
    result["tv"].write_text(tv)
    return result


@pytest.fixture
def cache(tmp_path):
    path = tmp_path / "index.sqlite3"
    build_index(path, files(tmp_path))
    result = DinsMappings(path)
    asyncio.run(result.prepare())
    yield result
    result.close()


def save(db, cache, *, details=None, md_item=None, candidate=None, now=100):
    return save_candidate_enrichment(
        db=db,
        candidate=candidate or Candidate(10, "movie", "Example", 1),
        details=details,
        md_item=md_item,
        now_ts=now,
        dins_mapping_cache=cache,
        extract_mappings_fn=extract_mappings,
        parse_mdblist_ratings_fn=parse_mdblist_ratings,
        parse_tmdb_vote_average_fn=parse_tmdb_vote_average,
    )


def seed(db, tmdb=10, provider="imdb", value="tt9999", media="movie"):
    with db.conn:
        db._upsert_title(tmdb, media, "Existing", None, 1, None, 10, None)
        db._upsert_mapping(tmdb, media, provider, value, 10)


def resolve(db, cache, imdb="tt0010", media="movie", cached=None):
    return resolve_imdb_to_tmdb_local(
        db=db,
        imdb_cache=SimpleNamespace(get=lambda *args: cached),
        imdb_id=imdb,
        media_type=media,
        normalize_imdb_title_id_fn=normalize_imdb_title_id,
        parse_int_fn=parse_int,
        dins_mapping_cache=cache,
    )


def test_config_opt_in_and_strict_validation(base_valid_config):
    assert not AppConfig.from_mapping(base_valid_config).dins.enabled
    base_valid_config["dins"] = {"enabled": True}
    assert AppConfig.from_mapping(base_valid_config).dins.enabled
    for invalid in ({"enabled": "true"}, {"enabled": 1}, {"url": "x"}):
        with pytest.raises(ConfigValidationError):
            DinsConfig.from_mapping(invalid)


def test_namespaces_partial_rows_and_no_tvmaze_contributions(cache):
    assert cache.lookup("movie", "tmdb", "10").mappings() == {"imdb": "tt0010", "tvdb": "55"}
    assert cache.lookup("tv", "imdb", "tt0010").mappings() == {"imdb": "tt0010", "tvdb": "65"}
    assert cache.lookup("movie", "tvdb", "56").tmdb_id == ""
    assert cache.lookup("movie", "tvmaze", "75") is None
    assert cache.lookup("episode", "imdb", "tt0010") is None
    assert cache.lookup("movie", "tmdb", "999") is None


@pytest.mark.parametrize("extra", ["tt0010,12,57\n", "tt0012,10,57\n", "tt0012,12,55\n"])
def test_duplicate_ownership_is_retained_and_rejected(tmp_path, extra):
    path = tmp_path / "index.sqlite3"
    build_index(path, files(tmp_path, MOVIE + extra))
    cache = DinsMappings(path)
    asyncio.run(cache.prepare())
    try:
        assert cache.lookup("movie", "tmdb", "10") is None
        assert cache.lookup("movie", "imdb", "tt0010") is None
    finally:
        cache.close()


@pytest.mark.parametrize(
    "movie",
    [
        "imdb_id,tmdb_id\ntt0010,10\n",
        "imdb_id,tmdb_id,tvdb_id\n",
        "imdb_id,tmdb_id,tvdb_id\ntt0010,,55\n",
        MOVIE + "oops,12,57\n",
        MOVIE + "tt0012,0,57\n",
        MOVIE + "tt0012,12\n",
        MOVIE + "tt0012,12,57,extra\n",
    ],
)
def test_bad_exports_cannot_become_cache(tmp_path, movie):
    with pytest.raises(ValueError):
        build_index(tmp_path / "bad.sqlite3", files(tmp_path, movie))


def test_fills_missing_mappings_and_is_idempotent(local_db, cache):
    first = save(local_db, cache, details={"title": "Example", "vote_average": 8})
    assert first[-1] == 2
    assert dict(local_db.conn.execute("SELECT id_type, id_value FROM mappings")) == {
        "imdb": "tt0010",
        "tvdb": "55",
    }
    assert local_db.queue_counts()["mappings_pending"] == 2
    assert save(local_db, cache, details={"title": "Example", "vote_average": 8}, now=101)[-1] == 0
    assert dict(local_db.conn.execute("SELECT label, score FROM ratings")) == {"TM": 80}


def test_imdb_anchor_can_supply_tvdb_without_dataset_tmdb_id(local_db, cache):
    save(
        local_db,
        cache,
        candidate=Candidate(11, "movie", "Other", 1),
        md_item={"ids": {"imdb": "tt0011"}},
    )
    assert dict(
        local_db.conn.execute("SELECT id_type, id_value FROM mappings WHERE tmdb_id=11")
    ) == {"imdb": "tt0011", "tvdb": "56"}


def test_imdb_anchor_cannot_join_different_dataset_tmdb(local_db, cache):
    save(
        local_db,
        cache,
        candidate=Candidate(99, "movie", "Other", 1),
        md_item={"ids": {"imdb": "tt0010"}},
    )
    assert dict(local_db.conn.execute("SELECT id_type, id_value FROM mappings")) == {
        "imdb": "tt0010"
    }


@pytest.mark.parametrize("provider,value", [("imdb", "tt9999"), ("tvdb", "999")])
def test_source_disagreement_skips_supplements(local_db, cache, provider, value):
    save(local_db, cache, md_item={"ids": {provider: value}})
    assert dict(local_db.conn.execute("SELECT id_type, id_value FROM mappings")) == {
        provider: value
    }


@pytest.mark.parametrize(
    "tmdb,provider,value",
    [
        (10, "imdb", "tt9999"),
        (10, "tvdb", "999"),
        (20, "imdb", "tt0010"),
        (20, "tvdb", "55"),
    ],
)
def test_stored_conflicts_and_other_owners_prevent_additions(
    local_db, cache, tmdb, provider, value
):
    seed(local_db, tmdb, provider, value)
    assert save(local_db, cache)[-1] == 0
    assert local_db.conn.execute("SELECT COUNT(*) FROM mappings").fetchone()[0] == 1


def test_title_and_archive_identity_guards(local_db, cache):
    seed(local_db, provider="trakt", value="2")
    local_db.conn.execute("UPDATE titles SET imdb_id='tt9999'")
    assert save(local_db, cache)[-1] == 0
    local_db.conn.execute("UPDATE titles SET imdb_id=NULL")
    local_db.get_enrichment_state = lambda *args: {"payload": {"imdb_id": "tt9999", "score": 80}}
    assert save(local_db, cache)[-1] == 0


def test_stored_same_ids_refresh_without_resetting_submission(local_db, cache):
    save(local_db, cache)
    local_db.conn.execute("UPDATE mappings SET pmdb_status='submitted', pmdb_item_id='remote-id', pmdb_item_value=id_value")
    assert save(local_db, cache, now=101)[-1] == 0
    assert {
        tuple(row)
        for row in local_db.conn.execute(
            "SELECT pmdb_status, pmdb_item_id, fetched_at FROM mappings"
        )
    } == {("submitted", "remote-id", 101)}


def test_archive_resolution_uses_dataset_but_keeps_local_conflict_guards(local_db, cache):
    assert resolve(local_db, cache).tmdb_id == 10
    assert resolve(local_db, cache, imdb="tt0011") is None  # No TMDB ID.
    assert resolve(local_db, cache, cached=(20, "Cache", 1)) is None
    seed(local_db, tmdb=20, value="tt0010")
    assert resolve(local_db, cache) is None
    seed(local_db, value="tt0010")
    assert resolve(local_db, cache) is None  # Two local owners.


def test_target_with_other_imdb_identity_cannot_receive_archive_score(local_db, cache):
    seed(local_db)
    assert resolve(local_db, cache) is None


def test_real_harvester_paths_avoid_find_and_fill_enrichment(
    local_db, cache, base_valid_config, tmp_path, monkeypatch
):
    import betterer_ratings.constants as constants

    monkeypatch.setattr(constants, "CONTAINER_IMDB_ARCHIVE_PATH", str(tmp_path / "imdb"))
    monkeypatch.setattr(constants, "CONTAINER_TEMP_PATH", str(tmp_path / "temp"))
    client = SimpleNamespace(
        build_source=TMDBClient.build_source,
        fetch_find_by_imdb=AsyncMock(
            return_value=APIResponse(
                200, {}, {"movie_results": [{"id": 99, "title": "Fallback"}]}, ""
            )
        ),
    )
    h = Harvester(
        config=AppConfig.from_mapping(base_valid_config),
        db=local_db,
        tmdb_client=client,
        mdblist_client=SimpleNamespace(),
        dins_mapping_cache=cache,
    )
    try:
        mapped, errors, missing = asyncio.run(
            h._map_imdb_candidates_to_tmdb(
                candidates=[IMDbArchiveCandidate("tt0010", "movie", 1000, 8)],
                stop_event=asyncio.Event(),
            )
        )
        assert (errors, missing) == (0, 0)
        assert mapped[0].tmdb_id == 10
        client.fetch_find_by_imdb.assert_not_awaited()
        h._save_candidate_enrichment(
            candidate=mapped[0], details={"title": "Real title"}, md_item={}, now_ts=100
        )
        assert (
            local_db.conn.execute("SELECT title FROM titles WHERE tmdb_id=10").fetchone()[0]
            == "Real title"
        )
        assert local_db.queue_counts()["mappings_pending"] == 2
        parents, _, _ = asyncio.run(
            h._map_imdb_episode_parents_to_tmdb(parent_ids=["tt0010"], stop_event=asyncio.Event())
        )
        assert parents["tt0010"].media_type == "tv"
        client.fetch_find_by_imdb.assert_not_awaited()
        # A conflict goes through the existing exact remote lookup.
        seed(local_db, tmdb=20, value="tt0010")
        mapped, _, _ = asyncio.run(
            h._map_imdb_candidates_to_tmdb(
                candidates=[IMDbArchiveCandidate("tt0010", "movie", 1000, 8)],
                stop_event=asyncio.Event(),
            )
        )
        assert mapped[0].tmdb_id == 99
        assert client.fetch_find_by_imdb.await_count == 1
    finally:
        h.close()


def test_daily_refresh_reuses_cache_and_restarts_without_network(tmp_path):
    cache = DinsMappings(tmp_path / "index.sqlite3")
    with respx.mock() as router:
        movie = router.get(f"{DATASET_BASE_URL}/movie_mappings.csv").mock(
            return_value=httpx.Response(200, text=MOVIE)
        )
        tv = router.get(f"{DATASET_BASE_URL}/tv_mappings.csv").mock(
            return_value=httpx.Response(200, text=TV)
        )

        async def prepare():
            await asyncio.gather(*(cache.prepare() for _ in range(8)))

        asyncio.run(prepare())
        assert movie.call_count == tv.call_count == 1
        assert cache.lookup("tv", "tmdb", "10").tvdb_id == "65"
        cache.close()
    restarted = DinsMappings(cache.cache_path)
    with respx.mock(assert_all_called=False) as router:
        asyncio.run(restarted.prepare())
        assert not router.calls
        assert restarted.lookup("movie", "tmdb", "10").tvdb_id == "55"
    restarted.close()
    assert not list(tmp_path.glob(".*.tmp*"))


def test_successful_refresh_replaces_both_exports_and_active_connection(cache):
    old_time = time.time() - REFRESH_SECONDS - 10
    os.utime(cache.cache_path, (old_time, old_time))
    cache._next_check = 0
    with respx.mock() as router:
        router.get(f"{DATASET_BASE_URL}/movie_mappings.csv").mock(
            return_value=httpx.Response(200, text=MOVIE.replace(",55", ",999"))
        )
        router.get(f"{DATASET_BASE_URL}/tv_mappings.csv").mock(
            return_value=httpx.Response(200, text=TV.replace(",65", ",888"))
        )
        asyncio.run(cache.prepare())
    assert cache.lookup("movie", "tmdb", "10").tvdb_id == "999"
    assert cache.lookup("tv", "tmdb", "10").tvdb_id == "888"
    assert time.time() - cache.cache_path.stat().st_mtime < 60


def test_corrupt_cache_is_recovered_by_download(tmp_path):
    path = tmp_path / "index.sqlite3"
    path.write_text("not a sqlite database")
    cache = DinsMappings(path)
    with respx.mock() as router:
        router.get(f"{DATASET_BASE_URL}/movie_mappings.csv").mock(
            return_value=httpx.Response(200, text=MOVIE)
        )
        router.get(f"{DATASET_BASE_URL}/tv_mappings.csv").mock(
            return_value=httpx.Response(200, text=TV)
        )
        asyncio.run(cache.prepare())
    try:
        assert cache.lookup("movie", "tmdb", "10").tvdb_id == "55"
    finally:
        cache.close()


@pytest.mark.parametrize(
    "bad_tv",
    [httpx.Response(503), httpx.Response(200, text="broken"), httpx.Response(200, content=b"\xff")],
)
def test_partial_refresh_failure_preserves_both_old_exports(cache, bad_tv):
    before = cache.cache_path.read_bytes()
    old_time = time.time() - REFRESH_SECONDS - 10
    os.utime(cache.cache_path, (old_time, old_time))
    cache._next_check = 0
    with respx.mock() as router:
        movie = router.get(f"{DATASET_BASE_URL}/movie_mappings.csv").mock(
            return_value=httpx.Response(200, text=MOVIE.replace(",55", ",999"))
        )
        router.get(f"{DATASET_BASE_URL}/tv_mappings.csv").mock(return_value=bad_tv)
        asyncio.run(cache.prepare())
        cache._next_check = 0
        asyncio.run(cache.prepare())
        assert movie.call_count == 1  # One-hour failure cooldown.
    assert cache.cache_path.read_bytes() == before
    assert cache.lookup("movie", "tmdb", "10").tvdb_id == "55"
    assert cache.lookup("tv", "tmdb", "10").tvdb_id == "65"


def test_first_download_failure_is_nonfatal_and_cools_down(tmp_path):
    cache = DinsMappings(tmp_path / "index.sqlite3")
    with respx.mock() as router:
        movie = router.get(f"{DATASET_BASE_URL}/movie_mappings.csv").mock(
            return_value=httpx.Response(503)
        )
        asyncio.run(cache.prepare())
        cache._next_check = 0
        asyncio.run(cache.prepare())
        assert movie.call_count == 1
    assert cache.lookup("movie", "imdb", "tt0010") is None
    assert not cache.cache_path.exists()


@pytest.mark.parametrize("enabled", [False, True])
def test_container_only_wires_enabled_cache(base_valid_config, tmp_path, monkeypatch, enabled):
    import betterer_ratings.constants as constants
    from betterer_ratings.wiring.container import build_container

    monkeypatch.setattr(constants, "CONTAINER_DATABASE_PATH", str(tmp_path / "app.sqlite3"))
    monkeypatch.setattr(constants, "CONTAINER_TEMP_PATH", str(tmp_path / "temp"))
    monkeypatch.setattr(constants, "CONTAINER_IMDB_ARCHIVE_PATH", str(tmp_path / "imdb"))
    base_valid_config["dins"] = {"enabled": enabled}
    c = build_container(config=AppConfig.from_mapping(base_valid_config))
    try:
        assert bool(c.dins_mapping_cache) == enabled
        assert c.harvester.dins_mapping_cache is c.dins_mapping_cache
        if enabled:
            assert c.dins_mapping_cache.cache_path == tmp_path / "temp/dins-mappings/index.sqlite3"
            assert not c.dins_mapping_cache.cache_path.exists()  # Wiring does not download.
    finally:
        c.harvester.close()
        c.db.close()

        async def close():
            for client in (c.tmdb_client, c.mdblist_client, c.pmdb_client):
                await client.aclose()

        asyncio.run(close())

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from betterer_ratings.domain.models import APIResponse, Candidate, IMDbEpisodeArchiveCandidate
from betterer_ratings.services.harvest.episode_identity import verify_episode_identities
from betterer_ratings.services.harvest.episodes import run_imdb_episode_cycle

ENTRY = IMDbEpisodeArchiveCandidate("tt0010", "tt0011", 1, 1, 80, 300)


def response(rows):
    return APIResponse(200, {}, {"tv_episode_results": rows}, "")


def coordinates(show=10, season=2, episode=5):
    return {"show_id": show, "season_number": season, "episode_number": episode}


def verify(db, client, now=100, stop=None, entries=None):
    return asyncio.run(verify_episode_identities(
        entries=entries or [ENTRY], tmdb_id=10, db=db, tmdb_client=client,
        stop_event=stop or asyncio.Event(), now_epoch_fn=lambda: now,
    ))


def test_exact_imdb_lookup_corrects_coordinates_and_caches(local_db):
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(return_value=response([coordinates()])))
    matched, failures, skipped = verify(local_db, client)
    assert failures == skipped == 0
    assert [(r.season, r.episode) for r in matched] == [(2, 5)]
    assert matched[0].episode_imdb_id == ENTRY.episode_imdb_id
    assert matched[0].score == 80
    verify(local_db, client, now=101)
    client.fetch_find_by_imdb.assert_awaited_once_with("tt0011")
    verify(local_db, client, now=100 + 7 * 86400)
    assert client.fetch_find_by_imdb.await_count == 2


@pytest.mark.parametrize("rows", [[], [coordinates(show=999)], [coordinates(), coordinates()],
    [coordinates(season=0)], [coordinates(season=True)], [coordinates(episode=-1)],
    [coordinates(show="10")], [None], [{}]])
def test_unmapped_ambiguous_wrong_show_and_invalid_coordinates_are_skipped(local_db, rows):
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(return_value=response(rows)))
    matched, failures, skipped = verify(local_db, client)
    assert matched == []
    assert failures == 0
    assert skipped == 1
    verify(local_db, client, now=101)
    assert client.fetch_find_by_imdb.await_count == 1


@pytest.mark.parametrize("reply", [APIResponse(503, {}, {}, "down"), APIResponse(429, {}, {}, "paused"),
                                  APIResponse(200, {}, {}, "malformed")])
def test_transient_failure_retries_without_caching_a_match(local_db, reply):
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(return_value=reply))
    assert verify(local_db, client) == ([], 1, 0)
    assert verify(local_db, client, now=101) == ([], 1, 0)
    assert client.fetch_find_by_imdb.await_count == 1
    client.fetch_find_by_imdb.return_value = response([coordinates()])
    matched, errors, _ = verify(local_db, client, now=400)
    assert len(matched) == 1 and errors == 0


def test_episode_lookup_cancellation_propagates_without_negative_cache(local_db):
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        verify(local_db, client)
    assert local_db.conn.execute("SELECT COUNT(*) FROM imdb_episode_identity").fetchone()[0] == 0


def test_two_imdb_ids_cannot_claim_the_same_episode(local_db):
    other = IMDbEpisodeArchiveCandidate("tt0010", "tt0012", 1, 2, 90, 300)
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(return_value=response([coordinates()])))
    assert verify(local_db, client, entries=[ENTRY, other]) == ([], 0, 2)


@pytest.mark.parametrize("temporary_failure", [False, True])
def test_episode_cycle_queues_only_verified_coordinates_and_preserves_retry_cursor(local_db, temporary_failure):
    client = SimpleNamespace(fetch_find_by_imdb=AsyncMock(return_value=(
        APIResponse(503, {}, {}, "down") if temporary_failure else response([coordinates()])
    )))
    h = SimpleNamespace(
        db=local_db, imdb_archive_source=object(), imdb_episodes_enabled=True,
        _imdb_episode_exhausted_key="exhausted", _imdb_episode_last_full_scan_key="last",
        episode_ratings_ttl_seconds=86400, details_concurrency=2, tmdb_client=client,
        _ensure_imdb_episode_index=Mock(),
        _read_imdb_episode_index_batch=Mock(return_value=([ENTRY], 1, 50, True)),
        _map_imdb_episode_parents_to_tmdb=AsyncMock(return_value=({"tt0010": Candidate(10,"tv","Show",0)},0,0)),
        _commit_imdb_episode_cursor=Mock(),
    )
    stats = asyncio.run(run_imdb_episode_cycle(
        harvester=h, stop_event=asyncio.Event(), logger=logging.getLogger("test"), now_epoch_fn=lambda: 100,
    ))
    rows = local_db.conn.execute("SELECT * FROM episode_ratings").fetchall()
    if temporary_failure:
        assert rows == []
        h._commit_imdb_episode_cursor.assert_not_called()
        assert stats["lookup_errors"] == 1
    else:
        assert [(r["season"], r["episode"], r["score"]) for r in rows] == [(2, 5, 80)]
        h._commit_imdb_episode_cursor.assert_called_once()

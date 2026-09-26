from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.scoring import parse_mdblist_ratings, parse_tmdb_vote_average
from betterer_ratings.domain.models import (
    APIResponse,
    Candidate,
    HarvestCycleResult,
    IMDbArchiveCandidate,
)
from betterer_ratings.infra.db.local_database import LocalDatabase
from betterer_ratings.infra.db.schema_repo import MIGRATIONS, apply_migrations
from betterer_ratings.services.harvest.cycle_mdblist_phase import run_mdblist_enrichment_phase
from betterer_ratings.services.harvest.details import fetch_tmdb_details
from betterer_ratings.services.harvest.enrichment import save_candidate_enrichment
from betterer_ratings.services.harvest.imdb_mapping_async import map_imdb_candidates_to_tmdb

TITLE = Candidate(10, "movie", "Example", 1.0, "ttl")
DETAILS = {"title": "Example", "release_date": "2020-01-01", "vote_average": 8.2,
           "external_ids": {"imdb_id": "tt0010", "tvdb_id": 55}}
LOG = logging.getLogger("test")


def save(db, **kwargs):
    return save_candidate_enrichment(
        db=db, parse_mdblist_ratings_fn=parse_mdblist_ratings,
        parse_tmdb_vote_average_fn=parse_tmdb_vote_average,
        extract_mappings_fn=extract_mappings, **kwargs,
    )


def phase(db, client, *, now=100, mal=None, stop=None, **extra):
    h = SimpleNamespace(db=db, mdblist_client=client, mal_client=mal,
                        ratings_ttl_seconds=1000, failed_retry_seconds=200,
                        _save_candidate_enrichment=lambda **kw: save(db, **kw), **extra)
    return asyncio.run(run_mdblist_enrichment_phase(
        harvester=h, logger=LOG, now_epoch_fn=lambda: now, stop_event=stop or asyncio.Event(),
        candidates=[TITLE], tmdb_details={("movie", 10): DETAILS},
        source_stats={}, local_stats={}, tmdb_list_request_errors=0,
        harvest_cycle_result_cls=HarvestCycleResult,
    ))


def pause(db, until):
    with db.conn:
        db.conn.execute("INSERT OR REPLACE INTO service_state(service, paused_until, pause_reason) "
                        "VALUES ('mdblist', ?, 'daily limit reached')", (until,))


def test_tmdb_cache_retains_metadata_and_refreshes_by_age(local_db):
    client = SimpleNamespace(fetch_details=AsyncMock(return_value=APIResponse(200, {}, DETAILS, "")))
    def fetch(now):
        return asyncio.run(fetch_tmdb_details(
            candidates=[TITLE], stop_event=asyncio.Event(), tmdb_client=client,
            db=local_db, details_concurrency=2, now_epoch_fn=lambda now=now: now,
            logger=LOG, refresh_seconds=100,
        ))[0][("movie", 10)]
    assert fetch(100) == DETAILS
    assert fetch(199) == DETAILS
    assert client.fetch_details.await_count == 1
    assert fetch(200) == DETAILS
    assert client.fetch_details.await_count == 2


def test_tmdb_failure_does_not_serve_expired_cache_or_erase_success(local_db):
    local_db.save_enrichment_state(10, "movie", "tmdb", payload=DETAILS, now_ts=100, next_due=200)
    client = SimpleNamespace(fetch_details=AsyncMock(return_value=APIResponse(503, {}, None, "down")))
    for now in (200, 201):
        result, _ = asyncio.run(fetch_tmdb_details(
            candidates=[TITLE], stop_event=asyncio.Event(), tmdb_client=client,
            db=local_db, details_concurrency=1, now_epoch_fn=lambda now=now: now,
            logger=LOG, refresh_seconds=100,
        ))
        assert result[("movie", 10)] is None
    assert client.fetch_details.await_count == 1
    assert local_db.get_enrichment_state(10, "movie", "tmdb")["fetched_at"] == 100


def test_quota_pause_persists_tmdb_and_retries_mdblist_at_reset(local_db):
    client = SimpleNamespace(fetch_for_candidates=AsyncMock())
    pause(local_db, 1000)
    phase(local_db, client)
    client.fetch_for_candidates.assert_not_awaited()
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='TM'").fetchone()[0] == 82
    assert local_db.get_title_mapping(tmdb_id=10, media_type="movie", id_type="tvdb") == "55"
    row = local_db.conn.execute("SELECT * FROM titles").fetchone()
    assert row["last_mdblist_fetch_at"] is None
    assert local_db.get_enrichment_state(10, "movie", "mdblist")["next_due"] == 1000
    local_db.save_enrichment_state(10, "movie", "tmdb", payload=DETAILS, now_ts=100, next_due=2000)
    assert local_db.select_provider_due_titles(now_ts=999, mdblist_pause_until=1000, mal_enabled=False) == []
    assert len(local_db.select_provider_due_titles(now_ts=1000, mdblist_pause_until=1000, mal_enabled=False)) == 1
    item = {"ratings": [{"source": "imdb", "score": 80}]}
    client.fetch_for_candidates.return_value = ({("movie", 10): item}, {("movie", 10)}, True, False, "", {})
    phase(local_db, client, now=1000)
    assert local_db.conn.execute("SELECT last_mdblist_fetch_at FROM titles").fetchone()[0] == 1000
    assert local_db.get_enrichment_state(10, "movie", "mdblist")["next_due"] == 2000
    assert local_db.select_provider_due_titles(now_ts=1001, mdblist_pause_until=1000, mal_enabled=False) == []


def test_mid_batch_halt_still_saves_other_provider_work(local_db):
    client = SimpleNamespace(fetch_for_candidates=AsyncMock(return_value=(
        {}, set(), False, True, "rate limited", {},
    )))
    result = phase(local_db, client)
    assert result.mdblist_request_failures == 1
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='TM'").fetchone()[0] == 82
    assert local_db.conn.execute("SELECT last_mdblist_fetch_at FROM titles").fetchone()[0] is None
    assert local_db.get_enrichment_state(10, "movie", "mdblist")["fetched_at"] is None
    assert local_db.get_enrichment_state(10, "movie", "mdblist")["next_due"] == 400


def test_missing_item_uses_missing_retry_not_quota_retry(local_db):
    client = SimpleNamespace(fetch_for_candidates=AsyncMock(return_value=(
        {}, {("movie", 10)}, True, False, "", {},
    )))
    phase(local_db, client)
    assert local_db.get_enrichment_state(10, "movie", "mdblist")["next_due"] == 300
    assert "MDBList item missing" in local_db.conn.execute("SELECT last_error FROM titles").fetchone()[0]


def test_cached_ids_allow_mal_work_while_mdblist_is_paused(local_db):
    local_db.save_enrichment_state(10, "movie", "mdblist", payload={"ids": {"mal": 123}}, now_ts=10, next_due=20)
    pause(local_db, 1000)
    mal = SimpleNamespace(fetch_anime=AsyncMock(return_value=APIResponse(200, {}, {"data": {
        "mal_id": 123, "title": "Example", "type": "Movie", "aired": {"from": "2020-02-01"},
        "score": 8.5, "scored_by": 500,
    }}, "")))
    client = SimpleNamespace(fetch_for_candidates=AsyncMock())
    phase(local_db, client, mal=mal)
    phase(local_db, client, now=101, mal=mal)
    assert mal.fetch_anime.await_count == 1
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='ML'").fetchone()[0] == 85
    client.fetch_for_candidates.assert_not_awaited()


def test_stop_does_not_mark_unattempted_work_done(local_db):
    stop = asyncio.Event()
    stop.set()
    client = SimpleNamespace(fetch_for_candidates=AsyncMock())
    result = phase(local_db, client, stop=stop)
    assert result.interrupted
    assert local_db.conn.execute("SELECT COUNT(*) FROM enrichment_state").fetchone()[0] == 0
    assert local_db.conn.execute("SELECT COUNT(*) FROM titles").fetchone()[0] == 0


def test_imdb_mapping_preserves_archive_data_for_local_and_remote_matches():
    cache = SimpleNamespace(upsert_many=lambda rows: None)
    candidates = [IMDbArchiveCandidate("tt0010", "movie", 300, 8.1),
                  IMDbArchiveCandidate("tt0020", "movie", 400, 7.9)]
    result, errors, missing = asyncio.run(map_imdb_candidates_to_tmdb(
        candidates=candidates, stop_event=asyncio.Event(), details_concurrency=2,
        resolve_imdb_to_tmdb_local_fn=lambda imdb, media: TITLE if imdb == "tt0010" else None,
        fetch_find_by_imdb_fn=AsyncMock(return_value=APIResponse(200, {}, {}, "")),
        extract_tmdb_from_find_payload_fn=lambda data, media: (20, "Other", 1.0),
        imdb_cache=cache, now_epoch_fn=lambda: 100,
    ))
    assert errors == missing == 0
    assert {(c.archive_imdb_id, c.archive_rating, c.archive_votes) for c in result} == {
        ("tt0010", 8.1, 300), ("tt0020", 7.9, 400),
    }


def test_archive_enriches_existing_title_without_consuming_other_providers(local_db):
    candidate = replace(TITLE, archive_imdb_id="tt0010", archive_rating=8.1, archive_votes=300)
    assert local_db.save_archive_title_rating(candidate, source_at=90, now_ts=100)
    assert local_db.conn.execute("SELECT last_harvested_at FROM titles").fetchone()[0] is None
    assert not local_db.save_archive_title_rating(candidate, source_at=90, now_ts=100)
    save(local_db, candidate=TITLE, details=DETAILS,
         md_item={"ratings": [{"source": "imdb", "score": 65}]}, now_ts=100)
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='IM'").fetchone()[0] == 81
    state = local_db.get_enrichment_state(10, "movie", "imdb")
    assert state["payload"] == {"imdb_id": "tt0010", "score": 81, "votes": 300}
    assert state["fetched_at"] == 90


@pytest.mark.parametrize("score,votes,source_at", [(float('nan'), 300, 90), (11, 300, 90),
    (0, 300, 90), (8, 0, 90), (8, 300, 101), (8, 300, -200000)])
def test_archive_rejects_invalid_or_stale_scores(local_db, score, votes, source_at):
    candidate = replace(TITLE, archive_imdb_id="tt0010", archive_rating=score, archive_votes=votes)
    assert not local_db.save_archive_title_rating(candidate, source_at=source_at, now_ts=100)
    assert local_db.conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0] == 0


def test_archive_refuses_identity_conflicts_and_older_snapshots(local_db):
    candidate = replace(TITLE, archive_imdb_id="tt0010", archive_rating=8, archive_votes=300)
    assert local_db.save_archive_title_rating(candidate, source_at=90, now_ts=100)
    assert not local_db.save_archive_title_rating(replace(candidate, archive_rating=7), source_at=80, now_ts=100)
    assert not local_db.save_archive_title_rating(replace(candidate, archive_imdb_id="tt9999"), source_at=100, now_ts=100)
    assert local_db.conn.execute("SELECT score FROM ratings").fetchone()[0] == 80


def test_migration_preserves_submitted_and_holds_unverified_episode_work(tmp_path):
    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    for _version, _name, migrate in MIGRATIONS[:2]:
        migrate(conn)
    with conn:
        for episode, status in enumerate(("pending", "retry", "in_flight", "submitted"), 1):
            conn.execute("INSERT INTO episode_ratings(tmdb_id,media_type,season,episode,label,score,fetched_at,pmdb_status,pmdb_item_id) "
                         "VALUES(10,'tv',1,?,'IM',80,100,?,'remote')", (episode, status))
    apply_migrations(conn)
    assert [r[0] for r in conn.execute("SELECT pmdb_status FROM episode_ratings ORDER BY episode")] == [
        "failed", "failed", "failed", "submitted",
    ]
    assert all(r[0] == "remote" for r in conn.execute("SELECT pmdb_item_id FROM episode_ratings"))
    conn.close()
    db = LocalDatabase(path)
    db.save_enrichment_state(10, "movie", "tmdb", payload=DETAILS, now_ts=100, next_due=200)
    db.close()
    db = LocalDatabase(path)
    assert db.get_enrichment_state(10, "movie", "tmdb")["payload"] == DETAILS
    assert db.conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    db.close()


def test_archive_scan_updates_existing_and_duplicate_titles_before_cursor(tmp_path, local_db):
    from betterer_ratings.services.harvest.discovery_imdb_scan import scan_imdb_archive_source

    candidate = replace(TITLE, archive_imdb_id="tt0010", archive_rating=8.1, archive_votes=300)
    save(local_db, candidate=TITLE, details=DETAILS, md_item={}, now_ts=100)
    (tmp_path / "title.ratings.tsv").write_text("fixture")
    source = SimpleNamespace(name="archive", title_batch_size=100, path=tmp_path)
    stats = {"archive": {"raw_seen": 0, "errors": 0, "skipped": 0, "duplicates": 0, "added": 0}}
    committed = []
    candidates = [TITLE]
    asyncio.run(scan_imdb_archive_source(
        stop_event=asyncio.Event(), db=local_db, imdb_archive_source=source,
        source_stats=stats, candidates=candidates, seen={("movie", 10)},
        ensure_imdb_index_fn=lambda source: 1,
        read_imdb_index_batch_fn=lambda source: ([object()], 1, 40, True),
        commit_imdb_cursor_fn=lambda **kw: committed.append(kw),
        map_imdb_candidates_to_tmdb_fn=AsyncMock(return_value=([candidate], 0, 0)),
        imdb_cursor_line_key="cursor", imdb_total_key="total", publish_scan_progress_fn=lambda *a, **kw: None,
        logger=LOG, pages_scanned=0, raw_seen_total=0,
    ))
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='IM'").fetchone()[0] == 81
    assert len(candidates) == 1
    assert committed == [{"cursor_line": 1, "cursor_byte": 40, "exhausted": True}]


def test_cycle_reaches_discovery_during_mdblist_quota_pause(local_db):
    from betterer_ratings.services.harvest.cycle_run_cycle import run_cycle

    pause(local_db, 1000)
    h = SimpleNamespace(
        db=local_db, _run_imdb_episode_cycle=AsyncMock(),
        _collect_local_candidates=AsyncMock(return_value=([TITLE], {}, False)),
        _source_scan_due=lambda now: False,
        _fetch_tmdb_details=AsyncMock(return_value=({("movie", 10): DETAILS}, False)),
        mdblist_client=SimpleNamespace(fetch_for_candidates=AsyncMock()),
        mal_client=None, _save_candidate_enrichment=lambda **kw: save(local_db, **kw),
    )
    result = asyncio.run(run_cycle(
        harvester=h, stop_event=asyncio.Event(), logger=LOG, now_epoch_fn=lambda: 100,
        local_day_key_fn=lambda now: "2026-09-26", to_iso_fn=str,
        harvest_cycle_result_cls=HarvestCycleResult,
    ))
    assert result.selected_candidates == 1
    h._collect_local_candidates.assert_awaited_once()
    h.mdblist_client.fetch_for_candidates.assert_not_awaited()
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='TM'").fetchone()[0] == 82


def test_real_mdb_chunk_does_not_persist_twice(local_db, base_valid_config):
    from betterer_ratings.config.schema import AppConfig
    from betterer_ratings.providers.mdblist_client import MDBListClient

    client = MDBListClient(api_key="test", config=AppConfig.from_mapping(base_valid_config).mdblist, gate=None)
    client._fetch_batch = AsyncMock(return_value=({10: {"ratings": [{"source": "trakt", "score": 75}]}}, {10}, True, False, ""))
    phase(local_db, client)
    state = local_db.get_enrichment_state(10, "movie", "mdblist")
    assert state["fetched_at"] == 100
    assert state["next_due"] == 1100
    assert local_db.conn.execute("SELECT score FROM ratings WHERE label='TR'").fetchone()[0] == 75
    phase(local_db, client, now=101)
    assert client._fetch_batch.await_count == 1
    asyncio.run(client.aclose())


def test_metadata_fetch_remains_due_until_enrichment_is_persisted(local_db):
    save(local_db, candidate=TITLE, details=DETAILS, md_item={}, now_ts=10)
    local_db.save_enrichment_state(10, "movie", "mdblist", payload={}, now_ts=10, next_due=2000)
    client = SimpleNamespace(fetch_details=AsyncMock(return_value=APIResponse(200, {}, DETAILS, "")))
    asyncio.run(fetch_tmdb_details(
        candidates=[TITLE], stop_event=asyncio.Event(), tmdb_client=client,
        db=local_db, details_concurrency=1, now_epoch_fn=lambda: 100, logger=LOG,
    ))
    # Simulate stopping between fetching metadata and persisting its ratings.
    assert len(local_db.select_provider_due_titles(now_ts=101, mdblist_pause_until=0, mal_enabled=False)) == 1
    phase(local_db, SimpleNamespace(fetch_for_candidates=AsyncMock()), now=101)
    assert local_db.select_provider_due_titles(now_ts=102, mdblist_pause_until=0, mal_enabled=False) == []
    assert local_db.get_enrichment_state(10, "movie", "tmdb")["fetched_at"] == 100


def test_migration_holds_legacy_trakt_until_real_source_returns(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    for _version, _name, migrate in MIGRATIONS[:2]:
        migrate(conn)
    with conn:
        conn.execute("INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status) "
                     "VALUES(10,'movie','TR',75,100,'pending')")
    conn.close()
    db = LocalDatabase(path)
    assert db.conn.execute("SELECT pmdb_status FROM ratings").fetchone()[0] == "failed"
    save(db, candidate=TITLE, details=DETAILS, md_item={"score": 75}, now_ts=200)
    assert db.conn.execute("SELECT pmdb_status FROM ratings WHERE label='TR'").fetchone()[0] == "failed"
    save(db, candidate=TITLE, details=DETAILS, md_item={"ratings": [{"source": "trakt", "score": 75}]}, now_ts=300)
    assert db.conn.execute("SELECT pmdb_status FROM ratings WHERE label='TR'").fetchone()[0] == "pending"
    db.close()


def test_ambiguous_local_imdb_mapping_requires_remote_resolution(local_db):
    from betterer_ratings.core.ids import normalize_imdb_title_id
    from betterer_ratings.core.parsing import parse_int
    from betterer_ratings.services.harvest.imdb_mapping_helpers import (
        extract_tmdb_from_find_payload,
        resolve_imdb_to_tmdb_local,
    )

    for tmdb_id in (10, 20):
        save(local_db, candidate=replace(TITLE, tmdb_id=tmdb_id), details=DETAILS, md_item={}, now_ts=100)
    assert resolve_imdb_to_tmdb_local(
        db=local_db, imdb_cache=SimpleNamespace(get=lambda *args: (10, "Cached", 1)),
        imdb_id="tt0010", media_type="movie", normalize_imdb_title_id_fn=normalize_imdb_title_id,
        parse_int_fn=parse_int,
    ) is None
    assert extract_tmdb_from_find_payload(
        payload={"movie_results": [{"id": 10}, {"id": 20}]}, media_type="movie", parse_int_fn=parse_int,
    )[0] is None

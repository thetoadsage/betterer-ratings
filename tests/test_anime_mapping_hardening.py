import asyncio
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from betterer_ratings.infra.db.schema_repo import MIGRATIONS, apply_migrations
from betterer_ratings.services.harvest.anime_offline_database import (
    AnimeOfflineDatabase,
    build_mal_lookup,
    resolve_mal_decision,
)
from betterer_ratings.services.harvest.fribb_mappings import FribbAnimeMappings, FribbIndex
from betterer_ratings.services.harvest.mal import MALResult
from betterer_ratings.services.submit.handler_mapping import submit_mapping, submit_mapping_group
from tests.test_anime_mapping_safety import fetch, response, save
from tests.test_fribb_mappings import ROW


def insert(db, provider='mal', value='999', status='pending', tmdb_id=128):
    db.conn.execute('''INSERT INTO mappings(tmdb_id,media_type,id_type,id_value,fetched_at,pmdb_status)
        VALUES (?,'movie',?,?,1,?)''', (tmdb_id, provider, value, status))
    db.conn.commit()


@pytest.mark.parametrize('group', [True, False])
@pytest.mark.parametrize('status', ['pending', 'retry'])
def test_conflict_holds_existing_queue_and_does_not_starve_safe_work(local_db, group, status):
    insert(local_db, status=status)
    insert(local_db, provider='imdb', value='tt0119698')
    result = fetch(local_db, SimpleNamespace(fetch_anime=AsyncMock()), md_item={'ids': {'mal': 164}})
    assert result.outcome == 'mapping_conflict'
    save(local_db, result)
    assert local_db.count_due_queue(kind='mappings', now_ts=100) == 1
    if group:
        rows = local_db.claim_next_pending_mapping_group(100)
    else:
        rows = [local_db.claim_next_pending_mapping(100)]
    assert [r['id_type'] for r in rows] == ['imdb']
    assert local_db.next_due_queue_kind(100) is None
    assert local_db.claim_next_pending_mapping(100) is None
    assert local_db.claim_next_pending_mapping_group(100) == []
    assert local_db.conn.execute("SELECT pmdb_status FROM mappings WHERE id_type='mal'").fetchone()[0] == status


def test_hold_persists_through_outage_and_releases_only_after_validated_identity(local_db):
    insert(local_db, value='164')
    save(local_db, MALResult(None, 'source_mapping', False, 'correction_available', proposed_mal_id=123))
    save(local_db, MALResult(None, 'source_mapping', False, 'provider_failure'))
    assert local_db.claim_next_pending_mapping(100) is None
    save(local_db, MALResult(164, 'source_mapping', True, 'missing_score'))
    assert local_db.claim_next_pending_mapping(100)['id_value'] == '164'


@pytest.mark.parametrize('provider', ['anilist', 'anidb'])
def test_legacy_supplements_are_not_claimed(local_db, provider):
    insert(local_db, provider=provider)
    assert local_db.count_due_queue(kind='mappings', now_ts=100) == 0
    assert local_db.next_due_queue_kind(100) is None
    assert local_db.claim_next_pending_mapping(100) is None
    assert local_db.claim_next_pending_mapping_group(100) == []


def test_migration_preserves_history_and_holds_old_unsent_mal(tmp_path):
    conn = sqlite3.connect(tmp_path / 'legacy.sqlite3')
    conn.row_factory = sqlite3.Row
    conn.execute('CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at INTEGER)')
    for version, name, migrate in MIGRATIONS[:-1]:
        migrate(conn)
        conn.execute('INSERT INTO schema_migrations VALUES (?,?,1)', (version, name))
    for i, status in enumerate(['pending', 'retry', 'in_flight', 'submitted', 'failed']):
        conn.execute("INSERT INTO mappings(tmdb_id,media_type,id_type,id_value,fetched_at,pmdb_status,pmdb_item_id) VALUES (?,'movie','mal','164',1,?,'keep')", (i, status))
    conn.commit()
    before = [tuple(r) for r in conn.execute('SELECT * FROM mappings')]
    apply_migrations(conn)
    assert [tuple(r) for r in conn.execute('SELECT * FROM mappings')] == before
    assert [r[0] for r in conn.execute('SELECT tmdb_id FROM anime_mapping_holds ORDER BY tmdb_id')] == [0, 1, 2]
    apply_migrations(conn)
    assert conn.execute('SELECT count(*) FROM anime_mapping_holds').fetchone()[0] == 3
    conn.close()


@pytest.mark.parametrize('group', [True, False])
def test_hold_added_after_claim_stops_submission_without_network(local_db, group):
    insert(local_db, value='164')
    rows = local_db.claim_next_pending_mapping_group(100)
    save(local_db, MALResult(None, 'source_mapping', False, 'mapping_conflict'))
    client = SimpleNamespace(_fetch_existing_mappings=AsyncMock(), submit_mapping=AsyncMock())
    logger = SimpleNamespace(info=lambda *a, **kw: None)
    if group:
        asyncio.run(submit_mapping_group(rows=rows, pmdb_client=client, db=local_db,
            submit_mapping_fn=AsyncMock(), now_epoch_fn=lambda: 100, logger=logger))
    else:
        asyncio.run(submit_mapping(row=rows[0], pmdb_client=client, db=local_db,
            verify_after_transient_statuses=set(), max_retry_attempts=5,
            format_manual_error_fn=lambda **kw: '', format_pmdb_error_fn=lambda **kw: '',
            retry_delay_seconds_fn=lambda *a: 1, now_epoch_fn=lambda: 100,
            first_non_empty_fn=lambda *a: None, logger=logger))
    client._fetch_existing_mappings.assert_not_awaited()
    client.submit_mapping.assert_not_awaited()
    assert local_db.conn.execute('SELECT pmdb_status FROM mappings').fetchone()[0] == 'pending'
    assert local_db.claim_next_pending_mapping(100) is None


@pytest.mark.parametrize('fallback', [False, True])
def test_authoritative_404_can_discover_validated_correction_with_default_fallback(local_db, fallback):
    client = SimpleNamespace(fetch_anime=AsyncMock(side_effect=lambda value: response(status=404) if value == 999 else response()))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=fallback),
        client=SimpleNamespace(fetch_anime=AsyncMock(return_value=response(status=404))),
        resolve_decision=AsyncMock(return_value=(164, 'matched')))
    result = fetch(local_db, client, md_item={'ids': {'mal': 999}}, jikan=jikan)
    assert result.outcome == 'correction_available'
    assert result.proposed_mal_id == 164 and result.mal_id is None and result.score is None
    jikan.resolve_decision.assert_awaited_once()


@pytest.mark.parametrize('official,fallback', [(503, 404), (429, 404), (404, 503)])
def test_outages_do_not_turn_into_correction_evidence(local_db, official, fallback):
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response(status=official)))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=True),
        client=SimpleNamespace(fetch_anime=AsyncMock(return_value=response(status=fallback))), resolve_decision=AsyncMock())
    result = fetch(local_db, client, md_item={'ids': {'mal': 999}}, jikan=jikan)
    assert not result.validated
    jikan.resolve_decision.assert_not_awaited()


def test_real_offline_bridge_conflict_is_not_silently_ignored(local_db, tmp_path):
    path = tmp_path / 'offline.jsonl'
    path.write_text('\n'.join(json.dumps({'sources': sources}) for sources in [
        ['https://anilist.co/anime/1', 'https://myanimelist.net/anime/164'],
        ['https://anidb.net/anime/2', 'https://myanimelist.net/anime/999'],
    ]))
    client = SimpleNamespace(fetch_anime=AsyncMock())
    result = fetch(local_db, client, md_item={'ids': {'mal': 164, 'anilist': 1, 'anidb': 2}}, anime_mapping_cache=AnimeOfflineDatabase(path))
    assert result.outcome == 'mapping_conflict'
    assert {c['mal_id'] for c in result.candidates} == {'164', '999'}
    assert result.mapping_decisions[0]['outcome'] == 'conflict'
    assert result.mapping_decisions[0]['evidence'][1]['mal_id'] == '999'
    client.fetch_anime.assert_not_awaited()


def test_malformed_multi_mal_record_remains_conflict_evidence():
    lookup = build_mal_lookup([json.dumps({'sources': ['https://anilist.co/anime/1',
        'https://myanimelist.net/anime/164', 'https://myanimelist.net/anime/999']})])
    assert resolve_mal_decision({'anilist': 1}, lookup).outcome == 'conflict'


@pytest.mark.parametrize('row,ids,outcome', [
    (ROW, {'anilist': '999'}, 'conflict'),
    ({**ROW, 'themoviedb_id': {'movie': [128, 129]}}, {}, 'unsafe_scope'),
])
def test_real_fribb_rejection_is_preserved_in_cached_result(local_db, tmp_path, row, ids, outcome):
    path = tmp_path / 'fribb.json'
    # Include one usable unrelated entry so loader can validate this dataset.
    path.write_text(json.dumps([row, {**ROW, 'mal_id': 2, 'anilist_id': 2, 'anidb_id': 2,
        'themoviedb_id': {'movie': [2]}, 'imdb_id': ['tt0000002']}]))
    client = SimpleNamespace(fetch_anime=AsyncMock())
    result = fetch(local_db, client, md_item={'ids': {'mal': 164, **ids}}, fribb_mapping_cache=FribbAnimeMappings(path))
    assert result.outcome == ('mapping_conflict' if outcome == 'conflict' else 'skipped_dataset_scope')
    assert result.mapping_decisions[0]['outcome'] == outcome
    client.fetch_anime.assert_not_awaited()


def test_fribb_missing_and_shared_owner_are_distinguished():
    idx = FribbIndex([ROW, {**ROW, 'themoviedb_id': {'movie': [2]}, 'imdb_id': ['tt0000002']}])
    assert idx.resolve_decision(media_type='movie', tmdb_id=3, mappings={}).outcome == 'missing'
    assert idx.resolve_decision(media_type='movie', tmdb_id=128, mappings={}).outcome == 'conflict'


def test_conflict_detected_during_group_preflight_prevents_marking_or_post(local_db):
    insert(local_db, value='164')
    rows = local_db.claim_next_pending_mapping_group(100)

    async def lookup(*args):
        save(local_db, MALResult(None, 'source_mapping', False, 'mapping_conflict'))
        return response({'mappings': {'mal': [{'id_value': '164'}]}})

    client = SimpleNamespace(_fetch_existing_mappings=lookup,
        _extract_mappings_for_type=lambda payload, provider: [{'id_value': '164'}],
        _mapping_entry_matches_value=lambda entry, value: True)
    post = AsyncMock()
    asyncio.run(submit_mapping_group(rows=rows, pmdb_client=client, db=local_db,
        submit_mapping_fn=post, now_epoch_fn=lambda: 100,
        logger=SimpleNamespace(info=lambda *a, **kw: None)))
    post.assert_not_awaited()
    assert local_db.conn.execute('SELECT pmdb_status FROM mappings').fetchone()[0] == 'pending'


def test_audit_surfaces_new_hold_and_structured_decision(local_db, tmp_path):
    from scripts.audit_anime_mappings import audit

    insert(local_db)
    result = MALResult(None, 'offline_bridge', False, 'mapping_conflict',
        mapping_decisions=[{'source': 'offline_bridge', 'outcome': 'conflict', 'mal_ids': ['164', '999']}])
    save(local_db, result)
    local_db.save_enrichment_state(128, 'movie', 'mal', payload={
        'policy': 'anime_validation_v4', 'result': vars(result)}, now_ts=100, next_due=200)
    path = tmp_path / 'snapshot.sqlite3'
    with sqlite3.connect(path) as target:
        local_db.conn.backup(target)
    report = audit(path, now_ts=150)
    assert report['submission_holds'] == {'mapping_conflict': 1}
    assert report['cached_policies'] == {'anime_validation_v4': 1}
    assert report['examples'][0]['mapping_decisions'][0]['outcome'] == 'conflict'
    assert report['examples'][0]['submission_holds'] == [{'id_type': 'mal', 'reason': 'mapping_conflict'}]

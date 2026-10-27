import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.scoring import parse_mdblist_ratings, parse_tmdb_vote_average
from betterer_ratings.domain.models import APIResponse
from betterer_ratings.services.harvest.enrichment import save_candidate_enrichment
from betterer_ratings.services.harvest.mal import fetch_candidate_mal_score
from scripts.audit_anime_mappings import audit
from tests.test_mal_enrichment import ANIME, CANDIDATE, MOVIE


def response(anime=ANIME, status=200):
    return APIResponse(status, {}, {'data': anime}, '')


def save(db, result=None, item=None):
    return save_candidate_enrichment(
        db=db, candidate=CANDIDATE, details=MOVIE, md_item=item, now_ts=100,
        mal_result=result, parse_mdblist_ratings_fn=parse_mdblist_ratings,
        parse_tmdb_vote_average_fn=parse_tmdb_vote_average, extract_mappings_fn=extract_mappings,
    )


def fetch(db, client, **kwargs):
    return asyncio.run(fetch_candidate_mal_score(
        client=client, db=db, candidate=CANDIDATE, details=MOVIE,
        stop_event=asyncio.Event(), **kwargs,
    ))


@pytest.mark.parametrize('mal_enabled', [True, False])
def test_raw_supplements_never_queued_even_with_valid_mal(local_db, mal_enabled):
    item = {'ids': {'mal': 164, 'anilist': 999, 'anidb': 998}}
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response()))
    result = fetch(local_db, client, md_item=item) if mal_enabled else None
    save(local_db, result, item)
    assert local_db.get_title_mapping(tmdb_id=128, media_type='movie', id_type='anilist') is None
    assert local_db.get_title_mapping(tmdb_id=128, media_type='movie', id_type='anidb') is None
    assert local_db.get_title_mapping(tmdb_id=128, media_type='movie', id_type='mal') == ('164' if mal_enabled else None)


@pytest.mark.parametrize('provider', ['mal', 'anilist', 'anidb'])
def test_conflicting_stored_and_source_ids_stop_before_provider_calls(local_db, provider):
    local_db.conn.execute("INSERT INTO mappings (tmdb_id,media_type,id_type,id_value,fetched_at) VALUES (128,'movie',?,'999',1)", (provider,))
    client = SimpleNamespace(fetch_anime=AsyncMock())
    result = fetch(local_db, client, md_item={'ids': {'mal': 164, provider: 164}})
    assert result.outcome == 'mapping_conflict'
    assert result.conflicts == [{'provider': provider, 'source_id': '164', 'stored_id': '999'}]
    client.fetch_anime.assert_not_awaited()
    save(local_db, result)
    assert local_db.get_title_mapping(tmdb_id=128, media_type='movie', id_type=provider) == '999'


@pytest.mark.parametrize('bridge', ['offline', 'fribb'])
def test_other_dataset_disagreement_is_visible(local_db, bridge):
    client = SimpleNamespace(fetch_anime=AsyncMock())
    kwargs = {'anime_mapping_cache': SimpleNamespace(resolve=AsyncMock(return_value='999'))} if bridge == 'offline' else {
        'fribb_mapping_cache': SimpleNamespace(resolve=AsyncMock(return_value={'mal': '999'}))}
    result = fetch(local_db, client, md_item={'ids': {'mal': 164, 'anilist': 1}}, **kwargs)
    assert result.outcome == 'mapping_conflict'
    assert {c['mal_id'] for c in result.candidates} == {'164', '999'}
    client.fetch_anime.assert_not_awaited()


@pytest.mark.parametrize('bad_id', ['164', 'invalid'])
def test_validated_correction_is_proposed_never_applied(local_db, bad_id):
    local_db.conn.execute("INSERT INTO mappings (tmdb_id,media_type,id_type,id_value,fetched_at) VALUES (128,'movie','mal',?,1)", (bad_id,))
    client = SimpleNamespace(fetch_anime=AsyncMock(side_effect=lambda value: response(
        {**ANIME, 'mal_id': value, 'title': 'Other', 'title_english': 'Other'} if value == 164 else {**ANIME, 'mal_id': value})))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=False), resolve_decision=AsyncMock(return_value=(123, 'matched')))
    result = fetch(local_db, client, md_item={}, jikan=jikan)
    assert result.outcome == 'correction_available'
    assert result.proposed_mal_id == 123
    assert not result.validated and result.score is None and result.mal_id is None
    save(local_db, result)
    assert local_db.get_title_mapping(tmdb_id=128, media_type='movie', id_type='mal') == bad_id


@pytest.mark.parametrize('status,anime', [(503, ANIME), (429, ANIME), (200, {**ANIME, 'mal_id': 999})])
def test_unavailable_or_corrupt_response_does_not_trigger_correction(local_db, status, anime):
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response(anime, status)))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=False), resolve_decision=AsyncMock())
    result = fetch(local_db, client, md_item={'ids': {'mal': 164}}, jikan=jikan)
    assert not result.validated
    jikan.resolve_decision.assert_not_awaited()


def test_audit_is_read_only_and_keeps_detailed_outcomes(local_db, tmp_path):
    result = fetch(local_db, SimpleNamespace(fetch_anime=AsyncMock(return_value=response({**ANIME, 'type': 'Special'}))), md_item={'ids': {'mal': 164}})
    save(local_db, result)
    local_db.save_enrichment_state(128, 'movie', 'mal', payload={'result': vars(result)}, now_ts=100, next_due=200)
    snapshot = tmp_path / 'audit.sqlite3'
    import sqlite3
    with sqlite3.connect(snapshot) as target:
        local_db.conn.backup(target)
    before = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    report = audit(snapshot, now_ts=150)
    assert report['cached_outcomes'] == {'skipped_format_mismatch': 1}
    assert report['pmdb_coverage_checked'] is False
    assert report['examples'][0]['candidates'] == [{'mal_id': '164', 'source': 'source_mapping'}]
    assert report['findings']['skipped_format_mismatch'] == 1
    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == before
    assert audit(snapshot, examples=0, now_ts=150)['examples'] == []
    with pytest.raises(ValueError):
        audit(snapshot, examples=-1)
    assert json.dumps(report)


def test_audit_reports_existing_unvalidated_and_unconfirmed_mappings(local_db, tmp_path):
    import sqlite3

    for provider in ('mal', 'anilist', 'anidb'):
        local_db.conn.execute("INSERT INTO mappings (tmdb_id,media_type,id_type,id_value,fetched_at) VALUES (128,'movie',?,'999',1)", (provider,))
    local_db.conn.commit()
    snapshot = tmp_path / 'existing.sqlite3'
    with sqlite3.connect(snapshot) as target:
        local_db.conn.backup(target)
    report = audit(snapshot)
    assert report['audited_titles'] == 1
    assert report['findings'] == {
        'missing_validation_record': 1, 'anilist_requires_provider_validation': 1,
        'anidb_requires_provider_validation': 1, 'stored_mal_not_confirmed_by_cache': 1,
    }
    assert report['local_mapping_statuses'] == {'mal:pending': 1, 'anilist:pending': 1, 'anidb:pending': 1}


@pytest.mark.parametrize('alternative', [None, 'mismatch', 'unavailable'])
def test_unverified_alternative_is_never_proposed(local_db, alternative):
    def details(value):
        if value == 123 and alternative == 'unavailable':
            return response(status=503)
        return response({**ANIME, 'mal_id': value, 'type': 'Special'})

    client = SimpleNamespace(fetch_anime=AsyncMock(side_effect=details))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=False), resolve_decision=AsyncMock(
        return_value=(None, 'ambiguous') if alternative is None else (123, 'matched')))
    result = fetch(local_db, client, md_item={'ids': {'mal': 164}}, jikan=jikan)
    assert result.outcome == 'skipped_format_mismatch'
    assert result.proposed_mal_id is None and not result.validated


def test_cancelled_correction_does_not_produce_a_proposal(local_db):
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response({**ANIME, 'type': 'Special'})))
    jikan = SimpleNamespace(config=SimpleNamespace(fallback=False), resolve_decision=AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        fetch(local_db, client, md_item={'ids': {'mal': 164}}, jikan=jikan)


def test_stored_supplement_change_invalidates_cached_mal_decision(local_db):
    import logging

    from betterer_ratings.domain.models import HarvestCycleResult
    from betterer_ratings.services.harvest.cycle_mdblist_phase import run_mdblist_enrichment_phase

    local_db.save_enrichment_state(128, 'movie', 'mdblist',
        payload={'ids': {'mal': 164, 'anilist': 1}}, now_ts=1, next_due=10000)
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response()))
    harvester = SimpleNamespace(db=local_db, mal_client=client,
        mdblist_client=SimpleNamespace(fetch_for_candidates=AsyncMock()),
        _save_candidate_enrichment=lambda **kwargs: save_candidate_enrichment(
            db=local_db, parse_mdblist_ratings_fn=parse_mdblist_ratings,
            parse_tmdb_vote_average_fn=parse_tmdb_vote_average, extract_mappings_fn=extract_mappings, **kwargs))

    async def cycle():
        await run_mdblist_enrichment_phase(
            harvester=harvester, logger=logging.getLogger('test'), now_epoch_fn=lambda: 100,
            stop_event=asyncio.Event(), candidates=[CANDIDATE],
            tmdb_details={('movie', 128): MOVIE}, source_stats={}, local_stats={},
            tmdb_list_request_errors=0, harvest_cycle_result_cls=HarvestCycleResult)

    asyncio.run(cycle())
    assert local_db.get_enrichment_state(128, 'movie', 'mal')['payload']['result']['validated']
    local_db.conn.execute("INSERT INTO mappings (tmdb_id,media_type,id_type,id_value,fetched_at) VALUES (128,'movie','anilist','2',1)")
    asyncio.run(cycle())
    result = local_db.get_enrichment_state(128, 'movie', 'mal')['payload']['result']
    assert result['outcome'] == 'mapping_conflict'
    assert result['conflicts'][0]['provider'] == 'anilist'
    client.fetch_anime.assert_awaited_once()


def test_numeric_id_normalization_does_not_create_false_conflicts(local_db):
    local_db.conn.execute("INSERT INTO mappings (tmdb_id,media_type,id_type,id_value,fetched_at) VALUES (128,'movie','mal','00164',1)")
    client = SimpleNamespace(fetch_anime=AsyncMock(return_value=response()))
    result = fetch(local_db, client, md_item={'ids': {'mal': 164}})
    assert result.validated and result.mal_id == 164

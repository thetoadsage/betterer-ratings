from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

from betterer_ratings.domain.models import PMDBSubmitResult
from betterer_ratings.infra.db.queue_recovery import recover_stale_in_flight_rows
from betterer_ratings.services.submit.handler_rating import submit_rating
from betterer_ratings.services.submit.worker import worker_loop


def test_worker_finishes_the_original_rating_and_leaves_the_refresh_pending(local_db):
    async def run():
        db = local_db
        key = (1, "movie", "TM")
        db._upsert_rating(*key, 74.0, 100)
        stop = asyncio.Event()
        active_claims = set()

        async def submit_to_pmdb(**payload):
            assert payload["score"] == 74.0
            db._upsert_rating(*key, 81.0, 102)
            assert db.claim_next_pending_rating(103) is None
            recovered = recover_stale_in_flight_rows(
                db.conn, lease_seconds=30, now_epoch_fn=lambda: 300, active_claims=active_claims,
            )
            assert recovered["ratings"] == 0
            stop.set()
            return PMDBSubmitResult(
                success=True, retryable=False, retry_after_seconds=0,
                duplicate_or_exists=False, error_text="", item_id="old", status_code=200,
            )

        async def submit(row):
            await submit_rating(
                row=row, pmdb_client=SimpleNamespace(submit_rating=submit_to_pmdb), db=db,
                verify_after_transient_statuses=set(), max_retry_attempts=3,
                format_manual_error_fn=str, format_pmdb_error_fn=str, retry_delay_seconds_fn=lambda *_: 30,
                now_epoch_fn=lambda: 103, first_non_empty_fn=lambda value: value,
                logger=logging.getLogger(__name__),
            )

        await worker_loop(
            stop_event=stop, db=db, poll_seconds=1, now_epoch_fn=lambda: 101,
            submit_mapping_group_fn=None, submit_rating_fn=submit,
            submit_episode_ratings_batch_fn=None, active_claims=active_claims, queue_order=("rating",),
        )
        assert not active_claims
        replacement = db.claim_next_pending_rating(104)
        assert (replacement["score"], replacement["pmdb_item_id"]) == (81.0, "old")
        db.mark_rating_submitted(*key, 105, "new", claim_token=replacement["pmdb_claim_token"])
        assert db.claim_next_pending_rating(106) is None

    asyncio.run(run())


def test_mapping_acknowledgement_keeps_remote_id_tied_to_the_submitted_value(local_db):
    db = local_db
    key = (2, "tv", "tvdb")
    db._upsert_mapping(*key, "10", 100)
    claim = db.claim_next_pending_mapping_group(101)[0]
    db._upsert_mapping(*key, "11", 102)
    assert not db.claim_next_pending_mapping_group(103)
    db.mark_mapping_submitted(
        *key, 103, "old-id", submitted_value=claim["id_value"], claim_token=claim["pmdb_claim_token"],
    )
    row = db.conn.execute("SELECT id_value, pmdb_item_value, pmdb_status FROM mappings").fetchone()
    assert tuple(row) == ("11", "10", "pending")


def test_episode_retry_from_an_old_claim_cannot_delay_a_replacement(local_db):
    db = local_db
    key = (3, "tv", 1, 1, "IM")

    def save(score, fetched_at):
        db._upsert_episode_rating(
            tmdb_id=3, media_type="tv", season=1, episode=1, label="IM", score=score,
            fetched_at=fetched_at, imdb_parent_id="tt1000000", imdb_episode_id="tt1000001", votes=100,
        )

    save(74.0, 100)
    claim = db.claim_next_pending_episode_ratings_batch(now_ts=101)[0]
    save(81.0, 102)
    db.mark_episode_rating_retry(*key, 500, "old error", claim_token=claim["pmdb_claim_token"])
    replacement = db.claim_next_pending_episode_ratings_batch(now_ts=103)[0]
    assert (replacement["score"], replacement["pmdb_attempts"]) == (81.0, 0)
    db.mark_episode_rating_failed(*key, "stale error", claim_token=claim["pmdb_claim_token"])
    db.mark_episode_rating_submitted(*key, 104, "stale-id", claim_token=claim["pmdb_claim_token"])
    row = db.conn.execute("SELECT pmdb_status, pmdb_item_id FROM episode_ratings").fetchone()
    assert tuple(row) == ("in_flight", None)
    assert db.submission_summary(104)["episode_ratings_today"] == 0

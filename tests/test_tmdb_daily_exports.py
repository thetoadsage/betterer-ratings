import asyncio
import datetime as dt
import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from betterer_ratings.config.schema import TMDBDailyExportsConfig
from betterer_ratings.services.harvest.discovery_tmdb_exports import (
    _download_export,
    commit_export_cursors,
    scan_daily_exports,
)


def _export(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_export_batch_checkpoint_and_daily_budget(local_db, tmp_path):
    day = dt.date(2026, 9, 27)
    now_ts = int(dt.datetime(2026, 9, 27, 12, tzinfo=dt.timezone.utc).timestamp())
    stamp = (day - dt.timedelta(days=1)).isoformat()
    _export(tmp_path / f"movie_ids_{stamp}.jsonl", [
        {"id": 1, "original_title": "One", "adult": False, "video": False},
        {"id": 2, "original_title": "Adult", "adult": True},
        {"id": 3, "original_title": "Three"},
    ])
    _export(tmp_path / f"tv_series_ids_{stamp}.jsonl", [
        {"id": 4, "original_name": "Four"},
        {"id": 5, "original_name": "Five"},
    ])
    config = TMDBDailyExportsConfig(True, 2, 3)

    async def scan():
        candidates = []
        pending = await scan_daily_exports(
            db=local_db, config=config, directory=tmp_path, candidates=candidates,
            seen=set(), stop_event=asyncio.Event(), logger=logging.getLogger(__name__),
            now_ts=now_ts,
        )
        return candidates, pending

    first, checkpoint = asyncio.run(scan())
    assert [(c.media_type, c.tmdb_id) for c in first] == [("movie", 1), ("tv", 4)]
    assert local_db.get_state_int("tmdb_export_movie_cursor", 0) == 0
    commit_export_cursors(local_db, tmp_path, checkpoint)
    second, checkpoint = asyncio.run(scan())
    assert [(c.media_type, c.tmdb_id) for c in second] == [("movie", 3)]
    commit_export_cursors(local_db, tmp_path, checkpoint)
    third, _ = asyncio.run(scan())
    assert third == []
    assert local_db.get_state_int("tmdb_export_budget_spent", 0) == 3


@pytest.mark.parametrize("value", [
    {"enabled": "yes"}, {"daily_detail_budget": 0}, {"max_new_titles_per_scan": -1},
])
def test_invalid_export_configuration_is_rejected(value):
    with pytest.raises(ValueError):
        TMDBDailyExportsConfig.from_mapping(value)


@pytest.mark.parametrize("truncated", [False, True])
def test_download_is_atomic_and_rejects_truncated_gzip(monkeypatch, tmp_path, truncated):
    import gzip

    path = tmp_path / "export.jsonl"
    path.write_text("previous valid snapshot")
    data = gzip.compress(b'{"id":1}\n')
    if truncated:
        data = data[:-4]

    @contextmanager
    def stream(*args, **kwargs):
        yield SimpleNamespace(raise_for_status=lambda: None, iter_raw=lambda: iter([data]))

    monkeypatch.setattr("betterer_ratings.services.harvest.discovery_tmdb_exports.httpx.stream", stream)
    if truncated:
        with pytest.raises(ValueError, match="truncated"):
            _download_export(path, "movie", "2026-09-26")
        assert path.read_text() == "previous valid snapshot"
    else:
        _download_export(path, "movie", "2026-09-26")
        assert path.read_text() == '{"id":1}\n'
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("errors,interrupted,committed", [(0, False, True), (1, False, False), (0, True, False)])
def test_cycle_checkpoints_only_completed_processing(monkeypatch, local_db, tmp_path, errors, interrupted, committed):
    from betterer_ratings.domain.models import Candidate, HarvestCycleResult
    from betterer_ratings.services.harvest import cycle_run_cycle

    h = SimpleNamespace(
        db=local_db, tmdb_export_directory=tmp_path,
        _run_imdb_episode_cycle=AsyncMock(), _mdblist_daily_quota_pause_until=lambda now: 0,
    )

    async def discover(**kwargs):
        h._pending_tmdb_export_cursors = {"movie": ("2026-09-26", 10)}
        return [Candidate(1, "movie", "Title", 0)], {}, 0, {}, "day", None

    monkeypatch.setattr(cycle_run_cycle.harvest_cycle_discovery_phase, "run_discovery_phase", discover)
    monkeypatch.setattr(cycle_run_cycle.harvest_cycle_tmdb_phase, "run_tmdb_details_phase", AsyncMock(return_value=({}, None)))
    monkeypatch.setattr(cycle_run_cycle.harvest_cycle_mdblist_phase, "run_mdblist_enrichment_phase",
                        AsyncMock(return_value=HarvestCycleResult(1, 0, errors, interrupted)))
    asyncio.run(cycle_run_cycle.run_cycle(
        harvester=h, stop_event=asyncio.Event(), logger=logging.getLogger(__name__),
        now_epoch_fn=lambda: 100, local_day_key_fn=lambda: "day", to_iso_fn=str,
        harvest_cycle_result_cls=HarvestCycleResult,
    ))
    assert local_db.get_state_int("tmdb_export_movie_cursor", 0) == (10 if committed else 0)

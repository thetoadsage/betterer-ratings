import asyncio
import datetime as dt
import json
import logging

from betterer_ratings.config.schema import TMDBDailyExportsConfig
from betterer_ratings.services.harvest.discovery_tmdb_exports import (
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

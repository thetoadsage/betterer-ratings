from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

from betterer_ratings import constants
from betterer_ratings.config.schema import ensure_app_config
from betterer_ratings.services.harvest import harvester, imdb_archive_refresh_refresh


def test_archive_download_and_both_index_builds_leave_sqlite_on_the_event_loop(
    local_db, base_valid_config, tmp_path, monkeypatch,
):
    monkeypatch.setattr(constants, "CONTAINER_IMDB_ARCHIVE_PATH", str(tmp_path / "archives"))
    monkeypatch.setattr(constants, "CONTAINER_TEMP_PATH", str(tmp_path / "indexes"))
    worker = harvester.Harvester(
        config=ensure_app_config(base_valid_config), db=local_db,
        tmdb_client=SimpleNamespace(build_source=lambda source: source), mdblist_client=None,
    )
    fixtures = {
        "title.basics.tsv": "tconst\ttitleType\tstartYear\ntt1000000\ttvSeries\t2020\n",
        "title.ratings.tsv": "tconst\taverageRating\tnumVotes\ntt1000000\t8.0\t500\ntt1000001\t7.5\t500\n",
        "title.episode.tsv": "tconst\tparentTconst\tseasonNumber\tepisodeNumber\ntt1000001\ttt1000000\t1\t1\n",
    }
    owner = threading.get_ident()

    def download(*, dataset_name, tsv_tmp_path, **kwargs):
        assert threading.get_ident() != owner
        tsv_tmp_path.write_text(fixtures[dataset_name], encoding="utf-8")

    for name in ("rebuild_imdb_index", "rebuild_imdb_episode_index"):
        build = getattr(harvester, name)

        def checked_build(*, build=build, **kwargs):
            assert threading.get_ident() != owner
            return build(**kwargs)

        monkeypatch.setattr(harvester, name, checked_build)
    monkeypatch.setattr(imdb_archive_refresh_refresh, "download_and_extract_imdb_dataset", download)

    async def run():
        source = worker.imdb_archive_source
        assert await worker._ensure_imdb_index(source) == 1
        assert await worker._ensure_imdb_episode_index(source) == 1
        assert worker._imdb_index_path.read_text() == "tt1000000\ttv\t500\t8.0\n"
        assert worker._imdb_episode_index_path.read_text() == "tt1000000\ttt1000001\t1\t1\t500\t75.0\n"
        assert local_db.get_state(worker._imdb_fingerprint_key)
        assert local_db.get_state(worker._imdb_episode_fingerprint_key)

    try:
        asyncio.run(run())
    finally:
        worker.close()

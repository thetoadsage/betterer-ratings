from __future__ import annotations

from pathlib import Path
from typing import Any

import betterer_ratings.constants as package_constants
from betterer_ratings.config.schema import AppConfig


def configure_harvester(
    *,
    harvester: Any,
    config: AppConfig,
    tmdb_client: Any,
    imdb_archive_source_name: str,
    imdb_tmdb_cache_cls: Any,
    imdb_archive_source_cls: Any,
    imdb_archive_fingerprint_key: str,
    imdb_archive_cursor_line_key: str,
    imdb_archive_cursor_byte_key: str,
    imdb_archive_total_key: str,
    imdb_archive_exhausted_key: str,
    imdb_episode_archive_fingerprint_key: str,
    imdb_episode_archive_cursor_line_key: str,
    imdb_episode_archive_cursor_byte_key: str,
    imdb_episode_archive_total_key: str,
    imdb_episode_archive_exhausted_key: str,
    imdb_archive_last_update_key: str,
    imdb_archive_last_update_attempt_key: str,
    source_scan_last_run_key: str,
) -> None:
    self = harvester

    self.source_scan_interval_seconds = max(1, config.runtime.source_scan_interval_hours) * 3600
    self.ratings_ttl_seconds = config.runtime.title_refresh_days * 86400
    self.failed_retry_seconds = max(0, config.runtime.failed_retry_days) * 86400
    self.imdb_archive_local_gz_max_age_seconds = 86400
    self.imdb_archive_download_timeout_seconds = 300
    self.cycle_sleep_seconds = config.runtime.harvester_cycle_sleep_seconds
    self.details_concurrency = config.tmdb.details_concurrency

    self.tmdb_sources = [tmdb_client.build_source(source) for source in config.tmdb.sources]
    self.tmdb_daily_exports = config.tmdb.daily_exports
    self.tmdb_export_directory = Path(config.runtime.temp_path).expanduser() / "tmdb_exports"
    self._pending_tmdb_export_cursors = {}
    imdb_path = Path(config.runtime.imdb_archive_path).expanduser()
    imdb_path.mkdir(parents=True, exist_ok=True)
    self.imdb_cache = imdb_tmdb_cache_cls(imdb_path / "imdb_tmdb_cache.sqlite3")
    self.imdb_archive_source = imdb_archive_source_cls(
        name=imdb_archive_source_name,
        titles_enabled=True,
        episodes_enabled=True,
        min_votes=config.imdb.min_votes,
        types=config.imdb.types,
        exclude_unknown_year=config.imdb.exclude_unknown_year,
        title_batch_size=package_constants.IMDB_ARCHIVE_TITLE_BATCH_SIZE,
        path=imdb_path,
    )
    self.imdb_titles_enabled = True
    self.imdb_episodes_enabled = True
    self.scan_sources = [*self.tmdb_sources, self.imdb_archive_source]

    temp_path = Path(config.runtime.temp_path).expanduser()
    self._imdb_index_path = temp_path / "imdb_archive_candidates_v1.tsv"
    self._imdb_index_path.parent.mkdir(parents=True, exist_ok=True)
    self._imdb_episode_index_path = temp_path / "imdb_archive_episode_candidates_v1.tsv"
    self._imdb_episode_index_path.parent.mkdir(parents=True, exist_ok=True)
    self._imdb_fingerprint_key = imdb_archive_fingerprint_key
    self._imdb_cursor_line_key = imdb_archive_cursor_line_key
    self._imdb_cursor_byte_key = imdb_archive_cursor_byte_key
    self._imdb_total_key = imdb_archive_total_key
    self._imdb_exhausted_key = imdb_archive_exhausted_key
    self._imdb_episode_fingerprint_key = imdb_episode_archive_fingerprint_key
    self._imdb_episode_cursor_line_key = imdb_episode_archive_cursor_line_key
    self._imdb_episode_cursor_byte_key = imdb_episode_archive_cursor_byte_key
    self._imdb_episode_total_key = imdb_episode_archive_total_key
    self._imdb_episode_exhausted_key = imdb_episode_archive_exhausted_key
    self._imdb_last_update_key = imdb_archive_last_update_key
    self._imdb_last_update_attempt_key = imdb_archive_last_update_attempt_key
    self._imdb_last_update_marker_path = imdb_path / ".imdb_archive_last_update_epoch"
    self._source_scan_last_run_key = source_scan_last_run_key

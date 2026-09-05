from __future__ import annotations

# mypy: disable-error-code=attr-defined
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import betterer_ratings.constants as package_constants
from betterer_ratings.config.schema import AppConfig
from betterer_ratings.core.clock import now_epoch
from betterer_ratings.core.ids import (
    imdb_title_type_to_media_type,
    is_valid_imdb_title_id,
)
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.core.scoring import clamp_0_100
from betterer_ratings.domain import models as domain_models
from betterer_ratings.infra.db.imdb_cache_repo import IMDbTMDBCache
from betterer_ratings.infra.db.local_database import LocalDatabase
from betterer_ratings.providers.mal_client import MALClient
from betterer_ratings.providers.mdblist_client import MDBListClient
from betterer_ratings.providers.tmdb_client import TMDBClient
from betterer_ratings.services.harvest import imdb_index_io as harvest_imdb_index_io
from betterer_ratings.services.harvest import setup as harvest_setup
from betterer_ratings.services.harvest.discovery_local import (
    mdblist_daily_quota_pause_until,
    source_scan_due,
)
from betterer_ratings.services.harvest.harvester_cycle_mixin import HarvesterCycleMixin
from betterer_ratings.services.harvest.harvester_imdb_map_mixin import HarvesterIMDbMapMixin
from betterer_ratings.services.harvest.imdb_archive_refresh_refresh import (
    refresh_imdb_archives_if_due,
)
from betterer_ratings.services.harvest.imdb_archive_refresh_utils import (
    download_and_extract_imdb_dataset,
    extract_local_imdb_gz_if_needed,
    file_timestamp_epoch,
    read_imdb_update_marker_epoch,
    safe_unlink,
    write_imdb_update_marker_epoch,
)
from betterer_ratings.services.harvest.imdb_index_build_episode import (
    build_imdb_episode_fingerprint,
    ensure_imdb_episode_index,
    rebuild_imdb_episode_index,
)
from betterer_ratings.services.harvest.imdb_index_build_title import (
    build_imdb_fingerprint,
    ensure_imdb_index,
    rebuild_imdb_index,
)

LOGGER = logging.getLogger("betterer-ratings")

Candidate = domain_models.Candidate
TMDBSource = domain_models.TMDBSource
IMDbArchiveSource = domain_models.IMDbArchiveSource
IMDbArchiveCandidate = domain_models.IMDbArchiveCandidate
IMDbEpisodeArchiveCandidate = domain_models.IMDbEpisodeArchiveCandidate
HarvestCycleResult = domain_models.HarvestCycleResult

IMDB_ARCHIVE_SOURCE_NAME = package_constants.IMDB_ARCHIVE_SOURCE_NAME
IMDB_ARCHIVE_FINGERPRINT_KEY = package_constants.IMDB_ARCHIVE_FINGERPRINT_KEY
IMDB_ARCHIVE_CURSOR_LINE_KEY = package_constants.IMDB_ARCHIVE_CURSOR_LINE_KEY
IMDB_ARCHIVE_CURSOR_BYTE_KEY = package_constants.IMDB_ARCHIVE_CURSOR_BYTE_KEY
IMDB_ARCHIVE_TOTAL_KEY = package_constants.IMDB_ARCHIVE_TOTAL_KEY
IMDB_ARCHIVE_EXHAUSTED_KEY = package_constants.IMDB_ARCHIVE_EXHAUSTED_KEY
IMDB_ARCHIVE_LAST_UPDATE_KEY = package_constants.IMDB_ARCHIVE_LAST_UPDATE_KEY
IMDB_ARCHIVE_LAST_UPDATE_ATTEMPT_KEY = package_constants.IMDB_ARCHIVE_LAST_UPDATE_ATTEMPT_KEY
IMDB_EPISODE_ARCHIVE_FINGERPRINT_KEY = package_constants.IMDB_EPISODE_ARCHIVE_FINGERPRINT_KEY
IMDB_EPISODE_ARCHIVE_CURSOR_LINE_KEY = package_constants.IMDB_EPISODE_ARCHIVE_CURSOR_LINE_KEY
IMDB_EPISODE_ARCHIVE_CURSOR_BYTE_KEY = package_constants.IMDB_EPISODE_ARCHIVE_CURSOR_BYTE_KEY
IMDB_EPISODE_ARCHIVE_TOTAL_KEY = package_constants.IMDB_EPISODE_ARCHIVE_TOTAL_KEY
IMDB_EPISODE_ARCHIVE_EXHAUSTED_KEY = package_constants.IMDB_EPISODE_ARCHIVE_EXHAUSTED_KEY
IMDB_EPISODE_LAST_FULL_SCAN_KEY = package_constants.IMDB_EPISODE_LAST_FULL_SCAN_KEY
IMDB_ARCHIVE_RETRY_COOLDOWN_SECONDS = package_constants.IMDB_ARCHIVE_RETRY_COOLDOWN_SECONDS
SOURCE_SCAN_LAST_RUN_KEY = package_constants.SOURCE_SCAN_LAST_RUN_KEY
IMDB_ARCHIVE_DATASET_URLS = package_constants.IMDB_ARCHIVE_DATASET_URLS
IMDB_EPISODE_DATASET_URLS = package_constants.IMDB_EPISODE_DATASET_URLS


class Harvester(HarvesterCycleMixin, HarvesterIMDbMapMixin):
    def __init__(
        self,
        *,
        config: AppConfig,
        db: LocalDatabase,
        tmdb_client: TMDBClient,
        mdblist_client: MDBListClient,
        mal_client: MALClient | None = None,
    ):
        self.config = config
        self.db = db
        self.tmdb_client = tmdb_client
        self.mdblist_client = mdblist_client
        self.mal_client = mal_client
        harvest_setup.configure_harvester(
            harvester=self,
            config=config,
            tmdb_client=tmdb_client,
            imdb_archive_source_name=IMDB_ARCHIVE_SOURCE_NAME,
            imdb_tmdb_cache_cls=IMDbTMDBCache,
            imdb_archive_source_cls=IMDbArchiveSource,
            imdb_archive_fingerprint_key=IMDB_ARCHIVE_FINGERPRINT_KEY,
            imdb_archive_cursor_line_key=IMDB_ARCHIVE_CURSOR_LINE_KEY,
            imdb_archive_cursor_byte_key=IMDB_ARCHIVE_CURSOR_BYTE_KEY,
            imdb_archive_total_key=IMDB_ARCHIVE_TOTAL_KEY,
            imdb_archive_exhausted_key=IMDB_ARCHIVE_EXHAUSTED_KEY,
            imdb_episode_archive_fingerprint_key=IMDB_EPISODE_ARCHIVE_FINGERPRINT_KEY,
            imdb_episode_archive_cursor_line_key=IMDB_EPISODE_ARCHIVE_CURSOR_LINE_KEY,
            imdb_episode_archive_cursor_byte_key=IMDB_EPISODE_ARCHIVE_CURSOR_BYTE_KEY,
            imdb_episode_archive_total_key=IMDB_EPISODE_ARCHIVE_TOTAL_KEY,
            imdb_episode_archive_exhausted_key=IMDB_EPISODE_ARCHIVE_EXHAUSTED_KEY,
            imdb_archive_last_update_key=IMDB_ARCHIVE_LAST_UPDATE_KEY,
            imdb_archive_last_update_attempt_key=IMDB_ARCHIVE_LAST_UPDATE_ATTEMPT_KEY,
            source_scan_last_run_key=SOURCE_SCAN_LAST_RUN_KEY,
        )
        self.episode_ratings_ttl_seconds = config.runtime.episode_refresh_days * 86400
        self._imdb_episode_last_full_scan_key = IMDB_EPISODE_LAST_FULL_SCAN_KEY

    def close(self) -> None:
        self.imdb_cache.close()

    def _mdblist_daily_quota_pause_until(self, now_ts: int) -> int:
        return mdblist_daily_quota_pause_until(
            db=self.db,
            now_ts=now_ts,
            parse_int_fn=parse_int,
        )

    def _source_scan_due(self, now_ts: int) -> bool:
        return source_scan_due(
            db=self.db,
            source_scan_last_run_key=self._source_scan_last_run_key,
            now_ts=now_ts,
            source_scan_interval_seconds=self.source_scan_interval_seconds,
        )

    def _reset_imdb_cursor(self, *, exhausted: bool = False) -> None:
        harvest_imdb_index_io.reset_cursor(
            db=self.db,
            cursor_line_key=self._imdb_cursor_line_key,
            cursor_byte_key=self._imdb_cursor_byte_key,
            exhausted_key=self._imdb_exhausted_key,
            exhausted=exhausted,
        )

    def _commit_imdb_cursor(self, *, cursor_line: int, cursor_byte: int, exhausted: bool) -> None:
        harvest_imdb_index_io.commit_cursor(
            db=self.db,
            cursor_line_key=self._imdb_cursor_line_key,
            cursor_byte_key=self._imdb_cursor_byte_key,
            exhausted_key=self._imdb_exhausted_key,
            cursor_line=cursor_line,
            cursor_byte=cursor_byte,
            exhausted=exhausted,
        )

    def _reset_imdb_episode_cursor(self, *, exhausted: bool = False) -> None:
        harvest_imdb_index_io.reset_cursor(
            db=self.db,
            cursor_line_key=self._imdb_episode_cursor_line_key,
            cursor_byte_key=self._imdb_episode_cursor_byte_key,
            exhausted_key=self._imdb_episode_exhausted_key,
            exhausted=exhausted,
        )

    def _commit_imdb_episode_cursor(
        self,
        *,
        cursor_line: int,
        cursor_byte: int,
        exhausted: bool,
    ) -> None:
        harvest_imdb_index_io.commit_cursor(
            db=self.db,
            cursor_line_key=self._imdb_episode_cursor_line_key,
            cursor_byte_key=self._imdb_episode_cursor_byte_key,
            exhausted_key=self._imdb_episode_exhausted_key,
            cursor_line=cursor_line,
            cursor_byte=cursor_byte,
            exhausted=exhausted,
        )

    def _build_imdb_fingerprint(
        self,
        source: IMDbArchiveSource,
    ) -> Tuple[str, Path, Path]:
        return build_imdb_fingerprint(source=source)

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        safe_unlink(path)

    def _read_imdb_update_marker_epoch(self) -> Optional[int]:
        return read_imdb_update_marker_epoch(
            marker_path=self._imdb_last_update_marker_path,
            parse_int_fn=parse_int,
        )

    def _write_imdb_update_marker_epoch(self, epoch_seconds: int) -> None:
        write_imdb_update_marker_epoch(
            marker_path=self._imdb_last_update_marker_path,
            epoch_seconds=epoch_seconds,
            safe_unlink_fn=self._safe_unlink,
        )

    @staticmethod
    def _file_timestamp_epoch(path: Path) -> Optional[int]:
        return file_timestamp_epoch(path)

    def _download_and_extract_imdb_dataset(
        self,
        *,
        dataset_name: str,
        url: str,
        gz_tmp_path: Path,
        tsv_tmp_path: Path,
    ) -> None:
        download_and_extract_imdb_dataset(
            dataset_name=dataset_name,
            url=url,
            gz_tmp_path=gz_tmp_path,
            tsv_tmp_path=tsv_tmp_path,
            timeout_seconds=self.imdb_archive_download_timeout_seconds,
        )

    def _extract_local_imdb_gz_if_needed(
        self,
        *,
        dataset_name: str,
        archive_dir: Path,
        now_ts: int,
        max_age_seconds: int,
    ) -> bool:
        return extract_local_imdb_gz_if_needed(
            dataset_name=dataset_name,
            archive_dir=archive_dir,
            now_ts=now_ts,
            max_age_seconds=max_age_seconds,
            logger=LOGGER,
            file_timestamp_epoch_fn=self._file_timestamp_epoch,
            safe_unlink_fn=self._safe_unlink,
        )

    def _refresh_imdb_archives_if_due(self, source: IMDbArchiveSource) -> None:
        refresh_imdb_archives_if_due(
            harvester=self,
            source=source,
            logger=LOGGER,
            now_epoch_fn=now_epoch,
            parse_int_fn=parse_int,
            imdb_archive_dataset_urls=IMDB_ARCHIVE_DATASET_URLS,
            imdb_episode_dataset_urls=IMDB_EPISODE_DATASET_URLS,
            imdb_archive_retry_cooldown_seconds=IMDB_ARCHIVE_RETRY_COOLDOWN_SECONDS,
        )

    def _rebuild_imdb_index(
        self,
        *,
        source: IMDbArchiveSource,
        ratings_path: Path,
        basics_path: Path,
        fingerprint: str,
    ) -> int:
        return rebuild_imdb_index(
            harvester=self,
            source=source,
            ratings_path=ratings_path,
            basics_path=basics_path,
            fingerprint=fingerprint,
            parse_int_fn=parse_int,
            is_valid_imdb_title_id_fn=is_valid_imdb_title_id,
            imdb_title_type_to_media_type_fn=imdb_title_type_to_media_type,
            now_epoch_fn=now_epoch,
            logger=LOGGER,
        )

    def _ensure_imdb_index(self, source: IMDbArchiveSource) -> int:
        return ensure_imdb_index(
            harvester=self,
            source=source,
            refresh_imdb_archives_if_due_fn=self._refresh_imdb_archives_if_due,
            build_imdb_fingerprint_fn=self._build_imdb_fingerprint,
            rebuild_imdb_index_fn=self._rebuild_imdb_index,
        )

    def _build_imdb_episode_fingerprint(
        self,
        source: IMDbArchiveSource,
    ) -> Tuple[str, Path, Path, Path]:
        return build_imdb_episode_fingerprint(
            source=source,
        )

    def _rebuild_imdb_episode_index(
        self,
        *,
        source: IMDbArchiveSource,
        episode_path: Path,
        ratings_path: Path,
        basics_path: Path,
        fingerprint: str,
    ) -> int:
        return rebuild_imdb_episode_index(
            harvester=self,
            source=source,
            episode_path=episode_path,
            ratings_path=ratings_path,
            basics_path=basics_path,
            fingerprint=fingerprint,
            parse_int_fn=parse_int,
            is_valid_imdb_title_id_fn=is_valid_imdb_title_id,
            clamp_0_100_fn=clamp_0_100,
            now_epoch_fn=now_epoch,
            logger=LOGGER,
        )

    def _ensure_imdb_episode_index(self, source: IMDbArchiveSource) -> int:
        return ensure_imdb_episode_index(
            harvester=self,
            source=source,
            refresh_imdb_archives_if_due_fn=self._refresh_imdb_archives_if_due,
            build_imdb_episode_fingerprint_fn=self._build_imdb_episode_fingerprint,
            rebuild_imdb_episode_index_fn=self._rebuild_imdb_episode_index,
        )

    def _read_imdb_episode_index_batch(
        self,
        *,
        source: IMDbArchiveSource,
        day_key: str,
    ) -> Tuple[
        List[IMDbEpisodeArchiveCandidate],
        int,
        int,
        bool,
    ]:
        return harvest_imdb_index_io.read_imdb_episode_index_batch(
            db=self.db,
            source=source,
            day_key=day_key,
            imdb_episode_exhausted_key=self._imdb_episode_exhausted_key,
            imdb_episode_cursor_line_key=self._imdb_episode_cursor_line_key,
            imdb_episode_cursor_byte_key=self._imdb_episode_cursor_byte_key,
            imdb_episode_total_key=self._imdb_episode_total_key,
            imdb_episode_index_path=self._imdb_episode_index_path,
            parse_int_fn=parse_int,
            is_valid_imdb_title_id_fn=is_valid_imdb_title_id,
            clamp_0_100_fn=clamp_0_100,
            imdb_episode_archive_candidate_cls=IMDbEpisodeArchiveCandidate,
        )

    def _read_imdb_index_batch(
        self,
        source: IMDbArchiveSource,
    ) -> Tuple[List[IMDbArchiveCandidate], int, int, bool]:
        return harvest_imdb_index_io.read_imdb_index_batch(
            db=self.db,
            source=source,
            imdb_exhausted_key=self._imdb_exhausted_key,
            imdb_cursor_line_key=self._imdb_cursor_line_key,
            imdb_cursor_byte_key=self._imdb_cursor_byte_key,
            imdb_total_key=self._imdb_total_key,
            imdb_index_path=self._imdb_index_path,
            parse_int_fn=parse_int,
            is_valid_imdb_title_id_fn=is_valid_imdb_title_id,
            imdb_archive_candidate_cls=IMDbArchiveCandidate,
        )

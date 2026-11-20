from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import betterer_ratings.constants as package_constants
from betterer_ratings.core.clock import format_duration
from betterer_ratings.services.harvest.imdb_archive_refresh_utils import (
    download_and_extract_imdb_dataset,
    extract_local_imdb_gz_if_needed,
    file_timestamp_epoch,
    read_imdb_update_marker_epoch,
    safe_unlink,
    write_imdb_update_marker_epoch,
)


def latest_imdb_archive_refresh_epoch(
    now_ts: int,
    *,
    refresh_hour_utc: int = package_constants.IMDB_ARCHIVE_REFRESH_HOUR_UTC,
) -> int:
    refresh_hour = max(0, min(23, int(refresh_hour_utc)))
    now = datetime.fromtimestamp(int(now_ts), timezone.utc)
    scheduled = now.replace(hour=refresh_hour, minute=0, second=0, microsecond=0)
    if now < scheduled:
        scheduled -= timedelta(days=1)
    return int(scheduled.timestamp())


async def refresh_imdb_archives_if_due(
    *,
    harvester: Any,
    source: Any,
    logger: Any,
    now_epoch_fn: Callable[[], int],
    parse_int_fn: Callable[[Any], Optional[int]],
    imdb_archive_dataset_urls: Dict[str, str],
    imdb_episode_dataset_urls: Dict[str, str],
    imdb_archive_retry_cooldown_seconds: int,
) -> None:
    self = harvester
    archive_dir = source.path
    archive_dir.mkdir(parents=True, exist_ok=True)

    now_ts = now_epoch_fn()
    scheduled_refresh_ts = latest_imdb_archive_refresh_epoch(now_ts)
    dataset_urls: Dict[str, str] = dict(imdb_archive_dataset_urls)
    if bool(source.episodes_enabled):
        dataset_urls.update(imdb_episode_dataset_urls)
    required_tsv_paths = [archive_dir / dataset for dataset in dataset_urls]
    missing_files = [path for path in required_tsv_paths if not path.exists()]
    extracted_from_local_gz = False
    if missing_files:
        for dataset_name in dataset_urls:
            extracted = await asyncio.to_thread(
                extract_local_imdb_gz_if_needed,
                dataset_name=dataset_name,
                archive_dir=archive_dir,
                now_ts=now_ts,
                max_age_seconds=self.imdb_archive_local_gz_max_age_seconds,
                logger=logger,
                file_timestamp_epoch_fn=file_timestamp_epoch,
                safe_unlink_fn=safe_unlink,
            )
            extracted_from_local_gz = extracted_from_local_gz or extracted
        missing_files = [path for path in required_tsv_paths if not path.exists()]
        if extracted_from_local_gz and not missing_files:
            write_imdb_update_marker_epoch(
                marker_path=self._imdb_last_update_marker_path,
                epoch_seconds=now_ts,
                safe_unlink_fn=safe_unlink,
            )

    update_due = bool(missing_files)
    if not update_due:
        last_update_reference = read_imdb_update_marker_epoch(
            marker_path=self._imdb_last_update_marker_path,
            parse_int_fn=parse_int_fn,
        )
        if last_update_reference is None:
            db_last_update = max(0, self.db.get_state_int(self._imdb_last_update_key, 0))
            if db_last_update > 0:
                last_update_reference = db_last_update
                write_imdb_update_marker_epoch(
                    marker_path=self._imdb_last_update_marker_path,
                    epoch_seconds=last_update_reference,
                    safe_unlink_fn=safe_unlink,
                )

        if last_update_reference is None:
            timestamp_values = [file_timestamp_epoch(path) for path in required_tsv_paths]
            if any(value is None for value in timestamp_values):
                logger.warning(
                    "[IMDbArchive] Auto-update skipped: file timestamps are unavailable for one or more TSV files."
                )
                return
            timestamp_reference = min(int(value) for value in timestamp_values if value is not None)
            last_update_reference = timestamp_reference
            write_imdb_update_marker_epoch(
                marker_path=self._imdb_last_update_marker_path,
                epoch_seconds=last_update_reference,
                safe_unlink_fn=safe_unlink,
            )

        update_due = int(last_update_reference) < scheduled_refresh_ts
    if not update_due:
        return

    last_attempt = max(0, self.db.get_state_int(self._imdb_last_update_attempt_key, 0))
    if (
        not missing_files
        and last_attempt > 0
        and now_ts - last_attempt < imdb_archive_retry_cooldown_seconds
    ):
        retry_in = imdb_archive_retry_cooldown_seconds - (now_ts - last_attempt)
        logger.debug(
            "[IMDbArchive] Auto-update recently attempted; retry in %s.",
            format_duration(max(1, retry_in)),
        )
        return

    self.db.set_state(self._imdb_last_update_attempt_key, now_ts)
    logger.info(
        "[IMDbArchive] Auto-update started (daily_schedule=%02d:00 UTC, path=%s).",
        package_constants.IMDB_ARCHIVE_REFRESH_HOUR_UTC,
        archive_dir,
    )

    staging: List[Tuple[Path, Path, str]] = []
    staged_paths: List[Path] = []
    try:
        for dataset_name, url in dataset_urls.items():
            tsv_final_path = archive_dir / dataset_name
            tsv_tmp_path = archive_dir / f".{dataset_name}.download.tmp"
            gz_tmp_path = archive_dir / f".{dataset_name}.gz.download.tmp"
            safe_unlink(gz_tmp_path)
            safe_unlink(tsv_tmp_path)

            logger.info("[IMDbArchive] Downloading %s", url)
            await asyncio.to_thread(
                download_and_extract_imdb_dataset,
                dataset_name=dataset_name,
                url=url,
                gz_tmp_path=gz_tmp_path,
                tsv_tmp_path=tsv_tmp_path,
                timeout_seconds=self.imdb_archive_download_timeout_seconds,
            )
            # After extraction, drop the compressed artifact.
            safe_unlink(gz_tmp_path)
            staged_paths.append(tsv_tmp_path)
            staging.append(
                (
                    tsv_tmp_path,
                    tsv_final_path,
                    dataset_name,
                )
            )

        for tsv_tmp_path, tsv_final_path, _dataset_name in staging:
            tsv_tmp_path.replace(tsv_final_path)
        for dataset_name in dataset_urls:
            # Cleanup old compressed snapshots after successful refresh.
            safe_unlink(archive_dir / f"{dataset_name}.gz")

        self.db.set_state(self._imdb_last_update_key, now_ts)
        write_imdb_update_marker_epoch(
            marker_path=self._imdb_last_update_marker_path,
            epoch_seconds=now_ts,
            safe_unlink_fn=safe_unlink,
        )
        logger.info(
            "[IMDbArchive] Auto-update completed: %s dataset(s) refreshed.",
            len(staging),
        )
    except Exception as exc:
        for tmp_path in staged_paths:
            safe_unlink(tmp_path)
        for dataset_name in dataset_urls:
            safe_unlink(archive_dir / f".{dataset_name}.download.tmp")
            safe_unlink(archive_dir / f".{dataset_name}.gz.download.tmp")

        still_missing = [path for path in required_tsv_paths if not path.exists()]
        if still_missing:
            raise RuntimeError(
                f"IMDb auto-update failed and required files are missing: {still_missing}"
            ) from exc
        logger.warning(
            "[IMDbArchive] Auto-update failed; using existing local files. reason=%s",
            exc,
        )

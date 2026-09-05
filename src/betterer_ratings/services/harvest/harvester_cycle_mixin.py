from __future__ import annotations

# mypy: disable-error-code=attr-defined
import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

from betterer_ratings.core.clock import local_day_key, now_epoch, to_iso
from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.core.scoring import parse_mdblist_ratings, parse_tmdb_vote_average
from betterer_ratings.domain.models import Candidate, HarvestCycleResult
from betterer_ratings.services.harvest import details as harvest_details
from betterer_ratings.services.harvest import enrichment as harvest_enrichment
from betterer_ratings.services.harvest import local_selection as harvest_local_selection
from betterer_ratings.services.harvest.cycle_run_cycle import run_cycle
from betterer_ratings.services.harvest.cycle_runner import run_harvester
from betterer_ratings.services.harvest.discovery_local import collect_local_candidates
from betterer_ratings.services.harvest.discovery_source_scan import collect_source_candidates

LOGGER = logging.getLogger("betterer-ratings")


class HarvesterCycleMixin:
    def _save_candidate_enrichment(
        self,
        *,
        candidate: Candidate,
        details: Optional[Dict[str, Any]],
        md_item: Optional[Dict[str, Any]],
        now_ts: int,
        mal_score: Optional[float] = None,
    ) -> Tuple[int, int, int, int, int]:
        return harvest_enrichment.save_candidate_enrichment(
            db=self.db,
            candidate=candidate,
            details=details,
            md_item=md_item,
            now_ts=now_ts,
            mal_score=mal_score,
            parse_mdblist_ratings_fn=parse_mdblist_ratings,
            parse_tmdb_vote_average_fn=parse_tmdb_vote_average,
            extract_mappings_fn=extract_mappings,
        )

    def _init_local_stats(self) -> Dict[str, int]:
        return harvest_local_selection.init_local_stats()

    def _local_due_counts(self, now_ts: int) -> Dict[str, int]:
        return harvest_local_selection.local_due_counts(
            db=self.db,
            now_ts=now_ts,
            ratings_ttl_seconds=self.ratings_ttl_seconds,
            failed_retry_seconds=self.failed_retry_seconds,
        )

    async def _collect_local_candidates(
        self,
        stop_event: asyncio.Event,
        *,
        day_key: str,
    ) -> Tuple[List[Candidate], Dict[str, int], bool]:
        return await collect_local_candidates(
            stop_event=stop_event,
            db=self.db,
            day_key=day_key,
            ratings_ttl_seconds=self.ratings_ttl_seconds,
            failed_retry_seconds=self.failed_retry_seconds,
            init_local_stats_fn=self._init_local_stats,
            local_due_counts_fn=self._local_due_counts,
            parse_int_fn=parse_int,
            now_epoch_fn=now_epoch,
            candidate_cls=Candidate,
        )

    async def _collect_source_candidates(
        self,
        stop_event: asyncio.Event,
    ) -> Tuple[List[Candidate], Dict[str, Dict[str, int]], bool]:
        return await collect_source_candidates(
            stop_event=stop_event,
            db=self.db,
            tmdb_client=self.tmdb_client,
            tmdb_sources=self.tmdb_sources,
            scan_sources=self.scan_sources,
            imdb_archive_source=self.imdb_archive_source,
            imdb_titles_enabled=self.imdb_titles_enabled,
            ensure_imdb_index_fn=self._ensure_imdb_index,
            read_imdb_index_batch_fn=self._read_imdb_index_batch,
            commit_imdb_cursor_fn=self._commit_imdb_cursor,
            map_imdb_candidates_to_tmdb_fn=self._map_imdb_candidates_to_tmdb,
            imdb_cursor_line_key=self._imdb_cursor_line_key,
            imdb_total_key=self._imdb_total_key,
            parse_int_fn=parse_int,
            now_epoch_fn=now_epoch,
            logger=LOGGER,
            candidate_cls=Candidate,
        )

    async def _fetch_tmdb_details(
        self, candidates: Sequence[Candidate], stop_event: asyncio.Event
    ) -> Tuple[Dict[Tuple[str, int], Optional[Dict[str, Any]]], bool]:
        return await harvest_details.fetch_tmdb_details(
            candidates=candidates,
            stop_event=stop_event,
            tmdb_client=self.tmdb_client,
            db=self.db,
            details_concurrency=self.details_concurrency,
            now_epoch_fn=now_epoch,
            logger=LOGGER,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await run_harvester(
            harvester=self,
            stop_event=stop_event,
            logger=LOGGER,
            now_epoch_fn=now_epoch,
        )

    async def _run_cycle(self, stop_event: asyncio.Event) -> HarvestCycleResult:
        return cast(HarvestCycleResult, await run_cycle(
            harvester=self,
            stop_event=stop_event,
            logger=LOGGER,
            now_epoch_fn=now_epoch,
            local_day_key_fn=local_day_key,
            to_iso_fn=to_iso,
            harvest_cycle_result_cls=HarvestCycleResult,
        ))

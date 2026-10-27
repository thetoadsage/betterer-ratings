from __future__ import annotations

# mypy: disable-error-code=attr-defined
import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from betterer_ratings.core.clock import now_epoch
from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.domain.models import Candidate
from betterer_ratings.services.harvest import episodes as harvest_episodes
from betterer_ratings.services.harvest.imdb_mapping_async import (
    map_episode_parents_to_tmdb,
    map_imdb_candidates_to_tmdb,
)
from betterer_ratings.services.harvest.imdb_mapping_helpers import (
    extract_tmdb_from_find_payload,
    resolve_imdb_to_tmdb_local,
)

LOGGER = logging.getLogger("betterer-ratings")


class HarvesterIMDbMapMixin:
    @staticmethod
    def _extract_tmdb_from_find_payload(
        payload: Dict[str, Any],
        media_type: str,
    ) -> Tuple[Optional[int], str, float]:
        return extract_tmdb_from_find_payload(
            payload=payload,
            media_type=media_type,
            parse_int_fn=parse_int,
        )

    def _resolve_imdb_to_tmdb_local(
        self,
        *,
        imdb_id: str,
        media_type: str,
    ) -> Optional[Candidate]:
        return resolve_imdb_to_tmdb_local(
            db=self.db,
            imdb_cache=self.imdb_cache,
            imdb_id=imdb_id,
            media_type=media_type,
            normalize_imdb_title_id_fn=normalize_imdb_title_id,
            parse_int_fn=parse_int,
            candidate_cls=Candidate,
            dins_mapping_cache=getattr(self, "dins_mapping_cache", None),
        )

    async def _map_imdb_candidates_to_tmdb(
        self,
        *,
        candidates: Sequence[Any],
        stop_event: asyncio.Event,
    ) -> Tuple[List[Candidate], int, int]:
        dins = getattr(self, "dins_mapping_cache", None)
        if dins is not None and not stop_event.is_set():
            await dins.prepare()

        def resolve_local(imdb_id: str, media_type: str) -> Optional[Candidate]:
            return self._resolve_imdb_to_tmdb_local(
                imdb_id=imdb_id,
                media_type=media_type,
            )

        def extract_from_payload(
            payload: Dict[str, Any], media_type: str
        ) -> Tuple[Optional[int], str, float]:
            return self._extract_tmdb_from_find_payload(payload, media_type)

        return await map_imdb_candidates_to_tmdb(
            candidates=candidates,
            stop_event=stop_event,
            details_concurrency=self.details_concurrency,
            resolve_imdb_to_tmdb_local_fn=resolve_local,
            fetch_find_by_imdb_fn=self.tmdb_client.fetch_find_by_imdb,
            extract_tmdb_from_find_payload_fn=extract_from_payload,
            imdb_cache=self.imdb_cache,
            now_epoch_fn=now_epoch,
            candidate_cls=Candidate,
        )

    async def _map_imdb_episode_parents_to_tmdb(
        self,
        *,
        parent_ids: Sequence[str],
        stop_event: asyncio.Event,
    ) -> Tuple[Dict[str, Candidate], int, int]:
        dins = getattr(self, "dins_mapping_cache", None)
        if dins is not None and not stop_event.is_set():
            await dins.prepare()

        def resolve_local(imdb_id: str, media_type: str) -> Optional[Candidate]:
            return self._resolve_imdb_to_tmdb_local(
                imdb_id=imdb_id,
                media_type=media_type,
            )

        def extract_from_payload(
            payload: Dict[str, Any], media_type: str
        ) -> Tuple[Optional[int], str, float]:
            return self._extract_tmdb_from_find_payload(payload, media_type)

        return await map_episode_parents_to_tmdb(
            parent_ids=parent_ids,
            stop_event=stop_event,
            details_concurrency=self.details_concurrency,
            resolve_imdb_to_tmdb_local_fn=resolve_local,
            fetch_find_by_imdb_fn=self.tmdb_client.fetch_find_by_imdb,
            extract_tmdb_from_find_payload_fn=extract_from_payload,
            imdb_cache=self.imdb_cache,
            normalize_imdb_title_id_fn=normalize_imdb_title_id,
            now_epoch_fn=now_epoch,
            candidate_cls=Candidate,
        )

    async def _run_imdb_episode_cycle(self, stop_event: asyncio.Event) -> Dict[str, int]:
        return await harvest_episodes.run_imdb_episode_cycle(
            harvester=self,
            stop_event=stop_event,
            logger=LOGGER,
            now_epoch_fn=now_epoch,
        )

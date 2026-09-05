from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Sequence, Tuple

import betterer_ratings.constants as package_constants
from betterer_ratings.core.ids import normalize_imdb_title_id
from betterer_ratings.core.parsing import parse_int
from betterer_ratings.core.scoring import clamp_0_100, score_to_tenths
from betterer_ratings.domain.models import IMDbEpisodeArchiveCandidate
from betterer_ratings.infra.db import harvest_repo as db_harvest_repo
from betterer_ratings.infra.db.enrichment_save import (
    save_enriched_item,
    save_imdb_episode_ratings,
)
from betterer_ratings.infra.db.enrichment_upserts import (
    upsert_episode_rating,
    upsert_mapping,
    upsert_rating,
    upsert_title,
)

HARVEST_REASON_ORDER = package_constants.HARVEST_REASON_ORDER
SUPPORTED_PMDB_MAPPING_TYPES = package_constants.SUPPORTED_PMDB_MAPPING_TYPES


class LocalDatabaseEnrichmentMixin:
    conn: sqlite3.Connection

    @staticmethod
    def _harvest_reason_case_sql() -> str:
        return db_harvest_repo.harvest_reason_case_sql()

    @staticmethod
    def _harvest_reason_case_params(
        *,
        now_ts: int,
        ratings_ttl_seconds: int,
        failed_retry_seconds: int,
    ) -> Tuple[int, int, int, int, int]:
        return db_harvest_repo.harvest_reason_case_params(
            now_ts=now_ts,
            ratings_ttl_seconds=ratings_ttl_seconds,
            failed_retry_seconds=failed_retry_seconds,
        )

    def local_due_counts(
        self,
        *,
        now_ts: int,
        ratings_ttl_seconds: int,
        failed_retry_seconds: int = 0,
    ) -> Dict[str, int]:
        return db_harvest_repo.local_due_counts(
            self.conn,
            now_ts=now_ts,
            ratings_ttl_seconds=ratings_ttl_seconds,
            failed_retry_seconds=failed_retry_seconds,
        )

    def title_key_set(self) -> set[Tuple[str, int]]:
        return db_harvest_repo.title_key_set(self.conn)

    def get_title_mapping(self, *, tmdb_id: int, media_type: str, id_type: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT id_value FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type=?",
            (tmdb_id, media_type, id_type),
        ).fetchone()
        return str(row["id_value"]) if row else None

    def title_has_imdb_mapping(self, *, tmdb_id: int, media_type: str) -> bool:
        return db_harvest_repo.title_has_imdb_mapping(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
        )

    def select_local_due_titles(
        self,
        *,
        now_ts: int,
        ratings_ttl_seconds: int,
        limit: int,
        failed_retry_seconds: int = 0,
    ) -> List[sqlite3.Row]:
        return db_harvest_repo.select_local_due_titles(
            self.conn,
            now_ts=now_ts,
            ratings_ttl_seconds=ratings_ttl_seconds,
            limit=limit,
            failed_retry_seconds=failed_retry_seconds,
            harvest_reason_order=HARVEST_REASON_ORDER,
        )

    def _upsert_title(
        self,
        tmdb_id: int,
        media_type: str,
        title: str,
        imdb_id: Optional[str],
        popularity: float,
        tmdb_vote_average: Optional[float],
        now_ts: int,
        error_message: Optional[str],
    ) -> None:
        upsert_title(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title,
            imdb_id=imdb_id,
            popularity=popularity,
            tmdb_vote_average=tmdb_vote_average,
            now_ts=now_ts,
            error_message=error_message,
            normalize_imdb_title_id_fn=normalize_imdb_title_id,
        )

    def _upsert_rating(
        self,
        tmdb_id: int,
        media_type: str,
        label: str,
        score: float,
        fetched_at: int,
    ) -> bool:
        return upsert_rating(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            label=label,
            score=score,
            fetched_at=fetched_at,
            score_to_tenths_fn=score_to_tenths,
        )

    def _upsert_episode_rating(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        season: int,
        episode: int,
        label: str,
        score: float,
        fetched_at: int,
        imdb_parent_id: Optional[str],
        imdb_episode_id: Optional[str],
        votes: Optional[int],
    ) -> bool:
        return upsert_episode_rating(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            season=season,
            episode=episode,
            label=label,
            score=score,
            fetched_at=fetched_at,
            imdb_parent_id=imdb_parent_id,
            imdb_episode_id=imdb_episode_id,
            votes=votes,
            score_to_tenths_fn=score_to_tenths,
        )

    def _upsert_mapping(
        self,
        tmdb_id: int,
        media_type: str,
        id_type: str,
        id_value: str,
        fetched_at: int,
    ) -> bool:
        return upsert_mapping(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            id_type=id_type,
            id_value=id_value,
            fetched_at=fetched_at,
            normalize_imdb_title_id_fn=normalize_imdb_title_id,
        )

    def save_enriched_item(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        title: str,
        imdb_id: Optional[str],
        popularity: float,
        tmdb_vote_average: Optional[float],
        enrichment_error: Optional[str],
        ratings: Dict[str, float],
        mappings: Dict[str, str],
        now_ts: int,
    ) -> Tuple[int, int]:
        return save_enriched_item(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title,
            imdb_id=imdb_id,
            popularity=popularity,
            tmdb_vote_average=tmdb_vote_average,
            enrichment_error=enrichment_error,
            ratings=ratings,
            mappings=mappings,
            now_ts=now_ts,
            supported_pmdb_mapping_types=SUPPORTED_PMDB_MAPPING_TYPES,
            clamp_0_100_fn=clamp_0_100,
            upsert_title_fn=self._upsert_title,
            upsert_rating_fn=self._upsert_rating,
            upsert_mapping_fn=self._upsert_mapping,
        )

    def save_imdb_episode_ratings(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        imdb_parent_id: str,
        entries: Sequence[IMDbEpisodeArchiveCandidate],
        now_ts: int,
        default_label: str = "IM",
    ) -> int:
        return save_imdb_episode_ratings(
            self.conn,
            tmdb_id=tmdb_id,
            media_type=media_type,
            imdb_parent_id=imdb_parent_id,
            entries=entries,
            now_ts=now_ts,
            default_label=default_label,
            parse_int_fn=parse_int,
            clamp_0_100_fn=clamp_0_100,
            normalize_imdb_title_id_fn=normalize_imdb_title_id,
            upsert_episode_rating_fn=self._upsert_episode_rating,
        )

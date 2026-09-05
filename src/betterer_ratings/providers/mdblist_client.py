from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence, Set, Tuple

from betterer_ratings.config.schema import MDBListConfig
from betterer_ratings.core.clock import to_log_time
from betterer_ratings.core.parsing import parse_int as core_parse_int
from betterer_ratings.domain.models import Candidate
from betterer_ratings.infra.http.client import HTTPClient


class MDBListClient:
    """MDBList API adapter."""

    def __init__(
        self,
        *,
        api_key: str,
        config: MDBListConfig,
        gate: Any,
        parse_int_fn: Callable[[Any], Optional[int]] = core_parse_int,
        logger: Optional[logging.Logger] = None,
    ):
        self.api_key = api_key
        self.base_url = config.base_url.rstrip("/")
        self.batch_size = config.batch_size
        self.max_pause_block_seconds = 20
        self.gate = gate
        self.http = HTTPClient(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self._parse_int = parse_int_fn
        self._logger = logger or logging.getLogger("betterer-ratings")

    def _md_media_type(self, media_type: str) -> str:
        return "movie" if media_type == "movie" else "show"

    def _quota_fields(self) -> Dict[str, Any]:
        reset = self._parse_int(getattr(self.gate, "rate_reset", None))
        return {
            "mdblist_rate_limit": self._parse_int(getattr(self.gate, "rate_limit", None)),
            "mdblist_rate_remaining": self._parse_int(
                getattr(self.gate, "rate_remaining", None)
            ),
            "mdblist_rate_reset_at": to_log_time(reset) if reset else "",
        }

    def _quota_text(self) -> str:
        quota = self._quota_fields()
        limit = quota["mdblist_rate_limit"]
        remaining = quota["mdblist_rate_remaining"]
        reset_at = quota["mdblist_rate_reset_at"] or "unknown"
        if limit is None or remaining is None:
            return "quota=unknown"
        return f"quota_remaining={remaining}/{limit} quota_reset={reset_at}"

    async def _fetch_batch(
        self, media_type: str, tmdb_ids: Sequence[int]
    ) -> Tuple[Dict[int, Dict[str, Any]], Set[int], bool, bool, str]:
        """Returns (results_by_tmdb_id, attempted_tmdb_ids, success, halt, halt_reason)."""
        if not tmdb_ids:
            return {}, set(), True, False, ""

        md_media_type = self._md_media_type(media_type)
        url = f"{self.base_url}/tmdb/{md_media_type}"

        response = await self.http.request_json(
            method="POST",
            url=url,
            params={"apikey": self.api_key},
            json_body={"ids": list(tmdb_ids)},
            gate=self.gate,
            max_pause_wait_seconds=self.max_pause_block_seconds,
        )

        if response.status == 429 and response.text == "service paused":
            self._logger.warning(
                "[MDBList] Paused too long, skipping batch of %s %s items for now.",
                len(tmdb_ids),
                media_type,
            )
            return {}, set(), False, True, "service paused"

        if response.status == 401:
            self._logger.error("[MDBList] Unauthorized (401). Check API key.")
            self.gate.pause_for(300, "Unauthorized API key")
            return {}, set(), False, True, "unauthorized"

        if response.status == 403:
            self._logger.error("[MDBList] Forbidden (403).")
            self.gate.pause_for(300, "Forbidden")
            return {}, set(), False, True, "forbidden"

        if response.status == 429:
            return {}, set(), False, True, "rate limited"

        if not response.ok:
            self._logger.warning(
                "[MDBList] Batch request failed (%s): %s",
                response.status,
                response.text[:300],
            )
            return {}, set(), False, False, f"http {response.status}"

        payload = response.data
        items: list[Dict[str, Any]] = []
        if isinstance(payload, list):
            items = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            candidate = payload.get("items")
            if isinstance(candidate, list):
                items = [x for x in candidate if isinstance(x, dict)]

        by_id: Dict[int, Dict[str, Any]] = {}
        for item in items:
            ids_raw = item.get("ids")
            ids_block: Dict[str, Any] = ids_raw if isinstance(ids_raw, dict) else {}
            tmdb_value = (
                ids_block.get("tmdb") or item.get("tmdb_id") or item.get("tmdbid") or item.get("id")
            )
            tmdb_int = self._parse_int(tmdb_value)
            if tmdb_int is None:
                continue
            by_id[tmdb_int] = item

        return by_id, {int(x) for x in tmdb_ids}, True, False, ""

    async def fetch_for_candidates(
        self,
        candidates: Sequence[Candidate],
        on_chunk: Optional[
            Callable[[str, Sequence[int], Dict[int, Dict[str, Any]], Set[int], int, int], None | Awaitable[None]]
        ] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Tuple[
        Dict[Tuple[str, int], Dict[str, Any]],
        Set[Tuple[str, int]],
        bool,
        bool,
        str,
        Dict[str, int],
    ]:
        grouped: Dict[str, list[int]] = defaultdict(list)
        for candidate in candidates:
            grouped[candidate.media_type].append(candidate.tmdb_id)

        results: Dict[Tuple[str, int], Dict[str, Any]] = {}
        attempted: Set[Tuple[str, int]] = set()
        all_success = True
        halted_early = False
        halt_reason = ""
        failed_batches: Dict[str, int] = {"movie": 0, "tv": 0}

        for media_type, tmdb_ids in grouped.items():
            if stop_event is not None and stop_event.is_set():
                halted_early = True
                halt_reason = "stop requested"
                break
            unique_ids = sorted(set(tmdb_ids))
            if not unique_ids:
                continue
            total_chunks = max(1, math.ceil(len(unique_ids) / self.batch_size))
            self._logger.info(
                "[MDBList] Fetching %s %s IDs in %s batch(es) (batch_size=%s, %s).",
                len(unique_ids),
                media_type,
                total_chunks,
                self.batch_size,
                self._quota_text(),
                extra={
                    "event": "batch.group_started",
                    "media_type": media_type,
                    "id_count": len(unique_ids),
                    "batch_count": total_chunks,
                    "batch_size": self.batch_size,
                    **self._quota_fields(),
                },
            )

            chunk_index = 0
            for i in range(0, len(unique_ids), self.batch_size):
                if stop_event is not None and stop_event.is_set():
                    halted_early = True
                    halt_reason = "stop requested"
                    break
                chunk_index += 1
                chunk = unique_ids[i : i + self.batch_size]
                (
                    chunk_results,
                    chunk_attempted,
                    chunk_success,
                    chunk_halt,
                    chunk_reason,
                ) = await self._fetch_batch(media_type, chunk)
                if not chunk_success:
                    failed_batches[media_type] = failed_batches.get(media_type, 0) + 1
                    all_success = False
                    if chunk_halt:
                        halted_early = True
                        halt_reason = chunk_reason or "paused"

                if chunk_index == 1 or chunk_index == total_chunks or chunk_index % 10 == 0:
                    self._logger.info(
                        "[MDBList] %s batch %s/%s: requested=%s attempted=%s found=%s success=%s %s",
                        media_type,
                        chunk_index,
                        total_chunks,
                        len(chunk),
                        len(chunk_attempted),
                        len(chunk_results),
                        chunk_success,
                        self._quota_text(),
                        extra={
                            "event": "batch.complete",
                            "media_type": media_type,
                            "batch_index": chunk_index,
                            "batch_count": total_chunks,
                            "requested": len(chunk),
                            "attempted": len(chunk_attempted),
                            "found": len(chunk_results),
                            "success": chunk_success,
                            **self._quota_fields(),
                        },
                    )

                if not chunk_success and chunk_halt:
                    self._logger.info(
                        "[MDBList] Halting remaining %s batches for this cycle (%s).",
                        media_type,
                        halt_reason,
                    )
                    break

                for tmdb_id in chunk_attempted:
                    attempted.add((media_type, tmdb_id))

                for tmdb_id, item in chunk_results.items():
                    results[(media_type, tmdb_id)] = item

                if on_chunk is not None:
                    chunk_callback = on_chunk(
                        media_type,
                        chunk,
                        chunk_results,
                        chunk_attempted,
                        chunk_index,
                        total_chunks,
                    )
                    if inspect.isawaitable(chunk_callback):
                        await chunk_callback

            if halted_early:
                break

        return (
            results,
            attempted,
            all_success,
            halted_early,
            halt_reason,
            failed_batches,
        )

    async def aclose(self) -> None:
        await self.http.aclose()

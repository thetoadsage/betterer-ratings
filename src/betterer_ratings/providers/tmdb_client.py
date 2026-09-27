from __future__ import annotations

from typing import Any, Mapping

from betterer_ratings.config.schema import TMDBConfig, TMDBSourceConfig
from betterer_ratings.domain.models import APIResponse, TMDBSource
from betterer_ratings.infra.http.client import HTTPClient
from betterer_ratings.providers import tmdb_source as provider_tmdb_source


class TMDBClient:
    """TMDB API adapter."""

    def __init__(
        self,
        *,
        api_key: str,
        config: TMDBConfig,
        gate: Any,
    ):
        self.api_key = api_key
        self.base_url = config.base_url.rstrip("/")
        self.language = config.language
        self.source_scan_concurrency = max(1, config.details_concurrency)
        self.http = HTTPClient(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self.gate = gate

    @staticmethod
    def build_source(entry: Mapping[str, Any] | TMDBSourceConfig) -> TMDBSource:
        payload = (
            {"name": entry.name, "max_pages": entry.max_pages}
            if isinstance(entry, TMDBSourceConfig)
            else dict(entry)
        )
        return provider_tmdb_source.build_source(payload)

    async def fetch_source_page(self, source: TMDBSource, page: int) -> APIResponse:
        url = f"{self.base_url}{source.endpoint}"
        return await self.http.request_json(
            method="GET",
            url=url,
            params={
                "api_key": self.api_key,
                "language": self.language,
                "page": page,
            },
            gate=self.gate,
        )

    async def fetch_details(self, media_type: str, tmdb_id: int, *, single_attempt: bool = False) -> APIResponse:
        endpoint_media = "movie" if media_type == "movie" else "tv"
        url = f"{self.base_url}/{endpoint_media}/{tmdb_id}"
        return await self.http.request_json(
            method="GET",
            url=url,
            params={
                "api_key": self.api_key,
                "language": self.language,
                "append_to_response": "external_ids",
            },
            gate=self.gate,
            max_attempts=1 if single_attempt else None,
        )

    async def fetch_find_by_imdb(self, imdb_id: str) -> APIResponse:
        safe_imdb_id = str(imdb_id or "").strip()
        url = f"{self.base_url}/find/{safe_imdb_id}"
        return await self.http.request_json(
            method="GET",
            url=url,
            params={
                "api_key": self.api_key,
                "language": self.language,
                "external_source": "imdb_id",
            },
            gate=self.gate,
        )

    async def aclose(self) -> None:
        await self.http.aclose()

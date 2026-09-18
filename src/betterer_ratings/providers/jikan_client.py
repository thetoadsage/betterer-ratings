from __future__ import annotations

from typing import Any

from betterer_ratings.domain.models import APIResponse
from betterer_ratings.infra.http.client import HTTPClient


class JikanClient:
    """GET-only access to an explicitly configured Jikan instance."""

    def __init__(self, *, base_url: str, gate: Any) -> None:
        self.base_url = base_url.rstrip("/")
        self.gate = gate
        self.http = HTTPClient(timeout_seconds=8, max_retries=1)

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> APIResponse:
        response = await self.http.request_json(
            method="GET", url=f"{self.base_url}/{path}", params=params,
            gate=self.gate, max_pause_wait_seconds=0,
        )
        if self.gate is not None and (response.status == 0 or response.status >= 500):
            self.gate.pause_for(60, "Jikan provider unavailable")
        return response

    async def fetch_anime(self, mal_id: int) -> APIResponse:
        return await self._get(f"anime/{mal_id}")

    async def search(self, *, query: str, page: int, media_type: str) -> APIResponse:
        params: dict[str, Any] = {"q": query, "page": page, "limit": 25}
        if media_type == "movie":
            params["type"] = "movie"
        return await self._get("anime", params)

    async def aclose(self) -> None:
        await self.http.aclose()

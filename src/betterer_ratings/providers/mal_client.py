from __future__ import annotations

from typing import Any

from betterer_ratings.domain.models import APIResponse
from betterer_ratings.infra.http.client import HTTPClient

MAL_FIELDS = (
    "alternative_titles,start_date,end_date,mean,num_scoring_users,"
    "media_type,status,num_episodes"
)


def normalize_anime(data: Any) -> dict[str, Any] | None:
    """Translate official MAL fields into the enrichment metadata contract."""
    if not isinstance(data, dict) or type(data.get("id")) is not int:
        return None
    alternative = data.get("alternative_titles")
    if not isinstance(alternative, dict):
        alternative = {}
    synonyms = alternative.get("synonyms")
    return {
        "mal_id": data["id"],
        "title": data.get("title"),
        "title_english": alternative.get("en"),
        "title_japanese": alternative.get("ja"),
        "titles": [{"title": title} for title in synonyms if isinstance(title, str)]
        if isinstance(synonyms, list) else [],
        "type": {"movie": "Movie", "tv": "TV", "ona": "ONA"}.get(str(data.get("media_type", ""))),
        "status": {"finished_airing": "Finished Airing"}.get(str(data.get("status", ""))),
        "episodes": data.get("num_episodes"),
        "aired": {"from": data.get("start_date"), "to": data.get("end_date")},
        "score": data.get("mean"),
        "scored_by": data.get("num_scoring_users"),
    }


class MALClient:
    """Read public metadata through the official MAL API using a client ID."""

    def __init__(self, *, client_id: str, gate: Any) -> None:
        self.client_id = client_id
        self.gate = gate
        self.http = HTTPClient(timeout_seconds=15, max_retries=2)

    async def fetch_anime(self, mal_id: int) -> APIResponse:
        response = await self.http.request_json(
            method="GET",
            url=f"https://api.myanimelist.net/v2/anime/{mal_id}",
            headers={"X-MAL-CLIENT-ID": self.client_id},
            params={"fields": MAL_FIELDS},
            gate=self.gate,
            max_pause_wait_seconds=0,
        )
        if response.status in {0, 500, 502, 503, 504}:
            self.gate.pause_for(60, "MAL provider unavailable")
        elif response.status in {401, 403}:
            self.gate.pause_for(300, "MAL client ID rejected")
        if response.status == 200:
            return APIResponse(
                status=200, headers=response.headers,
                data={"data": normalize_anime(response.data)}, text="",
            )
        return response

    async def aclose(self) -> None:
        await self.http.aclose()

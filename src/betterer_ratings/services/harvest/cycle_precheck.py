from __future__ import annotations

from typing import Any, Callable, Optional


def maybe_skip_title_enrichment(
    *,
    harvester: Any,
    logger: Any,
    now_epoch_fn: Callable[[], int],
    to_iso_fn: Callable[[int], str],
    harvest_cycle_result_cls: Any,
) -> Optional[Any]:
    if getattr(harvester, "mal_client", None) is not None:
        return None
    now_ts = now_epoch_fn()
    mdblist_pause_until = harvester._mdblist_daily_quota_pause_until(now_ts)
    if mdblist_pause_until <= now_ts:
        return None

    logger.info(
        "[Harvester] Skipping title enrichment: MDBList quota pause active until %s.",
        to_iso_fn(mdblist_pause_until),
    )
    return harvest_cycle_result_cls(
        selected_candidates=0,
        tmdb_list_request_errors=0,
        mdblist_request_failures=0,
        interrupted=False,
    )

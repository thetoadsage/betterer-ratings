from __future__ import annotations

import asyncio
from typing import Any

from betterer_ratings.core.clock import format_duration


async def lease_recovery_loop(
    *,
    stop_event: asyncio.Event,
    db: Any,
    lease_recovery_interval: float,
    in_flight_lease_seconds: int,
    active_claims: set[str],
    logger: Any,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=lease_recovery_interval,
            )
            break
        except asyncio.TimeoutError:
            pass

        recovered = db.recover_stale_in_flight_rows(in_flight_lease_seconds, active_claims)
        if (
            recovered["ratings"] > 0
            or recovered["episode_ratings"] > 0
            or recovered["mappings"] > 0
        ):
            logger.warning(
                "[Submitter] Recovered stale in_flight rows (ratings=%s episode_ratings=%s mappings=%s, lease=%s).",
                recovered["ratings"],
                recovered["episode_ratings"],
                recovered["mappings"],
                format_duration(in_flight_lease_seconds),
            )

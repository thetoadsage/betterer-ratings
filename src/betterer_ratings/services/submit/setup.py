from __future__ import annotations

from typing import Any

from betterer_ratings.config.schema import AppConfig
from betterer_ratings.providers.pmdb_helpers import PMDB_TRANSIENT_STATUSES


def configure_submitter(
    *,
    submitter: Any,
    config: AppConfig,
) -> None:
    self = submitter
    runtime = config.runtime
    self.poll_seconds = max(0.01, float(runtime.submitter_poll_seconds))
    self.worker_count = max(1, int(runtime.submitter_workers))
    self.in_flight_lease_seconds = max(30, int(runtime.submitter_in_flight_lease_seconds))
    self.max_retry_attempts = max(1, int(runtime.submitter_max_retry_attempts))
    self.lease_recovery_interval = max(10.0, min(120.0, float(self.in_flight_lease_seconds) / 2.0))
    self._verify_after_transient_statuses = set(PMDB_TRANSIENT_STATUSES)

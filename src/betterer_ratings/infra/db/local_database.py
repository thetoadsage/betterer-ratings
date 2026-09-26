from __future__ import annotations

import sqlite3
from pathlib import Path

from betterer_ratings.infra.db import schema_repo as db_schema_repo
from betterer_ratings.infra.db.local_database_enrichment_mixin import LocalDatabaseEnrichmentMixin
from betterer_ratings.infra.db.local_database_queue_mixin import LocalDatabaseQueueMixin
from betterer_ratings.infra.db.local_database_state_mixin import LocalDatabaseStateMixin
from betterer_ratings.infra.db.provider_state import ProviderStateMixin


class LocalDatabase(
    ProviderStateMixin,
    LocalDatabaseStateMixin,
    LocalDatabaseEnrichmentMixin,
    LocalDatabaseQueueMixin,
):
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        db_schema_repo.init_schema(self.conn)

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        db_schema_repo.ensure_column(
            self.conn,
            table_name=table_name,
            column_name=column_name,
            column_type=column_type,
        )

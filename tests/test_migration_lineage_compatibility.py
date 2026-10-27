import sqlite3

import pytest

from betterer_ratings.infra.db import schema_repo


@pytest.mark.parametrize("lineage,version", [
    ("upstream", 2), ("upstream", 3),
    ("fork", 3), ("fork", 4), ("fork", 5),
])
def test_existing_upstream_and_fork_databases_converge_without_replaying_validation(lineage, version):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    schema_repo.ensure_migrations_table(conn)
    for number, name, migrate in schema_repo.MIGRATIONS[:2]:
        migrate(conn)
        conn.execute("INSERT INTO schema_migrations VALUES (?, ?, 100)", (number, name))
    # Neither old initial-schema implementation had this column before the
    # upstream provider-hardening release.
    if "pmdb_item_value" in {row["name"] for row in conn.execute("PRAGMA table_info(mappings)")}:
        conn.execute("ALTER TABLE mappings DROP COLUMN pmdb_item_value")
    if version >= 3:
        if lineage == "fork":
            schema_repo._migration_003_enrichment_state(conn)
            name = "enrichment_state"
        else:
            schema_repo._migration_003_mapping_item_value(conn)
            name = "mapping_item_value"
        conn.execute("INSERT INTO schema_migrations VALUES (3, ?, 101)", (name,))
    for number, name, migrate in schema_repo.MIGRATIONS:
        if 4 <= number <= version:
            migrate(conn)
            conn.execute("INSERT INTO schema_migrations VALUES (?, ?, 102)", (number, name))
    conn.execute("INSERT INTO state(key,value) VALUES ('cursor','12345')")
    conn.execute("""INSERT INTO mappings(tmdb_id,media_type,id_type,id_value,fetched_at,
                 pmdb_status,pmdb_item_id) VALUES (128,'movie','mal','164',100,'submitted','owned-mapping')""")
    conn.execute("""INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status,pmdb_item_id)
                 VALUES (128,'movie','TR',77,100,'pending','owned-rating')""")
    conn.execute("""INSERT INTO episode_ratings(tmdb_id,media_type,season,episode,label,score,
                 fetched_at,pmdb_status,pmdb_item_id) VALUES (10,'tv',1,1,'IM',80,100,'pending','owned-episode')""")
    if lineage == "fork":
        conn.execute("INSERT INTO enrichment_state VALUES (128,'movie','tmdb','{}',100,200)")
        conn.execute("INSERT INTO imdb_episode_identity VALUES ('tt0011','{}',0,200)")
    conn.commit()
    ledger_before = dict(conn.execute("SELECT version,name FROM schema_migrations"))
    mapping_before = dict(conn.execute("SELECT * FROM mappings").fetchone())

    assert schema_repo.apply_migrations(conn) == max(version for version, _, _ in schema_repo.MIGRATIONS)
    assert "pmdb_item_value" in {row["name"] for row in conn.execute("PRAGMA table_info(mappings)")}
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"enrichment_state", "imdb_episode_identity", "coverage_gaps", "anime_mapping_holds"} <= tables
    ledger_after = dict(conn.execute("SELECT version,name FROM schema_migrations"))
    assert all(ledger_after[number] == name for number, name in ledger_before.items())
    mapping_after = dict(conn.execute("SELECT * FROM mappings").fetchone())
    assert all(mapping_after[key] == value for key, value in mapping_before.items())
    assert conn.execute("SELECT value FROM state WHERE key='cursor'").fetchone()[0] == "12345"
    expected = "pending" if lineage == "fork" else "failed"
    for table, remote in [("ratings", "owned-rating"), ("episode_ratings", "owned-episode")]:
        row = conn.execute(f"SELECT pmdb_status,pmdb_item_id FROM {table}").fetchone()
        assert tuple(row) == (expected, remote)
    if lineage == "fork":
        assert conn.execute("SELECT payload FROM enrichment_state").fetchone()[0] == "{}"
        assert conn.execute("SELECT next_due FROM imdb_episode_identity").fetchone()[0] == 200
    before_repeat = list(conn.iterdump())
    schema_repo.apply_migrations(conn)
    assert list(conn.iterdump()) == before_repeat
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    conn.close()

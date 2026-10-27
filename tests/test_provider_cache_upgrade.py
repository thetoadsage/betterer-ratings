import pytest

from betterer_ratings.infra.db.schema_repo import apply_migrations


@pytest.mark.parametrize("lineage", ["upstream3", "fork3", "fork5"])
def test_cache_upgrade_preserves_both_migration_three_histories(local_db, lineage):
    conn = local_db.conn
    conn.execute("DELETE FROM schema_migrations WHERE version=6")
    if lineage == "upstream3":
        conn.execute("DROP TABLE enrichment_state")
    else:
        conn.execute("UPDATE schema_migrations SET name='enrichment_state' WHERE version=3")
        conn.execute("ALTER TABLE mappings DROP COLUMN pmdb_item_value")
        local_db.save_enrichment_state(10, "movie", "tmdb", payload={"title": "Cached"}, now_ts=100, next_due=200)
    if lineage == "fork5":
        conn.execute("INSERT OR REPLACE INTO schema_migrations VALUES (5,'anime_mapping_holds',100)")
    else:
        conn.execute("DELETE FROM schema_migrations WHERE version=4")
        conn.execute("DROP TABLE coverage_gaps")
    conn.execute("""INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status,pmdb_item_id)
                 VALUES (10,'movie','IM',80,100,'submitted','owned')""")
    conn.execute("""INSERT INTO episode_ratings(tmdb_id,media_type,season,episode,label,score,fetched_at,pmdb_status)
                 VALUES (10,'tv',1,1,'IM',80,100,'pending')""")
    conn.commit()
    ledger = dict(conn.execute("SELECT version,name FROM schema_migrations"))
    ratings = list(conn.execute("SELECT * FROM ratings"))
    episodes = list(conn.execute("SELECT * FROM episode_ratings"))
    apply_migrations(conn)
    assert "pmdb_item_value" in {row["name"] for row in conn.execute("PRAGMA table_info(mappings)")}
    assert all(dict(conn.execute("SELECT version,name FROM schema_migrations"))[version] == name
               for version, name in ledger.items())
    assert list(conn.execute("SELECT * FROM ratings")) == ratings
    assert list(conn.execute("SELECT * FROM episode_ratings")) == episodes
    if lineage != "upstream3":
        assert local_db.get_enrichment_state(10, "movie", "tmdb")["payload"] == {"title": "Cached"}
    assert conn.execute("SELECT COUNT(*) FROM coverage_gaps").fetchone()[0] == 0
    before_repeat = list(conn.iterdump())
    apply_migrations(conn)
    assert list(conn.iterdump()) == before_repeat
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"

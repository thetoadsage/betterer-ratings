import pytest

from betterer_ratings.core.scoring import parse_mdblist_ratings
from betterer_ratings.infra.db.schema_repo import apply_migrations


@pytest.mark.parametrize("already_validated_fork", [False, True])
def test_source_upgrade_preserves_history_and_does_not_rehold_validated_fork_work(local_db, already_validated_fork):
    conn = local_db.conn
    conn.execute("DELETE FROM schema_migrations WHERE version=8")
    if already_validated_fork:
        conn.execute("UPDATE schema_migrations SET name='enrichment_state' WHERE version=3")
    statuses = ["pending", "retry", "in_flight", "submitted"]
    for tmdb_id, status in enumerate(statuses, start=1):
        conn.execute("""INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status,pmdb_item_id)
                     VALUES (?,'movie','TR',75,100,?,'owned')""", (tmdb_id, status))
    conn.commit()
    apply_migrations(conn)
    expected = statuses if already_validated_fork else ["failed"] * 3 + ["submitted"]
    assert [tuple(row) for row in conn.execute("SELECT pmdb_status,pmdb_item_id FROM ratings ORDER BY tmdb_id")] == [
        (status, "owned") for status in expected
    ]
    before_repeat = list(conn.iterdump())
    apply_migrations(conn)
    assert list(conn.iterdump()) == before_repeat


def test_legacy_trakt_work_resumes_only_when_a_real_source_returns(local_db):
    conn = local_db.conn
    conn.execute("DELETE FROM schema_migrations WHERE version=8")
    conn.execute("""INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status)
                 VALUES (10,'movie','TR',75,100,'pending')""")
    conn.commit()
    apply_migrations(conn)

    def save(payload):
        local_db.save_enriched_item(
            tmdb_id=10, media_type="movie", title="Title", imdb_id=None,
            popularity=1, tmdb_vote_average=None, enrichment_error=None,
            ratings=parse_mdblist_ratings(payload), mappings={}, now_ts=200,
        )

    save({"score": 75})
    assert conn.execute("SELECT pmdb_status FROM ratings WHERE label='TR'").fetchone()[0] == "failed"
    save({"ratings": [{"source": "trakt", "score": 75}]})
    assert conn.execute("SELECT pmdb_status FROM ratings WHERE label='TR'").fetchone()[0] == "pending"

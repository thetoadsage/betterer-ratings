"""Offline candidate coverage preview; no downloads, provider calls, or queue writes.

Run with PYTHONPATH=src python scripts/preview_fribb_mappings.py --help.
Candidates still require MAL identity validation and remote PMDB duplicate checks.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path

from betterer_ratings.core.mappings import extract_mappings
from betterer_ratings.services.harvest.fribb_mappings import load_fribb_index


def preview(database: Path, dataset: Path, *, limit: int = 1000, examples: int = 20) -> dict:
    if limit <= 0 or examples < 0:
        raise ValueError("limit must be positive and examples must be nonnegative")
    index = load_fribb_index(dataset)
    conn = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    counts = Counter()
    samples = []
    scanned = matched = missing_details = 0
    try:
        conn.execute("PRAGMA query_only=ON")
        rows = conn.execute("""
            SELECT t.tmdb_id, t.media_type, t.title, tm.payload AS details, md.payload AS md_item
            FROM titles t
            LEFT JOIN enrichment_state tm ON tm.tmdb_id=t.tmdb_id
                AND tm.media_type=t.media_type AND tm.provider='tmdb'
            LEFT JOIN enrichment_state md ON md.tmdb_id=t.tmdb_id
                AND md.media_type=t.media_type AND md.provider='mdblist'
            ORDER BY t.media_type, t.tmdb_id LIMIT ?
        """, (limit,))
        for row in rows:
            scanned += 1
            details = json.loads(row["details"]) if row["details"] else None
            if not isinstance(details, dict):
                missing_details += 1
                continue
            if row["media_type"] == "tv" and (
                details.get("status") != "Ended" or details.get("number_of_seasons") != 1
            ):
                continue
            md_item = json.loads(row["md_item"]) if row["md_item"] else None
            known = extract_mappings(row["media_type"], details, md_item)
            stored = dict(conn.execute(
                "SELECT id_type, id_value FROM mappings WHERE tmdb_id=? AND media_type=?",
                (row["tmdb_id"], row["media_type"]),
            ).fetchall())
            if any(provider in known and known[provider] != value for provider, value in stored.items()):
                continue
            known.update(stored)
            mappings = index.resolve(
                media_type=row["media_type"], tmdb_id=row["tmdb_id"], mappings=known,
            )
            if not mappings:
                continue
            matched += 1
            missing = {k: v for k, v in mappings.items() if k not in stored}
            counts.update(missing.keys())
            if missing and len(samples) < examples:
                samples.append({"tmdb_id": row["tmdb_id"], "media_type": row["media_type"],
                                "title": row["title"], "candidate_mappings": missing})
    finally:
        conn.close()
    return {"scanned_titles": scanned, "titles_without_cached_details": missing_details,
            "matched_candidates": matched, "potential_new_local_mappings": dict(counts),
            "requires_mal_validation": True, "pmdb_coverage_checked": False,
            "examples": samples}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True, help="Previously saved Fribb JSON")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum titles to inspect")
    parser.add_argument("--examples", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps(preview(args.database, args.dataset, limit=args.limit, examples=args.examples), indent=2))

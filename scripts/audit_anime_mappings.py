"""Audit cached anime identities and local queue state without network or writes.

Run with PYTHONPATH=src python scripts/audit_anime_mappings.py --database PATH.
Use a consistent SQLite backup when auditing a running service.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any


def audit(database: Path, *, examples: int = 30, now_ts: int | None = None) -> dict[str, Any]:
    if examples < 0:
        raise ValueError("examples must be nonnegative")
    now = int(time.time()) if now_ts is None else now_ts
    conn = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    outcomes: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    findings: Counter[str] = Counter()
    policies: Counter[str] = Counter()
    holds: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    total = 0
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")  # One consistent view, including per-title mapping reads.
        has_holds = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='anime_mapping_holds'").fetchone() is not None
        rows = conn.execute("""
            WITH identities AS (
                SELECT tmdb_id, media_type FROM mappings WHERE id_type IN ('mal', 'anilist', 'anidb')
                UNION
                SELECT tmdb_id, media_type FROM enrichment_state WHERE provider='mal'
            )
            SELECT i.tmdb_id, i.media_type, t.title, s.payload, s.next_due, s.fetched_at
            FROM identities i
            LEFT JOIN titles t USING (tmdb_id, media_type)
            LEFT JOIN enrichment_state s ON s.tmdb_id=i.tmdb_id
                AND s.media_type=i.media_type AND s.provider='mal'
            ORDER BY i.media_type, i.tmdb_id
        """)
        for row in rows:
            total += 1
            mappings = [dict(m) for m in conn.execute("""
                SELECT id_type, id_value, pmdb_status FROM mappings
                WHERE tmdb_id=? AND media_type=? AND id_type IN ('mal', 'anilist', 'anidb')
                ORDER BY id_type
            """, (row['tmdb_id'], row['media_type']))]
            for mapping in mappings:
                statuses[f"{mapping['id_type']}:{mapping['pmdb_status']}"] += 1
            issues = []
            try:
                payload = json.loads(row['payload']) if row['payload'] else {}
                result = payload.get('result', {})
                if not isinstance(result, dict):
                    raise ValueError("invalid cached result")
            except (ValueError, AttributeError, TypeError):
                payload = {}
                result = {}
                issues.append('invalid_cache')
            policies[str(payload.get('policy', 'legacy_or_unknown')) if isinstance(payload, dict) else 'invalid'] += 1
            hold_rows = [dict(h) for h in conn.execute(
                "SELECT id_type, reason FROM anime_mapping_holds WHERE tmdb_id=? AND media_type=?",
                (row['tmdb_id'], row['media_type']),
            )] if has_holds else []
            holds.update(h['reason'] for h in hold_rows)
            if hold_rows:
                issues.append('submission_hold')
            outcome = str(result.get('outcome', 'not_evaluated'))
            outcomes[outcome] += 1
            if not result:
                issues.append('missing_validation_record')
            if row['next_due'] is not None and row['next_due'] <= now:
                issues.append('validation_due')
            if result and result.get('validated') is not True and outcome != 'not_attempted':
                issues.append(outcome)
            for mapping in mappings:
                if mapping['id_type'] != 'mal':
                    issues.append(f"{mapping['id_type']}_requires_provider_validation")
                elif result.get('validated') is not True or str(result.get('mal_id')) != mapping['id_value']:
                    issues.append('stored_mal_not_confirmed_by_cache')
            findings.update(set(issues))
            if issues and len(samples) < examples:
                samples.append({
                    'tmdb_id': row['tmdb_id'], 'media_type': row['media_type'], 'title': row['title'],
                    'issues': sorted(set(issues)), 'outcome': outcome, 'mappings': mappings,
                    'candidates': result.get('candidates', []),
                    'conflicts': result.get('conflicts', []),
                    'mapping_decisions': result.get('mapping_decisions', []),
                    'submission_holds': hold_rows,
                    'proposed_mal_id': result.get('proposed_mal_id'),
                    'validation_fetched_at': row['fetched_at'],
                })
    finally:
        conn.close()
    return {
        'audited_titles': total, 'cached_outcomes': dict(sorted(outcomes.items())),
        'local_mapping_statuses': dict(sorted(statuses.items())),
        'cached_policies': dict(sorted(policies.items())),
        'submission_holds': dict(sorted(holds.items())),
        'holds_schema_available': has_holds,
        'findings': dict(sorted(findings.items())), 'examples': samples,
        'example_limit': examples, 'pmdb_coverage_checked': False,
        'note': 'Cached evidence only; findings require review, not automatic deletion or replacement.',
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--examples', type=int, default=30)
    args = parser.parse_args()
    print(json.dumps(audit(args.database, examples=args.examples), indent=2))

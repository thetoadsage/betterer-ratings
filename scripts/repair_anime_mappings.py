"""Standalone Python 3.11+ MAL mapping cleanup. No application imports or migrations.

Preview is GET/read-only. --apply requires the worker stopped, backs up SQLite,
deletes only matching contributor entries, creates missing correct mappings, then
updates local records. No ratings are changed. Re-run after interruptions to resume.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

REVIEWED = {'replacement_identity_verified', 'replacement_identity_manually_verified'}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward a bearer token to another URL.


class API:
    def __init__(self, base, token, interval=1.5, journal=None):
        parsed = urllib.parse.urlsplit(base)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('PMDB base URL must use HTTPS without credentials/query/fragment')
        if not isinstance(token, str) or not token.strip():
            raise ValueError('PMDB API key is missing')
        self.base, self.token, self.interval = base.rstrip('/'), token, max(1.5, min(30, interval))
        self.journal, self.last = journal, 0.0
        self.opener = urllib.request.build_opener(NoRedirect())

    def record(self, event):
        if self.journal:
            self.journal(json.loads(json.dumps(event).replace(self.token, '[REDACTED]')))

    def request(self, method, path, params=None, body=None):
        time.sleep(max(0, self.interval - (time.monotonic() - self.last)))
        url = self.base + path + ('?' + urllib.parse.urlencode(params) if params else '')
        req = urllib.request.Request(url, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer ' + self.token, 'Accept': 'application/json',
                     'Content-Type': 'application/json',
                     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'})
        self.last = time.monotonic()
        self.record({'phase': 'request_started', 'method': method, 'path': path, 'params': params, 'body': body})
        try:
            with self.opener.open(req, timeout=30) as response:
                raw, status = response.read(), response.status
        except urllib.error.HTTPError as exc:
            raw, status = exc.read(), exc.code
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {'error': 'Non-JSON response'}
        self.record({'phase': 'response', 'method': method, 'path': path, 'status': status, 'payload': payload})
        # This exact live API response means the reverse lookup has no owner;
        # other 404s (wrong route, hidden title, etc.) remain failures.
        if method == 'GET' and path == '/api/external/mappings/lookup' and status == 404 and payload == {'error': 'No matching TMDB ID found'}:
            return {'results': [], 'total': 0}
        if not 200 <= status < 300:
            raise RuntimeError(f'PMDB {method} {path} returned HTTP {status}; stopped without automatic retries')
        if not isinstance(payload, dict) or payload.get('success') is False:
            raise RuntimeError('PMDB returned an unsuccessful or invalid response')
        return payload


def load_plan(path):
    plan = json.loads(path.read_text())
    if plan.get('version') != 1 or not isinstance(plan.get('corrections'), list) or not plan['corrections']:
        raise ValueError('Expected a nonempty reviewed version-1 plan')
    keys = set()
    for r in plan['corrections']:
        key = (r['media_type'], r['tmdb_id'])
        if key in keys or r['media_type'] not in ('movie', 'tv') or type(r['tmdb_id']) is not int or r['tmdb_id'] <= 0:
            raise ValueError('Invalid or duplicate TMDB identity')
        keys.add(key)
        for field in ('old_mal_id', 'new_mal_id'):
            value = r[field]
            if not isinstance(value, str) or not value.isascii() or not value.isdigit() or int(value) <= 0 or str(int(value)) != value:
                raise ValueError('Expected canonical positive MAL IDs')
        if r['old_mal_id'] == r['new_mal_id'] or r['review_decision'] not in REVIEWED or not r.get('review_note'):
            raise ValueError('Only reviewed, distinct corrections are accepted')
        checked = datetime.fromisoformat(r['checked_at'])
        if checked.tzinfo is None or not -300 <= time.time() - checked.timestamp() <= 7*86400:
            raise ValueError('Review evidence must be refreshed: timestamp invalid or older than seven days')
    return plan['corrections'], hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()


def database(path, write=False):
    conn = sqlite3.connect(path.resolve().as_uri() + ('?mode=rw' if write else '?mode=ro'), uri=True)
    conn.row_factory = sqlite3.Row
    if not write:
        conn.execute('PRAGMA query_only=ON')
    return conn


def mappings(api, row):
    payload = api.request('GET', '/api/external/mappings', {'tmdb_id': row['tmdb_id'], 'media_type': row['media_type'], 'id_type': 'mal'})
    if payload.get('tmdb_id') != row['tmdb_id'] or payload.get('media_type') != row['media_type']:
        raise ValueError('PMDB title lookup returned a different title')
    values = payload.get('mappings', {}).get('mal', [])
    if not isinstance(values, list) or any(not isinstance(v, dict) or not v.get('id') or v.get('value') is None or not isinstance(v.get('contributor'), str) for v in values):
        raise ValueError('PMDB mappings response lacks exact IDs/values/contributors')
    return values


def owners(api, value, media_type):
    payload = api.request('GET', '/api/external/mappings/lookup', {'id_type': 'mal', 'id_value': value, 'media_type': media_type})
    rows = payload.get('results')
    if not isinstance(rows, list) or type(payload.get('total')) is not int or payload['total'] != len(rows):
        raise ValueError('PMDB reverse lookup is invalid or incomplete')
    result = set()
    for r in rows:
        if not isinstance(r, dict) or type(r.get('tmdb_id')) is not int or r['tmdb_id'] <= 0 or r.get('media_type') not in ('movie', 'tv'):
            raise ValueError('PMDB reverse lookup has invalid title scope')
        if r['media_type'] == media_type:
            result.add((r['media_type'], r['tmdb_id']))
    return result


def preview(conn, api, plan, contributor):
    ready, blocked = {}, {}
    owner_cache = {}
    for r in plan:
        key = (r['media_type'], r['tmdb_id'])
        row = {k: r[k] for k in ('title', 'media_type', 'tmdb_id', 'old_mal_id', 'new_mal_id')}
        reason = None
        local = conn.execute("SELECT * FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type='mal'", (key[1], key[0])).fetchone()
        if not local or local['id_value'] not in (r['old_mal_id'], r['new_mal_id']):
            reason = 'Local value differs from both reviewed IDs'
        elif local['pmdb_status'] == 'in_flight':
            reason = 'Local mapping is still in flight'
        state = conn.execute("SELECT payload FROM enrichment_state WHERE tmdb_id=? AND media_type=? AND provider='tmdb'", (key[1], key[0])).fetchone()
        details = json.loads(state['payload']) if state and state['payload'] else None
        if not isinstance(details, dict) or any(details.get(k) != v for k, v in r['tmdb_details'].items()):
            reason = 'TMDB metadata changed or is missing; re-review first'
        if reason:
            blocked[key] = {**row, 'reason': reason}
            continue
        entries = mappings(api, row)
        old = [e for e in entries if str(e['value']) == r['old_mal_id']]
        correct = [e for e in entries if str(e['value']) == r['new_mal_id']]
        for value in (r['old_mal_id'], r['new_mal_id']):
            ck = (key[0], value)
            if ck not in owner_cache:
                owner_cache[ck] = owners(api, value, key[0])
        old_owners = owner_cache[(key[0], r['old_mal_id'])]
        new_owners = owner_cache[(key[0], r['new_mal_id'])]
        if bool(old) != (key in old_owners) or bool(correct) != (key in new_owners):
            reason = 'PMDB forward/reverse lookups disagree; no deletion is safe'
        elif any(e['contributor'] != contributor for e in old):
            reason = 'Wrong remote mapping includes another contributor; leave it untouched'
        row.update({'delete': old, 'already_correct_on_pmdb': bool(correct), 'create': not correct,
                    'new_remote_owners': sorted(new_owners), 'local_before': dict(local)})
        if reason:
            blocked[key] = {**row, 'reason': reason}
        else:
            ready[key] = row
    # Remove conflicts to a fixed point, allowing reviewed swaps as one batch.
    duplicate_targets = { (r['media_type'], r['new_mal_id']) for r in ready.values()
        if sum(x['media_type']==r['media_type'] and x['new_mal_id']==r['new_mal_id'] for x in ready.values()) > 1 }
    changed = True
    while changed:
        changed = False
        for key, row in list(ready.items()):
            local_owners = conn.execute("SELECT tmdb_id,media_type FROM mappings WHERE id_type='mal' AND id_value=? AND media_type=? AND tmdb_id!=?", (row['new_mal_id'], key[0], key[1])).fetchall()
            external_local = [dict(o) for o in local_owners if (o['media_type'], o['tmdb_id']) not in ready or ready[(o['media_type'], o['tmdb_id'])]['new_mal_id'] == row['new_mal_id']]
            external_remote = []
            if row['create']:
                for owner in row['new_remote_owners']:
                    owner = tuple(owner)
                    if owner != key and (owner not in ready or not ready[owner]['delete'] or ready[owner]['old_mal_id'] != row['new_mal_id']):
                        external_remote.append(owner)
            if external_local or external_remote or (key[0], row['new_mal_id']) in duplicate_targets:
                blocked[key] = {**row, 'reason': 'Unresolved local/remote owner or duplicate target outside this batch', 'local_owners': external_local, 'remote_owners': external_remote}
                del ready[key]
                changed = True
    return {'ready': list(ready.values()), 'blocked': list(blocked.values()),
            'contributor_filter': contributor,
            'delete_count': sum(len(r['delete']) for r in ready.values()),
            'create_count': sum(r['create'] for r in ready.values()), 'applied': False}


def apply(database_path, api, report, output, *, service_stopped=False, only_ready=False):
    if not service_stopped:
        raise ValueError('Stop Betterer Ratings first and pass --service-stopped')
    if report['blocked'] and not only_ready:
        raise ValueError('Preview contains blocked entries; resolve them or explicitly use --only-ready')
    rows = report['ready']
    if not rows:
        return {**report, 'applied': True, 'updated_local': 0}
    backup = output / ('before-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.sqlite3')
    with closing(database(database_path)) as source, sqlite3.connect(backup) as target:
        source.backup(target)
        if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('Backup integrity check failed')
    # Recheck local values before any remote mutation. A stopped service is required.
    with closing(database(database_path)) as conn:
        for r in rows:
            current = conn.execute("SELECT * FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type='mal'", (r['tmdb_id'], r['media_type'])).fetchone()
            if not current or dict(current) != r['local_before']:
                raise ValueError('Local state changed since preview; no remote writes performed')
    # Verify ALL deletions before the first destructive call. Never use cached IDs.
    for r in rows:
        live = mappings(api, r)
        for old in r['delete']:
            if not any(e['id']==old['id'] and str(e['value'])==r['old_mal_id'] and e['contributor']==report['contributor_filter'] for e in live):
                raise ValueError('Remote deletion target changed; re-preview')
    for r in rows:
        for old in r['delete']:
            api.request('DELETE', '/api/external/mappings/' + urllib.parse.quote(old['id'], safe=''))
    created = {}
    for r in rows:
        # Verify after deletion; correct entries may already belong to another user.
        remote = owners(api, r['new_mal_id'], r['media_type'])
        key = (r['media_type'], r['tmdb_id'])
        if key not in remote:
            if remote:
                raise ValueError('Replacement still belongs to another title; stopped; inspect journal and re-preview')
            result = api.request('POST', '/api/external/mappings', body={'tmdb_id': r['tmdb_id'], 'media_type': r['media_type'], 'id_type': 'mal', 'id_value': r['new_mal_id']})
            created[key] = result.get('item', {}).get('id')
        if key not in owners(api, r['new_mal_id'], r['media_type']):
            raise ValueError('Correct mapping not confirmed; local database unchanged; inspect journal')
        if any(str(e['value'])==r['old_mal_id'] and e['contributor']==report['contributor_filter'] for e in mappings(api, r)):
            raise ValueError('Old contribution still visible; stopped before local update; re-preview after cache expiry')
    with closing(database(database_path, write=True)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        try:
            for r in rows:
                current = conn.execute("SELECT * FROM mappings WHERE tmdb_id=? AND media_type=? AND id_type='mal'", (r['tmdb_id'], r['media_type'])).fetchone()
                if not current or dict(current)!=r['local_before']:
                    raise ValueError('Local state changed during repair; remote changes are journaled')
                conn.execute("""UPDATE mappings SET id_value=?,pmdb_item_id=?,pmdb_status='submitted',
                    pmdb_claimed_at=NULL,pmdb_submitted_at=?,pmdb_attempts=0,pmdb_retry_after=0,pmdb_last_error=NULL
                    WHERE tmdb_id=? AND media_type=? AND id_type='mal'""",
                    (r['new_mal_id'], created.get((r['media_type'],r['tmdb_id'])), int(time.time()),r['tmdb_id'],r['media_type']))
                conn.execute("UPDATE enrichment_state SET payload=NULL,next_due=0 WHERE tmdb_id=? AND media_type=? AND provider='mal'", (r['tmdb_id'],r['media_type']))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    return {**report, 'applied': True, 'updated_local': len(rows), 'backup': str(backup)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database', type=Path, required=True)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True, help='Existing config.toml; API key is never printed')
    p.add_argument('--contributor', required=True, help='Exact PMDB username whose wrong entries may be deleted')
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    p.add_argument('--only-ready', action='store_true')
    p.add_argument('--service-stopped', action='store_true')
    args = p.parse_args()
    try:
        plan, digest = load_plan(args.plan)
        config = tomllib.loads(args.config.read_text())
        if args.apply and not args.service_stopped:
            raise ValueError('--apply requires Betterer Ratings stopped and --service-stopped')
        args.output_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        identity = {'plan_hash': digest, 'contributor': args.contributor, 'database': str(args.database.resolve())}
        stamp = args.output_dir/'run.json'
        if stamp.exists() and json.loads(stamp.read_text()) != identity:
            raise ValueError('Output directory belongs to a different repair plan/account/database')
        stamp.write_text(json.dumps(identity, indent=2)+'\n')
        def journal(event):
            with (args.output_dir/'journal.jsonl').open('a') as out:
                out.write(json.dumps({'time': datetime.now(timezone.utc).isoformat(), **event})+'\n')
                out.flush()
                os.fsync(out.fileno())
        api = API(config.get('pmdb',{}).get('base_url','https://publicmetadb.com'),config['api_keys']['pmdb'],journal=journal)
        with closing(database(args.database)) as conn:
            conn.execute('BEGIN')
            report = preview(conn, api, plan, args.contributor)
        (args.output_dir/'preview.json').write_text(json.dumps(report, indent=2)+'\n')
        if args.apply:
            report = apply(args.database,api,report,args.output_dir,service_stopped=args.service_stopped,only_ready=args.only_ready)
            (args.output_dir/'result.json').write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps({'ready':len(report['ready']), 'blocked':len(report['blocked']),
            'planned_deletes':report['delete_count'],'planned_creates':report['create_count'],
            'applied':report['applied'],'details':str(args.output_dir)}, indent=2))
    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error,RuntimeError) as exc:
        p.exit(1, f'Repair stopped: {exc}\nSee the journal; no automatic retry or rollback of remote writes.\n')


if __name__ == '__main__':
    main()

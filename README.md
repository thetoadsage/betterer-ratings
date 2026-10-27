# betterer-ratings

Docker-first worker for keeping a local ratings database in sync with configured
catalog, ratings, archive, and destination services.

The project is intended for personal automation. Bring your own service
credentials, keep request rates conservative, and make sure your usage matches
the terms of the services you configure.

## What It Does

The worker runs continuously and coordinates three jobs:

- discover and refresh title metadata from configured catalog sources
- normalize ratings and external identifiers into one local SQLite database
- submit due rating, mapping, and episode-rating work to the configured destination

There are no one-off discovery modes. Source scans, archive ingestion, episode
rating ingestion, stale-title refreshes, and submission retries all run inside
one long-lived process.

## Quick Start

```bash
cp config.example.toml config.toml
```

Edit `config.toml` and replace the placeholder values in `[api_keys]` with your
own credentials. Review the scan intervals, source lists, rate limits, and batch
size before starting the worker.

```bash
docker compose up -d --build
```

The dashboard and API are exposed on port `8087` by default:

```text
http://localhost:8087
```

## Runtime Model

The service has one mode: start, run forever, and stop gracefully on
`SIGTERM` or `SIGINT`.

At startup it validates configuration, opens the SQLite database, recovers
expired in-flight queue rows, starts the dashboard API, and runs the harvester
and submitter until the container stops.

The harvester loop:

- processes episode rating archives first
- selects local titles with due provider work, in batches of up to 1,000
- runs configured catalog source scans on the configured interval
- ingests archive-backed title candidates during source scans
- enriches candidates and queues destination writes

TMDB, MDBList, and optional MAL enrichment keep separate persistent refresh
state. An MDBList quota pause defers MDBList requests until its reset while
discovery, TMDB, IMDb archive ratings, and eligible MAL enrichment continue.
Deferred MDBList requests do not advance its last-fetch timestamp. Successful
title requests follow `worker.title_refresh_days`; missing MDBList items and
unusable MAL results follow `worker.failed_retry_days` (minimum one minute).
Failed TMDB requests retry after an hour. Other MDBList request failures retry
after an hour, or five minutes after a halted batch when no quota reset is known.

TMDB details are cached in SQLite for the title refresh interval, including the
metadata needed to validate MAL matches. Having an IMDb mapping does not bypass
metadata refresh. Expired metadata is retained for inspection but is not reused
for enrichment after a failed refresh.

IMDb title archive scores feed the `IM` queue directly for both new and existing
mapped titles. The archive snapshot date, IMDb ID, and vote count are retained
locally. Valid snapshots less than two days old take precedence over MDBList's
IMDb score; stale snapshots do not enqueue archive title ratings.
MDBList's combined score is never submitted as a Trakt rating.

IMDb episode ratings require an unambiguous TMDB lookup of the **episode IMDb
ID**, matching the parent show. Submissions use TMDB's returned season/episode
coordinates. Unmapped episodes, specials, ambiguous results, and show mismatches
are skipped; transient lookup failures retain the archive cursor for retry.
Episode processing is limited to 1,000 archive rows per cycle. Identity matches
are cached for seven days, misses for one day, and transient failures for five
minutes. Existing published records are not automatically removed when numbering
changes.

On first startup after this update, an additive SQLite migration creates the
provider/episode caches. Existing titles gradually populate the metadata cache.
Unsent IMDb episode ratings and Trakt ratings from the old pipeline are held in
`failed` status with a verification message until validated ingestion requeues
them. This includes old in-flight rows recovered across an upgrade. Submitted
records and their remote IDs are preserved; historical combined scores already
published under Trakt require a separate audit.

The submitter loop claims the oldest due mapping, title-rating, or
episode-rating work across all queues and retries failed work after the
configured delay.

## Storage

The compose file mounts local runtime state into the container:

- `./config.toml` -> `/config/config.toml`
- `./data/...` -> container data directories

The main database inside the container is:

```text
/data/db/betterer_ratings.sqlite3
```

Archive files, indexes, and temporary state are stored under the local `data/`
directory. Local runtime data is ignored by Git.

## Configuration

Use `config.example.toml` as the schema reference. Public configuration covers:

- API credentials
- log level
- source scan interval
- title and episode refresh windows
- source lists
- optional TMDB daily ID export backlog and detail request budgets
- archive filters
- provider rate limits
- ratings batch size

Runtime internals such as container database paths, archive paths, submitter
worker count, retry counts, and provider timeouts are intentionally fixed in
the application.

Set `tmdb.daily_exports.enabled = true` to scan TMDB's movie and TV ID exports
after the configured list sources. The exports provide IDs and filtering fields;
each new title still needs a TMDB detail request. `max_new_titles_per_scan`
limits the new candidates selected each source scan, and
`daily_detail_budget` caps those selections per UTC day. The worker caches
decompressed exports under `/data/temp/tmdb_exports`, pins each snapshot until
its cursor reaches the end, and commits cursor progress after enrichment finishes.
Existing configurations without this table keep the export backlog disabled.

Default behavior:

- title/movie/series ratings refresh after 7 days
- episode ratings refresh after 1 day
- archive refresh runs daily at 13:00 UTC
- ratings batch size is 100
- submitter worker count is 16

## Local Development

```bash
python3 -m pip install -e ".[dev]"
betterer-ratings --config config.toml
```

The CLI intentionally has no subcommands. It is the same worker entry point used
by Docker.

## Logs

Logs are structured JSON on stdout. Docker or your host logging stack should
handle collection, retention, and rotation. The included Compose configuration
rotates five files of up to 10 MB each per container. Dashboard logs are an
in-memory view: the latest 500 entries plus up to 500 retained warnings/errors,
deduplicated and ordered by time. They reset when the process restarts; the
dashboard Clear button only hides entries in the current browser view.

Each JSON entry includes `timestamp` (Unix seconds), `level`, `logger`, and
`message`. Recognized component labels also add `component`, `provider` where
applicable, and a template-based fallback `event`; explicit event names take
precedence. Common credential fields, authorization tokens, and URL credentials
are masked in messages, nested context, and tracebacks. Avoid logging raw
credentials or full request payloads; redaction recognizes common patterns,
not every possible secret format.

Useful events for troubleshooting:

- `http.request.completed`: final status, outcome, attempts, elapsed milliseconds
  (including gate waits and backoff), and whether a retry recovered. HTTP attempt
  logs share a `request_id` for that call. Routine successes, expected 404s,
  and gate deferrals stay at DEBUG; recovered requests appear at INFO and
  exhausted retries at WARNING.
- `mapping.retry_scheduled`, `rating.retry_scheduled`, `mapping.failed`, and
  `rating.failed`: title/provider identity, status/error code, attempt budget,
  error detail, and next retry time when scheduled.
- `episode_ratings.unresolved`: title, season, affected episodes, status and
  retryability. `mal.enrichment` includes candidate provenance, conflicts,
  rejection outcomes, and any review-only correction proposal. Routine missing
  mappings and scope skips stay at DEBUG; conflicts and correction proposals
  appear at WARNING so the dashboard retains them.
- `harvest.cycle.completed` / `harvest.cycle.failed`: cycle duration, selected
  candidates and discovery/enrichment request error counts, or a traceback.
  Empty successful cycles stay at DEBUG. Queue alerts and submission summaries
  remain available as `queue.alert.*` and `submission.summary`.

Set `[worker] log_level = "DEBUG"` to investigate individual HTTP calls; INFO is
the normal operating level. The same level applies to stdout and the dashboard.
For example, to inspect mapping failures from the last hour:

```bash
docker logs --since 1h betterer-ratings 2>&1 | jq -R 'fromjson? | select(.event == "mapping.failed")'
```

## Optional Dins ID mappings

Set `enabled = true` under `[dins]` to use
[dins-mappings](https://github.com/0xConstant1/dins-mappings) as a trusted supplemental
IMDb/TMDB/TVDB source. No additional credentials are required. Existing configurations
without this section keep it disabled.

The worker downloads both movie and TV exports daily and builds an indexed SQLite
cache at `/data/temp/dins-mappings/index.sqlite3`. Downloads and parsing must succeed
for both exports before the cache is replaced. Failed refreshes keep the last good
cache and retry after an hour; without a usable cache, normal lookups continue.

IMDb archive title and parent-show lookups use unambiguous cached mappings to avoid
TMDB `/find` requests. Missing or conflicting matches fall back to the existing
lookup flow. Title details still refresh normally; episode identities still require
the existing exact TMDB episode lookup.

During title enrichment, Dins fills missing IMDb and TVDB mappings through the normal
submission queue. It never replaces a different source/stored ID, and skips Dins
additions when an ID belongs to another local title. Movie and TV namespaces remain
separate, and duplicate ownership within an export is rejected. TVmaze IDs are not
submitted. Enabling this uses normal discovery/refresh scheduling, without a bulk
import or historical correction pass.

## Direct MyAnimeList ratings

Set `enabled = true` and `client_id = "your-client-id"` under `[mal]` to enrich
existing titles through the official MyAnimeList API. Register an application at
https://myanimelist.net/apiconfig to obtain the Client ID. Public rating reads do
not use a client secret or OAuth redirect. Keep your actual configuration out of
Git. Direct fetching is disabled in the example and when the section is omitted.

The worker uses MAL IDs supplied by MDBList, falling back to stored MAL mappings.
When neither exists but MDBList has an AniList or AniDB ID, it uses a local cache
of [anime-offline-database](https://github.com/cedya77/anime-offline-database)
to resolve a MAL ID. The cache is refreshed weekly from its release JSONL and
keeps the last successful copy if a refresh fails. This ID bridge never searches by title,
does not bridge TMDB/IMDb directly, and skips lookup conflicts. Direct results
must match a TMDB name (ignoring case and punctuation) and media format. Movies
must also match release year. TV/ONA entries must be finished, match an ended
single-season TMDB show, and agree on total episode count and first/last airing
dates. Missing or ambiguous metadata is skipped; this deliberately favors
accuracy over coverage.

A usable direct score replaces MDBList's `ML` score before the existing database
and submission queue are updated. Missing scores, mismatches, and provider errors
leave the MDBList fallback unchanged. Scores require at least one vote and are
converted from 0–10 to 0–100. Refreshes follow `worker.title_refresh_days`; unusable
optional MAL results use the provider refresh state and `worker.failed_retry_days`
without failing the title.

Requests are paced at one per 1.1 seconds, with the existing HTTP retry and
persisted service-pause handling. Each title's optional MAL fetch is bounded to
35 seconds. `[MAL]` logs report successes, skipped mappings, missing scores, and
failures; the `mal` service state records provider responses and pauses. Existing
MDBList ratings are not retrospectively filtered by these new matching rules.
AniList scores are not included.

#### Anime identity safety and audit

MAL candidates retain their source (MDBList, stored mapping, offline bridge,
Fribb, or Jikan). Dataset decisions distinguish missing data, conflicts, unsafe
scope, and unavailable caches. Conflicting candidate IDs block MAL contributions. If an
existing candidate is definitively rejected, enabled Jikan discovery may suggest
a replacement; that suggestion must pass the same identity checks and is saved
for review only. It never replaces a stored mapping or queues a replacement score.
Provider outages do not trigger correction searches. An official MAL 404 followed
by a Jikan detail 404 can still trigger correction discovery with fallback enabled.

New AniList and AniDB mappings from source metadata are withheld because they do
not yet have independent provider validation. Their IDs remain available for local
MAL discovery and conflict checks. Existing AniList/AniDB queue rows are preserved
but cannot be claimed. Detected MAL identity failures place a submission hold on
that title; both single and grouped claims skip held rows. The submitter rechecks
holds before sending work claimed earlier. Holds survive provider failures and
clear only after successful MAL identity validation. Database migration 5 holds
legacy unsent MAL work until it is revalidated. Submitted history is preserved;
this change does not delete or revalidate external records.

Audit a consistent database backup without network requests or database writes:

```bash
PYTHONPATH=src python scripts/audit_anime_mappings.py --database /path/to/backup.sqlite3 --examples 30
```

The JSON report counts detailed cached validation outcomes, local mapping queue
statuses, submission holds, policy versions, conflicts, correction proposals, and
stored IDs lacking matching cached validation. Counts cover the full audit; `--examples` limits only sample details.
Expired and missing validation records are flagged. Cached evidence is not proof
of current provider identity or remote PMDB coverage. Detailed outcomes live in
MAL enrichment state; the dashboard retains its broader coverage categories.

#### Reviewed mapping repair

`python scripts/repair_anime_mappings.py --help` describes the standalone repair
tool for a separately reviewed, version-1 MAL correction plan. Its default preview
uses PMDB GET requests and writes local reports without changing mappings. Applying
requires `--apply --service-stopped`, creates a verified full database backup, and
restricts deletions to the exact selected contributor's entries. It preserves
ratings and submission holds. Inspect the journal after an interruption; remote
writes are not automatically retried or rolled back. Keep correction plans,
configuration, reports, and backups private.

#### Optional direct anime mappings from Fribb

Set `fribb_mappings = true` in `[mal]` to use
[Fribb's combined anime list](https://github.com/Fribb/anime-lists) as an additional
TMDB/IMDb-to-MAL bridge. This requires
`mal.enabled = true` and defaults to false, including for existing configurations.
The original AniList/AniDB-to-MAL bridge and official MAL scoring remain in use.

The dataset is downloaded on first eligible use and refreshed weekly at
`/data/temp/anime-mappings/fribb-anime-list-full.json`, under the existing persistent
temp volume. A failed, malformed, or unusable
refresh keeps the last good cache; download retries have a one-hour cooldown.
Movie and TV ID namespaces remain separate. Conflicting anchors, shared anime ID
ownership, multipart films, unknown TV season scope, non-first seasons, and episode
offsets are skipped. TV candidates must still be ended single-season shows and
pass all existing MAL title, format, episode-count, and date checks.

Only a validated MAL identity permits a new Fribb-derived `mal` mapping into the
existing PMDB queue. A missing MAL score still permits that mapping. Fribb's
AniList/AniDB IDs are used for conflict detection only; MAL validation does not
establish their identities, so they are not contributed from this dataset. Kitsu
and Simkl IDs are also excluded. Old cached supplemental IDs are ignored.
AniDB contributions from all sources are excluded because the live PMDB API
rejected `id_type="anidb"` with HTTP 422 on 2026-10-07, despite listing it in its
documentation. AniDB IDs remain available for local MAL resolution.
Enabling or disabling the setting invalidates cached enrichment results;
disabling it does not delete previously queued or submitted mappings.

Before enabling it, inspect potential coverage from an already saved dataset:

```bash
PYTHONPATH=src python scripts/preview_fribb_mappings.py \
  --database /data/db/betterer_ratings.sqlite3 \
  --dataset /data/temp/anime-mappings/fribb-anime-list-full.json \
  --limit 1000 --examples 20
```

The preview opens SQLite read-only and makes no network requests or queue writes.
Its counts describe potential additions relative to the local database; candidates
still require MAL validation, and existing PMDB mappings are not checked.
AnimeAPI and the archived Trakt movie dataset are not used by this integration.

### Optional self-hosted Jikan

`[jikan]` is disabled by default. Enable it only alongside `[mal]`, with an explicit
`base_url` such as `http://jikan_rest:8080/v4`. Betterer Ratings and Jikan must share
a private Docker network; no public Jikan port is needed.

With `discover_missing = true`, Japanese-language animation without a resolved MAL
ID is searched using its TMDB display and original titles. Every result page for
both queries must complete within 25 seconds and `max_search_pages` (default 4 per
query, maximum 10). Failed, malformed, repeated, or truncated searches are rejected.
Only one distinct MAL ID may pass the existing title, format, and date/episode
checks, even if another matching entry has no score. Ongoing and multi-season
shows remain excluded. The selected ID is fetched through the official MAL API
and its metadata and score are validated again.

With `fallback = true`, an official MAL timeout, network failure, HTTP 404/429, or
5xx may use Jikan's anime-by-ID endpoint. Official successful responses remain
authoritative, including mismatches or missing scores; 401/403 responses do not
trigger fallback. Jikan scores must pass the same checks. Unusable enrichment
preserves MDBList's score. Jikan is paced at one request per 1.1 seconds, with a
10-second fallback budget; discovery, official lookup, and fallback together may
take up to 70 seconds plus waiting for the discovery lock. Cancellation propagates.

Discovery decisions are cached under `/data/temp/jikan-discovery`: matches for a
day, complete negative/ambiguous results for an hour, and failures/incomplete
searches for five minutes. Endpoint or matching-metadata changes invalidate the
decision. Scores are fetched again on enrichment; discovered IDs are not inserted
into the PMDB mapping queue. Logs include `jikan.discovery` outcomes and a `source`
of `official_mal` or `jikan` on `mal.enrichment` events. The generic service state
records Jikan requests and pauses. Jikan's own cache can be stale and remains
dependent on upstream MAL.

Preview selected titles without submissions or production database writes:

```bash
PYTHONPATH=src python scripts/preview_jikan.py \
  --config /config/config.toml \
  --database /data/db/betterer_ratings.sqlite3 \
  --jikan-url http://jikan_rest:8080/v4 \
  --title movie:1542261 --title movie:52795
```

The preview uses a temporary discovery cache, GET-only provider clients, and SQLite
`mode=ro` with `PRAGMA query_only=ON`. Normal Jikan GET requests may warm its cache.

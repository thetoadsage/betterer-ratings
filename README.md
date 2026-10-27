# betterer-ratings

Continuously discovers ratings through TMDB, IMDb, and MDBList, stores them in
SQLite, and submits due ratings, mappings, and episode ratings to PublicMetaDB.

This is personal automation software. Use your own credentials, respect provider
terms, and keep request rates appropriate for your plans.

## Run with Docker

```bash
cp config.example.toml config.toml
```

Add your API keys to `config.toml`, review the source and rate-limit settings,
then start the service:

```bash
docker compose pull
docker compose up -d
```

The dashboard is available at <http://localhost:8087>. Compose pulls the public
`ghcr.io/itsrenoria/betterer-ratings:latest` image for `linux/amd64` or
`linux/arm64`.

After the first successful image publication, the repository owner must confirm
that the GHCR package visibility is **Public** in GitHub Package Settings before
anonymous Compose pulls will work. This is a one-time setup step.

By default, runtime state stays on the host:

- `config.toml` is mounted read-only at `/config/config.toml`.
- `data/db` contains the SQLite database.
- `data/imdb` contains IMDb archives and indexes.
- `data/temp` contains temporary indexes.

Set `BETTERER_DATA_DB`, `BETTERER_DATA_IMDB`, or `BETTERER_DATA_TEMP` to
override these host paths.

Do not delete `data/db` unless you intend to reset the service.

## Configuration

`config.example.toml` documents every public option. The repository defaults
include:

- seven-day title rating refreshes
- daily episode rating refreshes
- daily IMDb archive refresh at 13:00 UTC
- TMDB at 40 requests per second
- MDBList batches of 200 IDs (supported range: 1–200)
- 16 submission workers

MDBList batch size and daily plan quota are separate limits.

## Optional validated anime enrichment

Enable `[mal]` with an official MAL `client_id` to enrich eligible titles with
MAL ratings and validated MAL mappings. Existing configurations keep this off.
Movies require matching title, type and year. TV mappings require a completed,
ended single-season series with matching dates and episode count; franchise,
cour, season and episode relationships are not flattened into title mappings.

The anime-offline database bridges existing AniList/AniDB candidates to MAL.
Optional `mal.fribb_mappings = true` adds cached Fribb candidates. Candidates
must agree with source/stored ownership and pass MAL identity validation;
unvalidated AniList/AniDB supplements are not contributed. Conflicts block new
MAL work and retain reviewable provenance. A validated alternative found by
Jikan is only a correction proposal and never an automatic replacement.

Optional `[jikan]` can use a self-hosted Jikan `base_url` for complete paginated
discovery and official-MAL detail fallback. It requires `mal.enabled`; incomplete,
ambiguous and malformed searches do not establish an identity. MAL/Jikan use
independent service gates, and an MDBList quota pause does not prevent eligible
anime work. Cached MDBList IDs and MAL decisions are reused without replaying stale
MDBList ratings or advancing an unattempted fetch timestamp.

Caches live beneath `/data/temp`: anime mappings in `anime-mappings/` and Jikan
searches in `jikan-discovery/`. Failed dataset refreshes retain the last good
copy. An additive migration holds legacy unsent MAL mappings for validation
without changing submitted history; mapping claims and final submission honor
those holds. AniDB contributions are withheld after observed API rejection.

Read-only review helpers are `scripts/audit_anime_mappings.py`,
`scripts/preview_fribb_mappings.py` and `scripts/preview_jikan.py`; use `--help`
for inputs. Cached validation is not proof of current remote ownership.

## Update or Roll Back

```bash
docker compose pull
docker compose up -d
```

For an exact rollback, copy the published manifest digest from the workflow
summary, set this in a Compose `.env` file, and recreate the service:

```text
BETTERER_IMAGE=ghcr.io/itsrenoria/betterer-ratings@sha256:<manifest-digest>
```

```bash
docker compose pull
docker compose up -d
```

To follow `latest` again, remove `BETTERER_IMAGE`, then run the two commands
above again.

## Development

```bash
python3 -m pip install -e ".[dev]"
pytest
ruff check .
mypy src
```

Run locally with `betterer-ratings --config config.toml`, or build a local image:

```bash
docker build -t betterer-ratings:local .
```

Logs are written as structured JSON to stdout. The supplied Compose service
rotates Docker logs.

## License

MIT. See `LICENSE`.

# Magnet Index (Render free tier)

On-demand magnet search: your query fans out to the sources in `sources.json`
in parallel, results are stored in SQLite and shown instantly.

## Deploy
1. Push this folder to a GitHub/GitLab repo.
2. Render dashboard -> New -> Blueprint -> pick the repo (reads `render.yaml`).
3. When prompted, set `SITE_PASSWORD` (recommended). `ADMIN_TOKEN` is generated for you.
4. Open the `.onrender.com` URL. Health check is `/healthz`.

## Free-tier behaviour
- Sleeps after ~15 min idle; the first request afterwards takes ~30-60 s to wake.
- Filesystem is ephemeral: the SQLite index resets on every restart, redeploy and
  wake-up. That is fine here, because live search repopulates it per query.
  For a persistent index, use a paid instance + disk, or an external Postgres.
- No cron on free web services, and libtorrent is not installed, so swarm
  verification (`verify`) isn't available; results show as unverified.
- Live searches are rate-limited per IP and cached for 10 min to protect upstream sites.

## Adding sources without committing secrets
Set the `SOURCES_JSON` env var in the dashboard, e.g.
`{"sources":[{"type":"torznab","name":"prowlarr","url":"https://host/1/api","apikey":"KEY"}]}`

## Local run
    pip install -r requirements.txt && python webapp.py

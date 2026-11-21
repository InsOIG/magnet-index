#!/usr/bin/env python3
"""
webapp.py - web UI + JSON API for magnet_indexer.py, hardened for a public deploy
(Render free tier). Local run:  python webapp.py   -> http://127.0.0.1:5000

Environment variables (all optional):
  MAGNET_DB         SQLite path                      (default magnets.db; /tmp/magnets.db on Render)
  SOURCES_JSON      sources config as a JSON string  (overrides sources.json; keeps API keys out of git)
  MAGNET_SOURCES    path to sources file             (default sources.json)
  SITE_PASSWORD     if set, whole site needs HTTP basic auth (any username)
  ADMIN_TOKEN       enables POST /api/crawl with header X-Admin-Token
  LIVE_DEFAULT      1 = "query live sources" ticked by default
  LIVE_TIMEOUT      seconds budget per live search   (default 15)
  LIVE_CACHE_TTL    don't re-query upstream for the same term within N seconds (default 600)
  LIVE_PER_MIN      max live searches per client IP per minute (default 6)
"""
import hmac
import json
import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict

from flask import Flask, Response, jsonify, render_template_string, request
from werkzeug.middleware.proxy_fix import ProxyFix

from magnet_indexer import DB, human, ingest

DB_PATH = os.environ.get("MAGNET_DB", "magnets.db")
CONFIG_PATH = os.environ.get("MAGNET_SOURCES", "sources.json")
LIVE_TIMEOUT = float(os.environ.get("LIVE_TIMEOUT", 15))
LIVE_DEFAULT = os.environ.get("LIVE_DEFAULT", "1") == "1"
LIVE_CACHE_TTL = int(os.environ.get("LIVE_CACHE_TTL", 600))
LIVE_PER_MIN = int(os.environ.get("LIVE_PER_MIN", 6))
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)  # real client IP behind Render's proxy
app.jinja_env.filters["human"] = human

_lock = threading.Lock()
_live_seen: dict = {}                    # normalized query -> last upstream fetch time
_hits: dict = defaultdict(deque)         # ip -> timestamps of recent live searches
_gate = threading.Semaphore(2)           # max concurrent upstream searches (small instance)


def load_sources() -> list:
    raw = os.environ.get("SOURCES_JSON")
    if raw:
        return json.loads(raw).get("sources", [])
    if os.path.exists(CONFIG_PATH):
        return json.load(open(CONFIG_PATH)).get("sources", [])
    return []


def run_sources(db: DB, query: str = None, timeout: float = LIVE_TIMEOUT) -> int:
    """Query/crawl all configured sources in parallel; results stream into the DB."""
    sources = load_sources()
    if not sources:
        return 0
    return sum(ingest(db, sources, query, timeout=timeout, workers=8, delay=1.0).values())


def ip_allowed(ip: str) -> bool:
    now = time.time()
    with _lock:
        d = _hits[ip]
        while d and now - d[0] > 60:
            d.popleft()
        if len(d) >= LIVE_PER_MIN:
            return False
        d.append(now)
        return True


def do_search(q: str, verified: bool, live: bool, ip: str):
    db = DB(DB_PATH)  # new connection per request (sqlite thread rules)
    note = ""
    if live and q:
        key, now = q.lower(), time.time()
        with _lock:
            fresh = now - _live_seen.get(key, 0) < LIVE_CACHE_TTL
        if fresh:
            note = "live sources checked recently"
        elif not ip_allowed(ip):
            note = "live search rate-limited; showing indexed results"
        elif not _gate.acquire(blocking=False):
            note = "server busy; showing indexed results"
        else:
            try:
                n = run_sources(db, q)
                with _lock:
                    _live_seen[key] = time.time()
                    if len(_live_seen) > 2000:  # keep memory bounded
                        for k in sorted(_live_seen, key=_live_seen.get)[:1000]:
                            _live_seen.pop(k, None)
                note = f"{n} record(s) fetched live"
            finally:
                _gate.release()
    results = db.search(q, verified_only=verified) if q else []
    return results, note


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Magnet Index</title>
<style>
  :root { color-scheme: light dark; --bg:#fafafa; --fg:#1b1b1f; --muted:#6b6b76;
          --card:#fff; --line:#e3e3e8; --accent:#3b6df0; --ok:#1a8f4c; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#121214; --fg:#ececf1; --muted:#9a9aa6; --card:#1b1b1f; --line:#2b2b31; --accent:#7a9bff; --ok:#4cc38a; } }
  * { box-sizing: border-box; }
  body { margin:0; font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif; background:var(--bg); color:var(--fg); }
  main { max-width: 860px; margin: 0 auto; padding: 32px 16px 64px; }
  h1 { font-size: 22px; margin: 0 0 20px; }
  form { display:flex; flex-wrap:wrap; gap:10px 16px; align-items:center; margin-bottom: 8px; }
  input[type=search] { flex:1 1 280px; padding:10px 12px; font-size:16px; border:1px solid var(--line);
          border-radius:8px; background:var(--card); color:var(--fg); }
  button, .btn { padding:9px 14px; font-size:14px; border:1px solid var(--line); border-radius:8px;
          background:var(--card); color:var(--fg); cursor:pointer; text-decoration:none; }
  button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
  label { color:var(--muted); font-size:14px; display:flex; gap:6px; align-items:center; }
  .meta { color:var(--muted); font-size:13px; margin: 12px 0 16px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; margin-bottom:10px; }
  .name { font-weight:600; word-break:break-word; }
  .row { display:flex; flex-wrap:wrap; gap:6px 14px; color:var(--muted); font-size:13px; margin:6px 0 10px; }
  .ok { color:var(--ok); font-weight:600; }
  .actions { display:flex; gap:8px; }
  details { margin-top:10px; font-size:13px; color:var(--muted); }
  details ul { margin:6px 0 0; padding-left:18px; max-height:180px; overflow:auto; }
  code { font-size:12px; word-break:break-all; }
  .empty { text-align:center; color:var(--muted); padding:40px 0; }
</style>
</head>
<body>
<main>
  <h1>Magnet Index</h1>
  <form method="get" action="/">
    <input type="search" name="q" value="{{ q }}" placeholder="Search indexed torrents…" autofocus>
    <button class="primary" type="submit">Search</button>
    <label><input type="checkbox" name="verified" value="1" {{ 'checked' if verified }}> Verified only</label>
    <label><input type="checkbox" name="live" value="1" {{ 'checked' if live }}> Also query live sources</label>
  </form>

  {% if q %}
    <div class="meta">
      {{ results|length }} result{{ '' if results|length == 1 else 's' }} for “{{ q }}”
      {% if note %}· {{ note }}{% endif %}
    </div>
    {% for t in results %}
      <div class="card">
        <div class="name">{{ t.name or '(unnamed)' }}</div>
        <div class="row">
          <span>{{ t.size|human }}</span>
          <span>seeders: {{ t.seeders if t.seeders is not none else '?' }}</span>
          <span>source: {{ t.source }}</span>
          {% if t.verified %}<span class="ok">✔ verified</span>{% else %}<span>unverified</span>{% endif %}
        </div>
        <div class="actions">
          <a class="btn" href="{{ t.magnet }}">Open magnet</a>
          <button type="button" data-magnet="{{ t.magnet }}" onclick="copyMagnet(this)">Copy link</button>
        </div>
        <details>
          <summary>Details</summary>
          <div>Infohash: <code>{{ t.infohash }}</code></div>
          {% if t.files %}
            <div>{{ t.files|length }} file(s){% if t.files|length >= 200 %} (first 200){% endif %}:</div>
            <ul>{% for f in t.files %}<li>{{ f.path }} <span>({{ f.size|human }})</span></li>{% endfor %}</ul>
          {% endif %}
        </details>
      </div>
    {% else %}
      <div class="empty">No matches. Try “Also query live sources”.</div>
    {% endfor %}
  {% else %}
    <div class="empty">Enter a search term to look through the local index.</div>
  {% endif %}
</main>
<script>
  async function copyMagnet(btn) {
    try {
      await navigator.clipboard.writeText(btn.dataset.magnet);
      const old = btn.textContent; btn.textContent = 'Copied ✓';
      setTimeout(() => btn.textContent = old, 1200);
    } catch (e) { prompt('Copy this magnet link:', btn.dataset.magnet); }
  }
</script>
</body>
</html>"""


@app.before_request
def guard():
    if request.path == "/healthz":
        return None
    if SITE_PASSWORD:
        auth = request.authorization
        if not (auth and hmac.compare_digest((auth.password or "").encode(), SITE_PASSWORD.encode())):
            return Response("Authentication required", 401,
                            {"WWW-Authenticate": 'Basic realm="Magnet Index"'})
    return None


def flags():
    q = request.args.get("q", "").strip()
    verified = request.args.get("verified") == "1"
    # form submitted (q present) -> honour checkbox; first page load -> default
    live = (request.args.get("live") == "1") if "q" in request.args else LIVE_DEFAULT
    return q, verified, live


@app.get("/healthz")
def healthz():
    return "ok"


@app.get("/")
def index():
    q, verified, live = flags()
    results, note = do_search(q, verified, live, request.remote_addr or "?")
    return render_template_string(PAGE, q=q, verified=verified, live=live,
                                  results=results, note=note)


@app.get("/api/search")
def api_search():
    q, verified, live = flags()
    results, note = do_search(q, verified, live, request.remote_addr or "?")
    return jsonify({
        "query": q, "note": note, "count": len(results),
        "results": [{**asdict(t), "magnet": t.magnet} for t in results],
    })


@app.post("/api/crawl")
def api_crawl():
    tok = request.headers.get("X-Admin-Token", "")
    if not ADMIN_TOKEN or not hmac.compare_digest(tok.encode(), ADMIN_TOKEN.encode()):
        return jsonify(error="forbidden"), 403
    n = run_sources(DB(DB_PATH), request.args.get("q") or None, timeout=45)
    return jsonify({"indexed": n})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5000)), debug=False)

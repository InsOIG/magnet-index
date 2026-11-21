#!/usr/bin/env python3
"""
webapp.py - web UI + JSON API for magnet_indexer.py, hardened for a public deploy
(Render free tier). Local run:  python webapp.py   -> http://127.0.0.1:5000

Sources come from two places:
  * built-in: SOURCES_JSON env var or sources.json (read-only in the UI)
  * user-added: managed at /sources (needs ADMIN_TOKEN), stored in the SQLite DB

Environment variables (all optional):
  MAGNET_DB            SQLite path                       (default magnets.db; /tmp/magnets.db on Render)
  SOURCES_JSON         built-in sources as a JSON string (overrides sources.json)
  MAGNET_SOURCES       path to built-in sources file     (default sources.json)
  SITE_PASSWORD        if set, whole site needs HTTP basic auth (any username)
  ADMIN_TOKEN          required to manage sources and to POST /api/crawl (header X-Admin-Token)
  ALLOW_PRIVATE_HOSTS  1 = allow sources on localhost/LAN (local use only; leave unset when public)
  LIVE_DEFAULT         1 = "query live sources" ticked by default
  LIVE_TIMEOUT         seconds budget per live search    (default 15)
  LIVE_CACHE_TTL       don't re-query upstream for the same term within N seconds (default 600)
  LIVE_PER_MIN         max live searches per client IP per minute (default 6)
  MAX_USER_SOURCES     cap on user-added sources         (default 20)
"""
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict
from itertools import islice

from flask import Flask, Response, jsonify, render_template_string, request
from werkzeug.middleware.proxy_fix import ProxyFix

from magnet_indexer import ADAPTERS, DB, check_public, human, ingest, search_sources

DB_PATH = os.environ.get("MAGNET_DB", "magnets.db")
CONFIG_PATH = os.environ.get("MAGNET_SOURCES", "sources.json")
LIVE_TIMEOUT = float(os.environ.get("LIVE_TIMEOUT", 15))
LIVE_DEFAULT = os.environ.get("LIVE_DEFAULT", "1") == "1"
LIVE_CACHE_TTL = int(os.environ.get("LIVE_CACHE_TTL", 600))
LIVE_PER_MIN = int(os.environ.get("LIVE_PER_MIN", 6))
MAX_USER_SOURCES = int(os.environ.get("MAX_USER_SOURCES", 20))
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
BLOCK_PRIVATE = os.environ.get("ALLOW_PRIVATE_HOSTS", "") != "1"

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)  # real client IP behind Render's proxy
app.jinja_env.filters["human"] = human

_lock = threading.Lock()
_live_seen: dict = {}                    # normalized query -> last upstream fetch time
_hits: dict = defaultdict(deque)         # ip -> timestamps of recent live searches
_gate = threading.Semaphore(2)           # max concurrent upstream searches (small instance)


# ---------------------------------------------------------------- sources ----
def builtin_sources() -> list:
    raw = os.environ.get("SOURCES_JSON")
    if raw:
        return json.loads(raw).get("sources", [])
    if os.path.exists(CONFIG_PATH):
        return json.load(open(CONFIG_PATH)).get("sources", [])
    return []


def sdb() -> DB:
    db = DB(DB_PATH)  # new connection per request (sqlite thread rules)
    db.c.execute("""CREATE TABLE IF NOT EXISTS user_sources(
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, config TEXT,
        enabled INTEGER DEFAULT 1, created REAL)""")
    return db


def user_sources(db: DB) -> list:
    return [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"]),
             "config": json.loads(r["config"])}
            for r in db.c.execute("SELECT * FROM user_sources ORDER BY id")]


def all_sources(db: DB) -> list:
    return builtin_sources() + [u["config"] for u in user_sources(db) if u["enabled"]]


NAME_RE = re.compile(r"^[\w .\-]{1,40}$")


def clean_source(d: dict) -> dict:
    """Validate + normalize a user-submitted source. Raises ValueError with a readable message."""
    t = d.get("type")
    if t not in ADAPTERS:
        raise ValueError("type must be one of: " + ", ".join(ADAPTERS))
    name = str(d.get("name", "")).strip()
    if not NAME_RE.match(name):
        raise ValueError("name must be 1-40 characters: letters, digits, space, . _ -")
    cfg = {"type": t, "name": name}
    if t != "archive":
        url = str(d.get("url", "")).strip()
        if not url.startswith(("http://", "https://")) or len(url) > 500:
            raise ValueError("a valid http(s) URL is required")
        if BLOCK_PRIVATE and not check_public(url):
            raise ValueError("URL host must resolve to a public address (localhost/LAN is blocked)")
        cfg["url"] = url
    if t == "torznab":
        key = str(d.get("apikey", "")).strip()
        if not key:
            raise ValueError("Torznab sources need an API key")
        cfg["apikey"] = key[:200]
    if t == "archive":
        try:
            cfg["max_items"] = max(1, min(int(d.get("max_items") or 20), 50))
        except (TypeError, ValueError):
            raise ValueError("max_items must be a number")
    if d.get("delay") not in (None, ""):
        try:
            cfg["delay"] = max(0.25, min(float(d["delay"]), 30.0))
        except (TypeError, ValueError):
            raise ValueError("delay must be a number of seconds")
    return cfg


def public_view(cfg: dict) -> dict:
    """Config minus secrets, for display."""
    out = {k: v for k, v in cfg.items() if k != "apikey"}
    if "apikey" in cfg:
        out["has_key"] = True
    return out


def run_sources(db: DB, query: str = None, timeout: float = LIVE_TIMEOUT) -> int:
    """Query/crawl all sources (built-in + enabled user ones) in parallel into the DB."""
    sources = all_sources(db)
    if not sources:
        return 0
    return sum(ingest(db, sources, query, timeout=timeout, workers=8, delay=1.0,
                      block_private=BLOCK_PRIVATE).values())


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
    db = sdb()
    note = ""
    nsrc = len(all_sources(db))
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
    return results, note, nsrc


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
  <h1 style="display:flex;justify-content:space-between;align-items:baseline">Magnet Index<a href="/sources" style="font-size:14px;font-weight:400;color:var(--accent)">Manage sources</a></h1>
  <form method="get" action="/">
    <input type="search" name="q" value="{{ q }}" placeholder="Search indexed torrents…" autofocus>
    <button class="primary" type="submit">Search</button>
    <label><input type="checkbox" name="verified" value="1" {{ 'checked' if verified }}> Verified only</label>
    <label><input type="checkbox" name="live" value="1" {{ 'checked' if live }}> Also query live sources ({{ nsrc }})</label>
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

SOURCES_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sources · Magnet Index</title>
<style>
  :root { color-scheme: light dark; --bg:#fafafa; --fg:#1b1b1f; --muted:#6b6b76;
          --card:#fff; --line:#e3e3e8; --accent:#3b6df0; --ok:#1a8f4c; --bad:#c43b3b; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#121214; --fg:#ececf1; --muted:#9a9aa6; --card:#1b1b1f; --line:#2b2b31; --accent:#7a9bff; --ok:#4cc38a; --bad:#ff7a7a; } }
  * { box-sizing: border-box; }
  body { margin:0; font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif; background:var(--bg); color:var(--fg); }
  main { max-width: 860px; margin: 0 auto; padding: 32px 16px 64px; }
  h1 { font-size:22px; margin:0 0 6px; display:flex; justify-content:space-between; align-items:baseline; }
  h2 { font-size:16px; margin:0 0 12px; }
  a { color: var(--accent); }
  .sub { color:var(--muted); font-size:14px; margin-bottom:20px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; margin-bottom:12px; }
  .grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px 14px; }
  .grid .wide { grid-column: 1 / -1; }
  label { display:block; font-size:13px; color:var(--muted); margin-bottom:3px; }
  input, select { width:100%; padding:8px 10px; font-size:15px; border:1px solid var(--line);
          border-radius:8px; background:var(--bg); color:var(--fg); }
  button { padding:8px 14px; font-size:14px; border:1px solid var(--line); border-radius:8px;
          background:var(--card); color:var(--fg); cursor:pointer; }
  button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
  button.danger { color:var(--bad); }
  .row { display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
  .src { display:flex; flex-wrap:wrap; gap:6px 12px; align-items:center; justify-content:space-between; }
  .src .name { font-weight:600; }
  .src .meta { color:var(--muted); font-size:13px; word-break:break-all; }
  .badge { font-size:11px; border:1px solid var(--line); border-radius:99px; padding:1px 8px; color:var(--muted); margin-left:6px; }
  .msg { font-size:14px; margin-top:10px; min-height:1.4em; }
  .msg.ok { color:var(--ok); } .msg.err { color:var(--bad); }
  pre { background:var(--bg); border:1px solid var(--line); border-radius:8px; padding:10px; overflow:auto; font-size:12px; }
  [hidden] { display:none !important; }
  @media (max-width:560px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<main>
  <h1>Sources <a href="/" style="font-size:14px;font-weight:400">← Search</a></h1>
  <div class="sub">Where live searches look for magnet links. Only add sources you are allowed to use.</div>

  <div class="card" id="auth">
    <label for="token">Admin token</label>
    <div class="row">
      <input id="token" type="password" placeholder="ADMIN_TOKEN from your Render environment" style="flex:1 1 260px;width:auto" autocomplete="off">
      <button class="primary" id="unlock">Unlock</button>
    </div>
    <div class="msg" id="authmsg"></div>
  </div>

  <div id="app" hidden>
    <div class="card">
      <h2>Add a source</h2>
      <div class="grid">
        <div><label>Type</label>
          <select id="f-type">
            <option value="rss">RSS feed</option>
            <option value="torznab">Torznab (Jackett / Prowlarr)</option>
            <option value="page">Web page with magnet links</option>
            <option value="archive">Internet Archive</option>
          </select></div>
        <div><label>Name</label><input id="f-name" maxlength="40" placeholder="my-feed"></div>
        <div class="wide" data-for="rss torznab page"><label>URL</label><input id="f-url" placeholder="https://…"></div>
        <div class="wide" data-for="torznab"><label>API key</label><input id="f-apikey" type="password" autocomplete="off"></div>
        <div data-for="archive"><label>Max items per search</label><input id="f-max" type="number" min="1" max="50" value="20"></div>
        <div><label>Delay between requests (s, optional)</label><input id="f-delay" type="number" min="0.25" step="0.25" placeholder="1"></div>
      </div>
      <div class="row" style="margin-top:12px">
        <button id="test">Test</button>
        <button class="primary" id="add">Add source</button>
      </div>
      <div class="msg" id="addmsg"></div>
    </div>

    <h2>Configured sources</h2>
    <div id="list"></div>

    <div class="card">
      <div class="row"><button id="export">Export as SOURCES_JSON</button>
        <span class="sub" style="margin:0">User-added sources live in the instance's temporary database and disappear when it restarts. Export and paste into the <code>SOURCES_JSON</code> env var to keep them (this includes API keys).</span></div>
      <pre id="out" hidden></pre>
    </div>
  </div>
</main>
<script>
const $ = s => document.querySelector(s);
let token = sessionStorage.getItem('adminToken') || '';

async function api(path, method = 'GET', body) {
  const r = await fetch(path, { method,
    headers: { 'X-Admin-Token': token, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || r.statusText);
  return data;
}
function say(sel, text, cls) { const e = $(sel); e.textContent = text; e.className = 'msg ' + (cls || ''); }

function syncFields() {
  const t = $('#f-type').value;
  document.querySelectorAll('[data-for]').forEach(el => { el.hidden = !el.dataset.for.split(' ').includes(t); });
}
function formData() {
  const d = { type: $('#f-type').value, name: $('#f-name').value, url: $('#f-url').value,
              apikey: $('#f-apikey').value, max_items: $('#f-max').value, delay: $('#f-delay').value };
  return d;
}

function render(data) {
  const list = $('#list'); list.replaceChildren();
  const mk = (s, builtin) => {
    const c = s.config || s;
    const card = document.createElement('div'); card.className = 'card src';
    const left = document.createElement('div');
    const nm = document.createElement('span'); nm.className = 'name'; nm.textContent = c.name || c.type;
    left.append(nm);
    const b = document.createElement('span'); b.className = 'badge'; b.textContent = c.type + (builtin ? ' · built-in' : ''); left.append(b);
    const meta = document.createElement('div'); meta.className = 'meta';
    meta.textContent = c.url || (c.type === 'archive' ? 'archive.org' : '');
    left.append(meta); card.append(left);
    if (!builtin) {
      const right = document.createElement('div'); right.className = 'row';
      const lab = document.createElement('label'); lab.style.cssText = 'display:flex;gap:6px;align-items:center;margin:0';
      const cb = document.createElement('input'); cb.type = 'checkbox'; cb.style.width = 'auto'; cb.checked = s.enabled;
      cb.onchange = async () => { try { await api('/api/sources/' + s.id, 'PATCH', { enabled: cb.checked }); } catch (e) { alert(e.message); load(); } };
      lab.append(cb, 'enabled');
      const t = document.createElement('button'); t.textContent = 'Test';
      t.onclick = async () => { t.textContent = '…'; try { const r = await api('/api/sources/test', 'POST', { id: s.id }); alert(r.message); } catch (e) { alert(e.message); } t.textContent = 'Test'; };
      const del = document.createElement('button'); del.className = 'danger'; del.textContent = 'Delete';
      del.onclick = async () => { if (!confirm('Delete "' + c.name + '"?')) return; try { await api('/api/sources/' + s.id, 'DELETE'); load(); } catch (e) { alert(e.message); } };
      right.append(lab, t, del); card.append(right);
    }
    return card;
  };
  data.builtin.forEach(s => list.append(mk(s, true)));
  data.user.forEach(s => list.append(mk(s, false)));
  if (!data.builtin.length && !data.user.length) {
    const e = document.createElement('div'); e.className = 'sub'; e.textContent = 'No sources yet. Add one above.'; list.append(e);
  }
}
async function load() { render(await api('/api/sources')); }

async function unlock() {
  token = $('#token').value || token;
  try { await load(); sessionStorage.setItem('adminToken', token); $('#auth').hidden = true; $('#app').hidden = false; }
  catch (e) { say('#authmsg', e.message, 'err'); }
}
$('#unlock').onclick = unlock;
$('#token').onkeydown = e => { if (e.key === 'Enter') unlock(); };
$('#f-type').onchange = syncFields;
$('#test').onclick = async () => {
  say('#addmsg', 'Testing…');
  try { const r = await api('/api/sources/test', 'POST', formData()); say('#addmsg', r.message, r.count ? 'ok' : 'err'); }
  catch (e) { say('#addmsg', e.message, 'err'); }
};
$('#add').onclick = async () => {
  try { await api('/api/sources', 'POST', formData()); say('#addmsg', 'Added.', 'ok');
        $('#f-name').value = $('#f-url').value = $('#f-apikey').value = ''; load(); }
  catch (e) { say('#addmsg', e.message, 'err'); }
};
$('#export').onclick = async () => {
  try { const r = await api('/api/sources/export'); const o = $('#out'); o.hidden = false; o.textContent = JSON.stringify(r); }
  catch (e) { alert(e.message); }
};
syncFields();
if (token) { $('#token').value = token; unlock(); }
</script>
</body>
</html>"""


# ----------------------------------------------------------------- routes ----
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


def admin_error():
    if not ADMIN_TOKEN:
        return jsonify(error="source management is disabled: set the ADMIN_TOKEN environment variable"), 403
    tok = request.headers.get("X-Admin-Token", "")
    if not hmac.compare_digest(tok.encode(), ADMIN_TOKEN.encode()):
        return jsonify(error="invalid admin token"), 403
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
    results, note, nsrc = do_search(q, verified, live, request.remote_addr or "?")
    return render_template_string(PAGE, q=q, verified=verified, live=live,
                                  results=results, note=note, nsrc=nsrc)


@app.get("/api/search")
def api_search():
    q, verified, live = flags()
    results, note, _ = do_search(q, verified, live, request.remote_addr or "?")
    return jsonify({
        "query": q, "note": note, "count": len(results),
        "results": [{**asdict(t), "magnet": t.magnet} for t in results],
    })


@app.post("/api/crawl")
def api_crawl():
    err = admin_error()
    if err:
        return err
    n = run_sources(sdb(), request.args.get("q") or None, timeout=45)
    return jsonify({"indexed": n})


# --- source management (all admin-only) ---
@app.get("/sources")
def sources_page():
    return SOURCES_PAGE  # static shell; data needs the admin token


@app.get("/api/sources")
def api_sources():
    err = admin_error()
    if err:
        return err
    db = sdb()
    return jsonify({
        "builtin": [public_view(c) for c in builtin_sources()],
        "user": [{"id": u["id"], "enabled": u["enabled"], "config": public_view(u["config"])}
                 for u in user_sources(db)],
    })


@app.post("/api/sources")
def api_sources_add():
    err = admin_error()
    if err:
        return err
    try:
        cfg = clean_source(request.get_json(silent=True) or {})
    except ValueError as e:
        return jsonify(error=str(e)), 400
    db = sdb()
    if len(user_sources(db)) >= MAX_USER_SOURCES:
        return jsonify(error=f"limit of {MAX_USER_SOURCES} user sources reached"), 400
    if cfg["name"] in {c.get("name") for c in builtin_sources()}:
        return jsonify(error="a built-in source already uses that name"), 409
    try:
        cur = db.c.execute("INSERT INTO user_sources(name, config, enabled, created) VALUES(?,?,1,?)",
                           (cfg["name"], json.dumps(cfg), time.time()))
        db.c.commit()
    except sqlite3.IntegrityError:
        return jsonify(error="a source with that name already exists"), 409
    with _lock:
        _live_seen.clear()  # let the new source be queried for terms already searched
    return jsonify(id=cur.lastrowid), 201


@app.patch("/api/sources/<int:sid>")
def api_sources_toggle(sid):
    err = admin_error()
    if err:
        return err
    enabled = bool((request.get_json(silent=True) or {}).get("enabled"))
    db = sdb()
    cur = db.c.execute("UPDATE user_sources SET enabled=? WHERE id=?", (int(enabled), sid))
    db.c.commit()
    with _lock:
        _live_seen.clear()
    return (jsonify(ok=True), 200) if cur.rowcount else (jsonify(error="not found"), 404)


@app.delete("/api/sources/<int:sid>")
def api_sources_delete(sid):
    err = admin_error()
    if err:
        return err
    db = sdb()
    cur = db.c.execute("DELETE FROM user_sources WHERE id=?", (sid,))
    db.c.commit()
    with _lock:
        _live_seen.clear()
    return (jsonify(ok=True), 200) if cur.rowcount else (jsonify(error="not found"), 404)


@app.post("/api/sources/test")
def api_sources_test():
    """Dry run: query one source (saved by id, or unsaved form data) and report what it returns."""
    err = admin_error()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    db = sdb()
    if "id" in body:
        row = db.c.execute("SELECT config FROM user_sources WHERE id=?", (body["id"],)).fetchone()
        if not row:
            return jsonify(error="not found"), 404
        cfg = json.loads(row["config"])
    else:
        try:
            cfg = clean_source(body)
        except ValueError as e:
            return jsonify(error=str(e)), 400
    if not _gate.acquire(blocking=False):
        return jsonify(error="server busy, try again in a moment"), 429
    try:
        query = "ubuntu" if cfg["type"] == "archive" else None  # others: crawl the feed/page as-is
        found = list(islice(search_sources([cfg], query, timeout=12, block_private=BLOCK_PRIVATE), 50))
    finally:
        _gate.release()
    if found:
        names = ", ".join((t.name or t.infohash[:8]) for t in found[:3])
        msg = f"OK: {len(found)} magnet(s) found, e.g. {names}"
    else:
        msg = ("No magnets found. Check the URL and format, whether the site's robots.txt "
               "allows crawling, and that the host is public.")
    return jsonify(count=len(found), message=msg)


@app.get("/api/sources/export")
def api_sources_export():
    err = admin_error()
    if err:
        return err
    db = sdb()
    return jsonify(sources=builtin_sources() + [u["config"] for u in user_sources(db) if u["enabled"]])


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5000)), debug=False)

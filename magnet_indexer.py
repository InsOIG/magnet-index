#!/usr/bin/env python3
"""
magnet_indexer.py - collect magnet links + metadata from configurable sources,
verify them, store them in SQLite (FTS5), and search.

Install:  pip install requests
Optional: pip install libtorrent   (enables real metadata verification over DHT)

Usage:
  python magnet_indexer.py search "ubuntu 24.04" --live      # on-demand: query all sources in parallel
  python magnet_indexer.py crawl  --config sources.json [--query "ubuntu 24.04"] [--workers 8]
  python magnet_indexer.py verify [--limit 50] [--timeout 45]
  python magnet_indexer.py search "ubuntu" [--verified] [--json]
  python magnet_indexer.py export --out magnets.json

sources.json example:
{
  "sources": [
    {"type": "archive", "name": "internet-archive", "max_items": 25},
    {"type": "rss", "name": "distro-feed", "url": "https://example.org/torrents.rss"},
    {"type": "torznab", "name": "my-prowlarr", "url": "http://localhost:9696/1/api", "apikey": "KEY"},
    {"type": "page", "name": "tracker-page", "url": "https://example.org/downloads"}
  ]
}

Index content you have the right to share/download (Linux ISOs, public-domain
media, open datasets, your own releases). You are responsible for complying with
each source's terms and your local law.
"""
import argparse
import base64
import hashlib
import json
import queue
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.robotparser
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from typing import Iterable, Iterator, Optional

import requests
import requests.adapters
from concurrent.futures import ThreadPoolExecutor

UA = "MagnetIndexer/0.1 (personal indexer; respects robots.txt)"
MAGNET_RE = re.compile(r"magnet:\?[^\s\"'<>]+", re.I)


# --------------------------------------------------------------------------
# Data model + magnet handling
# --------------------------------------------------------------------------
@dataclass
class Torrent:
    infohash: str
    name: str = ""
    size: Optional[int] = None
    trackers: list = field(default_factory=list)
    files: list = field(default_factory=list)
    seeders: Optional[int] = None
    source: str = ""
    verified: bool = False

    @property
    def magnet(self) -> str:
        q = [("xt", f"urn:btih:{self.infohash}")]
        if self.name:
            q.append(("dn", self.name))
        q += [("tr", t) for t in self.trackers]
        return "magnet:?" + urllib.parse.urlencode(q)


def normalize_btih(value: str) -> Optional[str]:
    """Return a 40-char lowercase hex infohash from hex or base32, else None."""
    value = value.strip()
    if re.fullmatch(r"[0-9a-fA-F]{40}", value):
        return value.lower()
    if re.fullmatch(r"[A-Za-z2-7]{32}", value):
        return base64.b32decode(value.upper()).hex()
    return None


def parse_magnet(uri: str, source: str = "") -> Optional[Torrent]:
    uri = uri.replace("&amp;", "&")
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(uri).query)
    for xt in qs.get("xt", []):
        if xt.lower().startswith("urn:btih:"):
            h = normalize_btih(xt[9:])
            if h:
                size = qs.get("xl", [None])[0]
                return Torrent(
                    infohash=h,
                    name=qs.get("dn", [""])[0],
                    size=int(size) if size and size.isdigit() else None,
                    trackers=qs.get("tr", []),
                    source=source,
                )
    return None


# --------------------------------------------------------------------------
# Minimal bencode (to derive an infohash from a .torrent file)
# --------------------------------------------------------------------------
def _bdecode(data: bytes, i: int = 0):
    c = data[i:i + 1]
    if c == b"i":
        j = data.index(b"e", i)
        return int(data[i + 1:j]), j + 1
    if c == b"l":
        out, i = [], i + 1
        while data[i:i + 1] != b"e":
            v, i = _bdecode(data, i)
            out.append(v)
        return out, i + 1
    if c == b"d":
        out, i = {}, i + 1
        while data[i:i + 1] != b"e":
            k, i = _bdecode(data, i)
            v, i = _bdecode(data, i)
            out[k] = v
        return out, i + 1
    j = data.index(b":", i)
    n = int(data[i:j])
    return data[j + 1:j + 1 + n], j + 1 + n


def torrent_file_to_record(data: bytes, source: str) -> Optional[Torrent]:
    """Parse .torrent bytes -> Torrent (infohash = sha1 of the raw info dict)."""
    try:
        if data[:1] != b"d":
            return None
        i, meta = 1, {}
        info_raw = None
        while data[i:i + 1] != b"e":
            k, i = _bdecode(data, i)
            start = i
            v, i = _bdecode(data, i)
            if k == b"info":
                info_raw = data[start:i]
            meta[k] = v
        info = meta[b"info"]
        files = info.get(b"files")
        if files:
            flist = [{"path": "/".join(p.decode("utf-8", "replace") for p in f[b"path"]),
                      "size": f[b"length"]} for f in files]
            size = sum(f["size"] for f in flist)
        else:
            size = info.get(b"length")
            flist = [{"path": info[b"name"].decode("utf-8", "replace"), "size": size}]
        trackers = []
        if b"announce" in meta:
            trackers.append(meta[b"announce"].decode("utf-8", "replace"))
        for tier in meta.get(b"announce-list", []):
            trackers += [t.decode("utf-8", "replace") for t in tier]
        return Torrent(
            infohash=hashlib.sha1(info_raw).hexdigest(),
            name=info[b"name"].decode("utf-8", "replace"),
            size=size, files=flist[:200],
            trackers=list(dict.fromkeys(trackers)), source=source,
        )
    except Exception:
        return None


# --------------------------------------------------------------------------
# Polite HTTP client: robots.txt, per-host rate limit, retries
# --------------------------------------------------------------------------
class Fetcher:
    """Thread-safe polite HTTP client.

    Rate limiting works by *reserving time slots* per host: each request books
    the next free slot under a lock, then sleeps outside the lock. Requests to
    the same host stay `delay` seconds apart, but their network latency
    overlaps, and different hosts never wait on each other. A global
    `deadline` (time.monotonic value) makes every call fail fast once passed.
    """

    def __init__(self, delay: float = 1.0, respect_robots: bool = True, pool: int = 16):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = UA
        ad = requests.adapters.HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
        self.s.mount("http://", ad)
        self.s.mount("https://", ad)
        self.delay, self.respect_robots = delay, respect_robots
        self.host_delay: dict = {}            # per-host override of `delay`
        self.deadline: Optional[float] = None
        self._next: dict = {}                 # host -> next free slot (monotonic)
        self._robots: dict = {}
        self._robots_locks: dict = {}
        self._lock = threading.Lock()

    def _remaining(self) -> Optional[float]:
        return None if self.deadline is None else self.deadline - time.monotonic()

    def _allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        p = urllib.parse.urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        with self._lock:
            lk = self._robots_locks.setdefault(base, threading.Lock())
        with lk:  # fetch each host's robots.txt only once, even under concurrency
            rp = self._robots.get(base)
            if rp is None:
                rp = urllib.robotparser.RobotFileParser()
                try:
                    r = self.s.get(base + "/robots.txt", timeout=10)
                    rp.parse(r.text.splitlines() if r.ok else [])
                except requests.RequestException:
                    rp.parse([])
                self._robots[base] = rp
        return rp.can_fetch(UA, url)

    def _wait_slot(self, host: str):
        delay = self.host_delay.get(host, self.delay)
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next.get(host, 0.0))
            self._next[host] = start + delay
        if start > now:
            time.sleep(start - now)

    def get(self, url: str, **kw) -> Optional[requests.Response]:
        rem = self._remaining()
        if rem is not None and rem <= 0:
            return None
        if not self._allowed(url):
            print(f"  [robots.txt disallows] {url}", file=sys.stderr)
            return None
        host = urllib.parse.urlparse(url).netloc
        self._wait_slot(host)
        for attempt in range(3):
            rem = self._remaining()
            if rem is not None and rem <= 0:
                return None
            try:
                timeout = 20 if rem is None else max(1.0, min(20.0, rem))
                r = self.s.get(url, timeout=timeout, **kw)
                if r.status_code == 429:
                    time.sleep(2 * (attempt + 1))
                    self._wait_slot(host)
                    continue
                return r if r.ok else None
            except requests.RequestException:
                time.sleep(0.5 * 2 ** attempt)
        return None


# --------------------------------------------------------------------------
# Source adapters
# --------------------------------------------------------------------------
class Adapter:
    HOST = ""             # set by adapters whose host isn't in cfg["url"]
    DEFAULT_DELAY = None  # per-host politeness override; None = use fetcher default

    def __init__(self, cfg: dict, fetcher: Fetcher):
        self.cfg, self.f = cfg, fetcher
        self.name = cfg.get("name", cfg["type"])
        host = urllib.parse.urlparse(cfg["url"]).netloc if cfg.get("url") else self.HOST
        delay = cfg.get("delay", self.DEFAULT_DELAY)
        if host and delay is not None:
            fetcher.host_delay[host] = delay

    def crawl(self) -> Iterator[Torrent]:
        return iter(())

    def search(self, query: str) -> Iterator[Torrent]:
        return iter(())


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_feed(xml_text: str, source: str) -> Iterator[Torrent]:
    """RSS / Torznab: finds magnets in link/enclosure/description + torznab attrs."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return
    for item in root.iter("item"):
        title, blobs, attrs = "", [], {}
        for el in item:
            tag = _local(el.tag)
            if tag == "title":
                title = (el.text or "").strip()
            elif tag == "attr":  # torznab:attr name= value=
                attrs[el.get("name")] = el.get("value")
            else:
                blobs.append(el.text or "")
                blobs.append(el.get("url") or "")
        t = None
        for m in MAGNET_RE.findall(" ".join(blobs)):
            t = parse_magnet(m, source)
            if t:
                break
        if not t and attrs.get("infohash"):
            h = normalize_btih(attrs["infohash"])
            t = Torrent(infohash=h, source=source) if h else None
        if not t and attrs.get("magneturl"):
            t = parse_magnet(attrs["magneturl"], source)
        if not t:
            continue
        t.name = t.name or title
        if attrs.get("size", "").isdigit():
            t.size = int(attrs["size"])
        if attrs.get("seeders", "").isdigit():
            t.seeders = int(attrs["seeders"])
        yield t


class RSSAdapter(Adapter):
    def crawl(self):
        r = self.f.get(self.cfg["url"])
        if r:
            yield from parse_feed(r.text, self.name)

    def search(self, query):
        q = query.lower()
        for t in self.crawl():
            if all(w in t.name.lower() for w in q.split()):
                yield t


class TorznabAdapter(Adapter):
    """Works with Jackett / Prowlarr / any Torznab endpoint."""
    def _q(self, **params):
        params["apikey"] = self.cfg["apikey"]
        r = self.f.get(self.cfg["url"] + "?" + urllib.parse.urlencode(params))
        if r:
            yield from parse_feed(r.text, self.name)

    def crawl(self):
        yield from self._q(t="search")

    def search(self, query):
        yield from self._q(t="search", q=query)


class InternetArchiveAdapter(Adapter):
    """Archive.org items expose a <id>_archive.torrent for most items."""
    API = "https://archive.org/advancedsearch.php"
    HOST = "archive.org"
    DEFAULT_DELAY = 0.4   # seconds between requests; latency overlaps via workers

    def search(self, query):
        params = {"q": query, "fl[]": "identifier", "rows": self.cfg.get("max_items", 25),
                  "output": "json", "sort[]": "downloads desc"}
        r = self.f.get(self.API + "?" + urllib.parse.urlencode(params, doseq=True))
        if not r:
            return
        idents = [d["identifier"] for d in r.json().get("response", {}).get("docs", [])]

        def grab(ident):
            tr = self.f.get(f"https://archive.org/download/{ident}/{ident}_archive.torrent")
            return torrent_file_to_record(tr.content, self.name) if tr else None

        with ThreadPoolExecutor(self.cfg.get("workers", 8)) as ex:
            for t in ex.map(grab, idents):
                if t:
                    yield t

    def crawl(self):
        yield from self.search(self.cfg.get("query", "mediatype:software AND collection:opensource"))


class PageAdapter(Adapter):
    """Generic: pull every magnet link from a page (e.g. a distro downloads page)."""
    def crawl(self):
        r = self.f.get(self.cfg["url"])
        if r:
            for m in set(MAGNET_RE.findall(r.text)):
                t = parse_magnet(m, self.name)
                if t:
                    yield t

    def search(self, query):
        words = query.lower().split()
        for t in self.crawl():
            if all(w in t.name.lower() for w in words):
                yield t


ADAPTERS = {"rss": RSSAdapter, "torznab": TorznabAdapter,
            "archive": InternetArchiveAdapter, "page": PageAdapter}


def search_sources(sources: list, query: Optional[str] = None, timeout: float = 20.0,
                   delay: float = 1.0, workers: int = 8,
                   respect_robots: bool = True) -> Iterator[Torrent]:
    """Run every source concurrently and yield Torrents as they arrive.

    query=None crawls instead of searching. Results from fast sources appear
    immediately; slow ones are cut off at `timeout` seconds (partial results kept).
    Consume this generator from ONE thread (it owns no DB state).
    """
    fetcher = Fetcher(delay, respect_robots, pool=max(16, workers * 2))
    start = time.monotonic()
    if timeout:
        fetcher.deadline = start + timeout
    adapters = [ADAPTERS[s["type"]](s, fetcher) for s in sources]
    if not adapters:
        return
    q: "queue.Queue[Optional[Torrent]]" = queue.Queue()

    def task(ad: Adapter):
        try:
            for t in (ad.search(query) if query else ad.crawl()):
                q.put(t)
        except Exception as e:  # one bad source must not sink the rest
            print(f"  [{ad.name}] failed: {e}", file=sys.stderr)
        finally:
            q.put(None)  # sentinel: this source is done

    ex = ThreadPoolExecutor(min(workers, len(adapters)))
    for ad in adapters:
        ex.submit(task, ad)
    done = 0
    hard_stop = start + timeout + 5 if timeout else None
    try:
        while done < len(adapters):
            wait = None if hard_stop is None else max(0.1, hard_stop - time.monotonic())
            try:
                t = q.get(timeout=wait)
            except queue.Empty:
                break  # a source hung past the deadline; return what we have
            if t is None:
                done += 1
            else:
                yield t
    finally:
        ex.shutdown(wait=False, cancel_futures=True)


def ingest(db: "DB", sources: list, query: Optional[str] = None, **kw) -> dict:
    """Run sources in parallel, store results (single writer, one commit). Returns {source: count}."""
    counts: dict = {}
    for t in search_sources(sources, query, **kw):
        db.upsert(t, commit=False)
        counts[t.source] = counts.get(t.source, 0) + 1
    db.c.commit()
    return counts


# --------------------------------------------------------------------------
# Verification
#   Level 1: syntax (valid 40-hex infohash)               - always
#   Level 2: metadata fetched from the swarm via DHT/trackers and its
#            infohash matches (needs libtorrent)           - proves it's real
# --------------------------------------------------------------------------
def verify_via_swarm(t: Torrent, timeout: int = 45) -> bool:
    try:
        import libtorrent as lt
    except ImportError:
        print("libtorrent not installed; skipping swarm verification", file=sys.stderr)
        return False
    ses = lt.session({"listen_interfaces": "0.0.0.0:0", "enable_dht": True})
    for host in ("router.bittorrent.com", "dht.transmissionbt.com"):
        ses.add_dht_router(host, 6881)
    params = lt.parse_magnet_uri(t.magnet)
    params.save_path = "/tmp/magnet_indexer"
    params.flags |= lt.torrent_flags.upload_mode  # never download payload
    h = ses.add_torrent(params)
    deadline = time.time() + timeout
    ok = False
    while time.time() < deadline:
        if h.status().has_metadata:
            ti = h.torrent_file()
            if str(ti.info_hash()) == t.infohash or str(ti.info_hashes().v1) == t.infohash:
                fs = ti.files()
                t.name = ti.name()
                t.size = ti.total_size()
                t.files = [{"path": fs.file_path(i), "size": fs.file_size(i)}
                           for i in range(min(fs.num_files(), 200))]
                t.seeders = h.status().num_seeds
                ok = True
            break
        time.sleep(1)
    ses.remove_torrent(h)
    return ok


# --------------------------------------------------------------------------
# Storage (SQLite + FTS5)
# --------------------------------------------------------------------------
class DB:
    def __init__(self, path="magnets.db"):
        self.c = sqlite3.connect(path)
        self.c.row_factory = sqlite3.Row
        self.c.execute("PRAGMA journal_mode=WAL")
        self.c.execute("PRAGMA synchronous=NORMAL")
        self.c.executescript("""
        CREATE TABLE IF NOT EXISTS torrents(
            infohash TEXT PRIMARY KEY, name TEXT, size INTEGER, trackers TEXT,
            files TEXT, seeders INTEGER, source TEXT, verified INTEGER DEFAULT 0,
            first_seen REAL, last_seen REAL);
        CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(infohash UNINDEXED, name);
        """)

    def upsert(self, t: Torrent, commit: bool = True):
        now = time.time()
        old = self.c.execute("SELECT * FROM torrents WHERE infohash=?", (t.infohash,)).fetchone()
        if old:
            name = t.name or old["name"]
            trackers = list(dict.fromkeys(json.loads(old["trackers"]) + t.trackers))
            files = t.files or json.loads(old["files"])
            self.c.execute(
                """UPDATE torrents SET name=?, size=COALESCE(?,size), trackers=?, files=?,
                   seeders=COALESCE(?,seeders), verified=MAX(verified,?), last_seen=? WHERE infohash=?""",
                (name, t.size, json.dumps(trackers), json.dumps(files), t.seeders,
                 int(t.verified), now, t.infohash))
        else:
            name = t.name
            self.c.execute("INSERT INTO torrents VALUES(?,?,?,?,?,?,?,?,?,?)",
                           (t.infohash, name, t.size, json.dumps(t.trackers), json.dumps(t.files),
                            t.seeders, t.source, int(t.verified), now, now))
        self.c.execute("DELETE FROM fts WHERE infohash=?", (t.infohash,))
        self.c.execute("INSERT INTO fts VALUES(?,?)", (t.infohash, name))
        if commit:
            self.c.commit()

    def search(self, q: str, verified_only=False, limit=50):
        fts_q = " ".join(f'"{w}"*' for w in re.findall(r"\w+", q))
        sql = ("SELECT t.* FROM fts JOIN torrents t USING(infohash) WHERE fts MATCH ?"
               + (" AND t.verified=1" if verified_only else "")
               + " ORDER BY COALESCE(t.seeders,0) DESC LIMIT ?")
        return [self._row(r) for r in self.c.execute(sql, (fts_q, limit))]

    def unverified(self, limit):
        rows = self.c.execute("SELECT * FROM torrents WHERE verified=0 LIMIT ?", (limit,))
        return [self._row(r) for r in rows]

    def all(self):
        return [self._row(r) for r in self.c.execute("SELECT * FROM torrents")]

    @staticmethod
    def _row(r) -> Torrent:
        return Torrent(r["infohash"], r["name"], r["size"], json.loads(r["trackers"]),
                       json.loads(r["files"]), r["seeders"], r["source"], bool(r["verified"]))


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def human(n):
    if n is None:
        return "?"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"


def print_results(ts: Iterable[Torrent], as_json=False):
    ts = list(ts)
    if as_json:
        print(json.dumps([{**asdict(t), "magnet": t.magnet} for t in ts], indent=2))
        return
    for t in ts:
        flag = "✔" if t.verified else "·"
        print(f"{flag} {t.name or '(unnamed)'}  [{human(t.size)}, seeds={t.seeders if t.seeders is not None else '?'}, src={t.source}]")
        print(f"    {t.magnet}")
    print(f"\n{len(ts)} result(s)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="magnets.db")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("crawl")
    c.add_argument("--config", default="sources.json")
    c.add_argument("--query")
    c.add_argument("--delay", type=float, default=1.0, help="min seconds between requests to one host")
    c.add_argument("--workers", type=int, default=8)
    c.add_argument("--timeout", type=float, default=60, help="overall time budget in seconds")
    c.add_argument("--ignore-robots", action="store_true", help="not recommended")

    v = sub.add_parser("verify")
    v.add_argument("--limit", type=int, default=25)
    v.add_argument("--timeout", type=int, default=45)

    s = sub.add_parser("search")
    s.add_argument("query")
    s.add_argument("--verified", action="store_true")
    s.add_argument("--json", action="store_true")
    s.add_argument("--live", action="store_true", help="query all sources in parallel first")
    s.add_argument("--config", default="sources.json")
    s.add_argument("--delay", type=float, default=1.0)
    s.add_argument("--workers", type=int, default=8)
    s.add_argument("--timeout", type=float, default=20)

    e = sub.add_parser("export")
    e.add_argument("--out", default="magnets.json")

    a = ap.parse_args()
    db = DB(a.db)

    if a.cmd == "crawl":
        sources = json.load(open(a.config))["sources"]
        t0 = time.monotonic()
        counts = ingest(db, sources, a.query, timeout=a.timeout, delay=a.delay,
                        workers=a.workers, respect_robots=not a.ignore_robots)
        for name, n in counts.items():
            print(f"[{name}] indexed {n}")
        print(f"done: {sum(counts.values())} records in {time.monotonic() - t0:.1f}s")

    elif a.cmd == "verify":
        for t in db.unverified(a.limit):
            print(f"verifying {t.infohash} {t.name[:50]!r} ...", end=" ", flush=True)
            t.verified = verify_via_swarm(t, a.timeout)
            print("OK" if t.verified else "no metadata")
            db.upsert(t)

    elif a.cmd == "search":
        if a.live:
            sources = json.load(open(a.config))["sources"]
            t0 = time.monotonic()
            counts = ingest(db, sources, a.query, timeout=a.timeout, delay=a.delay, workers=a.workers)
            print(f"live: {sum(counts.values())} record(s) from {len(counts)} source(s) "
                  f"in {time.monotonic() - t0:.1f}s", file=sys.stderr)
        print_results(db.search(a.query, a.verified), a.json)

    elif a.cmd == "export":
        rows = [{**asdict(t), "magnet": t.magnet} for t in db.all()]
        json.dump(rows, open(a.out, "w"), indent=2)
        print(f"wrote {len(rows)} records to {a.out}")


if __name__ == "__main__":
    main()

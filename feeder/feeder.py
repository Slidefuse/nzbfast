#!/usr/bin/env python3
"""nzbfast feeder: keeps nzbfast's queue full without Radarr/Sonarr in the hot path.

Radarr/Sonarr stay the library and system of record (what is wanted, profiles,
naming, imports, Overseerr/Bazarr/Plex). The feeder:

  1. reads what is wanted from Radarr/Sonarr,
  2. searches the indexers directly and in parallel,
  3. validates candidates with the app's own /parse (quality, custom-format score,
     series/episode/movie mapping) plus its size limits and blocklist,
  4. downloads the NZB itself and adds it to nzbfast under its own categories
     (which Radarr/Sonarr do not watch),
  5. on completion tells the app exactly what each file is (ManualImport with
     explicit series/episode or movie ids), on failure immediately tries the next
     candidate.

Upgrades of existing movie files still go through release/push, so the app decides
whether a release is an upgrade. See docs/feeder-proposal.md. Stdlib only.
"""
import argparse, collections, json, logging, math, os, re, secrets, signal, sqlite3, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

log = logging.getLogger("feeder")
NS = "{http://www.newznab.com/DTD/2010/feeds/attributes/}"
UA = "nzbfast-feeder/2"
STOP = threading.Event()
UI_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui.html")
VIDEO = (".mkv", ".mp4", ".avi", ".m4v", ".ts", ".m2ts", ".wmv", ".mov", ".mpg")


# ---------------------------------------------------------------- http helpers

def http(url, method="GET", body=None, headers=None, timeout=60, raw=None):
    h = {"User-Agent": UA}
    h.update(headers or {})
    data = raw
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def multipart(field, filename, data):
    b = "----feeder" + secrets.token_hex(12)
    head = (f"--{b}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\n"
            f"Content-Type: application/x-nzb\r\n\r\n").encode()
    return head + data + f"\r\n--{b}--\r\n".encode(), f"multipart/form-data; boundary={b}"


class Arr:
    def __init__(self, name, url, key):
        self.name, self.url, self.key = name, url.rstrip("/"), key
        self.push_lock = threading.Semaphore(2)

    def req(self, method, path, body=None, timeout=120):
        raw = http(f"{self.url}/api/v3{path}", method, body, {"X-Api-Key": self.key}, timeout)
        return json.loads(raw) if raw else None

    def paged(self, path, page_size=1000):
        out, page = [], 1
        sep = "&" if "?" in path else "?"
        while True:
            d = self.req("GET", f"{path}{sep}page={page}&pageSize={page_size}")
            out += d["records"]
            if page * page_size >= d["totalRecords"] or not d["records"]:
                return out
            page += 1


# ---------------------------------------------------------------- indexers

class QueueFull(Exception):
    """nzbfast is at its job/GB target; the item is retried shortly."""


class IndexerError(Exception):
    pass


class Indexer:
    def __init__(self, name, cfg):
        self.name = name
        self.url = cfg["url"].rstrip("/") + cfg.get("api_path", "/api")
        self.key = cfg["api_key"]
        self.arr_name = cfg.get("arr_name", f"{name} (Prowlarr)")
        self.sem = threading.Semaphore(cfg.get("concurrency", 8))
        self.daily_queries = cfg.get("daily_queries", 0)
        self.blocked_until = 0.0
        self.grab_blocked_until = 0.0  # NZB downloads failing (grab throttle) until then
        self.lock = threading.Lock()
        self.day, self.queries = "", 0

    def available(self):
        if time.time() < self.blocked_until:
            return False
        with self.lock:
            today = time.strftime("%Y-%m-%d")
            if today != self.day:
                self.day, self.queries = today, 0
            return not self.daily_queries or self.queries < self.daily_queries

    def search(self, params):
        if not self.available():
            return []
        q = dict(params, apikey=self.key, extended=1, limit=100, o="xml")
        url = f"{self.url}?{urllib.parse.urlencode(q)}"
        with self.sem:
            with self.lock:
                self.queries += 1
            try:
                raw = http(url, timeout=30)
            except urllib.error.HTTPError as e:
                if e.code in (429, 503) or e.code >= 500:
                    self.blocked_until = time.time() + (600 if e.code == 429 else 120)
                    log.warning("%s: HTTP %d, backing off", self.name, e.code)
                raise IndexerError(f"{self.name}: HTTP {e.code}")
            except Exception as e:
                raise IndexerError(f"{self.name}: {e}")
        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            raise IndexerError(f"{self.name}: bad xml {e}")
        if root.tag == "error":
            code = root.get("code", "")
            msg = root.get("description", "")
            # 429-equivalents / limits reached: back off for an hour.
            if code in ("429", "500", "501", "502", "503") or "limit" in msg.lower():
                self.blocked_until = time.time() + 3600
                log.warning("%s: api error %s %s, backing off 1h", self.name, code, msg)
            raise IndexerError(f"{self.name}: error {code} {msg}")
        out = []
        for it in root.iter("item"):
            attrs = {a.get("name"): a.get("value") for a in it.iter(f"{NS}attr")}
            enc = it.find("enclosure")
            link = (enc.get("url") if enc is not None else None) or it.findtext("link")
            size = int(attrs.get("size") or (enc.get("length") if enc is not None else 0) or 0)
            try:
                pub = parsedate_to_datetime(it.findtext("pubDate")).astimezone(timezone.utc)
            except Exception:
                pub = datetime.now(timezone.utc)
            if not link or not size:
                continue
            out.append({
                "title": (it.findtext("title") or "").strip(),
                "link": link, "size": size, "pub": pub,
                "grabs": int(attrs.get("grabs") or 0),
                "indexer": self,
            })
        return out

    def get_nzb(self, rel):
        """Download a release's NZB, or None. Grab throttling (429/5xx) pauses this indexer's grabs."""
        try:
            data = http(rel["link"], timeout=60)
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) or e.code >= 500:
                if time.time() > self.grab_blocked_until:
                    log.warning("%s: NZB download HTTP %d; skipping its releases for 5 min", self.name, e.code)
                self.grab_blocked_until = time.time() + 300
            else:
                log.info("NZB download %s failed HTTP %d", rel["title"], e.code)
            return None
        except Exception as e:
            log.info("NZB download %s failed: %s", rel["title"], e)
            return None
        if b"<nzb" not in data[:4096]:
            log.info("NZB download %s: not an NZB (%d bytes)", rel["title"], len(data))
            return None
        return data


# ---------------------------------------------------------------- title pre-filter
# Cheap local filter/ranking so only plausible candidates are sent to /parse.

RES = [("2160p", re.compile(r"\b(2160p|4k|uhd)\b", re.I)),
       ("1080p", re.compile(r"\b1080[pi]\b", re.I)),
       ("720p", re.compile(r"\b720p\b", re.I)),
       ("480p", re.compile(r"\b(480p|576p|sd|dvdrip|xvid)\b", re.I))]
BAD = re.compile(r"\b(cam|hdcam|ts|telesync|hdts|tc|telecine|scr|screener|dvdscr|workprint|"
                 r"remux|bdmv|br-?disk|complete\.?bluray|iso|3d|hsbs|h-sbs|sample|trailer|"
                 r"extras|featurettes?|bonus|password(ed)?|subpack)\b", re.I)
FOREIGN = re.compile(r"\b(german|deutsch|french|truefrench|vff|vfq|italian|ita|ger|fre|fra|spa|"
                     r"castellano|latino|esp|polish|pl|dutch|nl|nordic|swedish|danish|"
                     r"norwegian|finnish|russian|rus|ukr|hindi|tamil|telugu|korean|kor|"
                     r"japanese|jap|chinese|chs|cht|turkish|tur|czech|hun|hungarian|"
                     r"portuguese|por|brazilian|ptbr|greek|hebrew|arabic|vostfr|subbed)\b", re.I)
MULTI = re.compile(r"\b(multi|dual|dl)\b", re.I)


def norm(t):
    return re.sub(r"[\s_]+", ".", t)


def quality_of(title):
    t = norm(title)
    res = next((r for r, rx in RES if rx.search(t)), None)
    if re.search(r"\b(blu-?ray|bdrip|brrip|bd)\b", t, re.I):
        src = "Bluray"
    elif re.search(r"\bweb-?rip\b", t, re.I):
        src = "WEBRip"
    elif re.search(r"\b(web-?dl|web|amzn|nf|dsnp|hmax|atvp|pcok|hulu|max)\b", t, re.I):
        src = "WEBDL"
    elif re.search(r"\b(hdtv|pdtv|dsr|tvrip)\b", t, re.I):
        src = "HDTV"
    else:
        src = None
    if res is None:
        return None
    return f"{src or 'WEBDL'}-{res}"


def profile_ranks(profile):
    """quality name -> rank (higher is better) for allowed qualities, plus cutoff rank."""
    ranks, cutoff_rank, i = {}, None, 0
    for it in profile["items"]:
        i += 1
        names = [x["quality"]["name"] for x in it["items"]] if it.get("items") else [it["quality"]["name"]]
        ident = it.get("id") if it.get("items") else it["quality"]["id"]
        if it["allowed"]:
            for n in names:
                ranks[n] = i
        if ident == profile["cutoff"]:
            cutoff_rank = i
    return ranks, cutoff_rank or max(ranks.values(), default=0), profile.get("minFormatScore") or 0


class Rules:
    """One app's acceptance rules: profiles, size limits, ignored terms, blocklist."""
    def __init__(self, arr, blocklist):
        self.blocklist = blocklist  # shared set, filled by Feeder.load_blocklists
        self.profiles = {p["id"]: profile_ranks(p) for p in arr.req("GET", "/qualityprofile")}
        # quality name -> (min, max, preferred) in MB per minute of runtime
        self.sizes = {q["quality"]["name"]: (q.get("minSize") or 0, q.get("maxSize"), q.get("preferredSize"))
                      for q in arr.req("GET", "/qualitydefinition")}
        self.ignored = []
        for r in arr.req("GET", "/releaseprofile"):
            if r.get("enabled", True) and not r.get("tags"):
                ign = r.get("ignored") or []
                self.ignored += [x.lower() for x in (ign.split(",") if isinstance(ign, str) else ign) if x]

    def size_ok(self, quality, size, minutes):
        lo, hi, _ = self.sizes.get(quality, (0, None, None))
        mb = size / 1048576
        return not minutes or (mb >= lo * minutes * 1.02 and (not hi or mb <= hi * minutes * 0.98))

    def target_gb(self, quality, minutes, fallback):
        lo, hi, pref = self.sizes.get(quality, (0, None, None))
        return (pref or (hi or 60) * 0.7) * minutes / 1024 if minutes else fallback


def prescore(rel, rules, profile, minutes, fallback_gb):
    """Local sort key (higher first) or None when the release is clearly unacceptable."""
    t = norm(rel["title"])
    if BAD.search(t) or rel["title"].lower() in rules.blocklist:
        return None
    tl = rel["title"].lower()
    if any(x in tl for x in rules.ignored):
        return None
    ranks, cutoff, _ = rules.profiles[profile]
    q = quality_of(rel["title"])
    if q not in ranks or not rules.size_ok(q, rel["size"], minutes):
        return None
    eng = 0 if (FOREIGN.search(t) and not MULTI.search(t)) else 1
    target = rules.target_gb(q, minutes, fallback_gb)
    fit = -abs(math.log(max(rel["size"] / 1e9, 0.05) / max(target, 0.05)))
    age_days = (datetime.now(timezone.utc) - rel["pub"]).days
    return (eng, min(ranks[q], cutoff), round(fit, 1), -min(age_days // 365, 10), rel["grabs"])


TERMINAL_REJECT = re.compile(r"already (in|has).*queue|in queue|existing file|not an upgrade|"
                             r"meets cutoff|is not wanted|not monitored|already imported", re.I)
SE = re.compile(r"\bS(\d{1,2})[ ._-]?E(\d{1,3})(?:[ ._-]?E?(\d{1,3}))?\b", re.I)
SPACK = re.compile(r"\b(?:S(\d{1,2})(?![ ._-]?E\d)(?![ ._-]?-?[ ._-]?S\d)|Season[ ._-]?(\d{1,2}))\b", re.I)


def _alnum(t):
    return re.sub(r"[^a-z0-9]+", "", t.lower())


def match_episode(fname, season_eps):
    """Episode id for a pack file from its name: SxxEyy / 1x05 / E05 / a leading number
    ("28.Vendetta.mkv") / the episode title. None when unsure."""
    stem = os.path.splitext(fname)[0]
    by_num = {n: i for n, _, i in season_eps}
    for rx in (r"\bS\d{1,2}[ ._-]?E(\d{1,3})\b", r"\b\d{1,2}x(\d{2,3})\b", r"\bE(?:p(?:isode)?)?[ ._-]?(\d{1,3})\b",
               r"^(\d{1,3})(?=[ ._-]|$)"):
        m = re.search(rx, stem, re.I)
        if m and int(m.group(1)) in by_num:
            return by_num[int(m.group(1))]
    s = _alnum(stem)
    hit = [i for _, t, i in season_eps if len(_alnum(t)) >= 4 and _alnum(t) in s]
    return hit[0] if len(hit) == 1 else None


def mkv_title(path):
    """Segment title (Info/Title) of a Matroska file, or None."""
    try:
        with open(path, "rb") as f:
            d = f.read(1 << 20)
    except OSError:
        return None

    def vint(i, keep_marker):
        if i >= len(d) or d[i] == 0:
            return None, i
        n = 8 - d[i].bit_length() + 1
        v = d[i] if keep_marker else d[i] & ((1 << (8 - n)) - 1)
        for b in d[i + 1:i + n]:
            v = v << 8 | b
        return v, i + n
    eid, i = vint(0, True)
    if eid != 0x1A45DFA3:
        return None
    n, i = vint(i, False)
    i += n or 0
    eid, i = vint(i, True)
    if eid != 0x18538067:
        return None
    _, i = vint(i, False)
    while i < len(d):
        eid, i = vint(i, True)
        n, i = vint(i, False)
        if eid is None or n is None:
            return None
        if eid == 0x1549A966:
            end = min(i + n, len(d))
            while i < end:
                cid, i = vint(i, True)
                cn, i = vint(i, False)
                if cid is None or cn is None:
                    return None
                if cid == 0x7BA9:
                    return d[i:i + cn].decode("utf-8", "replace").strip() or None
                i += cn
            return None
        if eid == 0x1F43B675:
            return None
        i += n
    return None


def rejection_text(r):
    return r.get("reason", str(r)) if isinstance(r, dict) else str(r)


# ---------------------------------------------------------------- feeder

class Feeder:
    def __init__(self, cfg, dry_run):
        self.cfg, self.dry = cfg, dry_run
        self.radarr = Arr("radarr", cfg["radarr"]["url"], cfg["radarr"]["api_key"]) if cfg.get("radarr") else None
        self.sonarr = Arr("sonarr", cfg["sonarr"]["url"], cfg["sonarr"]["api_key"]) if cfg.get("sonarr") else None
        self.indexers = [Indexer(n, c) for n, c in cfg["indexers"].items() if c.get("use", True)]
        self.target_jobs = cfg.get("target_jobs", 150)
        self.target_gb = cfg.get("target_gb", 1500)
        self.max_grabs_day = cfg.get("max_grabs_per_day", 0)
        self.cats = cfg.get("categories", {"radarr": "feeder-movies", "sonarr": "feeder-series"})
        self.handlers = ThreadPoolExecutor(cfg.get("inflight", 16))
        self.pool = ThreadPoolExecutor(8)
        self.dblock = threading.RLock()
        os.makedirs(os.path.dirname(cfg["state_db"]), exist_ok=True)
        db = cfg["state_db"] + (".dry" if dry_run else "")
        self.db = sqlite3.connect(db, check_same_thread=False)
        self.db.executescript("""
            create table if not exists items(key text primary key, next_due real, last real, outcome text,
                                             tries integer default 0);
            create table if not exists grabs(t real, key text, title text, indexer text, size integer);
            create table if not exists jobs(nzo text primary key, key text, app text, title text, indexer text,
                                            size integer, target text, status text, t_added real, t_done real,
                                            cmd integer, note text);
            create index if not exists jobs_status on jobs(status);
            create table if not exists failed(title text primary key, t real, reason text);
        """)
        self.db.commit()
        self.nzb_url = cfg["nzbfast"]["url"].rstrip("/")
        self.nzb_key = self._nzbfast_key()
        self.wanted_at = 0
        self.movies, self.seasons = [], []
        self.rules = {}
        self.blocklists = {"radarr": set(), "sonarr": set()}
        self.labels = {}
        self.started = time.time()
        self.events = collections.deque(maxlen=500)  # recent INFO+ log lines for the UI
        self._live, self._live_at = {}, 0.0
        # movies / episodes with a feeder job in flight: never grabbed twice (the apps do not see our jobs)
        self.busy = {"radarr": set(), "sonarr": set()}
        for app, target in self.db.execute("select app, target from jobs where status in ('queued','importing')"):
            self.busy[app].update(self.target_ids(app, json.loads(target or "{}")))
        self.qlock = threading.Lock()
        self._q, self._q_at, self._q_added = (0, 0.0), 0.0, [0, 0.0]
        self.stats = {"searched": 0, "grabbed": 0, "pushed": 0, "nohits": 0, "imported": 0, "failed": 0}

    # -- nzbfast
    def _nzbfast_key(self):
        c = self.cfg["nzbfast"]
        if c.get("api_key"):
            return c["api_key"]
        sect = ""
        for line in open(c["sab_ini"], encoding="utf-8", errors="replace"):
            s = line.strip()
            if s.startswith("["):
                sect = s
            elif sect == "[misc]" and s.startswith("api_key"):
                return s.split("=", 1)[1].strip()
        raise SystemExit("nzbfast api key not found")

    def sab(self, **params):
        q = urllib.parse.urlencode(dict(params, output="json", apikey=self.nzb_key))
        return json.loads(http(f"{self.nzb_url}/api?{q}", timeout=30))

    def nzbfast_queue(self):
        q = self.sab(mode="queue", limit=1)["queue"]
        return int(q.get("noofslots_total", q.get("noofslots", 0))), float(q.get("mbleft", 0)) / 1024

    def nzbfast_add(self, data, title, cat):
        body, ctype = multipart("name", re.sub(r"[^\w.\- ]", "_", title) + ".nzb", data)
        q = urllib.parse.urlencode({"mode": "addfile", "output": "json", "apikey": self.nzb_key,
                                    "cat": cat, "nzbname": title})
        r = json.loads(http(f"{self.nzb_url}/api?{q}", "POST", headers={"Content-Type": ctype}, raw=body))
        ids = r.get("nzo_ids") or []
        if not r.get("status") or not ids:
            raise RuntimeError(r.get("error") or "addfile failed")
        return ids[0]

    @staticmethod
    def target_ids(app, target):
        return target.get("episodeIds") or [] if app == "sonarr" else [target["movieId"]] if "movieId" in target else []

    def unbusy(self, nzo):
        r = self.q("select app, target from jobs where nzo=?", (nzo,))
        if r:
            self.busy[r[0][0]].difference_update(self.target_ids(r[0][0], json.loads(r[0][1] or "{}")))

    def queue_room(self, reserve_gb=None):
        """True if nzbfast is below target, counting grabs made since the last queue poll.
        With reserve_gb, also books one job of that size."""
        if self.dry:
            return True
        with self.qlock:
            if time.time() - self._q_at > 5:
                self._q, self._q_at, self._q_added = self.nzbfast_queue(), time.time(), [0, 0.0]
            jobs, gb = self._q[0] + self._q_added[0], self._q[1] + self._q_added[1]
            if jobs >= self.target_jobs or gb >= self.target_gb:
                return False
            if reserve_gb is not None:
                self._q_added[0] += 1
                self._q_added[1] += reserve_gb
            return True

    # -- state
    def q(self, sql, args=(), commit=False):
        with self.dblock:
            cur = self.db.execute(sql, args)
            rows = cur.fetchall()
            if commit:
                self.db.commit()
            return rows

    def due(self, key, now):
        r = self.q("select next_due from items where key=?", (key,))
        return not r or r[0][0] <= now

    def mark(self, key, outcome, delay):
        now = time.time()
        self.q("insert into items(key,next_due,last,outcome,tries) values(?,?,?,?,1) "
               "on conflict(key) do update set next_due=excluded.next_due, last=excluded.last, "
               "outcome=excluded.outcome, tries=tries+1", (key, now + delay, now, outcome), commit=True)

    def grabs_today(self):
        return self.q("select count(*) from grabs where t > ?", (time.time() - 86400,))[0][0]

    def failed_titles(self):
        return {r[0] for r in self.q("select title from failed")}

    # -- wanted lists
    def refresh_wanted(self):
        t0 = time.time()
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        movies, seasons = [], []
        failed = self.failed_titles()
        if self.radarr:
            self.rules["radarr"] = Rules(self.radarr, self.blocklists["radarr"])
            self.blocklists["radarr"] |= failed
            queued = {r.get("movieId") for r in self.radarr.paged("/queue?includeUnknownMovieItems=false")}
            cutoff_ids = {r["id"] for r in self.radarr.paged("/wanted/cutoff?monitored=true")}
            for m in self.radarr.req("GET", "/movie"):
                if not m["monitored"] or m["id"] in queued or not m.get("isAvailable"):
                    continue
                if m["hasFile"] and m["id"] not in cutoff_ids:
                    continue
                if not m.get("imdbId") and not m.get("tmdbId"):
                    continue
                movies.append({"key": f"m:{m['id']}", "id": m["id"], "title": m["title"], "year": m.get("year"),
                               "imdb": m.get("imdbId"), "tmdb": m.get("tmdbId"), "profile": m["qualityProfileId"],
                               "runtime": m.get("runtime") or 0, "upgrade": m["hasFile"]})
            movies.sort(key=lambda m: m["upgrade"])  # missing first, then upgrades
        if self.sonarr:
            self.rules["sonarr"] = Rules(self.sonarr, self.blocklists["sonarr"])
            self.blocklists["sonarr"] |= failed
            series = {s["id"]: s for s in self.sonarr.req("GET", "/series")}
            queued = {(r.get("seriesId"), r.get("seasonNumber")) for r in self.sonarr.paged("/queue")}
            partial = []
            for s in series.values():
                if not s["monitored"] or not s.get("tvdbId"):
                    continue
                for x in s.get("seasons", []):
                    st, sn = x.get("statistics", {}), x["seasonNumber"]
                    if sn == 0 or not x["monitored"] or (s["id"], sn) in queued:
                        continue
                    have, want = st.get("episodeFileCount", 0), st.get("episodeCount", 0)
                    if want <= have or not st.get("previousAiring"):
                        continue
                    item = {"key": f"s:{s['id']}:{sn}", "series": s["id"], "season": sn, "title": s["title"],
                            "tvdb": s["tvdbId"], "profile": s["qualityProfileId"], "full": have == 0,
                            "eps": None, "aired": st["previousAiring"], "nep": want,
                            "runtime": s.get("runtime") or 0}
                    seasons.append(item)
                    if have:
                        partial.append(item)

            def episodes(sid):
                return sid, self.sonarr.req("GET", f"/episode?seriesId={sid}")
            by_series = {}
            for it in partial:
                by_series.setdefault(it["series"], []).append(it)
            for sid, eps in self.pool.map(episodes, list(by_series)):
                for it in by_series[sid]:
                    it["eps"] = {e["episodeNumber"]: e["id"] for e in eps
                                 if e["seasonNumber"] == it["season"] and e["monitored"] and not e["hasFile"]
                                 and e.get("airDateUtc") and e["airDateUtc"] <= now_iso}
            seasons = [x for x in seasons if x["eps"] is None or x["eps"]]
            seasons.sort(key=lambda x: x["aired"], reverse=True)
        self.movies, self.seasons = movies, seasons
        self.labels = {m["key"]: f"{m['title']} ({m['year']})" for m in movies}
        self.labels.update((x["key"], f"{x['title']} S{x['season']:02d}") for x in seasons)
        self.wanted_at = time.time()
        log.info("wanted: %d movies, %d seasons (%d full) in %.0fs", len(movies), len(seasons),
                 sum(s["full"] for s in seasons), time.time() - t0)

    def load_blocklists(self):
        """Full load once (Sonarr's is ~35k rows, minutes to page), then only the newest page."""
        full = True
        while not STOP.is_set():
            for app, arr in (("radarr", self.radarr), ("sonarr", self.sonarr)):
                if not arr:
                    continue
                try:
                    if full:
                        rows = arr.paged("/blocklist?sortKey=date&sortDirection=descending")
                    else:
                        rows = arr.req("GET", "/blocklist?page=1&pageSize=500&sortKey=date&sortDirection=descending")["records"]
                    self.blocklists[app].update(b["sourceTitle"].lower() for b in rows if b.get("sourceTitle"))
                except Exception as e:
                    log.warning("blocklist %s: %s", app, e)
            if full:
                log.info("blocklists loaded: %s", {k: len(v) for k, v in self.blocklists.items()})
            full = False
            STOP.wait(600)

    # -- searching
    def search_all(self, params, cover=None):
        """Hits from every indexer. `cover` (a list) gets False appended when one of them
        was unavailable or failed, so "no hits" is not trusted for long."""
        hits = []
        for ix in self.indexers:
            if not ix.available():
                if cover is not None:
                    cover.append(False)
                continue
            try:
                hits += ix.search(params)
            except IndexerError as e:
                log.debug("search error %s", e)
                if cover is not None:
                    cover.append(False)
        return hits

    def search_item(self, item, cover=None):
        if item["key"].startswith("m:"):
            p = {"t": "movie", "cat": "2000"}
            if item["imdb"]:
                p["imdbid"] = item["imdb"].removeprefix("tt")
            else:
                p["tmdbid"] = item["tmdb"]
            return self.search_all(p, cover)
        p = {"t": "tvsearch", "cat": "5000", "tvdbid": item["tvdb"], "season": item["season"]}
        hits = self.search_all(p, cover)
        # big seasons: fetch a second page so packs are not crowded out by episodes
        if len(hits) >= 100 * len(self.indexers) * 0.9:
            hits += self.search_all(dict(p, offset=100), cover)
        return hits

    # -- validation through the app's own parser
    def validate(self, app, item, rel, minutes):
        """Ask the app how it reads this title. Returns (rank_key, target) or None.
        target: {"movieId"} or {"seriesId", "episodeIds"}."""
        arr = self.radarr if app == "radarr" else self.sonarr
        rules = self.rules[app]
        try:
            d = arr.req("GET", "/parse?title=" + urllib.parse.quote(rel["title"]), timeout=30)
        except Exception as e:
            log.debug("parse %s: %s", rel["title"], e)
            return None
        ranks, cutoff, min_cf = rules.profiles[item["profile"]]
        info = d.get("parsedMovieInfo" if app == "radarr" else "parsedEpisodeInfo") or {}
        qname = ((info.get("quality") or {}).get("quality") or {}).get("name")
        if qname not in ranks or not rules.size_ok(qname, rel["size"], minutes):
            return None
        cf = d.get("customFormatScore") or 0
        if cf < min_cf:
            return None
        if app == "radarr":
            if (d.get("movie") or {}).get("id") != item["id"] or item["id"] in self.busy["radarr"]:
                return None
            target = {"movieId": item["id"]}
        else:
            if (d.get("series") or {}).get("id") != item["series"] or info.get("seasonNumber") != item["season"]:
                return None
            eps = d.get("episodes") or []
            if not eps or any(e.get("hasFile") or e["id"] in self.busy["sonarr"] for e in eps):
                return None  # never replace existing files here, never grab an episode twice
            if item["eps"] is not None and any(e["id"] not in item["eps"].values() for e in eps):
                return None
            target = {"seriesId": item["series"], "episodeIds": [e["id"] for e in eps],
                      "episodes": sorted(e["episodeNumber"] for e in eps)}
        return (min(ranks[qname], cutoff), cf), target

    def pick_and_grab(self, app, item, cands, minutes_of, n=6):
        """cands: [(prescore, rel)] for one slot (movie, pack or episode). Validates the
        top n with /parse and grabs the best valid one. Returns the target grabbed or None."""
        cands = sorted(cands, key=lambda x: x[0], reverse=True)
        seen, top = set(), []
        for s, h in cands:
            if h["title"].lower() in seen:
                continue
            seen.add(h["title"].lower())
            top.append((s, h))
            if len(top) >= n:
                break
        valid = []
        for (s, h), v in zip(top, self.pool.map(lambda sh: self.validate(app, item, sh[1], minutes_of(sh[1])), top)):
            if v:
                valid.append(((v[0], s), h, v[1]))
        valid.sort(key=lambda x: x[0], reverse=True)
        for _, h, target in valid:
            if time.time() < h["indexer"].grab_blocked_until:
                continue
            if self.grab(app, item, h, target):
                return target
        return None

    def grab(self, app, item, rel, target):
        if self.dry:
            log.info("DRY %s %s <- %s (%.1f GB, %s)", app, item["title"], rel["title"], rel["size"] / 1e9,
                     rel["indexer"].name)
            return True
        if not self.queue_room(reserve_gb=rel["size"] / 2**30):
            raise QueueFull()
        ids = self.target_ids(app, target)
        if self.busy[app].intersection(ids):
            return False  # grabbed meanwhile by another release for the same item
        data = rel["indexer"].get_nzb(rel)
        if not data:
            return False
        try:
            nzo = self.nzbfast_add(data, rel["title"], self.cats[app])
        except Exception as e:
            log.warning("nzbfast add %s failed: %s", rel["title"], e)
            return False
        self.busy[app].update(ids)
        now = time.time()
        self.q("insert into jobs(nzo,key,app,title,indexer,size,target,status,t_added) values(?,?,?,?,?,?,?,?,?)",
               (nzo, item["key"], app, rel["title"], rel["indexer"].name, rel["size"], json.dumps(target),
                "queued", now))
        self.q("insert into grabs values(?,?,?,?,?)", (now, item["key"], rel["title"], rel["indexer"].name,
                                                        rel["size"]), commit=True)
        self.stats["grabbed"] += 1
        log.info("GRAB %s %s <- %s (%.1f GB, %s)", app, item["title"], rel["title"], rel["size"] / 1e9,
                 rel["indexer"].name)
        return True

    # -- upgrades still go through the app (it decides what is an upgrade)
    def push(self, arr, item, rel, extra):
        body = {"title": rel["title"], "downloadUrl": rel["link"], "protocol": "usenet",
                "publishDate": rel["pub"].strftime("%Y-%m-%dT%H:%M:%SZ"), "size": rel["size"],
                "indexer": rel["indexer"].arr_name}
        body.update(extra)
        if self.dry:
            log.info("DRY push %s %s <- %s", arr.name, item["title"], rel["title"])
            return True, []
        with arr.push_lock:
            try:
                res = arr.req("POST", "/release/push", body, timeout=180)
            except urllib.error.HTTPError as e:
                log.warning("push %s failed HTTP %d", rel["title"], e.code)
                return False, [f"HTTP {e.code}"]
        r = res[0] if isinstance(res, list) and res else (res or {})
        self.stats["pushed"] += 1
        if r.get("approved"):
            self.q("insert into grabs values(?,?,?,?,?)", (time.time(), item["key"], rel["title"],
                                                            rel["indexer"].name, rel["size"]), commit=True)
            log.info("PUSH-GRAB %s %s <- %s", arr.name, item["title"], rel["title"])
            return True, []
        return False, [rejection_text(x) for x in r.get("rejections") or []]

    # -- per item
    def handle_movie(self, item, hits):
        rules = self.rules["radarr"]
        target = self.cfg.get("movie_target_gb", 10)
        cands = [(prescore(h, rules, item["profile"], item["runtime"], target), h) for h in hits]
        cands = [(s, h) for s, h in cands if s]
        if not cands:
            self.stats["nohits"] += 1
            return self.mark(item["key"], f"nohits:{len(hits)}", item.get("retry_s", 86400))
        if item["upgrade"]:
            extra = {"movieId": item["id"], "tmdbId": item["tmdb"] or 0}
            if item["imdb"] and item["imdb"][2:].isdigit():
                extra["imdbId"] = int(item["imdb"][2:])
            for _, h in sorted(cands, key=lambda x: x[0], reverse=True)[:3]:
                ok, rej = self.push(self.radarr, item, h, extra)
                if ok:
                    return self.mark(item["key"], "pushed", 12 * 3600)
                if any(TERMINAL_REJECT.search(x) for x in rej):
                    break
            return self.mark(item["key"], "upgrade-rejected", 86400)
        if self.pick_and_grab("radarr", item, cands, lambda h: item["runtime"]):
            return self.mark(item["key"], "grabbed", 12 * 3600)
        self.mark(item["key"], "rejected", item.get("retry_s", 86400))

    def handle_season(self, item, hits):
        rules = self.rules["sonarr"]
        want = item["eps"]  # None: whole season missing, any episode is wanted
        nep = item["nep"] or 10
        rt = item["runtime"]
        egb = self.cfg.get("episode_target_gb", 1.5)
        packs, eps = [], {}
        span = {}
        for h in hits:
            t = norm(h["title"])
            m = SE.search(t)
            if m:
                if int(m.group(1)) != item["season"]:
                    continue
                e1 = int(m.group(2))
                e2 = int(m.group(3)) if m.group(3) else e1
                if e2 < e1 or e2 - e1 > 3:
                    e2 = e1
                span[id(h)] = e2 - e1 + 1
                s = prescore(h, rules, item["profile"], rt * span[id(h)], egb * span[id(h)])
                if s and (want is None or e1 in want):
                    eps.setdefault(e1, []).append((s, h))
                continue
            m = SPACK.search(t)
            if m and int(m.group(1) or m.group(2)) == item["season"] and item["full"]:
                s = prescore(h, rules, item["profile"], rt * nep, egb * nep)
                if s:
                    packs.append((s, h))
        if packs and self.pick_and_grab("sonarr", item, packs, lambda h: rt * nep):
            return self.mark(item["key"], "grabbed-pack", 12 * 3600)
        if not eps:
            if not packs:
                self.stats["nohits"] += 1
            return self.mark(item["key"], f"nohits:{len(hits)}" if not packs else "rejected", item.get("retry_s", 86400))
        got, covered = 0, set()
        for e, cands in sorted(eps.items()):
            if e in covered:
                continue
            t = self.pick_and_grab("sonarr", item, cands, lambda h: rt * span.get(id(h), 1), n=4)
            if t:
                got += 1
                covered.update(t.get("episodes") or [e])
        if got:
            # the rest (episodes without hits / rejected) is retried on a later pass
            return self.mark(item["key"], f"grabbed-eps:{got}", 12 * 3600)
        self.mark(item["key"], "rejected-eps", item.get("retry_s", 86400))

    # -- completion: import or retry
    def importer(self):
        while not STOP.wait(self.cfg.get("import_poll_s", 20)):
            try:
                self.import_pass()
            except Exception:
                log.exception("import pass")

    def import_pass(self):
        active = {r[0]: r for r in self.q("select nzo,key,app,title,target,status,cmd from jobs "
                                          "where status in ('queued','importing')")}
        if not active:
            return
        # jobs queued by another process (--reimport-rejected) are in flight too
        for _, _, app, _, target, _, _ in active.values():
            self.busy[app].update(self.target_ids(app, json.loads(target or "{}")))
        hist = {}
        for cat in set(self.cats.values()):
            for s in self.sab(mode="history", cat=cat, limit=2000)["history"]["slots"]:
                hist[s["nzo_id"]] = s
        for nzo, key, app, title, target, status, cmd in active.values():
            arr = self.radarr if app == "radarr" else self.sonarr
            if status == "importing":
                self.check_import(nzo, key, arr, title, cmd)
                continue
            h = hist.get(nzo)
            if not h:
                continue
            if h["status"] == "Failed":
                self.on_failed(nzo, key, title, h.get("fail_message") or "failed")
            elif h["status"] == "Completed" and h.get("storage"):
                self.start_import(nzo, key, arr, title, json.loads(target), h["storage"])

    def on_failed(self, nzo, key, title, reason):
        self.stats["failed"] += 1
        log.info("FAILED %s: %s -> retrying item", title, reason[:120])
        self.unbusy(nzo)
        self.q("insert or replace into failed values(?,?,?)", (title.lower(), time.time(), reason[:300]))
        self.q("update jobs set status='failed', t_done=?, note=? where nzo=?", (time.time(), reason[:300], nzo))
        self.q("update items set next_due=0 where key=?", (key,), commit=True)
        for b in self.blocklists.values():
            b.add(title.lower())
        try:
            self.sab(mode="history", name="delete", value=nzo, del_files=1)
        except Exception:
            pass

    def start_import(self, nzo, key, arr, title, target, storage):
        # No seriesId/movieId here: with one, the apps ignore `folder` and list the library folder instead.
        prev = arr.req("GET", "/manualimport?" + urllib.parse.urlencode(
            {"folder": storage, "filterExistingFiles": "true"}))
        root = storage.rstrip("/") + "/"
        videos = [p for p in prev if p.get("path", "").startswith(root) and p["path"].lower().endswith(VIDEO)]
        files, reasons = [], []
        season_eps = None
        if arr.name == "sonarr" and len(videos) > 1:
            season_eps = self.season_episodes(arr, target)
        for p in videos:
            rej = [rejection_text(r) for r in p.get("rejections") or []]
            # samples are small; the app cannot always tell from a full-size file
            if (p.get("size") or 0) >= 100 * 2**20:
                rej = [r for r in rej if not re.search(r"unable to determine if file is a sample", r, re.I)]
            f = {"path": p["path"], "quality": p.get("quality"), "languages": p.get("languages") or [],
                 "releaseGroup": p.get("releaseGroup") or "", "indexerFlags": p.get("indexerFlags") or 0,
                 "downloadId": nzo}
            if arr.name == "sonarr":
                epids = [e["id"] for e in p.get("episodes") or []]
                if (p.get("series") or {}).get("id") not in (None, target["seriesId"]):
                    epids = []  # parsed as another series
                if not epids and len(videos) == 1:
                    epids = target["episodeIds"]  # obfuscated single file: we know what it is
                    rej = [r for r in rej if not re.search(r"unknown|unable to (identify|parse)", r, re.I)]
                elif season_eps and (not epids or len(epids) > 3):
                    # pack files named without SxxEyy ("28.Vendetta.mkv"): the app reads the
                    # folder name and assigns the whole season to each file
                    e = match_episode(os.path.basename(p["path"]), season_eps)
                    if not e and p["path"].lower().endswith(".mkv"):
                        t = mkv_title(p["path"])  # obfuscated packs often keep the name here
                        e = match_episode(t, season_eps) if t else None
                    if e:
                        epids = [e]
                        rej = [r for r in rej if not re.search(r"all episodes in season|unknown|unable to (identify|parse)", r, re.I)]
                if not epids:
                    reasons.append(f"{os.path.basename(p['path'])}: no episode match")
                    continue
                f.update(seriesId=target["seriesId"], episodeIds=epids,
                         releaseType=p.get("releaseType") or "singleEpisode")
            else:
                if len(videos) > 1 and p is not max(videos, key=lambda v: v.get("size", 0)):
                    continue  # extras/samples next to the main file
                rej = [r for r in rej if not re.search(r"unknown movie|unable to (identify|parse)", r, re.I)]
                f.update(movieId=target["movieId"])
            if rej:
                reasons.append(f"{os.path.basename(p['path'])}: {'; '.join(rej)}")
                continue
            files.append(f)
        if not files:
            reason = "; ".join(reasons)[:300] or "no video files"
            log.warning("IMPORT-REJECTED %s: %s", title, reason)
            self.unbusy(nzo)
            self.q("update jobs set status='import-rejected', t_done=?, note=? where nzo=?",
                   (time.time(), reason, nzo), commit=True)
            # never grab this release again (it was re-grabbed up to 16 times); the item
            # is searched again for another one
            self.q("insert or replace into failed values(?,?,?)", (title.lower(), time.time(), "import: " + reason[:290]))
            self.q("update items set next_due=0 where key=?", (key,), commit=True)
            for b in self.blocklists.values():
                b.add(title.lower())
            return
        cmd = arr.req("POST", "/command", {"name": "ManualImport", "files": files, "importMode": "move"})
        self.q("update jobs set status='importing', cmd=?, note=? where nzo=?",
               (cmd["id"], "; ".join(reasons)[:300] or None, nzo), commit=True)

    def reimport_rejected(self, tidy_bin):
        """One-off: downloads rejected at import (still on disk) are tidied by nzbfast
        (split files, missing extensions, nested archives) and queued for import again."""
        import subprocess
        store = {}
        for cat in set(self.cats.values()):
            for h in self.sab(mode="history", cat=cat, limit=20000)["history"]["slots"]:
                if h["status"] == "Completed" and h.get("storage"):
                    store[h["nzo_id"]] = h["storage"]
        rows = self.q("select nzo, title from jobs where status in ('import-rejected','import-failed') order by t_added desc")
        n = gone = dupes = 0
        seen = set()
        for nzo, title in rows:
            path = store.get(nzo)
            if not path or not os.path.isdir(path):
                gone += 1
                continue
            if title.lower() in seen:
                # an older copy of a release grabbed again later: drop it
                dupes += 1
                self.q("update jobs set status='superseded' where nzo=?", (nzo,), commit=True)
                try:
                    self.sab(mode="history", name="delete", value=nzo, del_files=1)
                except Exception as e:
                    log.warning("delete %s: %s", nzo, e)
                continue
            seen.add(title.lower())
            # undo the blocklisting: this copy gets another import attempt
            self.q("delete from failed where title=? and reason like 'import:%'", (title.lower(),), commit=True)
            try:
                out = subprocess.run([tidy_bin, "tidy", "--name", title, path], capture_output=True, text=True, timeout=3600).stdout.strip()
                if out:
                    log.info("TIDY %s: %s", title, out.replace("\n", "; ")[:200])
            except Exception as e:
                log.warning("tidy %s: %s", title, e)
            self.q("update jobs set status='queued', note=null, cmd=null where nzo=?", (nzo,), commit=True)
            n += 1
        log.info("re-queued %d rejected downloads for import (%d older duplicates deleted, %d no longer on disk)", n, dupes, gone)

    def season_episodes(self, arr, target):
        """[(number, title, id)] of the target season, for matching pack files."""
        ids = set(target.get("episodeIds") or [])
        try:
            eps = arr.req("GET", f"/episode?seriesId={target['seriesId']}")
        except Exception as e:
            log.debug("episodes %s: %s", target, e)
            return None
        seasons = {e["seasonNumber"] for e in eps if e["id"] in ids}
        return [(e["episodeNumber"], e.get("title") or "", e["id"]) for e in eps if e["seasonNumber"] in seasons]

    def not_imported(self, arr, target):
        """Ids of the target's episodes/movie that still have no file."""
        if arr.name == "sonarr":
            ids = target.get("episodeIds") or []
            eps = arr.req("GET", "/episode?" + "&".join(f"episodeIds={i}" for i in ids)) if ids else []
            return [e["id"] for e in eps if not e.get("hasFile")] + [i for i in ids if i not in {e["id"] for e in eps}]
        m = arr.req("GET", f"/movie/{target['movieId']}")
        return [] if m.get("hasFile") else [target["movieId"]]

    def check_import(self, nzo, key, arr, title, cmd):
        c = arr.req("GET", f"/command/{cmd}")
        st = c.get("status")
        if st in ("queued", "started"):
            return
        if st == "completed":
            missing = self.not_imported(arr, json.loads(self.q("select target from jobs where nzo=?", (nzo,))[0][0]))
            if missing:
                # keep the download: the command ran but the app did not take the file(s)
                msg = f"command completed but no file for {missing}"
                log.warning("IMPORT-FAILED %s: %s", title, msg)
                self.unbusy(nzo)
                self.q("update jobs set status='import-failed', t_done=?, note=? where nzo=?",
                       (time.time(), msg, nzo), commit=True)
                return
            self.stats["imported"] += 1
            log.info("IMPORTED %s", title)
            self.unbusy(nzo)
            self.q("update jobs set status='imported', t_done=? where nzo=?", (time.time(), nzo), commit=True)
            try:
                self.sab(mode="history", name="delete", value=nzo, del_files=1)
            except Exception:
                pass
        else:
            msg = (c.get("message") or c.get("exception") or st or "")[:300]
            log.warning("IMPORT-FAILED %s: %s", title, msg)
            self.unbusy(nzo)
            self.q("update jobs set status='import-failed', t_done=?, note=? where nzo=?",
                   (time.time(), msg, nzo), commit=True)

    # -- status UI (served on cfg["ui_listen"], default 127.0.0.1:18088; nginx proxies /feeder/)
    def live_stages(self):
        """nzo -> (stage, percent) for feeder jobs that nzbfast still holds, cached a few seconds."""
        if time.time() - self._live_at < 3:
            return self._live
        live = {}
        try:
            for cat in set(self.cats.values()):
                for s in self.sab(mode="queue", cat=cat)["queue"]["slots"]:
                    st = s["status"] if s["status"] != "Downloading" else s.get("nzbfast_phase") or "Downloading"
                    live[s["nzo_id"]] = (st, int(s.get("percentage") or 0), s.get("timeleft"))
                for s in self.sab(mode="history", cat=cat, limit=1000)["history"]["slots"]:
                    live[s["nzo_id"]] = (s["status"], 100, None)
        except Exception as e:
            log.debug("live stages: %s", e)
        self._live, self._live_at = live, time.time()
        return live

    def status(self, view, limit):
        now = time.time()
        where = {"active": "status in ('queued','importing')",
                 "done": "status='imported'",
                 "problems": "status in ('failed','import-failed','import-rejected')"}.get(view, "1")
        rows = self.q(f"select nzo,key,app,title,indexer,size,status,t_added,t_done,note from jobs where {where} "
                      "order by t_added desc limit ?", (limit,))
        live = self.live_stages() if view in ("active", "all") else {}
        jobs = []
        for nzo, key, app, title, ix, size, st, ta, td, note in rows:
            stage = live.get(nzo) if st == "queued" else None
            jobs.append({"nzo": nzo, "item": self.labels.get(key, key), "app": app, "title": title, "indexer": ix,
                         "size": size, "status": st, "added": ta, "done": td, "note": note,
                         "stage": stage[0] if stage else None, "pct": stage[1] if stage else None,
                         "eta": stage[2] if stage else None})
        counts = dict(self.q("select status, count(*) from jobs group by status"))
        g = self.q("select count(*), coalesce(sum(size),0) from grabs where t > ?", (now - 86400,))[0]
        g1 = self.q("select count(*) from grabs where t > ?", (now - 3600,))[0][0]
        hourly = self.q("select cast((t - ?) / 3600 as int), count(*), coalesce(sum(size),0) from grabs "
                        "where t > ? group by 1", (now - 86400, now - 86400))
        imp = self.q("select cast((t_done - ?) / 3600 as int), count(*) from jobs where status='imported' "
                     "and t_done > ? group by 1", (now - 86400, now - 86400))
        later = {r[0] for r in self.q("select key from items where next_due > ?", (now,))}
        due = sum(1 for x in self.movies + self.seasons if x["key"] not in later)
        return {
            "now": now, "dry": self.dry, "started": self.started, "stats": self.stats,
            "grabs_1h": g1, "grabs_24h": g[0], "bytes_24h": g[1],
            "hourly_grabs": hourly, "hourly_imports": imp,
            "wanted": {"movies": len(self.movies), "seasons": len(self.seasons), "due": due,
                       "refreshed": self.wanted_at},
            "queue": {"jobs": self._q[0], "gb": self._q[1], "target_jobs": self.target_jobs,
                      "target_gb": self.target_gb},
            "indexers": [{"name": ix.name, "available": ix.available(), "blocked_until": ix.blocked_until,
                          "grab_blocked_until": ix.grab_blocked_until, "queries_today": ix.queries,
                          "daily_queries": ix.daily_queries} for ix in self.indexers],
            "counts": counts, "jobs": jobs, "events": list(self.events)[-200:][::-1],
        }

    def serve_ui(self):
        host, _, port = self.cfg.get("ui_listen", "127.0.0.1:18088").rpartition(":")
        feeder = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def send(self, code, ctype, body):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(u.query)
                try:
                    if u.path.endswith("/api/status"):
                        view = qs.get("view", ["active"])[0]
                        limit = min(int(qs.get("limit", ["300"])[0]), 2000)
                        body = json.dumps(feeder.status(view, limit)).encode()
                        return self.send(200, "application/json", body)
                    if u.path in ("/", "/index.html"):
                        return self.send(200, "text/html; charset=utf-8", open(UI_HTML, "rb").read())
                    self.send(404, "text/plain", b"not found")
                except Exception as e:
                    log.exception("ui")
                    self.send(500, "text/plain", str(e).encode())

        srv = ThreadingHTTPServer((host, int(port)), H)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        log.info("status UI on http://%s:%s/", host, port)

    # -- main loop
    def work_items(self):
        now = time.time()
        ms = [m for m in self.movies if self.due(m["key"], now) and m["id"] not in self.busy["radarr"]]
        ss = [s for s in self.seasons if self.due(s["key"], now)]
        # interleave: one movie per four seasons (TV is where the volume is)
        out, i, j = [], 0, 0
        while i < len(ms) or j < len(ss):
            if i < len(ms):
                out.append(ms[i]); i += 1
            out += ss[j:j + 4]; j += 4
        return out

    def process(self, item):
        if STOP.is_set():
            return
        try:
            cover = []
            hits = self.search_item(item, cover)
            # not every indexer answered (daily limits): look again in hours, not a day
            item["retry_s"] = 86400 if all(cover) else 3 * 3600
            self.stats["searched"] += 1
            (self.handle_movie if item["key"].startswith("m:") else self.handle_season)(item, hits)
        except QueueFull:
            self.mark(item["key"], "deferred:queue-full", 120)  # anything already grabbed is busy, so no doubles
        except Exception:
            log.exception("handling %s", item["key"])
            self.mark(item["key"], "error", 1800)

    def gate(self):
        """Block until more work may be started (queue below target, budget left, an indexer up)."""
        while not STOP.is_set():
            if not self.dry:
                if not self.queue_room():
                    if time.time() - self._full_logged > 300:
                        log.info("queue full (%d jobs, %.0f GB); waiting", *self._q)
                        self._full_logged = time.time()
                    STOP.wait(10)
                    continue
                if self.max_grabs_day and self.grabs_today() >= self.max_grabs_day:
                    log.warning("daily grab budget reached; waiting")
                    STOP.wait(600)
                    continue
            if not any(ix.available() for ix in self.indexers):
                log.warning("all indexers unavailable; waiting")
                STOP.wait(120)
                continue
            return

    def run(self, once=False):
        # Continuous pipeline: up to `inflight` items are being searched/grabbed at any time.
        inflight_max = self.cfg.get("inflight", 16)
        self._full_logged = 0.0
        if self.cfg.get("ui_listen", "127.0.0.1:18088"):
            self.serve_ui()
        threading.Thread(target=self.load_blocklists, daemon=True).start()
        if not self.dry:
            threading.Thread(target=self.importer, daemon=True).start()
        while not STOP.is_set():
            try:
                if time.time() - self.wanted_at > self.cfg.get("wanted_refresh_s", 1800):
                    self.refresh_wanted()
                items = self.work_items()
                if not items:
                    if once:
                        return
                    log.info("nothing due; sleeping")
                    STOP.wait(120)
                    continue
                inflight, last_log = set(), time.time()
                for n, item in enumerate(items, 1):
                    while len(inflight) >= inflight_max and not STOP.is_set():
                        done, inflight = wait(inflight, timeout=5, return_when=FIRST_COMPLETED)
                    self.gate()
                    if STOP.is_set():
                        break
                    inflight.add(self.handlers.submit(self.process, item))
                    if time.time() - last_log > 60:
                        last_log = time.time()
                        log.info("progress %d/%d %s grabs24h=%d", n, len(items), self.stats, self.grabs_today())
                    # failed jobs make their items due again: pick those up promptly
                    if time.time() - self.wanted_at > self.cfg.get("wanted_refresh_s", 1800) or \
                            (n % 200 == 0 and self.q("select 1 from items where next_due=0 limit 1")):
                        break
                wait(inflight)
                if once:
                    return
            except Exception:
                log.exception("loop error")
                STOP.wait(60)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-c", "--config", default="/etc/nzbfast/feeder.json")
    ap.add_argument("--dry-run", action="store_true", help="search and validate, never grab")
    ap.add_argument("--once", action="store_true", help="one pass over due items, then exit")
    ap.add_argument("--limit", type=int, default=0, help="only process the first N due items (testing)")
    ap.add_argument("--import-only", action="store_true", help="only run import passes (testing)")
    ap.add_argument("--reimport-rejected", metavar="NZBFAST_BIN", help="tidy and re-queue import-rejected downloads, then exit")
    ap.add_argument("-v", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.v else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    cfg = json.load(open(a.config))
    f = Feeder(cfg, a.dry_run or cfg.get("dry_run", False))
    ring = logging.Handler(logging.INFO)
    ring.emit = lambda r: f.events.append([r.created, r.levelname, r.getMessage()])
    logging.getLogger().addHandler(ring)
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    if a.reimport_rejected:
        f.reimport_rejected(a.reimport_rejected)
        return
    if a.import_only:
        f.refresh_wanted()
        f.importer()
        return
    if a.limit:
        orig = f.work_items
        f.work_items = lambda: orig()[:a.limit]
    f.run(once=a.once or bool(a.limit))


if __name__ == "__main__":
    main()

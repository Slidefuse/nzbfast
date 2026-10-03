#!/usr/bin/env python3
"""Run one benchmark: one client downloads one suite (optionally under a provider
scenario) and the result is written as JSON.

usage: harness.py CLIENT SUITE_OR_SCENARIO TAG [--variant default|tuned]
CLIENT: nzbfast | sab | nzbget
"""
import base64, glob, hashlib, json, os, re, shutil, signal, socket, subprocess, sys, threading, time
import urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.environ.get("BENCH_ROOT", "/root/bench")
MOCK = f"{ROOT}/mock"
OUT = f"{ROOT}/results"
REAL = os.environ.get("REAL_DIR", f"{ROOT}/real")
PROV = f"{ROOT}/prov"
MOCK_STATS = f"{ROOT}/mock-stats.tsv"
MOCK_EPOCH = f"{ROOT}/mock-epoch"
B = "/dev/shm/b"
HZ = os.sysconf("SC_CLK_TCK")
DEADLINE = int(os.environ.get("BENCH_DEADLINE", "3600"))

SUITES = {
    "movies": ["Movie.M0%d.2160p.WEB-DL-BENCH" % i for i in range(1, 7)],
    "tv": ["Show.S01E%02d.1080p.WEB-BENCH" % i for i in range(1, 41)],
    "pp": ["Plain.P0%d.1080p.WEB-BENCH" % i for i in range(1, 5)]
    + ["Comp.C01.1080p-BENCH", "Comp.C02.1080p-BENCH", "SevenZ.Z01.1080p-BENCH", "SevenZ.Z02.1080p-BENCH",
       "Obf.O01.1080p-BENCH", "Obf.O02.1080p-BENCH", "Enc.E01.1080p-BENCH"],
    "repair": ["Repair.R01.1080p-BENCH", "Repair.R02.1080p-BENCH", "Repair.R03.1080p-BENCH", "Dead.D01.1080p-BENCH"],
    "rep1": ["Repair.R02.1080p-BENCH"],
    "dead1": ["Dead.D01.1080p-BENCH"],
    "real": sorted(os.path.basename(f)[:-4] for f in glob.glob(f"{REAL}/*.nzb")),
}

# Mock ports (see run.sh): 5563 serves every article except the damaged releases'
# missing ones; the others each behave like one kind of troublesome provider.
CLEAN, FULL, TAKEDOWN, SLOW, LIMITED, STALL, FLAKY, OUTAGE = 5563, 5570, 5571, 5572, 5573, 5574, 5575, 5576

# name -> (suite, [(server name, port, connections, priority)])
SCENARIOS = {
    # Half the episodes were taken down on the main provider, which takes 500 ms to say
    # "430 no such article"; a backup provider has everything.
    "takedown": ("tv", [("main", TAKEDOWN, 40, 0), ("backup", FULL, 20, 1)]),
    # Two providers of equal priority; one delivers 2 MB/s per connection.
    "slowprov": ("movies", [("fast", CLEAN, 25, 0), ("slow", SLOW, 25, 0)]),
    # The account allows 20 connections; the client is configured with 50.
    "connlimit": ("movies", [("main", LIMITED, 50, 0)]),
    # One of two providers accepts connections but never answers (60 s login stall).
    "deadprov": ("movies", [("good", CLEAN, 25, 0), ("dead", STALL, 25, 0)]),
    # The main provider drops the connection mid-article every 50 articles and answers
    # 412 for 2.5% of message-ids; a backup provider has everything.
    "flaky": ("movies", [("main", FLAKY, 40, 0), ("backup", FULL, 20, 1)]),
    # The only provider refuses and drops every connection from 1 s to 6 s into the run.
    "outage": ("movies", [("main", OUTAGE, 50, 0)]),
}
DEFAULT_SERVERS = [("mock", CLEAN, 50, 0)]
EXPECT_FAIL = {"Dead.D01.1080p-BENCH"}
MD5 = dict(reversed(l.split()) for l in open(f"{ROOT}/corpus.md5")) if os.path.exists(f"{ROOT}/corpus.md5") else {}


def http(url, data=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return body, time.perf_counter() - t


def wait_port(port, timeout=120):
    t = time.time()
    while time.time() - t < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            return time.time() - t
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} not up")


def tree(pid):
    """pid and all its descendants."""
    kids = {}
    for p in os.listdir("/proc"):
        if p.isdigit():
            try:
                st = open(f"/proc/{p}/stat").read()
                kids.setdefault(int(st[st.rfind(")") + 2:].split()[1]), []).append(int(p))
            except OSError:
                pass
    out, stack = [], [pid]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(kids.get(p, []))
    return out


def proc_sample(root):
    """(cpu seconds, rss, threads, anonymous rss) of a process tree."""
    cpu, rss, threads, anon = 0, 0, 0, 0
    for i, p in enumerate(tree(root)):
        try:
            st = open(f"/proc/{p}/stat").read()
            f = st[st.rfind(")") + 2:].split()
            cpu += int(f[11]) + int(f[12])
            if i == 0:
                cpu += int(f[13]) + int(f[14])  # reaped children (unrar, par2, 7z)
            threads += int(f[17])
            for l in open(f"/proc/{p}/status"):
                if l.startswith("VmRSS:"):
                    rss += int(l.split()[1]) * 1024
                elif l.startswith("RssAnon:"):
                    anon += int(l.split()[1]) * 1024
        except (OSError, IndexError):
            pass
    return cpu / HZ, rss, threads, anon


def nic_rx(nic):
    return int(open(f"/sys/class/net/{nic}/statistics/rx_bytes").read())


# ---------------------------------------------------------------- server configs

def nzbfast_servers(servers):
    return "".join(f'''
[[servers]]
name = "{n}"
host = "127.0.0.1"
port = {port}
tls = true
insecure = true
user = "bench"
pass = "bench"
conns = {c}
prio = {prio}
''' for n, port, c, prio in servers)


def sab_servers(servers):
    return "[servers]\n" + "".join(f'''[[{n}]]
name = {n}
displayname = {n}
host = 127.0.0.1
port = {port}
username = bench
password = bench
connections = {c}
ssl = 1
ssl_verify = 0
enable = 1
optional = 0
priority = {prio}
''' for n, port, c, prio in servers)


def nzbget_servers(servers):
    out = []
    for i, (n, port, c, prio) in enumerate(servers, 1):
        for k, v in [("Active", "yes"), ("Name", n), ("Level", prio), ("Optional", "no"), ("Group", 0), ("Host", "127.0.0.1"),
                     ("Encryption", "yes"), ("Port", port), ("Username", "bench"), ("Password", "bench"), ("JoinGroup", "no"),
                     ("Connections", c), ("Retention", 0), ("CertVerification", "none"), ("IpVersion", "auto")]:
            out.append(f"Server{i}.{k}={v}")
    return "\n".join(out) + "\n"


def private(path, text):
    open(path, "w").write(text)
    os.chmod(path, 0o600)


# ---------------------------------------------------------------- clients

class SabApi:
    """The SABnzbd API, spoken by both SABnzbd and nzbfast."""
    port = None

    def api(self, **kw):
        q = urllib.parse.urlencode(dict(apikey="benchkey", output="json", **kw))
        body, dt = http(f"http://127.0.0.1:{self.port}/api?{q}")
        self.lat.append((kw.get("mode"), dt))
        return json.loads(body)

    def add(self, path, name):
        bnd = "----bench" + hashlib.md5(name.encode()).hexdigest()
        body = (f"--{bnd}\r\nContent-Disposition: form-data; name=\"name\"; filename=\"{name}.nzb\"\r\n"
                f"Content-Type: application/x-nzb\r\n\r\n").encode() + open(path, "rb").read() + f"\r\n--{bnd}--\r\n".encode()
        q = urllib.parse.urlencode(dict(apikey="benchkey", output="json", mode="addfile", cat="bench", nzbname=name))
        r, dt = http(f"http://127.0.0.1:{self.port}/api?{q}", body, {"Content-Type": f"multipart/form-data; boundary={bnd}"})
        self.lat.append(("addfile", dt))
        return json.loads(r)

    def status(self):
        q = self.api(mode="queue", limit=1000)["queue"]
        h = self.api(mode="history", limit=1000)["history"]["slots"]
        done = {s["name"]: s for s in h if s["status"] in ("Completed", "Failed")}
        # A job the client paused by itself (SABnzbd: encrypted RAR without a password)
        # never finishes; count it as ended, not completed.
        active = 0
        for s in q.get("slots", []):
            if s.get("status") == "Paused" and s.get("filename"):
                done.setdefault(s["filename"], {"status": "Paused", "fail_message": "paused by client"})
            else:
                active += 1
        return active, done

    def result(self, slot):
        return slot["status"] == "Completed", slot.get("fail_message", "")


class Nzbfast(SabApi):
    name, port = "nzbfast", 18085

    def start(self, variant, servers):
        shutil.rmtree(f"{B}/nf", ignore_errors=True)
        os.makedirs(f"{B}/nf", exist_ok=True)
        base = open(f"{ROOT}/cfg/nzbfast.toml").read()
        if servers:
            srv = nzbfast_servers(servers)
        else:
            srv = open(f"{PROV}/nzbfast-servers.toml").read()
            base = base.replace(f'nic = "lo"', f'nic = "{NIC}"')
        private(f"{B}/nf.toml", base + srv)
        self.proc = subprocess.Popen([f"{ROOT}/bin/nzbfast", "serve", "--config", f"{B}/nf.toml"],
                                     stdout=open(f"{B}/nf.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)
        return wait_port(self.port)

    def done_dir(self):
        return f"{B}/nf/done"


class Sab(SabApi):
    name, port = "sab", 18080

    def start(self, variant, servers):
        shutil.rmtree(f"{B}/sab", ignore_errors=True)
        os.makedirs(f"{B}/sab/admin", exist_ok=True)
        ini = open(f"{ROOT}/cfg/sab.ini.tmpl").read()
        tuned = variant == "tuned"
        ini = ini.replace("CACHE_LINE", "cache_limit = 4G\nreceive_threads = 4" if tuned else "")
        ini = ini.replace("DIRECT_UNPACK_LINE", "direct_unpack = 1" if tuned else "")
        a, b = ini.index("[servers]"), ini.index("[categories]")
        ini = ini[:a] + (sab_servers(servers) if servers else open(f"{PROV}/sab-servers.ini").read()) + ini[b:]
        private(f"{B}/sab/sabnzbd.ini", ini)
        self.proc = subprocess.Popen([f"{ROOT}/sabvenv/bin/python", "-OO", f"{ROOT}/SABnzbd-5.1.3/SABnzbd.py", "-f", f"{B}/sab/sabnzbd.ini",
                                      "-s", f"127.0.0.1:{self.port}", "-b", "0", "--disable-file-log"],
                                     stdout=open(f"{B}/sab.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)
        t = wait_port(self.port)
        while True:
            try:
                self.api(mode="version")
                return t
            except Exception:
                time.sleep(0.05)
                t += 0.05

    def done_dir(self):
        return f"{B}/sab/done"


class Nzbget:
    name, port = "nzbget", 16789

    def rpc(self, method, *params):
        body = json.dumps({"method": method, "params": list(params), "id": 1}).encode()
        r, dt = http(f"http://127.0.0.1:{self.port}/jsonrpc", body,
                     {"Content-Type": "application/json", "Authorization": "Basic " + base64.b64encode(b"bench:bench").decode()})
        self.lat.append((method, dt))
        return json.loads(r)["result"]

    def start(self, variant, servers):
        shutil.rmtree(f"{B}/ng", ignore_errors=True)
        os.makedirs(f"{B}/ng", exist_ok=True)
        conf = open(f"{ROOT}/cfg/nzbget.conf.tmpl").read()
        if variant == "tuned":
            for a, b in [("ArticleCache=100", "ArticleCache=4000"), ("ParBuffer=100", "ParBuffer=2000"), ("PostStrategy=balanced", "PostStrategy=rocket")]:
                conf = conf.replace(f"\n{a}\n", f"\n{b}\n")
        conf = "\n".join(l for l in conf.split("\n") if not l.startswith("Server1."))
        conf += "\n" + (nzbget_servers(servers) if servers else open(f"{PROV}/nzbget-servers.conf").read())
        private(f"{B}/ng/nzbget.conf", conf)
        self.proc = subprocess.Popen([f"{ROOT}/nzbget/nzbget", "-c", f"{B}/ng/nzbget.conf", "-s"],
                                     stdout=open(f"{B}/ng.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)
        t = wait_port(self.port)
        while True:
            try:
                self.rpc("version")
                return t
            except Exception:
                time.sleep(0.05)
                t += 0.05

    def add(self, path, name):
        content = base64.b64encode(open(path, "rb").read()).decode()
        return self.rpc("append", name + ".nzb", content, "bench", 0, False, False, "", 0, "SCORE", [])

    def status(self):
        return len(self.rpc("listgroups", 0)), {x["Name"]: x for x in self.rpc("history", False)}

    def result(self, slot):
        return slot["Status"].startswith("SUCCESS"), slot["Status"]

    def done_dir(self):
        return f"{B}/ng/done"


CLIENTS = {"nzbfast": Nzbfast, "sab": Sab, "nzbget": Nzbget}


# ---------------------------------------------------------------- verification

def md5file(p):
    h = hashlib.md5()
    with open(p, "rb") as f:
        while b := f.read(8 << 20):
            h.update(b)
    return h.hexdigest()


def verify(done_dir, names):
    """Each release's payload is the largest file below a folder carrying its name."""
    files = [p for p in glob.glob(f"{done_dir}/**/*", recursive=True) if os.path.isfile(p)]

    def one(n):
        cands = [p for p in files if n in p]
        if not cands:
            return n, {"found": False}
        p = max(cands, key=os.path.getsize)
        return n, {"found": True, "file": os.path.relpath(p, done_dir), "size": os.path.getsize(p),
                   "md5_ok": (md5file(p) == MD5[n]) if n in MD5 else None, "name_ok": os.path.basename(p) == n + ".mkv"}

    with ThreadPoolExecutor(16) as ex:
        return dict(ex.map(one, names))


def port_series(t0, t1):
    """Bytes the mock sent per port, as (seconds since t0, {port: bytes}) every 0.5 s."""
    if not os.path.exists(MOCK_STATS):
        return None
    ports, rows = [], []
    for l in open(MOCK_STATS):
        if l.startswith("# ports"):
            ports = l.split()[2:]
            continue
        v = l.split()
        if len(v) == len(ports) + 1 and t0 - 0.5 <= float(v[0]) <= t1 + 0.5:
            rows.append((float(v[0]), [int(x) for x in v[1:]]))
    if not rows:
        return None
    base = rows[0][1]
    return {"ports": ports, "series": [(round(t - t0, 1), [b - a for a, b in zip(base, r)]) for i, (t, r) in enumerate(rows) if i % 5 == 0]}


# ---------------------------------------------------------------- run

def main():
    global NIC
    cname, which, tag = sys.argv[1:4]
    variant = sys.argv[sys.argv.index("--variant") + 1] if "--variant" in sys.argv else "default"
    suite, servers = SCENARIOS.get(which, (which, DEFAULT_SERVERS))
    real = suite == "real"
    if real:
        servers = None
    NIC = os.environ.get("BENCH_NIC", "eth0") if real else "lo"
    names = SUITES[suite]
    c = CLIENTS[cname]()
    c.lat = []
    os.makedirs(OUT, exist_ok=True)
    subprocess.run(["sync"])
    open("/proc/sys/vm/drop_caches", "w").write("1\n")

    startup = c.start(variant, servers)
    pid = c.proc.pid
    time.sleep(2)
    idle_cpu0, _, idle_thr, _ = proc_sample(pid)
    time.sleep(10)
    idle_cpu1, idle_rss, _, idle_anon = proc_sample(pid)

    samples = []
    stop = threading.Event()

    def shm_used():
        s = os.statvfs("/dev/shm")
        return (s.f_blocks - s.f_bfree) * s.f_frsize

    def sampler():
        while not stop.is_set():
            cpu, rss, thr, anon = proc_sample(pid)
            samples.append((time.time(), nic_rx(NIC), cpu, rss, thr, shm_used(), anon))
            stop.wait(0.5)

    if real:
        payload = sum(int(x) for n in names for x in re.findall(r'<segment[^>]*bytes="(\d+)"', open(f"{REAL}/{n}.nzb", errors="replace").read()))
    else:
        payload = sum(os.path.getsize(f) for n in names for f in glob.glob(f"{ROOT}/corpus/{n}/*"))
    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    t0 = time.time()
    open(MOCK_EPOCH, "w").write(f"{t0}\n")
    for n in names:
        c.add(f"{REAL if real else MOCK}/{n}.nzb", n)
    t_added = time.time()
    finished = {}
    while time.time() < t0 + DEADLINE:
        try:
            qlen, done = c.status()
        except Exception:
            time.sleep(0.5)
            continue
        for n in names:
            if n in done and n not in finished:
                finished[n] = (time.time() - t0, done[n])
        if len(finished) == len(names) and qlen == 0:
            break
        time.sleep(0.5)
    t1 = time.time()
    time.sleep(1)
    stop.set()
    th.join()
    ver = verify(c.done_dir(), names)
    os.killpg(pid, signal.SIGTERM)
    try:
        c.proc.wait(30)
    except subprocess.TimeoutExpired:
        os.killpg(pid, signal.SIGKILL)

    jobs = {}
    for n in names:
        if n in finished:
            ok, msg = c.result(finished[n][1])
            jobs[n] = {"t": round(finished[n][0], 2), "ok": ok, "msg": msg, **ver[n]}
        else:
            jobs[n] = {"t": None, "ok": False, "msg": "timeout", **ver[n]}
    rx0 = samples[0][1]
    lat = {}
    for m, dt in c.lat:
        lat.setdefault(m, []).append(dt)
    res = {
        "client": cname, "variant": variant, "suite": which, "base_suite": suite, "tag": tag, "payload_bytes": payload,
        "servers": servers, "startup_s": round(startup, 3), "idle_rss": idle_rss, "idle_anon": idle_anon, "idle_threads": idle_thr,
        "idle_cpu_s_per_10s": round(idle_cpu1 - idle_cpu0, 3),
        "wall_s": round(t1 - t0, 2), "add_s": round(t_added - t0, 3), "timed_out": len(finished) < len(names),
        "cpu_s": round(samples[-1][2] - samples[0][2], 2), "peak_rss": max(s[3] for s in samples),
        "peak_anon": max(s[6] for s in samples), "peak_shm": max(s[5] for s in samples) - samples[0][5],
        "rx_bytes": samples[-1][1] - rx0, "jobs": jobs,
        "api_latency_ms": {m: {"n": len(v), "p50": round(sorted(v)[len(v) // 2] * 1e3, 2), "p99": round(sorted(v)[int(len(v) * .99)] * 1e3, 2),
                               "max": round(max(v) * 1e3, 2)} for m, v in lat.items()},
        # (seconds, bytes received, cpu seconds, rss, threads, shm used, anonymous rss)
        "series": [(round(s[0] - t0, 2), s[1] - rx0, round(s[2], 2), s[3], s[4], s[5], s[6]) for s in samples],
        "mock": None if real else port_series(t0, t1),
    }
    fn = f"{OUT}/{which}.{cname}.{variant}.{tag}.json"
    json.dump(res, open(fn, "w"))
    okn = sum(j["ok"] for j in jobs.values())
    md5n = sum(bool(j.get("md5_ok")) for j in jobs.values())
    print(f"{which:9} {cname:8} {variant:7} {tag}: wall {res['wall_s']:7.1f}s  {payload * 8 / res['wall_s'] / 1e9:6.2f} Gbit/s  "
          f"cpu {res['cpu_s']:7.1f}s  peak RSS {res['peak_rss'] / 2**20:6.0f} MiB  ok {okn}/{len(names)}  md5 {md5n}/{len(names)}")


if __name__ == "__main__":
    main()

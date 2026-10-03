#!/usr/bin/env python3
"""Benchmark harness: run one suite of NZBs through one client and record metrics.

usage: harness.py CLIENT SUITE RUN_TAG [--variant default|tuned]
CLIENT: nzbfast | sab | nzbget
"""
import base64, glob, hashlib, json, os, shutil, signal, subprocess, sys, threading, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.environ.get("BENCH_ROOT", "/root/bench")
MOCK = f"{ROOT}/mock"
OUT = f"{ROOT}/results"
B = "/dev/shm/b"
HZ = os.sysconf("SC_CLK_TCK")

SUITES = {
    "movies": ["Movie.M0%d.2160p.WEB-DL-BENCH" % i for i in range(1, 7)],
    "tv": ["Show.S01E%02d.1080p.WEB-BENCH" % i for i in range(1, 41)],
    "pp": ["Plain.P0%d.1080p.WEB-BENCH" % i for i in range(1, 5)]
    + ["Comp.C01.1080p-BENCH", "Comp.C02.1080p-BENCH", "SevenZ.Z01.1080p-BENCH", "SevenZ.Z02.1080p-BENCH",
       "Obf.O01.1080p-BENCH", "Obf.O02.1080p-BENCH", "Enc.E01.1080p-BENCH"],
    "rep1": ["Repair.R02.1080p-BENCH"],
    "dead1": ["Dead.D01.1080p-BENCH"],
    "smoke": ["Show.S01E01.1080p.WEB-BENCH", "Show.S01E02.1080p.WEB-BENCH"],
    "repair": ["Repair.R01.1080p-BENCH", "Repair.R02.1080p-BENCH", "Repair.R03.1080p-BENCH", "Dead.D01.1080p-BENCH"],
}
REAL = os.environ.get("REAL_DIR", f"{ROOT}/real")
REAL_NZBS = sorted(os.path.basename(f)[:-4] for f in glob.glob(f"{REAL}/*.nzb"))
SUITES["real"] = REAL_NZBS
PROV = f"{ROOT}/prov"
IS_REAL = len(sys.argv) > 2 and sys.argv[2] == "real"
NIC = "ens18" if IS_REAL else "lo"
EXPECT_FAIL = {"Dead.D01.1080p-BENCH"}
MD5 = dict(reversed(l.split()) for l in open(f"{ROOT}/corpus.md5"))


def http(url, data=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return body, time.perf_counter() - t


def wait_port(port, timeout=120):
    import socket
    t = time.time()
    while time.time() - t < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            return time.time() - t
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} not up")


def tree(pid):
    """pid and all descendants."""
    kids = {}
    for p in os.listdir("/proc"):
        if p.isdigit():
            try:
                st = open(f"/proc/{p}/stat").read()
                ppid = int(st[st.rfind(")") + 2:].split()[1])
                kids.setdefault(ppid, []).append(int(p))
            except OSError:
                pass
    out, stack = [], [pid]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(kids.get(p, []))
    return out


def proc_sample(root):
    cpu, rss, threads, anon = 0, 0, 0, 0
    for i, p in enumerate(tree(root)):
        try:
            st = open(f"/proc/{p}/stat").read()
            f = st[st.rfind(")") + 2:].split()
            cpu += int(f[11]) + int(f[12])  # utime + stime
            if i == 0:
                cpu += int(f[13]) + int(f[14])  # reaped children
            threads += int(f[17])
            for l in open(f"/proc/{p}/status"):
                if l.startswith("VmRSS:"):
                    rss += int(l.split()[1]) * 1024
                elif l.startswith("RssAnon:"):
                    anon += int(l.split()[1]) * 1024
        except (OSError, IndexError):
            pass
    return cpu / HZ, rss, threads, anon


def lo_rx():
    return int(open(f"/sys/class/net/{NIC}/statistics/rx_bytes").read())


# ---------------------------------------------------------------- clients

class SabApi:
    """SABnzbd API, spoken by both SABnzbd and nzbfast."""
    port = None

    def api(self, **kw):
        q = urllib.parse.urlencode(dict(apikey="benchkey", output="json", **kw))
        body, dt = http(f"http://127.0.0.1:{self.port}/api?{q}")
        self.lat.append((kw.get("mode"), dt))
        return json.loads(body)

    def add(self, path, name):
        bnd = "----bench" + hashlib.md5(name.encode()).hexdigest()
        data = open(path, "rb").read()
        body = (f"--{bnd}\r\nContent-Disposition: form-data; name=\"name\"; filename=\"{name}.nzb\"\r\n"
                f"Content-Type: application/x-nzb\r\n\r\n").encode() + data + f"\r\n--{bnd}--\r\n".encode()
        q = urllib.parse.urlencode(dict(apikey="benchkey", output="json", mode="addfile", cat="bench", nzbname=name))
        r, dt = http(f"http://127.0.0.1:{self.port}/api?{q}", body, {"Content-Type": f"multipart/form-data; boundary={bnd}"})
        self.lat.append(("addfile", dt))
        return json.loads(r)

    def status(self):
        q = self.api(mode="queue", limit=1000)["queue"]
        h = self.api(mode="history", limit=1000)["history"]["slots"]
        done = {s["name"]: s for s in h if s["status"] in ("Completed", "Failed")}
        # A job the client paused itself (SABnzbd: encrypted RAR without password) never
        # finishes on its own; count it as ended, not completed.
        active = 0
        for s in q.get("slots", []):
            if s.get("status") == "Paused" and s.get("filename"):
                done.setdefault(s["filename"], {"status": "Paused", "fail_message": "paused by client (encrypted RAR?)"})
            else:
                active += 1
        return active, done

    def result(self, slot):
        return slot["status"] == "Completed", slot.get("storage") or "", slot.get("fail_message", "")


class Nzbfast(SabApi):
    name, port = "nzbfast", 18085

    def start(self, variant):
        shutil.rmtree(f"{B}/nf", ignore_errors=True)
        os.makedirs(f"{B}/nf", exist_ok=True)
        cfg = f"{ROOT}/cfg/nzbfast.toml"
        if IS_REAL:
            base = open(cfg).read()
            base = base[:base.index("[[servers]]")].replace('nic = "lo"', 'nic = "ens18"')
            cfg = f"{B}/nf.toml"
            open(cfg, "w").write(base + open(f"{PROV}/nzbfast-servers.toml").read() + '\n[[categories]]\nname = "bench"\ndir = "bench"\n')
            os.chmod(cfg, 0o600)
        self.proc = subprocess.Popen([f"{ROOT}/bin/nzbfast", "serve", "--config", cfg],
                                     stdout=open(f"{B}/nf.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)
        return wait_port(self.port)

    def done_dir(self):
        return f"{B}/nf/done"


class Sab(SabApi):
    name, port = "sab", 18080

    def start(self, variant):
        shutil.rmtree(f"{B}/sab", ignore_errors=True)
        os.makedirs(f"{B}/sab/admin", exist_ok=True)
        ini = open(f"{ROOT}/cfg/sab.ini.tmpl").read()
        if variant == "tuned":
            ini = ini.replace("CACHE_LINE", "cache_limit = 4G\nreceive_threads = 4").replace("DIRECT_UNPACK_LINE", "direct_unpack = 1")
        else:
            ini = ini.replace("CACHE_LINE", "").replace("DIRECT_UNPACK_LINE", "")
        if IS_REAL:
            a = ini.index("[servers]"); b = ini.index("[categories]")
            ini = ini[:a] + open(f"{PROV}/sab-servers.ini").read() + ini[b:]
        open(f"{B}/sab/sabnzbd.ini", "w").write(ini)
        os.chmod(f"{B}/sab/sabnzbd.ini", 0o600)
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

    def start(self, variant):
        shutil.rmtree(f"{B}/ng", ignore_errors=True)
        os.makedirs(f"{B}/ng", exist_ok=True)
        conf = open(f"{ROOT}/cfg/nzbget.conf.tmpl").read()
        if variant == "tuned":
            conf = conf.replace("\nArticleCache=100\n", "\nArticleCache=4000\n").replace("\nParBuffer=100\n", "\nParBuffer=2000\n").replace("\nPostStrategy=balanced\n", "\nPostStrategy=rocket\n")
        if IS_REAL:
            conf = "\n".join(l for l in conf.split("\n") if not l.startswith("Server1.")) + "\n" + open(f"{PROV}/nzbget-servers.conf").read()
        open(f"{B}/ng/nzbget.conf", "w").write(conf)
        os.chmod(f"{B}/ng/nzbget.conf", 0o600)
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
        g = self.rpc("listgroups", 0)
        h = self.rpc("history", False)
        done = {x["Name"]: x for x in h}
        return len(g), done

    def result(self, slot):
        ok = slot["Status"].startswith("SUCCESS")
        return ok, slot.get("DestDir", ""), slot["Status"]

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
    """Find each release's payload (largest file under a dir containing the name) and check md5 + name."""
    files = [p for p in glob.glob(f"{done_dir}/**/*", recursive=True) if os.path.isfile(p)]
    out = {}

    def one(n):
        cands = [p for p in files if n in p]
        if not cands:
            return n, {"found": False}
        p = max(cands, key=os.path.getsize)
        return n, {"found": True, "file": os.path.relpath(p, done_dir), "size": os.path.getsize(p),
                   "md5_ok": (md5file(p) == MD5[n]) if n in MD5 else None, "name_ok": os.path.basename(p) == n + ".mkv",
                   "leftovers": sorted(os.path.basename(c) for c in cands if c != p)}

    with ThreadPoolExecutor(16) as ex:
        for n, r in ex.map(one, names):
            out[n] = r
    return out


# ---------------------------------------------------------------- run

def main():
    cname, suite, tag = sys.argv[1:4]
    variant = sys.argv[sys.argv.index("--variant") + 1] if "--variant" in sys.argv else "default"
    names = SUITES[suite]
    c = CLIENTS[cname]()
    c.lat = []
    os.makedirs(OUT, exist_ok=True)
    subprocess.run(["sync"])
    open("/proc/sys/vm/drop_caches", "w").write("1\n")

    t_start = time.time()
    startup = c.start(variant)
    pid = c.proc.pid
    time.sleep(2)
    idle_cpu0, idle_rss, idle_thr, _ = proc_sample(pid)
    time.sleep(10)
    idle_cpu1, idle_rss2, _, idle_anon = proc_sample(pid)

    samples = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            cpu, rss, thr, anon = proc_sample(pid)
            samples.append((time.time(), lo_rx(), cpu, rss, thr, shm_used(), anon))
            stop.wait(0.5)

    def shm_used():
        s = os.statvfs("/dev/shm")
        return (s.f_blocks - s.f_bfree) * s.f_frsize

    if IS_REAL:
        import re
        payload = sum(int(x) for n in names for x in re.findall(r'<segment[^>]*bytes="(\d+)"', open(f"{REAL}/{n}.nzb", errors="replace").read()))
    else:
        payload = sum(os.path.getsize(f) for n in names for f in glob.glob(f"{ROOT}/corpus/{n}/*"))
    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    t0 = time.time()
    for n in names:
        c.add(f"{REAL if IS_REAL else MOCK}/{n}.nzb", n)
    t_added = time.time()
    finished = {}
    deadline = t0 + 3600
    while time.time() < deadline:
        try:
            qlen, done = c.status()
        except Exception as e:
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
            ok, storage, msg = c.result(finished[n][1])
            jobs[n] = {"t": round(finished[n][0], 2), "ok": ok, "msg": msg, **ver[n]}
        else:
            jobs[n] = {"t": None, "ok": False, "msg": "timeout", **ver[n]}
    rx0 = samples[0][1]
    rate = [(round(s[0] - t0, 2), s[1] - rx0, round(s[2], 2), s[3], s[4], s[5], s[6]) for s in samples]
    lat = {}
    for m, dt in c.lat:
        lat.setdefault(m, []).append(dt)
    res = {
        "client": cname, "variant": variant, "suite": suite, "tag": tag, "payload_bytes": payload,
        "startup_s": round(startup, 3), "idle_rss": idle_rss2, "idle_threads": idle_thr,
        "idle_cpu_s_per_10s": round(idle_cpu1 - idle_cpu0, 3),
        "wall_s": round(t1 - t0, 2), "add_s": round(t_added - t0, 3),
        "cpu_s": round(samples[-1][2] - samples[0][2], 2), "peak_rss": max(s[3] for s in samples), "peak_anon": max(s[6] for s in samples), "idle_anon": idle_anon,
        "peak_shm": max(s[5] for s in samples) - samples[0][5],
        "lo_bytes": samples[-1][1] - rx0, "jobs": jobs,
        "api_latency_ms": {m: {"n": len(v), "p50": round(sorted(v)[len(v) // 2] * 1e3, 2), "p99": round(sorted(v)[int(len(v) * .99)] * 1e3, 2), "max": round(max(v) * 1e3, 2)} for m, v in lat.items()},
        "series": rate,
    }
    fn = f"{OUT}/{suite}.{cname}.{variant}.{tag}.json"
    json.dump(res, open(fn, "w"))
    okn = sum(j["ok"] for j in jobs.values())
    md5n = sum(bool(j.get("md5_ok")) for j in jobs.values())
    print(f"{suite:7} {cname:8} {variant:7} {tag}: wall {res['wall_s']:7.1f}s  {payload*8/res['wall_s']/1e9:6.2f} Gbit/s e2e  cpu {res['cpu_s']:7.1f}s  "
          f"peakRSS {res['peak_rss']/2**20:7.0f} MiB  ok {okn}/{len(names)}  md5 {md5n}/{len(names)}  -> {fn}")


if __name__ == "__main__":
    main()

//! 10 Hz telemetry for the web UI: one sampler thread builds a JSON snapshot per
//! tick (shared by every connected browser) and keeps a 5-minute ring of rates so
//! a newly opened page starts with a full graph.

use crate::engine::Engine;
use crate::job;
use crate::outfile;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub const TICK: Duration = Duration::from_millis(100);
const RING: usize = 3000;
/// Active jobs listed per snapshot.
const MAX_JOBS: usize = 100;

struct Sample {
    rx: f32,
    nic: f32,
    srv: Vec<f32>,
}

/// Rates over a sliding window of ticks (bytes and seconds summed, so uneven
/// tick spacing does not distort them).
struct Window {
    ticks: VecDeque<(f64, f64, f64, Vec<f64>)>,
}

impl Window {
    fn push(&mut self, dt: f64, rx: f64, nic: f64, srv: Vec<f64>) {
        if self.ticks.len() == 50 {
            self.ticks.pop_front();
        }
        self.ticks.push_back((dt, rx, nic, srv));
    }

    /// (payload, nic, per server) bytes/s over the last `n` ticks.
    fn rates(&self, n: usize) -> (f64, f64, Vec<f64>) {
        let last: Vec<_> = self.ticks.iter().rev().take(n).collect();
        let t: f64 = last.iter().map(|x| x.0).sum::<f64>().max(1e-3);
        let k = last.first().map(|x| x.3.len()).unwrap_or(0);
        let srv = (0..k).map(|i| last.iter().map(|x| x.3[i]).sum::<f64>() / t).collect();
        (last.iter().map(|x| x.1).sum::<f64>() / t, last.iter().map(|x| x.2).sum::<f64>() / t, srv)
    }
}

/// Job list from the last snapshot that could take the store lock.
#[derive(Default)]
struct JobCache {
    jobs: Vec<Value>,
    q: (usize, usize, u64, usize),
}

pub struct Hub {
    snap: Mutex<(u64, Arc<str>)>,
    cv: Condvar,
    clients: AtomicUsize,
    ring: Mutex<(u64, VecDeque<Sample>)>,
    started_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn read_u64(path: &str) -> u64 {
    std::fs::read_to_string(path).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0)
}

/// (cpu seconds used by this process, resident bytes)
fn proc_usage() -> (f64, u64) {
    let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as u64;
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            // Fields after the parenthesised command name.
            let rest = &s[s.rfind(')')? + 2..];
            let f: Vec<&str> = rest.split(' ').collect();
            Some((f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?) / tck)
        })
        .unwrap_or(0.0);
    let rss = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split(' ').nth(1)?.parse::<u64>().ok()).unwrap_or(0) * page;
    (cpu, rss)
}

fn fs_free(path: &std::path::Path) -> (u64, u64) {
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else { return (0, 0) };
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return (0, 0);
    }
    (s.f_bavail as u64 * s.f_frsize as u64, s.f_blocks as u64 * s.f_frsize as u64)
}

pub fn phase_name(p: u8) -> &'static str {
    match p {
        job::PH_PROBE => "probing",
        job::PH_DOWNLOAD => "downloading",
        job::PH_VERIFY => "verifying",
        job::PH_REPAIR => "repairing",
        job::PH_PAR2 => "fetching par2",
        job::PH_EXTRACT => "extracting",
        _ => "finishing",
    }
}

impl Hub {
    pub fn start(eng: Arc<Engine>) -> Arc<Hub> {
        let hub = Arc::new(Hub {
            snap: Mutex::new((0, Arc::from("{}"))),
            cv: Condvar::new(),
            clients: AtomicUsize::new(0),
            ring: Mutex::new((0, VecDeque::with_capacity(RING))),
            started_ms: now_ms(),
        });
        let h = hub.clone();
        std::thread::Builder::new().name("stats".into()).spawn(move || h.sampler(eng)).unwrap();
        hub
    }

    fn sampler(&self, eng: Arc<Engine>) {
        let n = eng.stats.len();
        let nic_path = format!("/sys/class/net/{}/statistics/rx_bytes", eng.cfg.nic);
        let mut prev: Vec<u64> = eng.stats.iter().map(|s| s.bytes.load(Relaxed)).collect();
        let mut prev_nic = read_u64(&nic_path);
        let mut prev_moved = eng.moved.load(Relaxed);
        let mut last = Instant::now();
        let mut next = Instant::now();
        let mut tick: u64 = 0;
        let (mut cpu_prev, _) = proc_usage();
        let mut cpu_pct = 0.0;
        let mut rss = 0;
        let mut free = (0, 0);
        let mut move_rate = 0.0f64;
        let mut job_rate: HashMap<usize, (u64, f64)> = HashMap::new();
        let mut move_rate_j: HashMap<String, VecDeque<(Instant, u64)>> = HashMap::new();
        let mut win = Window { ticks: VecDeque::with_capacity(50) };
        let mut cache = JobCache::default();
        loop {
            next += TICK;
            let now = Instant::now();
            if next > now {
                std::thread::sleep(next - now);
            } else {
                next = now;
            }
            tick += 1;
            let now = Instant::now();
            let dt = now.duration_since(last).as_secs_f64().max(1e-3);
            last = now;
            let mut srv = Vec::with_capacity(n);
            let mut srv_b = Vec::with_capacity(n);
            let mut total = 0.0;
            for (i, s) in eng.stats.iter().enumerate() {
                let b = s.bytes.load(Relaxed);
                let d = (b - prev[i]) as f64;
                prev[i] = b;
                total += d / dt;
                srv.push(d / dt);
                srv_b.push(d);
            }
            let nb = read_u64(&nic_path);
            let nic_b = nb.saturating_sub(prev_nic) as f64;
            let nic = nic_b / dt;
            prev_nic = nb;
            win.push(dt, total * dt, nic_b, srv_b);
            let (r1, nic1, srv1) = win.rates(10);
            let (r5, nic5, _) = win.rates(50);
            eng.rate5.store(r5 as u64, Relaxed);
            {
                let mut ring = self.ring.lock().unwrap();
                ring.0 = tick;
                if ring.1.len() == RING {
                    ring.1.pop_front();
                }
                ring.1.push_back(Sample { rx: total as f32, nic: nic as f32, srv: srv.iter().map(|x| *x as f32).collect() });
            }
            if tick % 10 == 0 {
                let (c, r) = proc_usage();
                cpu_pct = (c - cpu_prev) * 100.0;
                cpu_prev = c;
                rss = r;
                free = fs_free(&eng.cfg.staging_dir);
                let m = eng.moved.load(Relaxed);
                move_rate = (m - prev_moved) as f64;
                prev_moved = m;
            }
            if self.clients.load(Relaxed) == 0 {
                continue;
            }
            let snap = self.build(&eng, tick, total, nic, (r1, nic1, r5, nic5), &srv, &srv1, cpu_pct, rss, free, move_rate, &mut job_rate, &mut move_rate_j, &mut cache, dt);
            let mut s = self.snap.lock().unwrap();
            *s = (tick, Arc::from(snap.as_str()));
            drop(s);
            self.cv.notify_all();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        &self,
        eng: &Engine,
        tick: u64,
        rx: f64,
        nic: f64,
        (r1, nic1, r5, nic5): (f64, f64, f64, f64),
        srv: &[f64],
        srv1: &[f64],
        cpu: f64,
        rss: u64,
        free: (u64, u64),
        move_rate: f64,
        job_rate: &mut HashMap<usize, (u64, f64)>,
        move_rate_j: &mut HashMap<String, VecDeque<(Instant, u64)>>,
        cache: &mut JobCache,
        dt: f64,
    ) -> String {
        let servers: Vec<Value> = eng
            .stats
            .iter()
            .enumerate()
            .map(|(i, s)| {
                json!([
                    srv1.get(i).copied().unwrap_or(srv[i]).round(),
                    s.live.load(Relaxed),
                    eng.servers[i].conns,
                    (eng.q.hit_rate(i) * 1000.0).round() / 10.0,
                    (eng.q.retry_rate(i) * 1000.0).round() / 10.0,
                    s.ok.load(Relaxed),
                    s.missing.load(Relaxed),
                    s.errors.load(Relaxed),
                    (eng.q.miss_ms(i) * 10.0).round() / 10.0,
                    eng.q.is_down(i) as u8,
                    s.bytes.load(Relaxed),
                    s.crc_errors.load(Relaxed),
                    srv[i].round(),
                ])
            })
            .collect();
        // Telemetry never waits on the store: if an API call holds it, reuse the last list.
        if let Ok(st) = eng.store.try_lock() {
            let mut jobs = vec![];
            let (mut nq, mut nact, mut left, mut npaused) = (0usize, 0usize, 0u64, 0usize);
            let mut seen = Vec::new();
            for e in &st.queue {
                nq += 1;
                let (t, d) = e.progress();
                left += t - d;
                if e.m.paused {
                    npaused += 1;
                }
                let Some(r) = &e.run else { continue };
                nact += 1;
                let id = r.job.id;
                seen.push(id);
                let ent = job_rate.entry(id).or_insert((d, 0.0));
                let inst = (d.saturating_sub(ent.0)) as f64 / dt;
                ent.0 = d;
                ent.1 = ent.1 * 0.9 + inst * 0.1;
                if jobs.len() < MAX_JOBS {
                    jobs.push(json!([
                        e.m.nzo,
                        e.m.name,
                        e.m.cat,
                        if e.m.paused { "paused" } else { phase_name(r.job.phase.load(Relaxed)) },
                        t,
                        d,
                        ent.1.round(),
                        r.job.missing.load(Relaxed),
                        e.m.prio,
                        r.started.elapsed().as_secs(),
                        // Per server: [items in flight, recent miss share %].
                        (0..eng.servers.len()).map(|i| json!([r.job.route.inflight(i), (r.job.route.miss_share(i) * 100.0).round()])).collect::<Vec<_>>(),
                    ]));
                }
            }
            drop(st);
            job_rate.retain(|k, _| seen.contains(k));
            *cache = JobCache { jobs, q: (nq, nact, left, npaused) };
        }
        let (nq, nact, left, npaused) = cache.q;
        let jobs = &cache.jobs;
        // Moves in progress: [nzo, name, cat, done, total, rate over the last 2 s, elapsed s].
        let moves: Vec<Value> = {
            let mv = eng.moves.lock().unwrap();
            move_rate_j.retain(|k, _| mv.iter().any(|p| &p.nzo == k));
            let now = Instant::now();
            mv.iter()
                .map(|p| {
                    let d = p.done.load(Relaxed);
                    let w = move_rate_j.entry(p.nzo.clone()).or_default();
                    w.push_back((now, d));
                    while w.len() > 2 && now.duration_since(w[1].0) >= Duration::from_secs(2) {
                        w.pop_front();
                    }
                    let (t0, d0) = w[0];
                    let span = now.duration_since(t0).as_secs_f64();
                    let rate = if span > 0.05 { (d - d0) as f64 / span } else { 0.0 };
                    json!([p.nzo, p.name, p.cat, d, p.total.max(d), rate.round(), p.t0.elapsed().as_secs()])
                })
                .collect()
        };
        let eta = if r5 > 1.0 { (left as f64 / r5) as u64 } else { 0 };
        let snap = json!({
            "t": now_ms(),
            "k": tick,
            "rx": rx.round(),
            "nic": nic.round(),
            "nic1": nic1.round(),
            "nic5": nic5.round(),
            "r1": r1.round(),
            "r5": r5.round(),
            "srv": servers,
            "q": [nq, nact, left, eta, npaused, eng.q.len()],
            "st": [eng.reserved.load(Relaxed), (eng.cfg.staging_limit_gb * 1e9) as u64, free.0, free.1],
            "mv": [eng.moving.load(Relaxed), move_rate.round(), eng.move_backlog.load(Relaxed)],
            "mvj": moves,
            "s": [eng.stats.iter().map(|s| s.bytes.load(Relaxed)).sum::<u64>(), eng.jobs_ok.load(Relaxed), eng.jobs_failed.load(Relaxed), eng.started.elapsed().as_secs()],
            "sys": [(cpu * 10.0).round() / 10.0, rss, outfile::CHUNKS_LIVE.load(Relaxed) as u64 * outfile::CHUNK, std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)],
            "p": eng.paused.load(Relaxed) as u8,
            "lim": crate::conn::LIMIT.rate.load(Relaxed),
            "jobs": jobs,
            "qv": eng.queue_ver.load(Relaxed),
            "hv": eng.hist_ver.load(Relaxed),
        });
        snap.to_string()
    }

    /// Static information plus the rate history, sent once when a browser connects.
    fn init_event(&self, eng: &Engine) -> String {
        let ring = self.ring.lock().unwrap();
        let n = eng.servers.len();
        let mut srv: Vec<Vec<f32>> = vec![Vec::with_capacity(ring.1.len()); n];
        for s in &ring.1 {
            for (i, v) in s.srv.iter().enumerate() {
                srv[i].push(v.round());
            }
        }
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "tick_ms": TICK.as_millis() as u64,
            "started": self.started_ms,
            "servers": eng.servers.iter().map(|s| json!({
                "name": s.name,
                "host": s.host,
                "via": s.connect,
                "conns": s.conns,
                "prio": s.priority,
            })).collect::<Vec<_>>(),
            "line": crate::api::line_speed(&eng.cfg.nic),
            "ring": {
                "rx": ring.1.iter().map(|s| s.rx.round()).collect::<Vec<_>>(),
                "nic": ring.1.iter().map(|s| s.nic.round()).collect::<Vec<_>>(),
                "srv": srv,
            },
        })
        .to_string()
    }

    /// Serves an SSE stream until the browser goes away.
    pub fn stream(self: &Arc<Self>, eng: &Engine, w: &mut dyn Write) {
        struct Guard<'a>(&'a AtomicUsize);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Relaxed);
            }
        }
        self.clients.fetch_add(1, Relaxed);
        let _g = Guard(&self.clients);
        if w.write_all(format!("retry: 1000\nevent: init\ndata: {}\n\n", self.init_event(eng)).as_bytes()).and_then(|_| w.flush()).is_err() {
            return;
        }
        let mut seen = self.snap.lock().unwrap().0;
        loop {
            let (seq, data) = {
                let g = self.snap.lock().unwrap();
                let (g, _) = self.cv.wait_timeout_while(g, Duration::from_secs(5), |s| s.0 == seen).unwrap();
                (g.0, g.1.clone())
            };
            let r = if seq == seen {
                w.write_all(b": ping\n\n")
            } else {
                seen = seq;
                let mut b = Vec::with_capacity(data.len() + 8);
                b.extend_from_slice(b"data: ");
                b.extend_from_slice(data.as_bytes());
                b.extend_from_slice(b"\n\n");
                w.write_all(&b)
            };
            if r.and_then(|_| w.flush()).is_err() {
                return;
            }
        }
    }
}

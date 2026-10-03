//! Work queues: one shared main queue plus a retry queue per server for articles
//! another server did not have.
//!
//! Routing adapts to what each server actually has: a decaying hit rate is kept per
//! server. Primary servers whose hit rate falls far below the best one stop taking
//! first attempts (they still serve retries and are re-sampled now and then), retries
//! go to the most promising untried server first, and servers that essentially never
//! have anything are skipped so missing articles are declared missing quickly.
//!
//! Each job also learns which servers lack its articles (a release removed from one
//! backbone, or older than its retention): once a server misses half of what it was
//! recently asked for a job, that job's first attempts skip it and its retries go to
//! other servers first, so the server keeps fetching other jobs instead of waiting on
//! slow "430" replies. First attempts are kept in per-job runs so that no single job
//! fills a server's pipelines while other jobs have work: a job that turns out to be
//! missing there then stalls only a share of the connections.
//!
//! Servers that cannot be reached are marked down: they stop taking first attempts,
//! and articles waiting on them are routed elsewhere (or declared missing) after a
//! grace period. When every server is down (a local outage), work simply waits.

use crate::job::Job;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long articles wait for a down server before being routed elsewhere.
const STALL_MS: u64 = 60_000;
/// A server that has never logged in gets no retries once a login has been pending this long.
const LOGIN_GRACE_MS: u64 = 3_000;

fn now_ms() -> u64 {
    static T0: OnceLock<Instant> = OnceLock::new();
    T0.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}

pub struct Work {
    pub job: Arc<Job>,
    pub file: u32,
    pub seg: u32,
    /// Bitmask of server indexes already tried.
    pub tried: u64,
    /// Times this item was in flight when a connection failed.
    pub bounces: u8,
    /// Availability sample: ask with STAT instead of fetching the body.
    pub stat: bool,
    /// Servers in `tried` that were skipped because they lack this job, not asked.
    pub skipped: u64,
}

/// A job's hit/miss tally per server (decaying), deciding which servers it skips.
pub struct JobRoute {
    /// The last answers per server as a bit history (1 = missing), and how many there are.
    recent: [AtomicU64; 64],
    answers: [AtomicU32; 64],
    /// Items handed to each server, and those settled (answered or given back).
    issued: [AtomicU32; 64],
    settled: [AtomicU32; 64],
    /// Items routed past each server since the job started skipping it.
    passed: [AtomicU32; 64],
}

/// A server is skipped for a job once at least SKIP_MIN answers are in and half or more
/// of the last SKIP_WINDOW were "missing": a miss can take a second on some providers
/// while a hit takes tens of milliseconds, so a server lacking half a job is better used
/// for other jobs while one that has it serves this one.
const SKIP_MIN: u32 = 8;
const SKIP_WINDOW: u32 = 32;
/// Until a server has answered SKIP_MIN items of a job, at most this many of them may be
/// in flight there, so a server lacking the job does not fill every connection's
/// pipeline with it before the job has learned to skip it.
const PROBE_INFLIGHT: u32 = SKIP_MIN;

impl Default for JobRoute {
    fn default() -> Self {
        let z = || std::array::from_fn(|_| AtomicU32::new(0));
        JobRoute { recent: std::array::from_fn(|_| AtomicU64::new(0)), answers: z(), issued: z(), settled: z(), passed: z() }
    }
}

impl JobRoute {
    pub fn record(&self, server: usize, hit: bool) {
        self.settle(server);
        if hit && self.lacks(server) {
            // A server this job was skipping has it after all (e.g. a later file): learn anew.
            self.recent[server].store(0, Relaxed);
            self.answers[server].store(0, Relaxed);
            self.passed[server].store(0, Relaxed);
        }
        let _ = self.recent[server].fetch_update(Relaxed, Relaxed, |r| Some(r << 1 | !hit as u64));
        let a = &self.answers[server];
        if a.load(Relaxed) < 64 {
            a.fetch_add(1, Relaxed);
        }
    }

    /// An item handed to `server` came back without an answer about the article.
    pub fn settle(&self, server: usize) {
        self.settled[server].fetch_add(1, Relaxed);
    }

    fn issue(&self, server: usize) {
        self.issued[server].fetch_add(1, Relaxed);
    }

    /// Items of this job in flight on `server`.
    pub fn inflight(&self, server: usize) -> u32 {
        self.issued[server].load(Relaxed).wrapping_sub(self.settled[server].load(Relaxed))
    }

    /// Share of the recent answers from `server` that were "missing" (0 with none yet).
    pub fn miss_share(&self, server: usize) -> f64 {
        let n = self.answers[server].load(Relaxed).min(SKIP_WINDOW);
        if n == 0 {
            return 0.0;
        }
        (self.recent[server].load(Relaxed) & ((1u64 << n) - 1)).count_ones() as f64 / n as f64
    }

    /// `server` may not take more of this job right now: it has not answered enough of
    /// it yet and already has its share in flight, or (`soft`) it holds `limit` items.
    fn capped(&self, server: usize, soft: Option<u32>) -> bool {
        let inflight = self.inflight(server);
        (self.settled[server].load(Relaxed) < SKIP_MIN && inflight >= PROBE_INFLIGHT) || soft.is_some_and(|l| inflight >= l)
    }

    /// The server lacks much of what it was recently asked for this job. Ever fewer items
    /// are still sent there (the 16th, 32nd, 64th... skipped one, then every 1024th): each
    /// costs a "not found" that can take a second and holds up a pipelined connection.
    fn skips(&self, server: usize) -> bool {
        if !self.lacks(server) {
            return false;
        }
        let n = self.passed[server].fetch_add(1, Relaxed) + 1;
        !(n >= 16 && (n.is_power_of_two() || n.is_multiple_of(1024)))
    }

    fn lacks(&self, server: usize) -> bool {
        self.answers[server].load(Relaxed) >= SKIP_MIN && self.miss_share(server) >= 0.5
    }
}

impl Work {
    /// The article is gone on every server that could have it.
    pub fn missing(self, q: &Queues) {
        if self.stat {
            self.job.on_stat(Some(false));
        } else {
            self.job.on_missing(self.file, self.seg, q);
        }
    }

    /// The item is discarded without an answer (job aborted or cancelled).
    pub fn dropped(self, q: &Queues) {
        if self.stat {
            self.job.on_stat(None);
        } else {
            self.job.drop_work(self.file, self.seg, q);
        }
    }
}

/// First attempts, in order, as runs of consecutive items of one job.
#[derive(Default)]
struct Main {
    runs: VecDeque<(Arc<Job>, VecDeque<Work>)>,
    n: usize,
}

impl Main {
    fn push_back(&mut self, w: Work) {
        self.n += 1;
        match self.runs.back_mut() {
            Some((j, r)) if Arc::ptr_eq(j, &w.job) => r.push_back(w),
            _ => self.runs.push_back((w.job.clone(), VecDeque::from([w]))),
        }
    }

    fn push_front(&mut self, w: Work) {
        self.n += 1;
        match self.runs.front_mut() {
            Some((j, r)) if Arc::ptr_eq(j, &w.job) => r.push_front(w),
            _ => self.runs.push_front((w.job.clone(), VecDeque::from([w]))),
        }
    }

    fn is_empty(&self) -> bool {
        self.n == 0
    }
}

struct Q {
    main: Main,
    retry: Vec<VecDeque<Work>>,
    closed: bool,
    paused: bool,
}

/// Decaying hit counter.
#[derive(Default)]
struct Rate {
    hits: AtomicU64,
    tries: AtomicU64,
}

const DECAY_AT: u64 = 2048;

impl Rate {
    fn record(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Relaxed);
        }
        if self.tries.fetch_add(1, Relaxed) + 1 >= DECAY_AT {
            self.tries.store(DECAY_AT / 2, Relaxed);
            self.hits.store(self.hits.load(Relaxed) / 2, Relaxed);
        }
    }

    /// Hit rate, optimistic until `min` samples exist.
    fn rate(&self, min: u64) -> f64 {
        let t = self.tries.load(Relaxed);
        if t < min {
            return 1.0;
        }
        self.hits.load(Relaxed).min(t) as f64 / t as f64
    }
}

#[derive(Default)]
struct Health {
    /// First attempts (main queue).
    first: Rate,
    /// Articles that other servers lacked.
    retry: Rate,
    sample: AtomicU64,
    /// EWMA of how long a "430 no such article" takes to arrive, in microseconds.
    miss_us: AtomicU64,
    /// `now_ms()` when the server was marked unreachable; 0 while it is up.
    down_since: AtomicU64,
    /// A login has succeeded at least once since start.
    ever_up: AtomicBool,
    /// The last connection attempt failed while no session was live.
    suspect: AtomicBool,
    /// `now_ms()` of the first login attempt that is still pending; 0 once one succeeds.
    connecting_since: AtomicU64,
    /// `now_ms()` before which no thread probes the server while it is down.
    next_probe: AtomicU64,
}

impl Health {
    fn rate(&self) -> f64 {
        self.first.rate(128)
    }

    fn retry_rate(&self) -> f64 {
        self.retry.rate(48)
    }

    /// Never fills gaps left by other servers; still sampled 1 in 32 to notice change.
    fn useless_for_retry(&self) -> bool {
        self.retry_rate() < 0.01 && !self.sample.fetch_add(1, Relaxed).is_multiple_of(32)
    }
}

pub struct Queues {
    m: Mutex<Q>,
    cv: Condvar,
    prio: Vec<u32>,
    min_prio: u32,
    health: Vec<Health>,
    /// Per server: the most items of one job it may have in flight while other jobs
    /// have first attempts waiting (an eighth of its pipeline slots).
    job_share: Vec<u32>,
}

impl Queues {
    /// `slots`: connections x pipeline depth of each server.
    pub fn new(prio: Vec<u32>, slots: Vec<u32>) -> Self {
        let n = prio.len();
        Queues {
            m: Mutex::new(Q { main: Main::default(), retry: (0..n).map(|_| VecDeque::new()).collect(), closed: false, paused: false }),
            cv: Condvar::new(),
            min_prio: *prio.iter().min().unwrap_or(&0),
            health: (0..n).map(|_| Health::default()).collect(),
            job_share: slots.iter().map(|&s| (s / 8).max(PROBE_INFLIGHT)).collect(),
            prio,
        }
    }

    pub fn record(&self, server: usize, hit: bool, retry: bool) {
        let h = &self.health[server];
        if retry {
            h.retry.record(hit)
        } else {
            h.first.record(hit)
        }
    }

    /// Records how long a miss took (time since the previous response on that connection).
    pub fn record_miss_time(&self, server: usize, us: u64) {
        let h = &self.health[server].miss_us;
        let old = h.load(Relaxed);
        h.store(if old == 0 { us } else { (old * 7 + us) / 8 }, Relaxed);
    }

    pub fn miss_ms(&self, server: usize) -> f64 {
        self.health[server].miss_us.load(Relaxed) as f64 / 1000.0
    }

    pub fn retry_rate(&self, server: usize) -> f64 {
        self.health[server].retry_rate()
    }

    pub fn hit_rate(&self, server: usize) -> f64 {
        self.health[server].rate()
    }

    pub fn is_down(&self, server: usize) -> bool {
        self.health[server].down_since.load(Relaxed) != 0
    }

    fn down_for(&self, server: usize) -> Option<u64> {
        match self.health[server].down_since.load(Relaxed) {
            0 => None,
            t => Some(now_ms().saturating_sub(t)),
        }
    }

    fn all_down(&self) -> bool {
        (0..self.prio.len()).all(|s| self.is_down(s))
    }
    /// Never logged in since start and failing now (wrong account, IP not allowed, a
    /// server that stalls the login): articles must not wait for it.
    fn unproven(&self, server: usize) -> bool {
        let h = &self.health[server];
        if h.ever_up.load(Relaxed) {
            return false;
        }
        let since = h.connecting_since.load(Relaxed);
        h.suspect.load(Relaxed) || (since != 0 && now_ms().saturating_sub(since) > LOGIN_GRACE_MS)
    }
    /// A connection to `server` is being opened (only tracked until a login succeeds).
    pub fn connecting(&self, server: usize) {
        let h = &self.health[server];
        if !h.ever_up.load(Relaxed) {
            let _ = h.connecting_since.compare_exchange(0, now_ms(), Relaxed, Relaxed);
        }
    }
    pub fn login_ok(&self, server: usize) {
        let h = &self.health[server];
        h.ever_up.store(true, Relaxed);
        h.suspect.store(false, Relaxed);
        h.connecting_since.store(0, Relaxed);
    }
    pub fn login_failed(&self, server: usize, live: u64) {
        if live == 0 {
            self.health[server].suspect.store(true, Relaxed);
        }
    }

    /// Claims the next reachability probe of a down server: one per second for the first
    /// minute (short outages end quickly), then one every ten seconds.
    pub fn claim_probe(&self, server: usize) -> bool {
        let h = &self.health[server];
        let now = now_ms();
        let next = h.next_probe.load(Relaxed);
        let every = if self.down_for(server).unwrap_or(0) < 60_000 { 1000 } else { 10_000 };
        now >= next && h.next_probe.compare_exchange(next, now + every, Relaxed, Relaxed).is_ok()
    }

    /// Marks a server unreachable (or reachable again).
    pub fn set_down(&self, server: usize, down: bool) {
        let h = &self.health[server].down_since;
        if down {
            let _ = h.compare_exchange(0, now_ms(), Relaxed, Relaxed);
        } else {
            h.store(0, Relaxed);
            self.cv.notify_all();
        }
    }

    /// Whether `server` should take first attempts from the main queue right now.
    fn takes_main(&self, server: usize) -> bool {
        if self.is_down(server) {
            return false;
        }
        // Lower-priority (backup) servers fill in when every primary server is down.
        let up_prio = (0..self.prio.len()).filter(|&s| !self.is_down(s)).map(|s| self.prio[s]).min().unwrap_or(self.min_prio);
        if self.prio[server] != up_prio {
            return false;
        }
        let best = (0..self.prio.len()).filter(|&s| self.prio[s] == up_prio && !self.is_down(s)).map(|s| self.health[s].rate()).fold(0.0, f64::max);
        let h = &self.health[server];
        // Miss latency is reported but not used: on long links the gap between
        // responses is dominated by transfer time, not by the server's lookup.
        h.rate() >= 0.5 * best || h.sample.fetch_add(1, Relaxed).is_multiple_of(32)
    }

    pub fn push_back(&self, items: impl IntoIterator<Item = Work>) {
        let mut q = self.m.lock().unwrap();
        for w in items {
            q.main.push_back(w);
        }
        drop(q);
        self.cv.notify_all();
    }

    pub fn push_front(&self, items: Vec<Work>) {
        let mut q = self.m.lock().unwrap();
        for w in items.into_iter().rev() {
            q.main.push_front(w);
        }
        drop(q);
        self.cv.notify_all();
    }

    /// Returns work items to the queue after a connection failure. An item that was
    /// in flight through several failures (e.g. one the server chokes on) is not
    /// tried on that server again; items with no server left are returned.
    pub fn give_back(&self, server: usize, items: Vec<Work>) -> Vec<Work> {
        let mut gone = vec![];
        let mut back = vec![];
        for mut w in items {
            w.job.route.settle(server);
            w.bounces = w.bounces.saturating_add(1);
            if w.bounces >= 3 {
                w.bounces = 0;
                if let Some(w) = self.retry_elsewhere(server, w) {
                    gone.push(w);
                }
            } else {
                back.push(w);
            }
        }
        let mut q = self.m.lock().unwrap();
        for w in back.into_iter().rev() {
            if w.tried == 0 {
                let b = w.job.seg_bytes(w.file, w.seg);
                let _ = w.job.taken.fetch_update(Relaxed, Relaxed, |t| Some(t.saturating_sub(b)));
                q.main.push_front(w);
            } else {
                q.retry[server].push_front(w);
            }
        }
        drop(q);
        self.cv.notify_all();
        gone
    }

    /// Picks the server an article should be tried on next, given the servers in
    /// `w.tried`. Up servers come first (by priority, then by how often they fill
    /// gaps); an article may wait for a server that went down recently, or for any
    /// server while all of them are down. `None` means the article is missing.
    fn route(&self, w: &Work) -> Option<usize> {
        let untried = |s: &usize| w.tried & (1u64 << s) == 0;
        let best_up =
            (0..self.prio.len()).filter(|s| untried(s) && !self.is_down(*s) && !self.unproven(*s) && !self.health[*s].useless_for_retry()).min_by(|&a, &b| {
                let (ra, rb) = (self.health[a].retry_rate(), self.health[b].retry_rate());
                let lacks = |s: usize| w.job.route.lacks(s);
                self.prio[a].cmp(&self.prio[b]).then(lacks(a).cmp(&lacks(b))).then(rb.partial_cmp(&ra).unwrap())
            });
        if best_up.is_some() {
            return best_up;
        }
        let none_usable = (0..self.prio.len()).all(|s| self.is_down(s) || self.unproven(s));
        (0..self.prio.len())
            .filter(|s| untried(s) && (self.is_down(*s) || self.unproven(*s)))
            .find(|&s| none_usable || (!self.unproven(s) && self.down_for(s).unwrap_or(0) < STALL_MS))
    }

    /// Routes an article that `server` lacked to the most promising untried server.
    /// Returns the work back if no useful server is left.
    pub fn retry_elsewhere(&self, server: usize, mut w: Work) -> Option<Work> {
        w.tried |= 1u64 << server;
        let mut to = self.route(&w);
        if to.is_none() && w.skipped != 0 {
            // Nobody else has it: ask the servers that were skipped after all.
            w.tried &= !w.skipped;
            w.skipped = 0;
            to = self.route(&w);
        }
        match to {
            Some(s) => {
                let mut q = self.m.lock().unwrap();
                q.retry[s].push_back(w);
                drop(q);
                self.cv.notify_all();
                None
            }
            None => Some(w),
        }
    }

    /// Moves articles off servers that have been down longer than the grace period.
    /// Returns the articles no other server can provide (the caller declares them missing).
    pub fn sweep(&self) -> Vec<Work> {
        if self.all_down() {
            return vec![];
        }
        let mut out = vec![];
        for s in 0..self.prio.len() {
            if !self.unproven(s) && self.down_for(s).is_none_or(|t| t < STALL_MS) {
                continue;
            }
            let items: Vec<Work> = self.m.lock().unwrap().retry[s].drain(..).collect();
            let mut main_back = vec![];
            for w in items {
                if w.tried == 0 {
                    main_back.push(w);
                } else if let Some(w) = self.retry_elsewhere(s, w) {
                    out.push(w);
                }
            }
            if !main_back.is_empty() {
                self.push_front(main_back);
            }
        }
        out
    }

    /// Removes every queued item of `job` (for pausing or cancelling it).
    pub fn purge(&self, job: &Arc<Job>) -> Vec<Work> {
        let mut q = self.m.lock().unwrap();
        let mut out = vec![];
        let main = std::mem::take(&mut q.main);
        for (j, mut r) in main.runs {
            if Arc::ptr_eq(&j, job) {
                out.extend(r.drain(..));
            } else {
                q.main.n += r.len();
                q.main.runs.push_back((j, r));
            }
        }
        let mut split = |dq: &mut VecDeque<Work>| {
            if !dq.iter().any(|w| Arc::ptr_eq(&w.job, job)) {
                return;
            }
            for w in std::mem::take(dq) {
                if Arc::ptr_eq(&w.job, job) {
                    out.push(w);
                } else {
                    dq.push_back(w);
                }
            }
        };
        for r in q.retry.iter_mut() {
            split(r);
        }
        out
    }

    /// Stops (or restarts) handing out work; in-flight requests still complete.
    pub fn set_paused(&self, paused: bool) {
        self.m.lock().unwrap().paused = paused;
        self.cv.notify_all();
    }

    pub fn pop(&self, server: usize, wait: Option<Duration>) -> Option<Work> {
        let deadline = wait.map(|w| Instant::now() + w);
        let mut q = self.m.lock().unwrap();
        loop {
            if !q.paused {
                if let Some(w) = q.retry[server].pop_front() {
                    w.job.route.issue(server);
                    return Some(w);
                }
                if !q.main.is_empty() && self.takes_main(server) {
                    match self.pop_main(&mut q, server) {
                        Some(w) => return Some(w),
                        // Only jobs this server may not take more of right now: look
                        // again soon (answers free their share without a wakeup).
                        None if !q.main.is_empty() => {
                            let d = deadline?;
                            let now = Instant::now();
                            if now >= d {
                                return None;
                            }
                            q = self.cv.wait_timeout(q, (d - now).min(Duration::from_millis(20))).unwrap().0;
                            continue;
                        }
                        None => continue,
                    }
                }
            }
            if q.closed {
                return None;
            }
            let now = Instant::now();
            let d = deadline?;
            if now >= d {
                return None;
            }
            q = self.cv.wait_timeout(q, d - now).unwrap().0;
        }
    }

    /// Takes the first item of the first job `server` may take more of (and that has
    /// staging granted for it). Items of jobs that skip this server are routed to their
    /// next server on the way.
    fn pop_main(&self, q: &mut Q, server: usize) -> Option<Work> {
        let w = self.pop_main_capped(q, server, Some(self.job_share[server]));
        // No other job to spread over: one job may fill the pipelines.
        w.or_else(|| self.pop_main_capped(q, server, None))
    }

    fn pop_main_capped(&self, q: &mut Q, server: usize, share: Option<u32>) -> Option<Work> {
        let bit = 1u64 << server;
        let mut moved = false;
        let mut i = 0;
        let mut out = None;
        while i < q.main.runs.len() {
            let job = q.main.runs[i].0.clone();
            if job.route.capped(server, share) || job.taken.load(Relaxed) >= job.grant.load(Relaxed) {
                i += 1;
                continue;
            }
            while let Some(mut w) = q.main.runs[i].1.pop_front() {
                q.main.n -= 1;
                job.taken.fetch_add(job.seg_bytes(w.file, w.seg), Relaxed);
                if job.route.skips(server) {
                    w.tried |= bit;
                    w.skipped |= bit;
                    if let Some(t) = self.route(&w).filter(|&t| t != server) {
                        q.retry[t].push_back(w);
                        moved = true;
                        continue;
                    }
                    w.tried &= !bit;
                    w.skipped &= !bit;
                }
                job.route.issue(server);
                out = Some(w);
                break;
            }
            if q.main.runs[i].1.is_empty() {
                q.main.runs.remove(i);
            }
            if out.is_some() {
                break;
            }
        }
        if moved {
            self.cv.notify_all();
        }
        out
    }

    /// Waits until `pop(server)` would return work, without taking it.
    pub fn wait_work(&self, server: usize, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        let mut q = self.m.lock().unwrap();
        loop {
            if !q.paused && (!q.retry[server].is_empty() || (!q.main.is_empty() && self.takes_main(server))) {
                return true;
            }
            if q.closed {
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            q = self.cv.wait_timeout(q, deadline - now).unwrap().0;
        }
    }

    /// Articles not yet tried anywhere (what every server draws first attempts from).
    pub fn main_len(&self) -> usize {
        self.m.lock().unwrap().main.n
    }

    pub fn len(&self) -> usize {
        let q = self.m.lock().unwrap();
        q.main.n + q.retry.iter().map(|r| r.len()).sum::<usize>()
    }

    pub fn close(&self) {
        self.m.lock().unwrap().closed = true;
        self.cv.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.m.lock().unwrap().closed
    }
}

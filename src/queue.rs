//! Work queues: one shared main queue plus a retry queue per server for articles
//! another server did not have.
//!
//! Routing adapts to what each server actually has: a decaying hit rate is kept per
//! server. Primary servers whose hit rate falls far below the best one stop taking
//! first attempts (they still serve retries and are re-sampled now and then), retries
//! go to the most promising untried server first, and servers that essentially never
//! have anything are skipped so missing articles are declared missing quickly.
//!
//! Servers that cannot be reached are marked down: they stop taking first attempts,
//! and articles waiting on them are routed elsewhere (or declared missing) after a
//! grace period. When every server is down (a local outage), work simply waits.

use crate::job::Job;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long articles wait for a down server before being routed elsewhere.
const STALL_MS: u64 = 60_000;

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
}

struct Q {
    main: VecDeque<Work>,
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
        self.retry_rate() < 0.01 && self.sample.fetch_add(1, Relaxed) % 32 != 0
    }
}

pub struct Queues {
    m: Mutex<Q>,
    cv: Condvar,
    prio: Vec<u32>,
    min_prio: u32,
    health: Vec<Health>,
}

impl Queues {
    pub fn new(prio: Vec<u32>) -> Self {
        let n = prio.len();
        Queues {
            m: Mutex::new(Q { main: VecDeque::new(), retry: (0..n).map(|_| VecDeque::new()).collect(), closed: false, paused: false }),
            cv: Condvar::new(),
            min_prio: *prio.iter().min().unwrap_or(&0),
            health: (0..n).map(|_| Health::default()).collect(),
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
        let best = (0..self.prio.len())
            .filter(|&s| self.prio[s] == up_prio && !self.is_down(s))
            .map(|s| self.health[s].rate())
            .fold(0.0, f64::max);
        let h = &self.health[server];
        // Miss latency is reported but not used: on long links the gap between
        // responses is dominated by transfer time, not by the server's lookup.
        h.rate() >= 0.5 * best || h.sample.fetch_add(1, Relaxed) % 32 == 0
    }

    pub fn push_back(&self, items: impl IntoIterator<Item = Work>) {
        let mut q = self.m.lock().unwrap();
        q.main.extend(items);
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

    /// Returns work items to the queue after a connection failure.
    pub fn give_back(&self, server: usize, items: Vec<Work>) {
        let mut q = self.m.lock().unwrap();
        for w in items.into_iter().rev() {
            if w.tried == 0 {
                q.main.push_front(w);
            } else {
                q.retry[server].push_front(w);
            }
        }
        drop(q);
        self.cv.notify_all();
    }

    /// Picks the server an article should be tried on next, given the servers in
    /// `w.tried`. Up servers come first (by priority, then by how often they fill
    /// gaps); an article may wait for a server that went down recently, or for any
    /// server while all of them are down. `None` means the article is missing.
    fn route(&self, w: &Work) -> Option<usize> {
        let untried = |s: &usize| w.tried & (1u64 << s) == 0;
        let best_up = (0..self.prio.len())
            .filter(|s| untried(s) && !self.is_down(*s) && !self.health[*s].useless_for_retry())
            .min_by(|&a, &b| {
                let (ra, rb) = (self.health[a].retry_rate(), self.health[b].retry_rate());
                self.prio[a].cmp(&self.prio[b]).then(rb.partial_cmp(&ra).unwrap())
            });
        if best_up.is_some() {
            return best_up;
        }
        let all_down = self.all_down();
        (0..self.prio.len()).filter(|s| untried(s) && self.is_down(*s)).find(|&s| all_down || self.down_for(s).unwrap_or(0) < STALL_MS)
    }

    /// Routes an article that `server` lacked to the most promising untried server.
    /// Returns the work back if no useful server is left.
    pub fn retry_elsewhere(&self, server: usize, mut w: Work) -> Option<Work> {
        w.tried |= 1u64 << server;
        match self.route(&w) {
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
            if self.down_for(s).is_none_or(|t| t < STALL_MS) {
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
        split(&mut q.main);
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
                    return Some(w);
                }
                if !q.main.is_empty() && self.takes_main(server) {
                    return q.main.pop_front();
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

    pub fn len(&self) -> usize {
        let q = self.m.lock().unwrap();
        q.main.len() + q.retry.iter().map(|r| r.len()).sum::<usize>()
    }

    pub fn close(&self) {
        self.m.lock().unwrap().closed = true;
        self.cv.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.m.lock().unwrap().closed
    }
}

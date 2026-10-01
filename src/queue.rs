//! Work queues: one shared main queue plus a retry queue per server for articles
//! another server did not have.
//!
//! Routing adapts to what each server actually has: a decaying hit rate is kept per
//! server. Primary servers whose hit rate falls far below the best one stop taking
//! first attempts (they still serve retries and are re-sampled now and then), retries
//! go to the most promising untried server first, and servers that essentially never
//! have anything are skipped so missing articles are declared missing quickly.

use crate::job::Job;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

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
            m: Mutex::new(Q { main: VecDeque::new(), retry: (0..n).map(|_| VecDeque::new()).collect(), closed: false }),
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

    pub fn retry_rate(&self, server: usize) -> f64 {
        self.health[server].retry_rate()
    }

    pub fn hit_rate(&self, server: usize) -> f64 {
        self.health[server].rate()
    }

    /// Whether `server` should take first attempts from the main queue right now.
    fn takes_main(&self, server: usize) -> bool {
        if self.prio[server] != self.min_prio {
            return false;
        }
        let best = (0..self.prio.len()).filter(|&s| self.prio[s] == self.min_prio).map(|s| self.health[s].rate()).fold(0.0, f64::max);
        let h = &self.health[server];
        h.rate() >= 0.85 * best || h.sample.fetch_add(1, Relaxed) % 32 == 0
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

    /// Routes an article that `server` lacked to the most promising untried server.
    /// Returns the work back if no useful server is left.
    pub fn retry_elsewhere(&self, server: usize, mut w: Work) -> Option<Work> {
        w.tried |= 1u64 << server;
        let next = (0..self.prio.len())
            .filter(|&s| w.tried & (1u64 << s) == 0 && !self.health[s].useless_for_retry())
            .min_by(|&a, &b| {
                let (ra, rb) = (self.health[a].retry_rate(), self.health[b].retry_rate());
                self.prio[a].cmp(&self.prio[b]).then(rb.partial_cmp(&ra).unwrap())
            });
        match next {
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

    pub fn pop(&self, server: usize, wait: Option<Duration>) -> Option<Work> {
        let mut q = self.m.lock().unwrap();
        loop {
            if let Some(w) = q.retry[server].pop_front() {
                return Some(w);
            }
            if !q.main.is_empty() && self.takes_main(server) {
                return q.main.pop_front();
            }
            if q.closed {
                return None;
            }
            let Some(t) = wait else { return None };
            let (g, to) = self.cv.wait_timeout(q, t).unwrap();
            q = g;
            if to.timed_out() {
                if let Some(w) = q.retry[server].pop_front() {
                    return Some(w);
                }
                return None;
            }
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

//! Work queues: one shared main queue (served by primary servers) plus a retry
//! queue per server for articles another server did not have.

use crate::job::Job;
use std::collections::VecDeque;
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

pub struct Queues {
    m: Mutex<Q>,
    cv: Condvar,
    primary: Vec<bool>,
    /// Server indexes in fallback order (priority, then config order).
    order: Vec<usize>,
}

impl Queues {
    pub fn new(primary: Vec<bool>, order: Vec<usize>) -> Self {
        let n = primary.len();
        Queues {
            m: Mutex::new(Q { main: VecDeque::new(), retry: (0..n).map(|_| VecDeque::new()).collect(), closed: false }),
            cv: Condvar::new(),
            primary,
            order,
        }
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

    /// Returns a work item to the queue it came from after a connection failure.
    pub fn give_back(&self, server: usize, items: Vec<Work>) {
        let mut q = self.m.lock().unwrap();
        for w in items.into_iter().rev() {
            if self.primary[server] && w.tried & !(1u64 << server) == 0 {
                q.main.push_front(w);
            } else {
                q.retry[server].push_front(w);
            }
        }
        drop(q);
        self.cv.notify_all();
    }

    /// Routes an article that `server` lacked to the next untried server.
    /// Returns the work back if every server has been tried.
    pub fn retry_elsewhere(&self, server: usize, mut w: Work) -> Option<Work> {
        w.tried |= 1u64 << server;
        let next = self.order.iter().copied().find(|&s| w.tried & (1u64 << s) == 0);
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
            if self.primary[server] {
                if let Some(w) = q.main.pop_front() {
                    return Some(w);
                }
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
                if self.primary[server] {
                    return q.main.pop_front();
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

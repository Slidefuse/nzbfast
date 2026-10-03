//! Output files assembled in RAM in aligned chunks and written with O_DIRECT once
//! each chunk is complete. Connection threads never take the inode lock; a small
//! I/O pool issues large sequential writes straight to the device.

use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};

pub const CHUNK: u64 = 8 << 20;
const ALIGN: usize = 4096;

#[cfg(target_os = "linux")]
pub const O_DIRECT: i32 = libc::O_DIRECT;
#[cfg(not(target_os = "linux"))]
pub const O_DIRECT: i32 = 0;

/// Reserves the file's blocks up front (falls back to a sparse resize).
fn preallocate(file: &File, size: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as i64) } == 0 {
        return Ok(());
    }
    file.set_len(size)
}

struct ABuf(*mut u8);
unsafe impl Send for ABuf {}
unsafe impl Sync for ABuf {}

fn layout() -> Layout {
    Layout::from_size_align(CHUNK as usize, ALIGN).unwrap()
}

static POOL: Mutex<Vec<ABuf>> = Mutex::new(Vec::new());
pub static CHUNKS_LIVE: AtomicUsize = AtomicUsize::new(0);
/// Write-buffer budget in chunks (128 = 1 GiB): bounds complete chunks queued for disk
/// (the I/O channels block when full, which throttles downloading on a slow disk) and the
/// free buffers kept for reuse. Partial chunks are not counted: there is roughly one per
/// article in flight, and they only complete as more articles arrive.
static BUDGET: AtomicUsize = AtomicUsize::new(128);

/// Sets the write-buffer budget in MiB (0 = 1/16 of RAM, 256 MiB..4 GiB; minimum 2
/// chunks). Call before `start_io`. Returns the budget in MiB.
pub fn set_budget_mb(mb: usize) -> usize {
    let mb = if mb > 0 { mb } else { (total_ram_mb() / 16).clamp(256, 4096) };
    BUDGET.store((mb << 20).div_ceil(CHUNK as usize).max(2), Relaxed);
    mb
}

fn total_ram_mb() -> usize {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| m.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|k| k.parse::<usize>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(8192)
}

fn get_buf() -> ABuf {
    CHUNKS_LIVE.fetch_add(1, Relaxed);
    if let Some(b) = POOL.lock().unwrap().pop() {
        return b;
    }
    let p = unsafe { alloc(layout()) };
    assert!(!p.is_null(), "out of memory");
    ABuf(p)
}

fn put_buf(b: ABuf) {
    CHUNKS_LIVE.fetch_sub(1, Relaxed);
    let mut p = POOL.lock().unwrap();
    if p.len() < BUDGET.load(Relaxed) {
        p.push(b);
    } else {
        unsafe { dealloc(b.0, layout()) };
    }
}

struct Chunk {
    idx: u64,
    len: u64,
    buf: ABuf,
    filled: AtomicU64,
    submitted: AtomicBool,
}

/// Something waiting on output I/O (a job) — notified when each chunk write completes.
pub trait IoOwner: Send + Sync {
    fn io_done(&self);
    fn io_started(&self);
}

pub struct OutFile {
    pub file: File,
    pub size: u64,
    direct: bool,
    chunks: Mutex<HashMap<u64, Arc<Chunk>>>,
    pub write_errors: AtomicUsize,
}

struct IoReq {
    out: Arc<OutFile>,
    chunk: Arc<Chunk>,
    owner: Arc<dyn IoOwner>,
}

struct IoPool {
    txs: Vec<SyncSender<IoReq>>,
    rr: AtomicUsize,
}

static IO: OnceLock<IoPool> = OnceLock::new();

pub fn start_io(threads: usize) {
    let mut txs = vec![];
    let depth = (BUDGET.load(Relaxed) / threads.max(1)).max(2);
    for _ in 0..threads {
        let (tx, rx) = sync_channel::<IoReq>(depth);
        txs.push(tx);
        std::thread::spawn(move || io_thread(rx));
    }
    let _ = IO.set(IoPool { txs, rr: AtomicUsize::new(0) });
}

fn io_thread(rx: Receiver<IoReq>) {
    while let Ok(r) = rx.recv() {
        r.out.write_chunk(&r.chunk);
        r.owner.io_done();
    }
}

impl OutFile {
    pub fn create(path: &Path, size: u64, direct: bool) -> std::io::Result<Arc<OutFile>> {
        let mut o = OpenOptions::new();
        o.create(true).write(true).read(true).truncate(true);
        let (file, direct) = if direct && size > 0 && O_DIRECT != 0 {
            match OpenOptions::new().create(true).write(true).read(true).truncate(true).custom_flags(O_DIRECT).open(path) {
                Ok(f) => (f, true),
                Err(_) => (o.open(path)?, false),
            }
        } else {
            (o.open(path)?, false)
        };
        if size > 0 {
            preallocate(&file, size)?;
        }
        Ok(Arc::new(OutFile { file, size, direct, chunks: Mutex::new(HashMap::new()), write_errors: AtomicUsize::new(0) }))
    }

    fn chunk_len(&self, idx: u64) -> u64 {
        (self.size - idx * CHUNK).min(CHUNK)
    }

    /// Copies `data` to file offset `off`. Completed chunks are queued for I/O.
    pub fn write(self: &Arc<Self>, off: u64, data: &[u8], owner: &Arc<dyn IoOwner>) {
        if self.size == 0 || off + data.len() as u64 > self.size {
            if self.file.write_all_at(data, off).is_err() {
                self.write_errors.fetch_add(1, Relaxed);
            }
            return;
        }
        let mut pos = off;
        let mut d = data;
        while !d.is_empty() {
            let idx = pos / CHUNK;
            let in_off = pos - idx * CHUNK;
            let n = (d.len() as u64).min(self.chunk_len(idx) - in_off) as usize;
            let ch = {
                let mut m = self.chunks.lock().unwrap();
                m.entry(idx)
                    .or_insert_with(|| {
                        Arc::new(Chunk { idx, len: self.chunk_len(idx), buf: get_buf(), filled: AtomicU64::new(0), submitted: AtomicBool::new(false) })
                    })
                    .clone()
            };
            // SAFETY: articles cover disjoint byte ranges, so concurrent copies never overlap.
            unsafe { std::ptr::copy_nonoverlapping(d.as_ptr(), ch.buf.0.add(in_off as usize), n) };
            let prev = ch.filled.fetch_add(n as u64, AcqRel);
            if prev + n as u64 >= ch.len {
                self.submit(ch, owner);
            }
            pos += n as u64;
            d = &d[n..];
        }
    }

    fn submit(self: &Arc<Self>, ch: Arc<Chunk>, owner: &Arc<dyn IoOwner>) {
        if ch.submitted.swap(true, AcqRel) {
            return;
        }
        self.chunks.lock().unwrap().remove(&ch.idx);
        owner.io_started();
        let io = IO.get().expect("io pool");
        // O_DIRECT writes run in parallel; buffered writes to one file serialize on the
        // inode lock anyway, so keep each file on one I/O thread.
        let i = if self.direct { io.rr.fetch_add(1, Relaxed) % io.txs.len() } else { (Arc::as_ptr(self) as usize >> 6) % io.txs.len() };
        let _ = io.txs[i].send(IoReq { out: self.clone(), chunk: ch, owner: owner.clone() });
    }

    /// Submits all partially filled chunks (missing data stays zero).
    pub fn flush_partial(self: &Arc<Self>, owner: &Arc<dyn IoOwner>) {
        let all: Vec<Arc<Chunk>> = self.chunks.lock().unwrap().values().cloned().collect();
        for ch in all {
            self.submit(ch, owner);
        }
    }

    fn write_chunk(&self, ch: &Chunk) {
        let mut len = ch.len as usize;
        if self.direct {
            len = len.div_ceil(ALIGN) * ALIGN;
        }
        // A partial chunk (missing articles) may contain stale bytes from a recycled
        // buffer in its gaps; such files fail verification and need par2 repair anyway.
        let s = unsafe { std::slice::from_raw_parts(ch.buf.0, len) };
        if self.file.write_all_at(s, ch.idx * CHUNK).is_err() {
            self.write_errors.fetch_add(1, Relaxed);
        }
        // The chunk was removed from the map and fully written; nobody touches the buffer again.
        put_buf(ABuf(ch.buf.0));
    }

    /// Trims O_DIRECT padding past the real end of file.
    pub fn finish(&self) {
        if self.direct && !self.size.is_multiple_of(ALIGN as u64) {
            let _ = self.file.set_len(self.size);
        }
        let _ = self.file.sync_data();
    }
}

//! Service mode: a persistent job queue fed through the SABnzbd-compatible API, a
//! scheduler that keeps the download pipeline full within a staging budget, and a
//! mover that drains finished jobs from staging (RAM/NVMe) to their category folder
//! on the slower final tier.
//!
//! Lifecycle: queue (Queued/Paused) -> active (Downloading; verification, repair and
//! extraction happen inline) -> history "Moving" -> "Completed" or "Failed".

use crate::config::ServerCfg;
use crate::conn::{self, ConnCtx, SStats};
use crate::job::{self, Job, JobResult, OnFinish};
use crate::nzb;
use crate::outfile;
use crate::queue::Queues;
use crate::svccfg::{Category, Loaded, SvcCfg};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const PRIO_PAUSED: i32 = -2;
pub const PRIO_LOW: i32 = -1;
pub const PRIO_NORMAL: i32 = 0;
pub const PRIO_HIGH: i32 = 1;
pub const PRIO_FORCE: i32 = 2;

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn random_id() -> String {
    let mut b = [0u8; 12];
    let _ = File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b));
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let s: String = b.iter().map(|x| A[*x as usize % A.len()] as char).collect();
    format!("SABnzbd_nzo_{s}")
}

/// Folder-safe job name.
pub fn clean_name(s: &str) -> String {
    let mut n: String = s.chars().map(|c| if c == '/' || c == '\\' || c.is_control() { '_' } else { c }).collect();
    n = n.trim().trim_matches('.').trim().to_string();
    while n.len() > 200 {
        n.pop();
    }
    if n.is_empty() {
        "unnamed".into()
    } else {
        n
    }
}

/// Persistent description of a job.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub nzo: String,
    pub name: String,
    pub nzb_name: String,
    pub cat: String,
    pub prio: i32,
    #[serde(default)]
    pub paused: bool,
    pub added: u64,
    /// NZB bytes that will be downloaded (everything except par2 recovery volumes).
    pub bytes: u64,
    pub bytes_par2: u64,
    pub files: u32,
    /// Archive password given as `name{{password}}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

pub struct Run {
    pub job: Arc<Job>,
    pub started: Instant,
    pub reserved: u64,
}

pub struct Entry {
    pub m: Meta,
    pub run: Option<Run>,
    loading: bool,
}

impl Entry {
    /// (total, done) in NZB bytes.
    pub fn progress(&self) -> (u64, u64) {
        match &self.run {
            Some(r) => {
                let t = r.job.enc_total.load(Relaxed).max(1);
                (t, r.job.enc_done.load(Relaxed).min(t))
            }
            None => (self.m.bytes, 0),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Hist {
    pub m: Meta,
    pub status: String,
    #[serde(default)]
    pub fail_message: String,
    /// Final folder (local path, before path mapping).
    #[serde(default)]
    pub storage: String,
    #[serde(default)]
    pub tmp_dest: String,
    pub completed: u64,
    pub download_time: u64,
    pub postproc_time: u64,
    pub downloaded: u64,
    #[serde(default)]
    pub log: Vec<String>,
}

pub struct Store {
    pub queue: Vec<Entry>,
    pub history: VecDeque<Hist>,
    /// Cancelled jobs whose threads are still winding down: (job, staging reservation).
    zombies: HashMap<usize, (Arc<Job>, u64)>,
    next_job: usize,
}

struct Done {
    id: usize,
    ok: bool,
    parts: Vec<String>,
    downloaded: u64,
}

/// Progress of one job being moved to its category folder.
pub struct MoveProg {
    pub nzo: String,
    pub name: String,
    pub cat: String,
    pub total: u64,
    pub done: AtomicU64,
    pub t0: Instant,
}

struct MoveTask {
    nzo: String,
    src: PathBuf,
    meta: Meta,
    reserved: u64,
}

#[derive(Default)]
struct Kick {
    m: Mutex<bool>,
    cv: Condvar,
}

impl Kick {
    fn notify(&self) {
        *self.m.lock().unwrap() = true;
        self.cv.notify_one();
    }

    fn wait(&self, t: Duration) {
        let g = self.m.lock().unwrap();
        let (mut g, _) = self.cv.wait_timeout_while(g, t, |k| !*k).unwrap();
        *g = false;
    }
}

#[derive(Serialize, Deserialize, Default)]
struct QueueFile {
    queue: Vec<Meta>,
    paused: bool,
    limit: u64,
}

pub struct Engine {
    pub cfg: SvcCfg,
    pub servers: Vec<ServerCfg>,
    pub cats: Vec<Category>,
    pub q: Arc<Queues>,
    pub stats: Vec<Arc<SStats>>,
    pub store: Mutex<Store>,
    kick: Arc<Kick>,
    done_rx: Mutex<mpsc::Receiver<Done>>,
    finished: OnFinish,
    move_tx: Mutex<mpsc::Sender<MoveTask>>,
    pub paused: AtomicBool,
    pub queue_ver: AtomicU64,
    pub hist_ver: AtomicU64,
    /// Staging bytes reserved by active jobs and jobs waiting to be moved.
    pub reserved: AtomicU64,
    pub moved: AtomicU64,
    pub moving: AtomicUsize,
    /// Moves in progress (touched only when a move starts or ends, and by the sampler).
    pub moves: Mutex<Vec<Arc<MoveProg>>>,
    pub move_backlog: AtomicU64,
    pub jobs_ok: AtomicU64,
    pub jobs_failed: AtomicU64,
    /// 5-second average download rate (bytes/s), kept up to date by the stats sampler.
    pub rate5: AtomicU64,
    pub low_water: usize,
    pub started: Instant,
    dirty_q: AtomicBool,
    dirty_h: AtomicBool,
    dirty_s: AtomicBool,
    /// Per server name: day ("YYYY-MM-DD") -> bytes.
    pub daily: Mutex<BTreeMap<String, BTreeMap<String, u64>>>,
    /// Destination folders claimed by moves in progress.
    dest_taken: Mutex<std::collections::HashSet<PathBuf>>,
    /// Head-of-queue job waiting for staging space, and since when.
    head_wait: Mutex<Option<(String, Instant)>>,
    /// NZBs of completed jobs, deleted once the history recording that is saved.
    nzb_trash: Mutex<Vec<PathBuf>>,
    _lock: File,
}

pub static TERM: AtomicBool = AtomicBool::new(false);

/// Exclusive lock on the state directory (one process owns queue and history).
fn lock_state(dir: &Path) -> io::Result<File> {
    use std::os::unix::io::AsRawFd;
    fs::create_dir_all(dir)?;
    let f = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("lock"))?;
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::other(format!("{} is in use by another nzbfast process", dir.display())));
    }
    Ok(f)
}

/// Adds jobs and history entries to the saved state (service must be stopped).
/// Entries whose id already exists are skipped. Returns the new (queue, history) sizes.
pub fn import_state(cfg: &SvcCfg, metas: Vec<Meta>, hists: Vec<Hist>) -> io::Result<(usize, usize)> {
    use std::os::unix::fs::MetadataExt;
    let dir = &cfg.state_dir;
    let _lock = lock_state(dir)?;
    let mut qf: QueueFile = fs::read(dir.join("queue.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    let mut hist: Vec<Hist> = fs::read(dir.join("history.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    let known: std::collections::HashSet<String> = qf.queue.iter().map(|m| m.nzo.clone()).chain(hist.iter().map(|h| h.m.nzo.clone())).collect();
    qf.queue.extend(metas.into_iter().filter(|m| !known.contains(&m.nzo)));
    let mut new_h: Vec<Hist> = hists.into_iter().filter(|h| !known.contains(&h.m.nzo)).collect();
    new_h.extend(hist);
    hist = new_h;
    hist.sort_by_key(|h| std::cmp::Reverse(h.completed));
    write_atomic(&dir.join("queue.json"), &serde_json::to_vec(&qf)?)?;
    write_atomic(&dir.join("history.json"), &serde_json::to_vec(&hist)?)?;
    // Files belong to whoever owns the state directory (the service user).
    let md = fs::metadata(dir)?;
    let own = |p: &Path| std::os::unix::fs::chown(p, Some(md.uid()), Some(md.gid()));
    for p in [dir.join("queue.json"), dir.join("history.json"), dir.join("lock"), dir.join("nzb")] {
        own(&p)?;
    }
    for e in fs::read_dir(dir.join("nzb"))?.flatten() {
        own(&e.path())?;
    }
    Ok((qf.queue.len(), hist.len()))
}

extern "C" fn on_term(_: libc::c_int) {
    TERM.store(true, Relaxed);
}

fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, path)
}

fn day_string(t: u64) -> String {
    // Civil date from days since epoch (UTC).
    let z = (t / 86400) as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{:02}-{:02}", if m <= 2 { y + 1 } else { y }, m, d)
}

impl Engine {
    pub fn start(l: Loaded) -> Result<Arc<Engine>, String> {
        let Loaded { cfg, servers, categories } = l;
        if servers.is_empty() {
            return Err("no servers configured".into());
        }
        let lock = lock_state(&cfg.state_dir).map_err(|e| e.to_string())?;
        for d in [cfg.state_dir.join("nzb"), cfg.staging_dir.clone()] {
            fs::create_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
        }
        fs::create_dir_all(&cfg.complete_dir).map_err(|e| format!("{}: {e}", cfg.complete_dir))?;
        // Jobs that were being moved resume their move (their files are complete in
        // staging); other work left in staging is restarted from scratch.
        let hist: Vec<Hist> = fs::read(cfg.state_dir.join("history.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let resumable: std::collections::HashSet<String> =
            hist.iter().filter(|h| h.status == "Moving" && cfg.staging_dir.join(&h.m.nzo).is_dir()).map(|h| h.m.nzo.clone()).collect();
        if let Ok(rd) = fs::read_dir(&cfg.staging_dir) {
            for e in rd.flatten() {
                let n = e.file_name().to_string_lossy().into_owned();
                if n.starts_with("SABnzbd_nzo_") && !resumable.contains(&n) {
                    let _ = fs::remove_dir_all(e.path());
                }
            }
        }
        unsafe {
            libc::signal(libc::SIGTERM, on_term as *const () as libc::sighandler_t);
            libc::signal(libc::SIGINT, on_term as *const () as libc::sighandler_t);
        }

        let wb = outfile::set_budget_mb(cfg.write_buffer_mb);
        eprintln!("write buffer {wb} MiB");
        outfile::start_io(cfg.io_threads.max(1));
        let slots = servers.iter().map(|s| (s.conns * if s.depth > 0 { s.depth } else { cfg.depth }) as u32).collect();
        let q = Arc::new(Queues::new(servers.iter().map(|s| s.priority).collect(), slots));
        let _ = job::QUEUE.set(q.clone());
        let stats: Vec<Arc<SStats>> = servers.iter().map(|_| Arc::new(SStats::default())).collect();
        let mut total = 0;
        for (i, s) in servers.iter().enumerate() {
            let depth = if s.depth > 0 { s.depth } else { cfg.depth };
            let tls = conn::tls_config(s.insecure);
            eprintln!(
                "server {:<18} {}:{}{} tls={} conns={} depth={depth} prio={}",
                s.name,
                s.host,
                s.port,
                s.connect.as_ref().map(|c| format!(" via {c}")).unwrap_or_default(),
                s.tls,
                s.conns,
                s.priority
            );
            for _ in 0..s.conns {
                let ctx = ConnCtx { idx: i, cfg: s.clone(), depth, tls: tls.clone(), q: q.clone(), st: stats[i].clone() };
                std::thread::Builder::new().stack_size(256 << 10).spawn(move || conn::run(ctx)).map_err(|e| e.to_string())?;
            }
            total += s.conns * depth;
        }

        let kick = Arc::new(Kick::default());
        let (done_tx, done_rx) = mpsc::channel::<Done>();
        let finished = {
            let tx = Mutex::new(done_tx);
            let kick = kick.clone();
            Arc::new(move |j: &Job, r: JobResult| {
                let _ = tx.lock().unwrap().send(Done { id: j.id, ok: r.ok, parts: r.parts, downloaded: j.bytes_done.load(Relaxed) });
                kick.notify();
            }) as OnFinish
        };
        let (move_tx, move_rx) = mpsc::channel::<MoveTask>();

        let qf: QueueFile = fs::read(cfg.state_dir.join("queue.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let daily = fs::read(cfg.state_dir.join("servers.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let mut queue: Vec<Entry> = vec![];
        let mut history = VecDeque::new();
        let mut resume = vec![];
        for mut h in hist {
            if h.status == "Completed" || h.status == "Failed" {
                history.push_back(h);
            } else if resumable.contains(&h.m.nzo) {
                resume.push(h.m.clone());
                history.push_back(h);
            } else if h.status == "Moving" && !h.storage.is_empty() && Path::new(&h.storage).is_dir() {
                // The move had finished; only the status update was lost.
                h.status = "Completed".into();
                h.tmp_dest = String::new();
                history.push_back(h);
            } else {
                // Interrupted before its files were complete: download again.
                if !h.tmp_dest.is_empty() {
                    let _ = fs::remove_dir_all(&h.tmp_dest);
                }
                queue.push(Entry { m: h.m, run: None, loading: false });
            }
        }
        queue.extend(qf.queue.into_iter().map(|m| Entry { m, run: None, loading: false }));
        queue.sort_by_key(|e| -e.m.prio);
        conn::LIMIT.rate.store(if qf.limit > 0 { qf.limit } else { (cfg.speed_limit_mbs * 1e6) as u64 }, Relaxed);
        if qf.paused {
            q.set_paused(true);
        }

        let eng = Arc::new(Engine {
            low_water: (total * 20).max(4000),
            cfg,
            servers,
            cats: categories,
            q,
            stats,
            store: Mutex::new(Store { queue, history, zombies: HashMap::new(), next_job: 0 }),
            kick,
            done_rx: Mutex::new(done_rx),
            finished,
            move_tx: Mutex::new(move_tx),
            paused: AtomicBool::new(qf.paused),
            queue_ver: AtomicU64::new(1),
            hist_ver: AtomicU64::new(1),
            reserved: AtomicU64::new(0),
            moved: AtomicU64::new(0),
            moving: AtomicUsize::new(0),
            moves: Mutex::new(Vec::new()),
            move_backlog: AtomicU64::new(0),
            jobs_ok: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            rate5: AtomicU64::new(0),
            started: Instant::now(),
            dirty_q: AtomicBool::new(true),
            dirty_h: AtomicBool::new(true),
            dirty_s: AtomicBool::new(false),
            daily: Mutex::new(daily),
            dest_taken: Mutex::new(Default::default()),
            head_wait: Mutex::new(None),
            nzb_trash: Mutex::new(vec![]),
            _lock: lock,
        });
        let rx = Arc::new(Mutex::new(move_rx));
        for _ in 0..eng.cfg.mover_jobs.max(1) {
            let (e, rx) = (eng.clone(), rx.clone());
            std::thread::spawn(move || e.mover(rx));
        }
        for m in resume {
            eprintln!("resuming move of {}", m.name);
            eng.reserved.fetch_add(m.bytes, Relaxed);
            eng.move_backlog.fetch_add(m.bytes, Relaxed);
            let _ = eng.move_tx.lock().unwrap().send(MoveTask { nzo: m.nzo.clone(), src: eng.cfg.staging_dir.join(&m.nzo), reserved: m.bytes, meta: m });
        }
        let e = eng.clone();
        std::thread::spawn(move || e.run_loop());
        Ok(eng)
    }

    pub fn nzb_path(&self, nzo: &str) -> PathBuf {
        self.cfg.state_dir.join("nzb").join(format!("{nzo}.nzb.gz"))
    }

    pub fn category(&self, name: &str) -> Option<&Category> {
        self.cats.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Applies `path_map` so clients see paths the way they mount them.
    pub fn map_path(&self, p: &str) -> String {
        for m in &self.cfg.path_map {
            if let Some(rest) = p.strip_prefix(m.from.trim_end_matches('/')) {
                if rest.is_empty() || rest.starts_with('/') {
                    return format!("{}{rest}", m.to.trim_end_matches('/'));
                }
            }
        }
        p.to_string()
    }

    fn cat_dir(&self, cat: &str) -> PathBuf {
        let root = PathBuf::from(&self.cfg.complete_dir);
        match self.category(cat) {
            Some(c) if !c.dir.trim_end_matches('*').is_empty() => {
                let d = c.dir.trim_end_matches('*');
                if d.starts_with('/') {
                    PathBuf::from(d)
                } else {
                    root.join(d)
                }
            }
            _ => root,
        }
    }

    pub fn touch_queue(&self) {
        self.queue_ver.fetch_add(1, Relaxed);
        self.dirty_q.store(true, Relaxed);
        self.kick.notify();
    }

    fn touch_history(&self) {
        self.hist_ver.fetch_add(1, Relaxed);
        self.dirty_h.store(true, Relaxed);
    }

    // ---------- adding ----------

    /// Adds an NZB (plain or gzip). Returns the new job id.
    pub fn add_nzb(&self, raw: &[u8], filename: &str, nzbname: Option<&str>, cat: Option<&str>, prio: Option<i32>) -> Result<String, String> {
        let data = if raw.starts_with(&[0x1f, 0x8b]) {
            let mut s = vec![];
            flate2::read::GzDecoder::new(raw).read_to_end(&mut s).map_err(|e| format!("bad gzip: {e}"))?;
            s
        } else {
            raw.to_vec()
        };
        let base = Path::new(filename).file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let mut stem = base.clone();
        for ext in [".gz", ".nzb"] {
            if stem.to_ascii_lowercase().ends_with(ext) {
                stem.truncate(stem.len() - ext.len());
            }
        }
        let mut name = nzbname.filter(|s| !s.trim().is_empty()).unwrap_or(&stem).to_string();
        // SABnzbd/NZBGet convention: "Name{{password}}".
        let mut password = None;
        if let (Some(a), true) = (name.find("{{"), name.ends_with("}}")) {
            password = Some(name[a + 2..name.len() - 2].to_string()).filter(|p| !p.is_empty());
            name.truncate(a);
        }
        // Some indexer feeds escape more than once, so titles arrive as
        // "Minions.&amp;.Monsters" or even "Asterix.&amp;amp;.Obelix".
        for _ in 0..3 {
            let u = nzb::xml_unescape(&name);
            if u == name {
                break;
            }
            name = u;
        }
        let name = clean_name(&name);
        let parsed = nzb::parse(&String::from_utf8_lossy(&data), name.clone())?;
        let (mut bytes, mut bytes_par2) = (0u64, 0u64);
        for f in &parsed.files {
            let b: u64 = f.segs.iter().map(|s| s.bytes as u64).sum();
            if job::is_par2_volume(&nzb::subject_filename(&f.subject)) {
                bytes_par2 += b;
            } else {
                bytes += b;
            }
        }
        let cat = match cat.map(str::trim).filter(|c| !c.is_empty() && *c != "*" && !c.eq_ignore_ascii_case("default")) {
            Some(c) => self.category(c).map(|c| c.name.clone()).unwrap_or_else(|| "*".into()),
            None => "*".into(),
        };
        let cat_prio = self.category(&cat).map(|c| c.priority).unwrap_or(-100);
        let (prio, paused) = match prio.unwrap_or(-100) {
            -100 => (if cat_prio == -100 || cat_prio == PRIO_PAUSED { PRIO_NORMAL } else { cat_prio.clamp(PRIO_LOW, PRIO_FORCE) }, cat_prio == PRIO_PAUSED),
            PRIO_PAUSED => (PRIO_NORMAL, true),
            p => (p.clamp(PRIO_LOW, PRIO_FORCE), false),
        };
        let nzo = random_id();
        let mut gz = flate2::write::GzEncoder::new(vec![], flate2::Compression::fast());
        gz.write_all(&data).map_err(|e| e.to_string())?;
        let gz = gz.finish().map_err(|e| e.to_string())?;
        write_atomic(&self.nzb_path(&nzo), &gz).map_err(|e| format!("cannot store NZB: {e}"))?;
        let m = Meta {
            nzo: nzo.clone(),
            name,
            nzb_name: if base.is_empty() { format!("{stem}.nzb") } else { base },
            cat,
            prio,
            paused,
            added: unix_now(),
            bytes,
            bytes_par2,
            files: parsed.files.len() as u32,
            password,
        };
        let mut st = self.store.lock().unwrap();
        let pos = st.queue.iter().position(|e| e.m.prio < prio).unwrap_or(st.queue.len());
        st.queue.insert(pos, Entry { m, run: None, loading: false });
        drop(st);
        self.touch_queue();
        Ok(nzo)
    }

    // ---------- queue operations ----------

    fn ids_match(ids: &[String], nzo: &str) -> bool {
        ids.iter().any(|i| i == nzo || i == "all")
    }

    pub fn delete_queue(&self, ids: &[String]) -> Vec<String> {
        let mut st = self.store.lock().unwrap();
        let mut removed = vec![];
        let mut cancel = vec![];
        let mut i = 0;
        while i < st.queue.len() {
            if Self::ids_match(ids, &st.queue[i].m.nzo) {
                let e = st.queue.remove(i);
                if let Some(r) = e.run {
                    st.zombies.insert(r.job.id, (r.job.clone(), r.reserved));
                    cancel.push(r.job);
                }
                let _ = fs::remove_file(self.nzb_path(&e.m.nzo));
                removed.push(e.m.nzo);
            } else {
                i += 1;
            }
        }
        drop(st);
        for j in cancel {
            j.cancel(&self.q);
        }
        if !removed.is_empty() {
            self.touch_queue();
        }
        removed
    }

    pub fn pause_jobs(&self, ids: &[String], pause: bool) -> Vec<String> {
        let mut st = self.store.lock().unwrap();
        let mut hit = vec![];
        for e in st.queue.iter_mut().filter(|e| Self::ids_match(ids, &e.m.nzo)) {
            e.m.paused = pause;
            if let Some(r) = &e.run {
                if pause {
                    r.job.pause(&self.q)
                } else {
                    r.job.resume(&self.q)
                }
            }
            hit.push(e.m.nzo.clone());
        }
        drop(st);
        self.touch_queue();
        hit
    }

    /// Sets the priority and re-sorts; returns the new position of the (first) job.
    pub fn set_priority(&self, ids: &[String], prio: i32) -> Option<usize> {
        let mut st = self.store.lock().unwrap();
        let mut pos = None;
        let mut moved = vec![];
        let mut i = 0;
        while i < st.queue.len() {
            if Self::ids_match(ids, &st.queue[i].m.nzo) {
                moved.push(st.queue.remove(i));
            } else {
                i += 1;
            }
        }
        for mut e in moved {
            if prio == PRIO_PAUSED {
                e.m.paused = true;
                if let Some(r) = &e.run {
                    r.job.pause(&self.q);
                }
            } else {
                e.m.prio = prio.clamp(PRIO_LOW, PRIO_FORCE);
            }
            let p = st.queue.iter().position(|x| x.m.prio < e.m.prio).unwrap_or(st.queue.len());
            pos.get_or_insert(p);
            st.queue.insert(p, e);
        }
        drop(st);
        self.touch_queue();
        pos
    }

    /// Moves a job to `to` (0 = top). Its priority adopts that of its new neighbours.
    pub fn move_job(&self, nzo: &str, to: usize) -> Option<(usize, i32)> {
        let mut st = self.store.lock().unwrap();
        let i = st.queue.iter().position(|e| e.m.nzo == nzo)?;
        let mut e = st.queue.remove(i);
        let to = to.min(st.queue.len());
        // Keep the queue sorted by priority.
        let above = if to > 0 { Some(st.queue[to - 1].m.prio) } else { None };
        let below = st.queue.get(to).map(|x| x.m.prio);
        if let Some(a) = above {
            e.m.prio = e.m.prio.min(a);
        }
        if let Some(b) = below {
            e.m.prio = e.m.prio.max(b);
        }
        let prio = e.m.prio;
        st.queue.insert(to, e);
        drop(st);
        self.touch_queue();
        Some((to, prio))
    }

    pub fn change_cat(&self, ids: &[String], cat: &str) -> usize {
        let cat = self.category(cat).map(|c| c.name.clone()).unwrap_or_else(|| "*".into());
        let mut st = self.store.lock().unwrap();
        let mut n = 0;
        for e in st.queue.iter_mut().filter(|e| Self::ids_match(ids, &e.m.nzo)) {
            e.m.cat = cat.clone();
            n += 1;
        }
        drop(st);
        self.touch_queue();
        n
    }

    pub fn rename(&self, nzo: &str, name: &str) -> bool {
        let mut st = self.store.lock().unwrap();
        let ok = match st.queue.iter_mut().find(|e| e.m.nzo == nzo) {
            // The name of a running job is baked into its file names.
            Some(e) if e.run.is_none() => {
                e.m.name = clean_name(name);
                true
            }
            _ => false,
        };
        drop(st);
        self.touch_queue();
        ok
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Relaxed);
        self.q.set_paused(paused);
        self.touch_queue();
    }

    pub fn set_limit(&self, bytes_per_sec: u64) {
        conn::LIMIT.rate.store(bytes_per_sec, Relaxed);
        self.touch_queue();
    }

    // ---------- history operations ----------

    /// `ids` may contain job ids or "all" / "failed" / "completed".
    pub fn delete_history(&self, ids: &[String], del_files: bool) -> Vec<String> {
        let want = |h: &Hist| {
            ids.iter().any(|i| i == &h.m.nzo || i == "all" || (i == "failed" && h.status == "Failed") || (i == "completed" && h.status == "Completed"))
        };
        let mut st = self.store.lock().unwrap();
        let mut removed = vec![];
        let mut keep = VecDeque::with_capacity(st.history.len());
        for h in std::mem::take(&mut st.history) {
            // Jobs still being moved stay until the move is done.
            if want(&h) && (h.status == "Completed" || h.status == "Failed") {
                removed.push(h);
            } else {
                keep.push_back(h);
            }
        }
        st.history = keep;
        drop(st);
        for h in &removed {
            let _ = fs::remove_file(self.nzb_path(&h.m.nzo));
            if del_files && h.status == "Completed" {
                self.remove_output(&h.storage);
            }
        }
        if !removed.is_empty() {
            self.touch_history();
        }
        removed.into_iter().map(|h| h.m.nzo).collect()
    }

    /// Deletes a job's output folder, but only strictly inside a category folder.
    fn remove_output(&self, storage: &str) {
        if storage.is_empty() {
            return;
        }
        let p = Path::new(storage);
        let inside = self.cats.iter().any(|c| {
            let d = self.cat_dir(&c.name);
            p.starts_with(&d) && p != d
        }) || (p.starts_with(&self.cfg.complete_dir) && p != Path::new(&self.cfg.complete_dir));
        if inside && !storage.contains("/../") {
            let _ = fs::remove_dir_all(p);
        }
    }

    /// Re-queues a failed job. Returns false if it is unknown or its NZB is gone.
    pub fn retry(&self, nzo: &str) -> bool {
        let mut st = self.store.lock().unwrap();
        let Some(i) = st.history.iter().position(|h| h.m.nzo == nzo && h.status == "Failed") else {
            return false;
        };
        if !self.nzb_path(nzo).exists() {
            return false;
        }
        let h = st.history.remove(i).unwrap();
        let mut m = h.m;
        m.paused = false;
        let pos = st.queue.iter().position(|e| e.m.prio < m.prio).unwrap_or(st.queue.len());
        st.queue.insert(pos, Entry { m, run: None, loading: false });
        drop(st);
        self.touch_history();
        self.touch_queue();
        true
    }

    // ---------- scheduling ----------

    fn run_loop(self: Arc<Self>) {
        let mut last_save = Instant::now();
        let mut last_sec = Instant::now();
        let mut prev: Vec<u64> = vec![0; self.stats.len()];
        loop {
            self.handle_done();
            self.grow_grants();
            self.activate();
            if last_sec.elapsed() >= Duration::from_secs(1) {
                last_sec = Instant::now();
                for w in self.q.sweep() {
                    w.missing(&self.q);
                }
                let day = day_string(unix_now());
                let mut d = self.daily.lock().unwrap();
                for (i, s) in self.stats.iter().enumerate() {
                    let b = s.bytes.load(Relaxed);
                    if b > prev[i] {
                        *d.entry(self.servers[i].name.clone()).or_default().entry(day.clone()).or_default() += b - prev[i];
                        self.dirty_s.store(true, Relaxed);
                    }
                    prev[i] = b;
                }
            }
            let term = TERM.load(Relaxed);
            if term || last_save.elapsed() >= Duration::from_secs(2) {
                last_save = Instant::now();
                self.save();
            }
            if term {
                eprintln!("nzbfast: state saved, exiting");
                std::process::exit(0);
            }
            self.kick.wait(Duration::from_millis(50));
        }
    }

    /// Hands staging space freed by the movers to jobs started with only part of
    /// theirs, before any new job may take it.
    fn grow_grants(&self) {
        let limit = (self.cfg.staging_limit_gb * 1e9) as u64;
        let mut st = self.store.lock().unwrap();
        for e in st.queue.iter_mut() {
            let Some(run) = e.run.as_mut() else { continue };
            if run.job.grant.load(Relaxed) == u64::MAX {
                continue;
            }
            let reserved = self.reserved.load(Relaxed);
            let want = e.m.bytes.saturating_sub(run.reserved);
            // Alone in staging, a job may exceed the budget (as one job always may).
            let alone = reserved == run.reserved && self.moving.load(Relaxed) == 0;
            let g = if alone { want } else { want.min(limit.saturating_sub(reserved)) };
            if g == 0 {
                continue;
            }
            self.reserved.fetch_add(g, Relaxed);
            run.reserved += g;
            if g == want {
                run.job.grant.store(u64::MAX, Relaxed);
            } else {
                run.job.grant.fetch_add(g, Relaxed);
            }
        }
    }

    fn activate(&self) {
        let limit = (self.cfg.staging_limit_gb * 1e9) as u64;
        loop {
            // Retry backlogs do not count: articles waiting for one particular server must
            // not starve the others of first attempts.
            if self.paused.load(Relaxed) || self.q.main_len() >= self.low_water {
                return;
            }
            let (nzo, need) = {
                let mut st = self.store.lock().unwrap();
                let active = st.queue.iter().filter(|e| e.run.is_some() || e.loading).count();
                if active >= self.cfg.active_jobs {
                    return;
                }
                // Freed space goes to a job still short of its staging first.
                if st.queue.iter().any(|e| e.run.as_ref().is_some_and(|r| r.job.grant.load(Relaxed) != u64::MAX)) {
                    return;
                }
                let eligible = |e: &Entry| e.run.is_none() && !e.loading && !e.m.paused;
                let Some(head) = st.queue.iter().position(eligible) else {
                    return;
                };
                // Always allow one job, even if it alone exceeds the budget.
                let busy = active > 0 || self.moving.load(Relaxed) > 0;
                let reserved = self.reserved.load(Relaxed);
                let fits = |e: &Entry| !busy || reserved + e.m.bytes <= limit;
                // Staging granted up front; `None` = all of it.
                let mut part = None;
                let i = if fits(&st.queue[head]) {
                    *self.head_wait.lock().unwrap() = None;
                    head
                } else {
                    // A big job waiting for staging space should not idle the network:
                    // start smaller ones behind it, unless it has waited long enough.
                    // Then it starts in the space there is and grows as the movers free
                    // more, instead of the network idling until all of it is free.
                    let mut hw = self.head_wait.lock().unwrap();
                    let nzo = &st.queue[head].m.nzo;
                    if hw.as_ref().is_none_or(|(n, _)| n != nzo) {
                        *hw = Some((nzo.clone(), Instant::now()));
                    }
                    if hw.as_ref().is_some_and(|(_, t)| t.elapsed() > Duration::from_secs(300)) {
                        *hw = None;
                        part = Some(limit.saturating_sub(reserved));
                        head
                    } else {
                        drop(hw);
                        match st.queue.iter().enumerate().skip(head + 1).filter(|(_, e)| eligible(e)).take(64).find(|(_, e)| fits(e)) {
                            Some((j, _)) => j,
                            None => return,
                        }
                    }
                };
                let need = part.unwrap_or(st.queue[i].m.bytes);
                st.queue[i].loading = true;
                (st.queue[i].m.nzo.clone(), need)
            };
            let parsed = nzb::load(&self.nzb_path(&nzo).to_string_lossy());
            let mut st = self.store.lock().unwrap();
            let Some(i) = st.queue.iter().position(|e| e.m.nzo == nzo) else {
                continue;
            };
            st.queue[i].loading = false;
            match parsed {
                Ok(mut n) => {
                    st.next_job += 1;
                    let id = st.next_job;
                    n.name = st.queue[i].m.name.clone();
                    if n.password.is_none() {
                        n.password = st.queue[i].m.password.clone();
                    }
                    let job = Job::new(id, n, self.cfg.staging_dir.join(&nzo), None, self.finished.clone());
                    if st.queue[i].m.paused {
                        job.pause(&self.q);
                    }
                    if need < st.queue[i].m.bytes {
                        job.grant.store(need, Relaxed);
                    }
                    self.reserved.fetch_add(need, Relaxed);
                    st.queue[i].run = Some(Run { job: job.clone(), started: Instant::now(), reserved: need });
                    drop(st);
                    let w = job.probe_work();
                    job.enqueue(&self.q, w, true);
                }
                Err(e) => {
                    let ent = st.queue.remove(i);
                    st.history.push_front(Hist {
                        m: ent.m,
                        status: "Failed".into(),
                        fail_message: format!("NZB unreadable: {e}"),
                        storage: String::new(),
                        tmp_dest: String::new(),
                        completed: unix_now(),
                        download_time: 0,
                        postproc_time: 0,
                        downloaded: 0,
                        log: vec![],
                    });
                    drop(st);
                    self.jobs_failed.fetch_add(1, Relaxed);
                    self.touch_history();
                }
            }
            self.queue_ver.fetch_add(1, Relaxed);
        }
    }

    /// Picks and claims `<cat dir>/<name>`, adding .1, .2, ... if taken. Runs on a
    /// mover thread: these lookups can be slow on a busy NFS mount.
    fn unique_dest(&self, m: &Meta) -> PathBuf {
        let dir = self.cat_dir(&m.cat);
        let mut k = 0;
        loop {
            let name = if k == 0 { m.name.clone() } else { format!("{}.{k}", m.name) };
            let dest = dir.join(&name);
            if !self.dest_taken.lock().unwrap().contains(&dest) && !dest.exists() && self.dest_taken.lock().unwrap().insert(dest.clone()) {
                return dest;
            }
            k += 1;
        }
    }

    fn handle_done(&self) {
        loop {
            let d = match self.done_rx.lock().unwrap().try_recv() {
                Ok(d) => d,
                Err(_) => return,
            };
            // Only bookkeeping happens under the store lock; file system work is done
            // after releasing it (or by the movers).
            let mut st = self.store.lock().unwrap();
            if let Some((job, reserved)) = st.zombies.remove(&d.id) {
                drop(st);
                let _ = fs::remove_dir_all(&job.work);
                self.reserved.fetch_sub(reserved, Relaxed);
                continue;
            }
            let Some(pos) = st.queue.iter().position(|e| e.run.as_ref().is_some_and(|r| r.job.id == d.id)) else {
                continue;
            };
            let e = st.queue.remove(pos);
            let run = e.run.expect("running");
            let mut h = Hist {
                m: e.m,
                status: if d.ok { "Moving".into() } else { "Failed".into() },
                fail_message: String::new(),
                storage: String::new(),
                tmp_dest: String::new(),
                completed: unix_now(),
                download_time: run.started.elapsed().as_secs(),
                postproc_time: 0,
                downloaded: d.downloaded,
                log: d.parts.clone(),
            };
            let mut cleanup = vec![];
            if d.ok {
                self.move_backlog.fetch_add(run.reserved, Relaxed);
                let _ =
                    self.move_tx.lock().unwrap().send(MoveTask { nzo: h.m.nzo.clone(), src: run.job.work.clone(), meta: h.m.clone(), reserved: run.reserved });
            } else {
                h.fail_message = d.parts.first().cloned().unwrap_or_else(|| "failed".into());
                self.jobs_failed.fetch_add(1, Relaxed);
            }
            st.history.push_front(h);
            while st.history.len() > self.cfg.history_keep.max(100) {
                if let Some(old) = st.history.pop_back() {
                    cleanup.push(self.nzb_path(&old.m.nzo));
                }
            }
            drop(st);
            if !d.ok {
                let _ = fs::remove_dir_all(&run.job.work);
                self.reserved.fetch_sub(run.reserved, Relaxed);
            }
            for p in cleanup {
                let _ = fs::remove_file(p);
            }
            self.touch_history();
            self.queue_ver.fetch_add(1, Relaxed);
            self.dirty_q.store(true, Relaxed);
        }
    }

    // ---------- moving ----------

    fn mover(self: Arc<Self>, rx: Arc<Mutex<mpsc::Receiver<MoveTask>>>) {
        loop {
            let Ok(t) = rx.lock().unwrap().recv() else {
                return;
            };
            self.moving.fetch_add(1, Relaxed);
            let t0 = Instant::now();
            // The partial copy has a fixed name, so a move interrupted by a restart
            // picks up where it was; the final name is chosen when it is complete.
            let tmp = self.cat_dir(&t.meta.cat).join(format!("_FAST_{}", t.nzo));
            // Files a resumed move already put in place count as done.
            let already = tree_size(&tmp);
            let prog = Arc::new(MoveProg {
                nzo: t.nzo.clone(),
                name: t.meta.name.clone(),
                cat: t.meta.cat.clone(),
                total: already + tree_size(&t.src),
                done: AtomicU64::new(already),
                t0,
            });
            self.moves.lock().unwrap().push(prog.clone());
            // Staging space is handed back file by file, so new jobs can start while
            // a large job is still being copied.
            let mut left = t.reserved;
            let mut release = |b: u64| {
                let r = b.min(left);
                left -= r;
                self.reserved.fetch_sub(r, Relaxed);
                self.move_backlog.fetch_sub(r, Relaxed);
                self.kick.notify();
            };
            let res = fs::create_dir_all(&tmp)
                .and_then(|_| move_tree(&t.src, &tmp, &[&self.moved, &prog.done], self.cfg.mover_threads.max(1), &mut release))
                // After a restart some files may already be in place: count what is there.
                .and_then(|_| if count_files(&tmp) == 0 { Err(io::Error::other("no files were produced")) } else { Ok(()) })
                .and_then(|_| {
                    let dest = self.unique_dest(&t.meta);
                    // Recorded before the rename, so a restart right after it can tell
                    // the move finished.
                    if let Some(h) = self.store.lock().unwrap().history.iter_mut().find(|h| h.m.nzo == t.nzo) {
                        h.storage = dest.to_string_lossy().into_owned();
                    }
                    self.touch_history();
                    let r = fs::rename(&tmp, &dest).map(|_| dest.clone());
                    self.dest_taken.lock().unwrap().remove(&dest);
                    r
                });
            if res.is_err() {
                let _ = fs::remove_dir_all(&tmp);
            }
            let _ = fs::remove_dir_all(&t.src);
            release(u64::MAX);
            self.moves.lock().unwrap().retain(|p| !Arc::ptr_eq(p, &prog));
            self.moving.fetch_sub(1, Relaxed);
            let mut done_nzb = None;
            let mut st = self.store.lock().unwrap();
            if let Some(h) = st.history.iter_mut().find(|h| h.m.nzo == t.nzo) {
                h.postproc_time = t0.elapsed().as_secs();
                h.completed = unix_now();
                match &res {
                    Ok(dest) => {
                        h.storage = dest.to_string_lossy().into_owned();
                        h.tmp_dest = String::new();
                        h.status = "Completed".into();
                        self.jobs_ok.fetch_add(1, Relaxed);
                        done_nzb = Some(self.nzb_path(&t.nzo));
                    }
                    Err(e) => {
                        h.status = "Failed".into();
                        h.fail_message = format!("Moving failed: {e}");
                        self.jobs_failed.fetch_add(1, Relaxed);
                    }
                }
            }
            drop(st);
            if let Some(p) = done_nzb {
                self.nzb_trash.lock().unwrap().push(p);
            }
            self.touch_history();
            self.kick.notify();
        }
    }

    // ---------- persistence ----------

    fn save(&self) {
        let dq = self.dirty_q.swap(false, Relaxed);
        let dh = self.dirty_h.swap(false, Relaxed);
        let ds = self.dirty_s.swap(false, Relaxed);
        if !(dq || dh || ds) {
            return;
        }
        // NZBs whose job is recorded as completed in the snapshot taken below; they are
        // deleted only once that snapshot is on disk.
        let trash = if dh { std::mem::take(&mut *self.nzb_trash.lock().unwrap()) } else { vec![] };
        // Copy under the lock, serialize after releasing it.
        let (qf, hist) = {
            let st = self.store.lock().unwrap();
            let qf = dq.then(|| QueueFile {
                queue: st.queue.iter().map(|e| e.m.clone()).collect(),
                paused: self.paused.load(Relaxed),
                limit: conn::LIMIT.rate.load(Relaxed),
            });
            (qf, dh.then(|| st.history.clone()))
        };
        let dir = &self.cfg.state_dir;
        if let Some(q) = qf {
            if let Err(e) = write_atomic(&dir.join("queue.json"), &serde_json::to_vec(&q).unwrap_or_default()) {
                eprintln!("save queue: {e}");
                self.dirty_q.store(true, Relaxed);
            }
        }
        if let Some(h) = hist {
            match write_atomic(&dir.join("history.json"), &serde_json::to_vec(&h).unwrap_or_default()) {
                Ok(()) => {
                    for p in &trash {
                        let _ = fs::remove_file(p);
                    }
                }
                Err(e) => {
                    eprintln!("save history: {e}");
                    self.dirty_h.store(true, Relaxed);
                    self.nzb_trash.lock().unwrap().extend(trash);
                }
            }
        }
        if ds {
            let b = serde_json::to_vec(&*self.daily.lock().unwrap()).unwrap_or_default();
            let _ = write_atomic(&dir.join("servers.json"), &b);
        }
    }

    /// Bytes per server name over the last `days` days (0 = all time).
    pub fn server_totals(&self, days: u64) -> BTreeMap<String, u64> {
        let since = if days == 0 { String::new() } else { day_string(unix_now().saturating_sub((days - 1) * 86400)) };
        self.daily.lock().unwrap().iter().map(|(n, d)| (n.clone(), d.iter().filter(|(k, _)| **k >= since).map(|(_, v)| v).sum())).collect()
    }
}

/// Moves a directory tree; files that cannot be renamed (other filesystem) are copied
/// in parallel ranges. `done` is called with each file's size. Returns the number of files.
fn move_tree(src: &Path, dst: &Path, ctr: &[&AtomicU64], threads: usize, done: &mut dyn FnMut(u64)) -> io::Result<usize> {
    fs::create_dir_all(dst)?;
    let mut n = 0;
    for e in fs::read_dir(src)? {
        let e = e?;
        let name = e.file_name();
        if name == ".aux" {
            continue;
        }
        let (p, d) = (e.path(), dst.join(&name));
        let ft = e.file_type()?;
        if ft.is_dir() {
            n += move_tree(&p, &d, ctr, threads, done)?;
        } else if ft.is_file() {
            let len = e.metadata().map(|m| m.len()).unwrap_or(0);
            if fs::rename(&p, &d).is_ok() {
                ctr.iter().for_each(|c| _ = c.fetch_add(len, Relaxed));
            } else {
                copy_file(&p, &d, ctr, threads)?;
                // Free the staging copy right away.
                let _ = fs::remove_file(&p);
            }
            done(len);
            n += 1;
        }
    }
    Ok(n)
}

/// Copies through the page cache with one writer per file and one fsync at the end:
/// the NFS client then streams large asynchronous WRITEs and commits once per file,
/// which the NAS absorbs far faster than per-write commits (O_DIRECT) or several
/// writers contending for the same inode.
fn copy_file(src: &Path, dst: &Path, ctr: &[&AtomicU64], _threads: usize) -> io::Result<()> {
    let s = File::open(src)?;
    let len = s.metadata()?.len();
    let d = OpenOptions::new().write(true).create(true).truncate(true).open(dst)?;
    let mut buf = vec![0u8; 8 << 20];
    let mut pos = 0u64;
    while pos < len {
        let k = ((len - pos) as usize).min(buf.len());
        s.read_exact_at(&mut buf[..k], pos)?;
        d.write_all_at(&buf[..k], pos)?;
        ctr.iter().for_each(|c| _ = c.fetch_add(k as u64, Relaxed));
        pos += k as u64;
    }
    d.sync_all()
}

/// Bytes in the regular files under `dir` (0 if it does not exist).
fn tree_size(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_name() != ".aux")
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => tree_size(&e.path()),
                    Ok(t) if t.is_file() => e.metadata().map(|m| m.len()).unwrap_or(0),
                    _ => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

fn count_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => count_files(&e.path()),
                    Ok(t) if t.is_file() => 1,
                    _ => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

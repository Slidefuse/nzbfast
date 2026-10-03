//! A download job (one NZB).
//!
//!  1. Probe: fetch the first article of every file to learn names, sizes and
//!     archive layout.
//!  2. Run: every remaining article is decoded and written directly to its final
//!     location. Stored RAR volumes are never written: their payload goes straight
//!     into the extracted file, and the RAR CRCs are verified by combining the
//!     per-article CRCs.
//!  3. Repair (only if articles are missing): par2 verification runs over
//!     "virtual volumes" (saved header + extracted data + saved trailer), the
//!     smallest set of recovery volumes is fetched, and missing slices are rebuilt
//!     with Reed-Solomon over GF(2^16) directly into the extracted file.
//!
//! Jobs whose missing data exceeds the par2 recovery capacity listed in the NZB
//! are aborted immediately instead of chasing every article on every server.

use crate::gf16;
use crate::nzb::{subject_filename, Nzb, NzbSeg};
use crate::outfile::{IoOwner, OutFile};
use crate::par2::{self, Par2Set};
use crate::queue::{JobRoute, Queues, Work};
use crate::rar::{self, RarVol};
use crate::yenc::YInfo;
use crate::zip;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

pub struct JFile {
    pub guess: String,
    pub segs: Vec<NzbSeg>,
    /// Recovery volumes are only fetched when a repair needs them.
    pub skip: bool,
    pub par2_blocks: Option<u32>,
}

struct Probe {
    info: YInfo,
    data: Vec<u8>,
    crc: u32,
}

#[derive(Clone, Copy)]
enum Target {
    Skip,
    Plain { out: usize },
    Rar { vol: usize },
}

struct VolMap {
    out: usize,
    size: u64,
    data_start: u64,
    data_len: u64,
    out_off: u64,
    info: RarVol,
    head: Vec<u8>,
    tail: Mutex<Vec<u8>>,
}

struct RarSet {
    name: String,
    out: usize,
    vols: Vec<usize>,
    unp_size: u64,
}

#[derive(Default, Clone)]
struct FileId {
    yname: String,
    size: u64,
    md5_16k: Option<par2::Id>,
}

struct Plan {
    targets: Vec<Target>,
    outs: Vec<Arc<OutFile>>,
    paths: Vec<PathBuf>,
    vols: Vec<VolMap>,
    sets: Vec<RarSet>,
    notes: Vec<String>,
    main: Vec<String>,
    ids: Vec<FileId>,
    par2_outs: Vec<usize>,
    raw_rar: Vec<usize>,
    /// 7z archives (single files or `.7z.NNN` volumes), unpacked after the download.
    sevenz: Vec<usize>,
}

#[derive(Default)]
struct RepairFetch {
    outs: HashMap<u32, Arc<OutFile>>,
    paths: Vec<PathBuf>,
}

#[derive(Clone, Copy)]
struct Piece {
    vol: u32,
    off: u64,
    len: u64,
    crc: u32,
}

struct JState {
    missing_files: std::collections::HashSet<u32>,
    probes_left: usize,
    probes: Vec<Option<Probe>>,
    pieces: Vec<Piece>,
}

/// What a job is doing right now (for status displays).
pub const PH_PROBE: u8 = 0;
pub const PH_DOWNLOAD: u8 = 1;
pub const PH_VERIFY: u8 = 2;
pub const PH_REPAIR: u8 = 3;
pub const PH_PAR2: u8 = 4;
pub const PH_EXTRACT: u8 = 5;
pub const PH_DONE: u8 = 6;

pub struct Job {
    pub id: usize,
    pub name: String,
    pub work: PathBuf,
    /// Where the finished job is renamed to; `None` leaves it in `work`.
    pub done_dir: Option<PathBuf>,
    /// Password for encrypted archives (from the NZB or the job name).
    pub password: Option<String>,
    /// Output files renamed after planning (to the names par2 knows them by).
    renamed: Mutex<HashMap<PathBuf, PathBuf>>,
    pub files: Vec<JFile>,
    state: Mutex<JState>,
    plan: OnceLock<Plan>,
    repair: Mutex<RepairFetch>,
    remaining: AtomicUsize,
    pending_io: AtomicUsize,
    stage: AtomicU8,
    aborted: AtomicBool,
    missing_bytes: AtomicU64,
    recoverable: AtomicU64,
    /// STAT availability sample: articles asked, still unanswered, answered (known), and
    /// found missing; plus the data bytes (non-par2) the sample stands for.
    sample_n: AtomicUsize,
    sample_left: AtomicUsize,
    sample_known: AtomicUsize,
    sample_missing: AtomicUsize,
    want_bytes: AtomicU64,
    pub bytes_total: u64,
    pub bytes_done: AtomicU64,
    /// Progress in NZB (encoded) bytes: segments handled (downloaded or given up on)
    /// versus segments scheduled, including recovery volumes fetched for a repair.
    pub enc_total: AtomicU64,
    pub enc_done: AtomicU64,
    pub missing: AtomicUsize,
    pub phase: AtomicU8,
    cancelled: AtomicBool,
    /// `Some` while paused: work items held back from the queue.
    parked: Mutex<Option<Vec<Work>>>,
    /// Which servers have this job's articles.
    pub route: JobRoute,
    /// Staging granted to this job: bytes of first attempts it may take from the main
    /// queue (`u64::MAX` = all), and how many it has taken.
    pub grant: AtomicU64,
    pub taken: AtomicU64,
    pub started: Instant,
    pub finished: Arc<dyn Fn(&Job, JobResult) + Send + Sync>,
}

pub struct JobResult {
    pub ok: bool,
    pub summary: String,
    pub parts: Vec<String>,
}

enum RepairOutcome {
    Fetching,
    Repaired(String),
    Failed(String),
}

#[derive(Clone, Copy)]
enum VSrc {
    Out { out: usize, len: u64 },
    Vol { vol: usize },
}

/// Availability sample: every SAMPLE_EVERY-th article of each data file is checked with
/// STAT first (when that yields at least SAMPLE_MIN items); a job is aborted early only if
/// at least SAMPLE_MIN_MISSING are gone and the projected loss exceeds 2x the recovery.
const SAMPLE_EVERY: usize = 25;
const SAMPLE_MIN: usize = 40;
const SAMPLE_MIN_MISSING: u64 = 5;

fn is_par2_name(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".par2")
}

pub fn is_par2_volume(name: &str) -> bool {
    is_par2_name(name) && name.to_ascii_lowercase().contains(".vol")
}

fn sanitize(rel: &str) -> PathBuf {
    let mut p = PathBuf::new();
    for c in rel.split(['/', '\\']) {
        if c.is_empty() || c == "." || c == ".." {
            continue;
        }
        p.push(c.replace('\0', ""));
    }
    if p.as_os_str().is_empty() {
        p.push("unnamed");
    }
    p
}

const VIDEO_EXT: &[&str] = &["mkv", "mp4", "avi", "m4v", "ts", "m2ts", "wmv", "mov", "mpg"];

fn is_video(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    VIDEO_EXT.iter().any(|e| l.ends_with(&format!(".{e}")))
}

/// Video container from a file's first bytes, for posts whose names drop the extension
/// (Radarr/Sonarr only import files with a video extension).
fn video_magic(d: &[u8]) -> Option<&'static str> {
    if d.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
        Some("mkv")
    } else if d.len() >= 12 && &d[4..8] == b"ftyp" {
        Some(if &d[8..12] == b"qt  " { "mov" } else { "mp4" })
    } else if d.len() >= 12 && d.starts_with(b"RIFF") && &d[8..12] == b"AVI " {
        Some("avi")
    } else if d.starts_with(&[0x30, 0x26, 0xb2, 0x75, 0x8e, 0x66, 0xcf, 0x11]) {
        Some("wmv")
    } else if d.len() > 376 && d[0] == 0x47 && d[188] == 0x47 && d[376] == 0x47 {
        Some("ts")
    } else if d.len() > 388 && d[4] == 0x47 && d[196] == 0x47 && d[388] == 0x47 {
        Some("m2ts")
    } else if d.starts_with(&[0, 0, 1, 0xba]) {
        Some("mpg")
    } else {
        None
    }
}

/// Segment title (`Segment/Info/Title`) from the first bytes of a Matroska file.
fn mkv_title(d: &[u8]) -> Option<String> {
    fn id(d: &[u8], i: &mut usize) -> Option<u32> {
        let b = *d.get(*i)?;
        let l = b.leading_zeros() as usize + 1;
        if l > 4 || *i + l > d.len() {
            return None;
        }
        let v = d[*i..*i + l].iter().fold(0u32, |a, &x| a << 8 | x as u32);
        *i += l;
        Some(v)
    }
    fn size(d: &[u8], i: &mut usize) -> Option<u64> {
        let b = *d.get(*i)?;
        let l = b.leading_zeros() as usize + 1;
        if l > 8 || *i + l > d.len() {
            return None;
        }
        let mut v = (b as u64) & ((1u64 << (8 - l)) - 1);
        for &x in &d[*i + 1..*i + l] {
            v = v << 8 | x as u64;
        }
        *i += l;
        // All value bits set: unknown size.
        Some(if v == (1u64 << (7 * l)) - 1 { u64::MAX } else { v })
    }
    let mut i = 0;
    if id(d, &mut i)? != 0x1A45_DFA3 {
        return None;
    }
    let header = size(d, &mut i)? as usize;
    i = i.checked_add(header)?;
    if id(d, &mut i)? != 0x1853_8067 {
        return None;
    }
    size(d, &mut i)?;
    while i < d.len() {
        let (eid, len) = (id(d, &mut i)?, size(d, &mut i)?);
        if eid == 0x1549_A966 {
            let end = i.checked_add(len as usize)?.min(d.len());
            while i < end {
                let (cid, clen) = (id(d, &mut i)?, size(d, &mut i)? as usize);
                if cid == 0x7BA9 {
                    let t = String::from_utf8_lossy(d.get(i..i.checked_add(clen)?)?).trim().to_string();
                    return (!t.is_empty()).then_some(t);
                }
                i = i.checked_add(clen)?;
            }
            return None;
        }
        if eid == 0x1F43_B675 || len == u64::MAX {
            return None;
        }
        i = i.checked_add(len as usize)?;
    }
    None
}

/// An MKV title usable as the file name: a release name with SxxEyy (packs of
/// obfuscated episodes otherwise cannot be told apart).
fn episode_title(d: &[u8]) -> Option<String> {
    let t = mkv_title(d)?;
    let l = t.to_ascii_lowercase();
    let b = l.as_bytes();
    let has_se = (0..b.len()).any(|k| {
        b[k] == b's' && (k == 0 || !b[k - 1].is_ascii_alphanumeric()) && {
            let n = b[k + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
            (1..=2).contains(&n) && b.get(k + 1 + n) == Some(&b'e') && b.get(k + 2 + n).is_some_and(|c| c.is_ascii_digit())
        }
    });
    let name: String = t.chars().map(|c| if c.is_control() || "/\\:*?\"<>|".contains(c) { '_' } else { c }).collect();
    (has_se && name.len() <= 200).then_some(name)
}

/// Random-looking names (hashes, base62 blobs) carry no information for Plex/*arr.
fn is_obfuscated(name: &str) -> bool {
    let stem = name.rsplit('/').next().unwrap_or(name);
    let stem = stem.rsplit_once('.').map(|(a, _)| a).unwrap_or(stem);
    stem.len() >= 10 && !stem.contains([' ', '.', '_', '-'])
}

/// Files Plex/*arr never need; damage there does not require a repair.
fn non_essential(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    [".par2", ".nfo", ".sfv", ".srr", ".srs", ".nzb", ".jpg", ".png", ".txt", ".url", ".md5"].iter().any(|e| l.ends_with(e))
}

const SIG_7Z: &[u8] = &[0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c];

/// `x.7z` or a split volume `x.7z.001`.
fn looks_like_7z(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.ends_with(".7z") || sevenz_volume(&l).is_some()
}

/// Volume number of `x.7z.NNN` (1-based) and the archive name `x.7z`.
fn sevenz_volume(name: &str) -> Option<(u32, String)> {
    let (stem, ext) = name.rsplit_once('.')?;
    if ext.len() == 3 && ext.bytes().all(|c| c.is_ascii_digit()) && stem.to_ascii_lowercase().ends_with(".7z") {
        return Some((ext.parse().ok()?, stem.to_ascii_lowercase()));
    }
    None
}

/// Read + Seek over several files back to back (split 7z volumes).
struct Chain {
    parts: Vec<(File, u64)>,
    total: u64,
    pos: u64,
}

impl Read for Chain {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut base = 0;
        for (f, len) in &self.parts {
            if self.pos < base + len {
                let n = ((base + len - self.pos) as usize).min(buf.len());
                let got = f.read_at(&mut buf[..n], self.pos - base)?;
                self.pos += got as u64;
                return Ok(got);
            }
            base += len;
        }
        Ok(0)
    }
}

impl Seek for Chain {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let p = match to {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(d) => self.total as i64 + d,
            SeekFrom::Current(d) => self.pos as i64 + d,
        };
        if p < 0 {
            return Err(std::io::Error::other("seek before start"));
        }
        self.pos = p as u64;
        Ok(self.pos)
    }
}

/// Volume designation of a split-archive name ("x.part15.rar" -> "rar:15", "x.r07" ->
/// "r:7", "x.7z.003" -> "7z:3", "x.004" -> "n:4"), used to pair files whose obfuscated
/// names differ from the names par2 knows them by.
fn vol_suffix(name: &str) -> Option<String> {
    let l = name.to_ascii_lowercase();
    if let Some(stem) = l.strip_suffix(".rar") {
        if let Some(i) = stem.rfind(".part") {
            if let Ok(n) = stem[i + 5..].parse::<u32>() {
                return Some(format!("rar:{n}"));
            }
        }
        return Some("rar:0".into());
    }
    let (stem, ext) = l.rsplit_once('.')?;
    if ext.len() == 3 && ext.bytes().skip(1).all(|c| c.is_ascii_digit()) && (ext.starts_with('r') || ext.starts_with('s')) {
        return Some(format!("{}:{}", &ext[..1], ext[1..].parse::<u32>().ok()?));
    }
    if ext.len() == 3 && ext.bytes().all(|c| c.is_ascii_digit()) {
        let n: u32 = ext.parse().ok()?;
        return Some(if stem.ends_with(".7z") { format!("7z:{n}") } else { format!("n:{n}") });
    }
    None
}

fn looks_like_rar(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.ends_with(".rar") || rar::name_order(&l).is_some_and(|_| !l.ends_with(".par2"))
}

fn open_rw(p: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(true).open(p)
}

/// Reads as much as exists at `off`; the rest of `buf` is zero-filled.
fn read_fill(f: &File, buf: &mut [u8], off: u64) {
    let mut got = 0;
    while got < buf.len() {
        match f.read_at(&mut buf[got..], off + got as u64) {
            Ok(0) | Err(_) => break,
            Ok(n) => got += n,
        }
    }
    buf[got..].fill(0);
}

impl IoOwner for Job {
    fn io_started(&self) {
        self.pending_io.fetch_add(1, Relaxed);
    }
    fn io_done(&self) {
        self.pending_io.fetch_sub(1, Relaxed);
    }
}

impl Job {
    pub fn new(
        id: usize,
        nzb: Nzb,
        work: PathBuf,
        done_dir: Option<PathBuf>,
        finished: Arc<dyn Fn(&Job, JobResult) + Send + Sync>,
    ) -> Arc<Job> {
        let files: Vec<JFile> = nzb
            .files
            .into_iter()
            .map(|f| {
                let guess = subject_filename(&f.subject);
                let skip = is_par2_volume(&guess);
                let par2_blocks = if skip { par2::vol_blocks(&guess) } else { None };
                JFile { guess, segs: f.segs, skip, par2_blocks }
            })
            .collect();
        let bytes_total = files.iter().filter(|f| !f.skip).flat_map(|f| f.segs.iter()).map(|s| s.bytes as u64).sum();
        let probes_left = files.iter().filter(|f| !f.skip).count();
        // Until the probes reveal which files really are par2, assume named ones are.
        let named_par2: u64 = files.iter().filter(|f| is_par2_name(&f.guess)).flat_map(|f| f.segs.iter()).map(|s| s.bytes as u64).sum();
        let recoverable = if files.iter().any(|f| is_par2_name(&f.guess)) { named_par2 } else { u64::MAX };
        Arc::new(Job {
            id,
            work,
            done_dir,
            password: nzb.password,
            renamed: Mutex::new(HashMap::new()),
            name: nzb.name,
            state: Mutex::new(JState { missing_files: Default::default(), probes_left, probes: files.iter().map(|_| None).collect(), pieces: vec![] }),
            files,
            plan: OnceLock::new(),
            repair: Mutex::new(RepairFetch::default()),
            remaining: AtomicUsize::new(0),
            pending_io: AtomicUsize::new(0),
            stage: AtomicU8::new(0),
            aborted: AtomicBool::new(false),
            missing_bytes: AtomicU64::new(0),
            recoverable: AtomicU64::new(recoverable),
            sample_n: AtomicUsize::new(0),
            sample_left: AtomicUsize::new(0),
            sample_known: AtomicUsize::new(0),
            sample_missing: AtomicUsize::new(0),
            want_bytes: AtomicU64::new(0),
            bytes_total,
            bytes_done: AtomicU64::new(0),
            enc_total: AtomicU64::new(bytes_total),
            enc_done: AtomicU64::new(0),
            missing: AtomicUsize::new(0),
            phase: AtomicU8::new(PH_PROBE),
            cancelled: AtomicBool::new(false),
            parked: Mutex::new(None),
            route: JobRoute::default(),
            grant: AtomicU64::new(u64::MAX),
            taken: AtomicU64::new(0),
            started: Instant::now(),
            finished,
        })
    }

    /// Tidies an already finished download folder (`nzbfast tidy`).
    pub fn tidy_dir(dir: &Path, name: &str, password: Option<String>) -> Vec<String> {
        let nzb = Nzb { name: name.to_string(), files: vec![], password };
        let job = Job::new(0, nzb, dir.to_path_buf(), None, Arc::new(|_: &Job, _| {}));
        job.tidy_output()
    }

    /// Queues work for this job, or holds it back while the job is paused.
    pub fn enqueue(self: &Arc<Self>, q: &Queues, items: Vec<Work>, front: bool) {
        if items.is_empty() {
            return;
        }
        let mut p = self.parked.lock().unwrap();
        if let Some(v) = p.as_mut() {
            v.extend(items);
        } else if front {
            q.push_front(items);
        } else {
            q.push_back(items);
        }
    }

    pub fn pause(self: &Arc<Self>, q: &Queues) {
        let mut p = self.parked.lock().unwrap();
        if p.is_none() {
            *p = Some(q.purge(self));
        }
    }

    pub fn resume(self: &Arc<Self>, q: &Queues) {
        let items = self.parked.lock().unwrap().take();
        if let Some(v) = items {
            q.push_front(v);
        }
    }

    /// Stops the job: queued work is dropped, in-flight articles are discarded as they
    /// arrive, and `finished` is still called once all I/O has drained.
    pub fn cancel(self: &Arc<Self>, q: &Queues) {
        self.cancelled.store(true, Relaxed);
        self.aborted.store(true, Relaxed);
        let mut items = q.purge(self);
        if let Some(v) = self.parked.lock().unwrap().take() {
            items.extend(v);
        }
        for w in items {
            w.dropped(q);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Relaxed)
    }

    /// Current location of a planned output file.
    fn cur(&self, p: &Path) -> PathBuf {
        self.renamed.lock().unwrap().get(p).cloned().unwrap_or_else(|| p.to_path_buf())
    }

    /// Gives leftover archive volumes the names par2 knows them by (obfuscated posts
    /// often carry meaningless names, while the order of RAR4 volumes is only in
    /// their names).
    fn rename_by_par2(&self, plan: &Plan, set: &Par2Set, map: &[Option<VSrc>]) -> usize {
        let aux = self.work.join(".aux");
        let mut n = 0;
        for (k, m) in map.iter().enumerate() {
            let Some(VSrc::Out { out, .. }) = m else { continue };
            let Some(orig) = plan.paths.get(*out) else { continue };
            let path = self.cur(orig);
            let want = sanitize(&set.files[&set.recovery_ids[k]].name);
            if !path.starts_with(&aux) || path.file_name() == want.file_name() {
                continue;
            }
            let Some(dir) = path.parent() else { continue };
            let dst = dir.join(want.file_name().unwrap_or_default());
            if !dst.exists() && fs::rename(&path, &dst).is_ok() {
                self.renamed.lock().unwrap().insert(orig.clone(), dst);
                n += 1;
            }
        }
        n
    }

    /// Gives plain output files (nfo, subtitles, samples of obfuscated posts) the names par2
    /// knows them by, matched by length and the MD5 of their first 16 KiB as par2 does.
    fn rename_plain_by_par2(&self, plan: &Plan) -> usize {
        if plan.par2_outs.is_empty() {
            return 0;
        }
        let set = self.load_par2(plan);
        if set.files.is_empty() {
            return 0;
        }
        let known: std::collections::HashSet<&str> = set.files.values().map(|f| f.name.as_str()).collect();
        let mut n = 0;
        for t in &plan.targets {
            let Target::Plain { out } = *t else { continue };
            if plan.par2_outs.contains(&out) {
                continue;
            }
            let path = self.cur(&plan.paths[out]);
            let Some(name) = path.file_name().map(|s| s.to_string_lossy().into_owned()) else { continue };
            if known.contains(name.as_str()) {
                continue;
            }
            let Ok(meta) = fs::metadata(&path) else { continue };
            let Ok(f) = File::open(&path) else { continue };
            let mut head = vec![0u8; (meta.len() as usize).min(16384)];
            read_fill(&f, &mut head, 0);
            let id = par2::md5_16k(&head);
            let mut hits = set.files.values().filter(|d| d.len == meta.len() && d.md5_16k == id);
            let (Some(d), None) = (hits.next(), hits.next()) else { continue };
            let want = sanitize(&d.name);
            let Some(dst) = path.parent().map(|p| p.join(want.file_name().unwrap_or_default())) else { continue };
            if !dst.exists() && fs::rename(&path, &dst).is_ok() {
                self.renamed.lock().unwrap().insert(plan.paths[out].clone(), dst);
                n += 1;
            }
        }
        n
    }

    /// The par2 set of this job (index plus any recovery volumes fetched so far).
    fn load_par2(&self, plan: &Plan) -> Par2Set {
        let mut set = Par2Set::default();
        for &o in &plan.par2_outs {
            set.add_file(fs::read(self.cur(&plan.paths[o])).unwrap_or_default());
        }
        for p in self.repair.lock().unwrap().paths.clone() {
            set.add_file(fs::read(p).unwrap_or_default());
        }
        set
    }

    pub fn seg_bytes(&self, file: u32, seg: u32) -> u64 {
        self.files[file as usize].segs[seg as usize].bytes as u64
    }

    /// Work items for the probe phase (first article of every wanted file).
    pub fn probe_work(self: &Arc<Self>) -> Vec<Work> {
        let mut v = vec![];
        for (i, f) in self.files.iter().enumerate() {
            if !f.skip {
                v.push(Work { job: self.clone(), file: i as u32, seg: 0, tried: 0, bounces: 0, stat: false, skipped: 0 });
            }
        }
        if v.is_empty() {
            self.phase.store(PH_DONE, Relaxed);
            (self.finished)(self, JobResult { ok: false, summary: "no downloadable files".into(), parts: vec!["no downloadable files".into()] });
        }
        v
    }

    pub fn msgid(&self, file: u32, seg: u32) -> &str {
        &self.files[file as usize].segs[seg as usize].msgid
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Relaxed)
    }

    pub fn on_article(self: &Arc<Self>, file: u32, seg: u32, info: &YInfo, data: &[u8], crc: u32, q: &Queues) {
        self.bytes_done.fetch_add(data.len() as u64, Relaxed);
        self.enc_done.fetch_add(self.seg_bytes(file, seg), Relaxed);
        if seg == 0 && self.plan.get().is_none() {
            self.on_probe(file, Some(Probe { info: info.clone(), data: data.to_vec(), crc }), q);
            return;
        }
        self.apply(file, info, data, crc);
        self.segment_done();
    }

    pub fn on_missing(self: &Arc<Self>, file: u32, seg: u32, q: &Queues) {
        self.missing.fetch_add(1, Relaxed);
        self.state.lock().unwrap().missing_files.insert(file);
        let b = self.files[file as usize].segs[seg as usize].bytes as u64;
        let mb = self.missing_bytes.fetch_add(b, Relaxed) + b;
        if self.stage.load(Relaxed) == 0 && mb > self.recoverable.load(Relaxed).saturating_add(1 << 20) {
            self.aborted.store(true, Relaxed);
        }
        self.drop_work(file, seg, q);
    }

    /// One STAT answer: `Some(found)`, or `None` when the server could not tell (or the
    /// item was dropped). Once the whole sample is in, a job whose projected loss is far
    /// beyond its par2 recovery is aborted before most of it is downloaded. The sample is
    /// spread evenly over every data file, unlike the in-order download, so a release
    /// that only lost a few whole volumes is not mistaken for a dead one.
    pub fn on_stat(&self, found: Option<bool>) {
        if let Some(f) = found {
            self.sample_known.fetch_add(1, Relaxed);
            if !f {
                self.sample_missing.fetch_add(1, Relaxed);
            }
        }
        if self.sample_left.fetch_sub(1, Relaxed) != 1 {
            return;
        }
        let (known, miss) = (self.sample_known.load(Relaxed) as u64, self.sample_missing.load(Relaxed) as u64);
        if self.stage.load(Relaxed) != 0 || self.remaining.load(Relaxed) == 0 || miss < SAMPLE_MIN_MISSING || known * 2 < self.sample_n.load(Relaxed) as u64 {
            return;
        }
        let projected = self.want_bytes.load(Relaxed) / known * miss;
        if projected > self.recoverable.load(Relaxed).saturating_mul(2).saturating_add(1 << 20) {
            self.aborted.store(true, Relaxed);
        }
    }

    /// Accounts for a work item without downloading it (missing or job aborted).
    pub fn drop_work(self: &Arc<Self>, file: u32, seg: u32, q: &Queues) {
        self.enc_done.fetch_add(self.seg_bytes(file, seg), Relaxed);
        if seg == 0 && self.plan.get().is_none() {
            self.on_probe(file, None, q);
        } else {
            self.segment_done();
        }
    }

    fn on_probe(self: &Arc<Self>, file: u32, p: Option<Probe>, q: &Queues) {
        let mut st = self.state.lock().unwrap();
        st.probes[file as usize] = p;
        st.probes_left -= 1;
        if st.probes_left > 0 {
            return;
        }
        let probes = std::mem::take(&mut st.probes);
        drop(st);
        let plan = self.build_plan(&probes);
        let mut rest = vec![];
        let mut remaining = 0;
        let hopeless = probes.iter().all(|p| p.is_none()) || self.is_aborted();
        for (i, t) in plan.targets.iter().enumerate() {
            if hopeless || matches!(t, Target::Skip) {
                continue;
            }
            for s in 1..self.files[i].segs.len() {
                rest.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0, bounces: 0, stat: false, skipped: 0 });
                remaining += 1;
            }
        }
        // Exact recovery capacity: everything that turned out to be par2.
        let mut rec: u64 = self.files.iter().filter(|f| f.skip).flat_map(|f| f.segs.iter()).map(|s| s.bytes as u64).sum();
        for (i, t) in plan.targets.iter().enumerate() {
            if let Target::Plain { out } = t {
                if plan.par2_outs.contains(out) {
                    rec += self.files[i].segs.iter().map(|s| s.bytes as u64).sum::<u64>();
                }
            }
        }
        self.recoverable.store(rec, Relaxed);
        // A file whose first article is gone is almost always gone entirely (takedown).
        let projected: u64 = probes
            .iter()
            .enumerate()
            .filter(|(i, p)| p.is_none() && !self.files[*i].skip)
            .map(|(i, _)| self.files[i].segs.iter().map(|s| s.bytes as u64).sum::<u64>())
            .sum();
        if projected.max(self.missing_bytes.load(Relaxed)) > rec.saturating_add(1 << 20) {
            self.aborted.store(true, Relaxed);
            rest.clear();
            remaining = 0;
        }
        // Segments that will never be requested count as handled for progress.
        let skipped: u64 = self
            .files
            .iter()
            .enumerate()
            .filter(|(i, f)| !f.skip && (rest.is_empty() || matches!(plan.targets[*i], Target::Skip)))
            .flat_map(|(_, f)| f.segs.iter().skip(1))
            .map(|s| s.bytes as u64)
            .sum();
        self.enc_done.fetch_add(skipped, Relaxed);
        // +1 guard so the job cannot finish while probes are still being written.
        self.remaining.store(remaining + 1, Relaxed);
        let _ = self.plan.set(plan);
        self.phase.store(PH_DOWNLOAD, Relaxed);
        for (i, p) in probes.iter().enumerate() {
            if let Some(p) = p {
                self.apply(i as u32, &p.info, &p.data, p.crc);
            }
        }
        // Availability sample (STAT) ahead of the download, for jobs with par2 to judge by.
        let mut sample = vec![];
        if !rest.is_empty() && rec > 0 {
            let plan = self.plan.get().expect("plan");
            let mut want = 0u64;
            for (i, t) in plan.targets.iter().enumerate() {
                if matches!(t, Target::Skip) || plan.par2_outs.iter().any(|o| matches!(t, Target::Plain { out } if out == o)) {
                    continue;
                }
                let f = &self.files[i];
                want += f.segs.iter().map(|s| s.bytes as u64).sum::<u64>();
                for s in (SAMPLE_EVERY / 2..f.segs.len()).step_by(SAMPLE_EVERY) {
                    sample.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0, bounces: 0, stat: true, skipped: 0 });
                }
            }
            if sample.len() >= SAMPLE_MIN {
                self.want_bytes.store(want, Relaxed);
                self.sample_n.store(sample.len(), Relaxed);
                self.sample_left.store(sample.len(), Relaxed);
            } else {
                sample.clear();
            }
        }
        self.enqueue(q, rest, false);
        self.enqueue(q, sample, true);
        self.segment_done();
    }

    fn segment_done(self: &Arc<Self>) {
        if self.remaining.fetch_sub(1, Relaxed) == 1 {
            let job = self.clone();
            std::thread::spawn(move || job.finalize());
        }
    }

    fn apply(self: &Arc<Self>, file: u32, info: &YInfo, data: &[u8], crc: u32) {
        let plan = self.plan.get().expect("plan");
        let owner: Arc<dyn IoOwner> = self.clone();
        match plan.targets[file as usize] {
            Target::Skip => {
                let out = self.repair.lock().unwrap().outs.get(&file).cloned();
                if let Some(out) = out {
                    out.write(info.offset(), data, &owner);
                }
            }
            Target::Plain { out } => plan.outs[out].write(info.offset(), data, &owner),
            Target::Rar { vol } => {
                let vm = &plan.vols[vol];
                let a = info.offset();
                let b = a + data.len() as u64;
                let ds = vm.data_start;
                let de = ds + vm.data_len;
                let lo = a.max(ds);
                let hi = b.min(de);
                if lo < hi {
                    let slice = &data[(lo - a) as usize..(hi - a) as usize];
                    plan.outs[vm.out].write(vm.out_off + (lo - ds), slice, &owner);
                    let pcrc = if lo == a && hi == b { crc } else { crc32fast::hash(slice) };
                    self.state.lock().unwrap().pieces.push(Piece { vol: vol as u32, off: lo - ds, len: hi - lo, crc: pcrc });
                }
                if b > de {
                    // Archive trailer bytes: kept so par2 can verify the virtual volume.
                    let t0 = a.max(de);
                    let mut tail = vm.tail.lock().unwrap();
                    let at = (t0 - de) as usize;
                    let src = &data[(t0 - a) as usize..];
                    let end = (at + src.len()).min(tail.len());
                    if at < end {
                        tail[at..end].copy_from_slice(&src[..end - at]);
                    }
                }
            }
        }
    }

    fn build_plan(&self, probes: &[Option<Probe>]) -> Plan {
        let aux = self.work.join(".aux");
        let mut targets: Vec<Target> = Vec::with_capacity(self.files.len());
        let mut outs = vec![];
        let mut paths = vec![];
        let mut notes = vec![];
        let mut rar_cands: Vec<(usize, RarVol, u64, String)> = vec![];
        let mut main = vec![];
        let mut par2_outs = vec![];
        let mut raw_rar = vec![];
        let mut sevenz = vec![];
        let mut ids = vec![FileId::default(); self.files.len()];
        let job_name = self.name.clone();
        // `data` is the file's first article. Obfuscated video names become the job name;
        // a video posted without an extension gets one from its header (not split pieces
        // like `x.mkv.001`, whose first part also starts with a video header).
        let deobf = |name: &str, data: &[u8]| -> String {
            if is_video(name) {
                if !is_obfuscated(name) {
                    return name.to_string();
                }
                let ext = name.rsplit('.').next().unwrap_or("mkv");
                return match episode_title(data) {
                    Some(t) => format!("{t}.{ext}"),
                    None => format!("{job_name}.{ext}"),
                };
            }
            let ext = name.rsplit_once('.').map(|(_, e)| e);
            if ext.is_some_and(|e| e.bytes().all(|c| c.is_ascii_digit())) {
                return name.to_string();
            }
            match video_magic(data) {
                Some(v) if is_obfuscated(name) => format!("{}.{v}", episode_title(data).unwrap_or_else(|| job_name.clone())),
                Some(v) => format!("{name}.{v}"),
                None => name.to_string(),
            }
        };
        // Renamed outputs must not land on each other (or on a posted name).
        let mut used: std::collections::HashSet<String> = self.files.iter().map(|f| sanitize(&f.guess).to_string_lossy().to_ascii_lowercase()).collect();
        let make = |dir: &Path, name: &str, size: u64, outs: &mut Vec<Arc<OutFile>>, paths: &mut Vec<PathBuf>| -> Target {
            let path = dir.join(sanitize(name));
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            match OutFile::create(&path, size, true) {
                Ok(file) => {
                    outs.push(file);
                    paths.push(path);
                    Target::Plain { out: outs.len() - 1 }
                }
                Err(e) => {
                    eprintln!("[{}] cannot create {name}: {e}", self.name);
                    Target::Skip
                }
            }
        };
        // One name per file: some posters give every file the same yEnc name, which
        // would make them overwrite each other. Fall back to the subject's name, then
        // to a numbered name.
        let mut chosen: Vec<String> = self
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| match &probes[i] {
                Some(p) if !p.info.name.is_empty() => p.info.name.clone(),
                _ => f.guess.clone(),
            })
            .collect();
        {
            let mut count: HashMap<String, usize> = HashMap::new();
            for (i, n) in chosen.iter().enumerate() {
                if !self.files[i].skip {
                    *count.entry(n.to_lowercase()).or_default() += 1;
                }
            }
            let guesses: HashMap<String, usize> = self.files.iter().fold(HashMap::new(), |mut m, f| {
                *m.entry(f.guess.to_lowercase()).or_default() += 1;
                m
            });
            for (i, n) in chosen.iter_mut().enumerate() {
                if self.files[i].skip || count.get(&n.to_lowercase()).copied().unwrap_or(0) < 2 {
                    continue;
                }
                let g = &self.files[i].guess;
                *n = if guesses.get(&g.to_lowercase()).copied().unwrap_or(0) == 1 { g.clone() } else { format!("{n}.{i}") };
            }
        }
        for (i, f) in self.files.iter().enumerate() {
            if f.skip {
                targets.push(Target::Skip);
                continue;
            }
            let Some(p) = &probes[i] else {
                notes.push(format!("first article missing for {}", f.guess));
                let dir = if looks_like_rar(&f.guess) {
                    aux.join("rar")
                } else if looks_like_7z(&f.guess) {
                    aux.join("7z")
                } else {
                    aux.clone()
                };
                let t = make(&dir, &f.guess, 0, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    if looks_like_rar(&f.guess) {
                        raw_rar.push(out);
                    } else if looks_like_7z(&f.guess) {
                        sevenz.push(out);
                    }
                }
                ids[i] = FileId { yname: f.guess.clone(), size: 0, md5_16k: None };
                targets.push(t);
                continue;
            };
            let name = chosen[i].clone();
            if p.info.offset() > 0 {
                // The NZB lacks the file's first segment: its type cannot be read from
                // the data, so go by name and leave the gap to par2.
                notes.push(format!("first segment not in NZB for {name}"));
                let (dir, kind) = if looks_like_rar(&name) {
                    (aux.join("rar"), 1)
                } else if looks_like_7z(&name) {
                    (aux.join("7z"), 2)
                } else if is_par2_name(&name) {
                    (aux.clone(), 3)
                } else {
                    (self.work.clone(), 0)
                };
                let t = make(&dir, &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    match kind {
                        1 => raw_rar.push(out),
                        2 => sevenz.push(out),
                        3 => par2_outs.push(out),
                        _ => {}
                    }
                }
                ids[i] = FileId { yname: name.clone(), size: p.info.size, md5_16k: None };
                targets.push(t);
                continue;
            }
            let whole = p.data.len() as u64 == p.info.size;
            ids[i] = FileId {
                yname: name.clone(),
                size: p.info.size,
                md5_16k: if p.data.len() >= 16384 || whole { Some(par2::md5_16k(&p.data)) } else { None },
            };
            if rar::is_rar(&p.data) {
                if let Some(v) = rar::parse_volume(&p.data) {
                    if v.stored && !v.encrypted && v.data_start <= p.data.len() as u64 {
                        rar_cands.push((i, v, p.info.size, name));
                        targets.push(Target::Skip); // replaced below
                        continue;
                    }
                    notes.push(format!("{name}: {} archive, unpacked after download", if v.encrypted { "encrypted" } else { "compressed" }));
                }
                let t = make(&aux.join("rar"), &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    raw_rar.push(out);
                }
                targets.push(t);
            } else if p.data.starts_with(SIG_7Z) || looks_like_7z(&name) {
                let t = make(&aux.join("7z"), &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    sevenz.push(out);
                }
                targets.push(t);
            } else if p.data.starts_with(b"PAR2\0PKT") {
                let t = make(&aux, &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    par2_outs.push(out);
                }
                targets.push(t);
            } else {
                let mut fname = deobf(&name, &p.data);
                if fname != name {
                    let (stem, ext) = fname.rsplit_once('.').map(|(a, b)| (a.to_string(), b.to_string())).unwrap_or_default();
                    let mut k = 2;
                    while !used.insert(sanitize(&fname).to_string_lossy().to_ascii_lowercase()) {
                        fname = format!("{stem}.{k}.{ext}");
                        k += 1;
                    }
                }
                if is_video(&fname) {
                    main.push(format!("{fname} ({:.2} GB)", p.info.size as f64 / 1e9));
                }
                targets.push(make(&self.work, &fname, p.info.size, &mut outs, &mut paths));
            }
        }

        // Group stored RAR volumes into sets by the file they contain.
        let mut groups: HashMap<String, Vec<(usize, RarVol, u64, String)>> = HashMap::new();
        for c in rar_cands {
            groups.entry(c.1.inner_name.clone()).or_default().push(c);
        }
        let mut vols = vec![];
        let mut sets = vec![];
        let mut checked = vec![];
        for (inner, mut g) in groups {
            let all5 = g.iter().all(|c| c.1.vol_num.is_some());
            let keyed: Option<Vec<u64>> =
                if all5 { Some(g.iter().map(|c| c.1.vol_num.unwrap()).collect()) } else { g.iter().map(|c| rar::name_order(&c.3)).collect() };
            let valid = keyed.is_some() && {
                let keys = keyed.unwrap();
                let mut idx: Vec<usize> = (0..g.len()).collect();
                idx.sort_by_key(|&k| keys[k]);
                g = idx.iter().map(|&k| g[k].clone()).collect();
                let n = g.len();
                let mut ok = true;
                let mut sum = 0;
                for (k, c) in g.iter().enumerate() {
                    let v = &c.1;
                    ok &= v.split_before == (k > 0) && v.split_after == (k + 1 < n);
                    ok &= v.data_start + v.pack_size <= c.2 && c.2 - (v.data_start + v.pack_size) <= 4096;
                    sum += v.pack_size;
                }
                ok && sum == g[0].1.unp_size
            };
            checked.push((inner, g, valid));
        }
        // One odd set (typically an archive holding several large files, whose later
        // files start mid-volume) means volumes cannot be mapped file by file: keep them
        // all and unpack after the download.
        let all_valid = checked.iter().all(|c| c.2);
        if !all_valid {
            notes.push("RAR layout not streamable (several files or incomplete set): unpacking after download".into());
        }
        for (inner, g, _) in checked {
            if !all_valid {
                for c in g {
                    let t = make(&aux.join("rar"), &c.3, c.2, &mut outs, &mut paths);
                    if let Target::Plain { out } = t {
                        raw_rar.push(out);
                    }
                    targets[c.0] = t;
                }
                continue;
            }
            // The packed file's first bytes follow the RAR headers in the first volume.
            let first = probes[g[0].0].as_ref().and_then(|p| p.data.get(g[0].1.data_start as usize..)).unwrap_or(&[]);
            let out_name = deobf(&inner, first);
            let Target::Plain { out } = make(&self.work, &out_name, g[0].1.unp_size, &mut outs, &mut paths) else { continue };
            let mut off = 0;
            let mut vidx = vec![];
            for c in &g {
                let ds = c.1.data_start as usize;
                let head = probes[c.0].as_ref().map(|p| p.data[..ds].to_vec()).unwrap_or_default();
                let tail_len = (c.2 - c.1.data_start - c.1.pack_size) as usize;
                vols.push(VolMap {
                    out,
                    size: c.2,
                    data_start: c.1.data_start,
                    data_len: c.1.pack_size,
                    out_off: off,
                    info: c.1.clone(),
                    head,
                    tail: Mutex::new(vec![0; tail_len]),
                });
                off += c.1.pack_size;
                targets[c.0] = Target::Rar { vol: vols.len() - 1 };
                vidx.push(vols.len() - 1);
            }
            sets.push(RarSet { name: out_name, out, vols: vidx, unp_size: g[0].1.unp_size });
        }
        Plan { targets, outs, paths, vols, sets, notes, main, ids, par2_outs, raw_rar, sevenz }
    }

    // ---------- verification ----------

    fn check_set(plan: &Plan, set: &RarSet, part_crcs: &[u32]) -> Option<String> {
        let mut full = crc32fast::Hasher::new();
        let n = set.vols.len();
        for (k, &v) in set.vols.iter().enumerate() {
            let vm = &plan.vols[v];
            full.combine(&crc32fast::Hasher::new_with_initial_len(part_crcs[k], vm.data_len));
            if let Some(want) = vm.info.data_crc {
                let last = k + 1 == n;
                if want != part_crcs[k] && !(last && want == full.clone().finalize()) {
                    return Some(format!("volume {k} CRC mismatch"));
                }
            }
        }
        None
    }

    /// Verifies stored RAR sets: from per-article CRCs, or by re-reading the
    /// extracted file after a repair.
    fn verify_sets(&self, plan: &Plan, from_disk: bool) -> (Vec<String>, bool) {
        let mut pieces = std::mem::take(&mut self.state.lock().unwrap().pieces);
        pieces.sort_by_key(|p| (p.vol, p.off));
        let mut res = vec![];
        let mut all_ok = true;
        for set in &plan.sets {
            let mut crcs = vec![];
            let mut bad = None;
            if from_disk {
                let f = File::open(&plan.paths[set.out]);
                let mut buf = vec![0u8; 8 << 20];
                for &v in &set.vols {
                    let vm = &plan.vols[v];
                    let mut h = crc32fast::Hasher::new();
                    let mut pos = 0;
                    while let Ok(f) = &f {
                        if pos >= vm.data_len {
                            break;
                        }
                        let n = (vm.data_len - pos).min(buf.len() as u64) as usize;
                        read_fill(f, &mut buf[..n], vm.out_off + pos);
                        h.update(&buf[..n]);
                        pos += n as u64;
                    }
                    crcs.push(h.finalize());
                }
            } else {
                for (k, &v) in set.vols.iter().enumerate() {
                    let vm = &plan.vols[v];
                    let mut part = crc32fast::Hasher::new();
                    let mut pos = 0;
                    for p in pieces.iter().filter(|p| p.vol as usize == v) {
                        if p.off != pos {
                            bad = Some(format!("gap in volume {k} at {pos}"));
                            break;
                        }
                        part.combine(&crc32fast::Hasher::new_with_initial_len(p.crc, p.len));
                        pos += p.len;
                    }
                    if bad.is_none() && pos != vm.data_len {
                        bad = Some(format!("volume {k} short: {pos}/{}", vm.data_len));
                    }
                    if bad.is_some() {
                        break;
                    }
                    crcs.push(part.finalize());
                }
            }
            if bad.is_none() {
                bad = Self::check_set(plan, set, &crcs);
            }
            all_ok &= bad.is_none();
            res.push(match bad {
                None => format!("{} ({:.2} GB) extracted+verified", set.name, set.unp_size as f64 / 1e9),
                Some(e) => format!("{} FAILED: {e}", set.name),
            });
        }
        (res, all_ok)
    }

    /// Writes small stored files that follow the main file in a set's last volume
    /// (subtitles, nfo) from the bytes kept for par2 verification.
    fn extract_tail_files(&self, plan: &Plan) -> Vec<String> {
        let mut res = vec![];
        for set in &plan.sets {
            let Some(&last) = set.vols.last() else { continue };
            let vm = &plan.vols[last];
            let tail = vm.tail.lock().unwrap().clone();
            for f in rar::tail_files(&tail, vm.info.rar5) {
                if !f.stored || !f.complete {
                    res.push(format!("{}: not extracted (packed after the main file)", f.name));
                    continue;
                }
                let data = &tail[f.data.clone()];
                if f.crc.is_some_and(|c| c != crc32fast::hash(data)) {
                    res.push(format!("{}: CRC mismatch, skipped", f.name));
                    continue;
                }
                let dst = self.work.join(sanitize(&self.output_name(&f.name)));
                if let Some(parent) = dst.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                match fs::write(&dst, data) {
                    Ok(()) => res.push(format!("{} extracted", f.name)),
                    Err(e) => res.push(format!("{}: {e}", f.name)),
                }
            }
        }
        res
    }

    /// Extracts RAR sets that had to be downloaded as volumes: stored sets by copying
    /// their payload, anything else (compressed, encrypted) with the UnRAR library.
    fn extract_raw(&self, plan: &Plan) -> (Vec<String>, bool) {
        let mut res = vec![];
        let mut ok = true;
        let mut groups: HashMap<String, Vec<(PathBuf, RarVol, u64)>> = HashMap::new();
        let mut others: Vec<(PathBuf, Option<RarVol>, usize)> = vec![];
        let nzb_order = |o: usize| plan.targets.iter().position(|t| matches!(t, Target::Plain { out } if *out == o)).unwrap_or(usize::MAX);
        let mut order: HashMap<PathBuf, usize> = HashMap::new();
        // The directory is authoritative: repair may have added or renamed volumes.
        let index: HashMap<PathBuf, usize> = plan.raw_rar.iter().map(|&o| (self.cur(&plan.paths[o]), nzb_order(o))).collect();
        let mut vols: Vec<PathBuf> = fs::read_dir(self.work.join(".aux").join("rar")).map(|rd| rd.flatten().map(|e| e.path()).filter(|p| p.is_file()).collect()).unwrap_or_default();
        vols.sort();
        for p in &vols {
            let o_order = index.get(p).copied().unwrap_or(usize::MAX);
            order.insert(p.clone(), o_order);
            let mut head = vec![0u8; 65536];
            let n = File::open(p).and_then(|mut f| f.read(&mut head)).unwrap_or(0);
            head.truncate(n);
            match rar::parse_volume(&head) {
                Some(v) if v.stored && !v.encrypted => {
                    let size = fs::metadata(p).map(|m| m.len()).unwrap_or(0);
                    groups.entry(v.inner_name.clone()).or_default().push((p.clone(), v, size));
                }
                v => others.push((p.clone(), v, o_order)),
            }
        }
        for g in groups.values_mut() {
            let all5 = g.iter().all(|c| c.1.vol_num.is_some());
            g.sort_by_key(|c| if all5 { c.1.vol_num.unwrap() } else { rar::name_order(&c.0.to_string_lossy()).unwrap_or(u64::MAX) });
        }
        // Stored volumes can be copied file by file only if every file starts in its own
        // volume and nothing else needs the library: a stored file can share a volume
        // with the start of a compressed one (e.g. a small "rename" file in part01), and
        // extracting it would consume that volume.
        let clean = others.is_empty()
            && groups.values().all(|g| {
                g.iter().map(|c| c.1.pack_size).sum::<u64>() == g[0].1.unp_size && !g[0].1.split_before && !g.last().unwrap().1.split_after
            });
        if !clean {
            for (_, g) in groups.drain() {
                for (p, v, _) in g {
                    let k = order.get(&p).copied().unwrap_or(usize::MAX);
                    others.push((p, Some(v), k));
                }
            }
        }
        for (inner, g) in groups {
            let total: u64 = g.iter().map(|c| c.1.pack_size).sum();
            if total != g[0].1.unp_size || g[0].1.split_before || g.last().unwrap().1.split_after {
                res.push(format!("{inner} FAILED: incomplete RAR set ({} volumes)", g.len()));
                ok = false;
                continue;
            }
            let name = self.output_name(&inner);
            let dst = self.work.join(sanitize(&name));
            let r = (|| -> std::io::Result<Option<String>> {
                if let Some(parent) = dst.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut out = File::create(&dst)?;
                let mut buf = vec![0u8; 8 << 20];
                let mut full = crc32fast::Hasher::new();
                let n = g.len();
                for (k, (p, v, _)) in g.iter().enumerate() {
                    let mut f = File::open(p)?;
                    f.seek(SeekFrom::Start(v.data_start))?;
                    let mut left = v.pack_size;
                    let mut h = crc32fast::Hasher::new();
                    while left > 0 {
                        let m = left.min(buf.len() as u64) as usize;
                        f.read_exact(&mut buf[..m])?;
                        h.update(&buf[..m]);
                        out.write_all(&buf[..m])?;
                        left -= m as u64;
                    }
                    full.combine(&h);
                    let pc = h.finalize();
                    if let Some(want) = v.data_crc {
                        if want != pc && !(k + 1 == n && want == full.clone().finalize()) {
                            return Ok(Some(format!("volume {k} CRC mismatch")));
                        }
                    }
                }
                out.sync_data()?;
                Ok(None)
            })();
            match r {
                Ok(None) => {
                    res.push(format!("{name} ({:.2} GB) extracted from volumes+verified", total as f64 / 1e9));
                    for (p, _, _) in &g {
                        let _ = fs::remove_file(p);
                    }
                }
                Ok(Some(e)) => {
                    res.push(format!("{name} FAILED: {e}"));
                    ok = false;
                }
                Err(e) => {
                    res.push(format!("{name} FAILED: {e}"));
                    ok = false;
                }
            }
        }
        if !others.is_empty() {
            match self.unrar_all(others) {
                Ok(msgs) => res.extend(msgs),
                Err(e) => {
                    res.insert(0, format!("Unpacking failed: {e}"));
                    ok = false;
                }
            }
        }
        (res, ok)
    }

    /// Output name for an extracted file: obfuscated video names become the job name.
    fn output_name(&self, inner: &str) -> String {
        if is_video(inner) && is_obfuscated(inner) {
            format!("{}.{}", self.name, inner.rsplit('.').next().unwrap_or("mkv"))
        } else {
            inner.to_string()
        }
    }

    /// Unpacks compressed/encrypted RAR volumes. Volumes are hard-linked under canonical
    /// names (`v.part001.rar` or `v.rar`/`v.r00`) so the library can walk the set even
    /// when the posted names are obfuscated.
    fn unrar_all(&self, vols: Vec<(PathBuf, Option<RarVol>, usize)>) -> Result<Vec<String>, String> {
        let mut sets: std::collections::BTreeMap<String, Vec<(PathBuf, Option<RarVol>, usize)>> = Default::default();
        for v in vols {
            let name = v.0.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            sets.entry(rar::base_name(&name)).or_default().push(v);
        }
        let mut msgs = vec![];
        for (k, (base, mut g)) in sets.into_iter().enumerate() {
            let fname = |p: &Path| p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            if g.iter().all(|x| x.1.as_ref().is_some_and(|v| v.rar5 && !v.encrypted && v.vol_num.is_some())) {
                g.sort_by_key(|x| x.1.as_ref().and_then(|v| v.vol_num));
            } else if g.iter().all(|x| rar::name_order(&fname(&x.0)).is_some()) {
                g.sort_by_key(|x| rar::name_order(&fname(&x.0)));
            } else {
                let tails: Vec<Option<u64>> = g.iter().map(|x| Self::rar4_volnum(&x.0)).collect();
                if tails.iter().all(|t| t.is_some()) {
                    let mut idx: Vec<usize> = (0..g.len()).collect();
                    idx.sort_by_key(|&i| tails[i]);
                    let mut sorted = Vec::with_capacity(g.len());
                    let mut old: Vec<Option<(PathBuf, Option<RarVol>, usize)>> = g.into_iter().map(Some).collect();
                    for i in idx {
                        sorted.push(old[i].take().unwrap());
                    }
                    g = sorted;
                } else {
                    // Posting order usually matches volume order.
                    g.sort_by_key(|x| x.2);
                }
            }
            let newnum = g.iter().find_map(|x| x.1.as_ref().map(|v| v.new_numbering)).unwrap_or(true);
            let dir = self.work.join(".aux").join(format!("unrar{k}"));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            for (i, x) in g.iter().enumerate() {
                let d = dir.join(rar::volume_name(i, g.len(), newnum));
                fs::hard_link(&x.0, &d).or_else(|_| fs::rename(&x.0, &d)).map_err(|e| format!("staging volume: {e}"))?;
            }
            let first = dir.join(rar::volume_name(0, g.len(), newnum));
            let label = if base.is_empty() { "obfuscated set".to_string() } else { base.clone() };
            let (n, bytes) = self.unrar_one(&first).map_err(|e| format!("{label}: {e}"))?;
            msgs.push(format!("unpacked {n} file(s), {:.2} GB from {} RAR volumes ({label})", bytes as f64 / 1e9, g.len()));
            let _ = fs::remove_dir_all(&dir);
            for x in &g {
                let _ = fs::remove_file(&x.0);
            }
        }
        Ok(msgs)
    }

    /// Unpacks 7z archives (split `.7z.NNN` volumes are read back to back).
    fn extract_7z(&self, _plan: &Plan) -> (Vec<String>, bool) {
        let mut sets: std::collections::BTreeMap<String, Vec<(u32, PathBuf)>> = Default::default();
        let mut zvols: Vec<PathBuf> = fs::read_dir(self.work.join(".aux").join("7z")).map(|rd| rd.flatten().map(|e| e.path()).filter(|p| p.is_file()).collect()).unwrap_or_default();
        zvols.sort();
        for p in zvols {
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            match sevenz_volume(&name) {
                Some((n, base)) => sets.entry(base).or_default().push((n, p)),
                None => sets.entry(name.to_ascii_lowercase()).or_default().push((1, p)),
            }
        }
        let mut res = vec![];
        let mut ok = true;
        for (base, mut vols) in sets {
            vols.sort_by_key(|v| v.0);
            match self.unpack_7z(&vols) {
                Ok((n, bytes)) => {
                    res.push(format!("unpacked {n} file(s), {:.2} GB from 7z ({base}, {} volume(s))", bytes as f64 / 1e9, vols.len()));
                    for (_, p) in &vols {
                        let _ = fs::remove_file(p);
                    }
                }
                Err(e) => {
                    res.insert(0, format!("Unpacking failed: {base}: {e}"));
                    ok = false;
                }
            }
        }
        (res, ok)
    }

    fn unpack_7z(&self, vols: &[(u32, PathBuf)]) -> Result<(usize, u64), String> {
        if vols.iter().enumerate().any(|(i, v)| v.0 as usize != i + 1) {
            return Err(format!("missing volumes (have {:?})", vols.iter().map(|v| v.0).collect::<Vec<_>>()));
        }
        let mut parts = vec![];
        let mut total = 0;
        for (_, p) in vols {
            let f = File::open(p).map_err(|e| e.to_string())?;
            let len = f.metadata().map_err(|e| e.to_string())?.len();
            total += len;
            parts.push((f, len));
        }
        let pw = match &self.password {
            Some(p) => sevenz_rust2::Password::from(p.as_str()),
            None => sevenz_rust2::Password::empty(),
        };
        let mut ar = sevenz_rust2::ArchiveReader::new(Chain { parts, total, pos: 0 }, pw).map_err(|e| e.to_string())?;
        let (mut n, mut bytes) = (0usize, 0u64);
        let mut err: Option<String> = None;
        let mut buf = vec![0u8; 4 << 20];
        let mut written: Vec<PathBuf> = vec![];
        let r = ar.for_each_entries(|entry, reader| {
            // Entry names come from the archive: never let them escape the job folder.
            let rel = sanitize(&entry.name().replace('\\', "/"));
            if entry.is_directory() {
                let _ = fs::create_dir_all(self.work.join(&rel));
                return Ok(true);
            }
            let rel = match rel.file_name() {
                Some(f) => rel.with_file_name(self.output_name(&f.to_string_lossy())),
                None => rel,
            };
            let dest = self.work.join(&rel);
            if let Some(parent) = dest.parent() {
                let _ = fs::create_dir_all(parent);
            }
            written.push(dest.clone());
            let w = (|| -> std::io::Result<u64> {
                let mut out = File::create(&dest)?;
                let mut written = 0u64;
                loop {
                    let k = reader.read(&mut buf)?;
                    if k == 0 {
                        break;
                    }
                    out.write_all(&buf[..k])?;
                    written += k as u64;
                }
                Ok(written)
            })();
            match w {
                Ok(b) => {
                    n += 1;
                    bytes += b;
                    Ok(true)
                }
                Err(e) => {
                    err = Some(format!("{}: {e}", rel.display()));
                    Ok(false)
                }
            }
        });
        let e = err.or_else(|| r.err().map(|e| e.to_string())).or_else(|| (n == 0).then(|| "archive contains no files".to_string()));
        if let Some(e) = e {
            for p in &written {
                let _ = fs::remove_file(p);
            }
            return Err(e);
        }
        Ok((n, bytes))
    }

    fn rar4_volnum(p: &Path) -> Option<u64> {
        let f = File::open(p).ok()?;
        let len = f.metadata().ok()?.len();
        let n = len.min(64) as usize;
        let mut tail = vec![0u8; n];
        f.read_exact_at(&mut tail, len - n as u64).ok()?;
        rar::rar4_end_volnum(&tail)
    }

    fn unrar_one(&self, first: &Path) -> Result<(usize, u64), String> {
        let arch = match &self.password {
            Some(p) => unrar::Archive::with_password(first, p.as_bytes()),
            None => unrar::Archive::new(first),
        };
        let mut a = arch.open_for_processing().map_err(|e| e.to_string())?;
        let (mut n, mut bytes) = (0, 0);
        let mut written: Vec<PathBuf> = vec![];
        // Partial output of a failed unpack is removed so a retry starts clean.
        let fail = |written: &[PathBuf], e: String| -> Result<(usize, u64), String> {
            for p in written {
                let _ = fs::remove_file(p);
            }
            Err(e)
        };
        loop {
            let h = match a.read_header() {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(e) => return fail(&written, e.to_string()),
            };
            let e = h.entry();
            if !e.is_file() {
                a = match h.skip() {
                    Ok(a) => a,
                    Err(e) => return fail(&written, e.to_string()),
                };
                continue;
            }
            if e.is_encrypted() && self.password.is_none() {
                return fail(&written, "archive is encrypted and no password was given".into());
            }
            // Entry names come from the archive: never let them escape the job folder.
            let raw = e.filename.to_string_lossy().replace('\\', "/");
            let rel = sanitize(&raw);
            let rel = match rel.file_name() {
                Some(f) => rel.with_file_name(self.output_name(&f.to_string_lossy())),
                None => rel,
            };
            let dest = self.work.join(&rel);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let size = e.unpacked_size;
            written.push(dest.clone());
            a = match h.extract_to(&dest) {
                Ok(a) => a,
                Err(e) => return fail(&written, format!("{raw}: {e}")),
            };
            n += 1;
            bytes += size;
        }
        if n == 0 {
            return Err("archive contains no files".into());
        }
        Ok((n, bytes))
    }

    // ---------- output tidy-up ----------

    /// Files in the job folder (not `.aux`), recursively.
    fn output_files(&self) -> Vec<PathBuf> {
        let mut out = vec![];
        let mut dirs = vec![self.work.clone()];
        while let Some(d) = dirs.pop() {
            for e in fs::read_dir(&d).into_iter().flatten().flatten() {
                let p = e.path();
                match e.file_type() {
                    Ok(t) if t.is_dir() && e.file_name() != ".aux" => dirs.push(p),
                    Ok(t) if t.is_file() => out.push(p),
                    _ => {}
                }
            }
        }
        out.sort();
        out
    }

    /// Fixes what the unpackers leave behind that Radarr/Sonarr reject as "no video
    /// files": raw split files (`x.mkv.001`…), files whose names lost their extension,
    /// and archives inside the archive (unpacked only while there is no video yet, so
    /// bundled extras are left alone).
    pub fn tidy_output(&self) -> Vec<String> {
        let mut msgs = vec![];
        for _ in 0..3 {
            let n = self.join_splits();
            if n > 0 {
                msgs.push(format!("joined {n} split file(s)"));
            }
            let n = self.name_by_magic() + self.name_episodes();
            if n > 0 {
                msgs.push(format!("{n} file(s) named by content"));
            }
            if self.output_files().iter().any(|p| is_video(&p.to_string_lossy())) {
                break;
            }
            let (m, n) = self.extract_nested();
            msgs.extend(m);
            if n == 0 {
                break;
            }
        }
        msgs
    }

    /// Joins `x.NNN` pieces (consecutive numbers from 000 or 001) into `x`.
    fn join_splits(&self) -> usize {
        let mut groups: std::collections::BTreeMap<PathBuf, Vec<(u32, PathBuf)>> = Default::default();
        for p in self.output_files() {
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            let Some((base, ext)) = name.rsplit_once('.') else { continue };
            if ext.len() != 3 || !ext.bytes().all(|c| c.is_ascii_digit()) || !base.contains('.') {
                continue;
            }
            groups.entry(p.with_file_name(base)).or_default().push((ext.parse().unwrap(), p.clone()));
        }
        let mut joined = 0;
        for (dest, mut g) in groups {
            g.sort();
            let first = g[0].0;
            if g.len() < 2 || first > 1 || g.iter().enumerate().any(|(i, x)| x.0 != first + i as u32) || dest.exists() {
                continue;
            }
            let r = (|| -> std::io::Result<()> {
                fs::rename(&g[0].1, &dest)?;
                let mut out = OpenOptions::new().append(true).open(&dest)?;
                for (_, p) in &g[1..] {
                    std::io::copy(&mut File::open(p)?, &mut out)?;
                    fs::remove_file(p)?;
                }
                Ok(())
            })();
            if r.is_ok() {
                joined += 1;
            }
        }
        joined
    }

    /// Gives files without a usable extension one from their first bytes (video gets
    /// the job name when its own name is obfuscated).
    fn name_by_magic(&self) -> usize {
        const KNOWN: &[&str] = &[
            "mkv", "mp4", "avi", "m4v", "ts", "m2ts", "wmv", "mov", "mpg", "mpeg", "webm", "flv", "vob", "iso", "img", "ogm", "divx",
            "3gp", "rmvb", "rm", "rar", "zip", "7z", "par2", "nfo", "sfv", "srr", "srs", "nzb", "jpg", "jpeg", "png", "gif", "txt",
            "url", "md5", "srt", "sub", "idx", "ass", "ssa", "vtt", "sup", "exe", "pdf", "mp3", "flac", "m4a", "ac3", "dts", "aac",
            "epub", "html", "htm", "db", "xml", "json",
        ];
        let mut n = 0;
        for p in self.output_files() {
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            let ext = name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).unwrap_or_default();
            if KNOWN.contains(&ext.as_str()) || (ext.len() == 3 && ext.bytes().all(|c| c.is_ascii_digit())) || (!ext.is_empty() && ext.len() <= 4 && ext.starts_with('r') && ext[1..].bytes().all(|c| c.is_ascii_digit())) {
                continue;
            }
            let Ok(f) = File::open(&p) else { continue };
            if f.metadata().map(|m| m.len()).unwrap_or(0) < 1 << 20 {
                continue;
            }
            let mut head = [0u8; 512];
            read_fill(&f, &mut head, 0);
            let new = if let Some(v) = video_magic(&head) {
                self.output_name(&format!("{name}.{v}"))
            } else if head.starts_with(b"Rar!\x1a\x07") {
                format!("{name}.rar")
            } else if head.starts_with(&[0x50, 0x4b, 0x03, 0x04]) {
                format!("{name}.zip")
            } else if head.starts_with(SIG_7Z) {
                format!("{name}.7z")
            } else {
                continue;
            };
            let mut dest = p.with_file_name(&new);
            let mut k = 1;
            while dest.exists() {
                let (stem, e) = new.rsplit_once('.').unwrap_or((&new, ""));
                dest = p.with_file_name(format!("{stem}.{k}.{e}"));
                k += 1;
            }
            if fs::rename(&p, &dest).is_ok() {
                n += 1;
            }
        }
        n
    }

    /// Videos named after the job (`job.mkv`, `job.2.mkv`… from obfuscated packs) or
    /// still obfuscated get the SxxEyy release name stored in their MKV title.
    fn name_episodes(&self) -> usize {
        let job = self.name.to_ascii_lowercase();
        let mut n = 0;
        for p in self.output_files() {
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            let Some((stem, ext)) = name.rsplit_once('.') else { continue };
            if !is_video(&name) {
                continue;
            }
            let l = stem.to_ascii_lowercase();
            let numbered = l.strip_prefix(&job).is_some_and(|r| r.is_empty() || (r.len() > 1 && r.starts_with('.') && r[1..].bytes().all(|c| c.is_ascii_digit())));
            if !numbered && !is_obfuscated(&name) {
                continue;
            }
            let Ok(f) = File::open(&p) else { continue };
            let mut head = vec![0u8; 1 << 20];
            read_fill(&f, &mut head, 0);
            let Some(t) = episode_title(&head) else { continue };
            let dest = p.with_file_name(format!("{t}.{ext}"));
            if !dest.exists() && fs::rename(&p, &dest).is_ok() {
                n += 1;
            }
        }
        n
    }

    /// Unpacks RAR, 7z and ZIP archives found in the output. Returns messages and the
    /// number of archives unpacked.
    fn extract_nested(&self) -> (Vec<String>, usize) {
        let (mut msgs, mut n) = (vec![], 0);
        let files = self.output_files();
        let fname = |p: &Path| p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        // RAR sets: one entry per (folder, base name), opened at its first volume.
        let mut rars: std::collections::BTreeMap<(PathBuf, String), Vec<(u64, PathBuf)>> = Default::default();
        for p in &files {
            let name = fname(p);
            if looks_like_rar(&name) {
                if let Some(o) = rar::name_order(&name) {
                    rars.entry((p.parent().unwrap_or(&self.work).to_path_buf(), rar::base_name(&name))).or_default().push((o, p.clone()));
                }
            }
        }
        for (_, mut vols) in rars {
            vols.sort();
            let first = &vols[0].1;
            match self.unrar_one(first) {
                Ok((k, b)) => {
                    msgs.push(format!("unpacked {k} file(s), {:.2} GB from nested {}", b as f64 / 1e9, fname(first)));
                    for (_, p) in &vols {
                        let _ = fs::remove_file(p);
                    }
                    n += 1;
                }
                Err(e) => msgs.push(format!("nested {}: {e}", fname(first))),
            }
        }
        for p in &files {
            let l = fname(p).to_ascii_lowercase();
            let r = if l.ends_with(".7z") {
                self.unpack_7z(&[(1, p.clone())])
            } else if l.ends_with(".zip") {
                self.unzip(p)
            } else {
                continue;
            };
            match r {
                Ok((k, b)) => {
                    msgs.push(format!("unpacked {k} file(s), {:.2} GB from nested {}", b as f64 / 1e9, fname(p)));
                    let _ = fs::remove_file(p);
                    n += 1;
                }
                Err(e) => msgs.push(format!("nested {}: {e}", fname(p))),
            }
        }
        (msgs, n)
    }

    fn unzip(&self, p: &Path) -> Result<(usize, u64), String> {
        let f = File::open(p).map_err(|e| e.to_string())?;
        let mut written: Vec<PathBuf> = vec![];
        let r = (|| {
            let (mut n, mut bytes) = (0, 0);
            for e in zip::entries(&f)? {
                // Entry names come from the archive: never let them escape the job folder.
                let rel = sanitize(&e.name.replace('\\', "/"));
                if e.is_dir() {
                    let _ = fs::create_dir_all(self.work.join(&rel));
                    continue;
                }
                let rel = match rel.file_name() {
                    Some(x) => rel.with_file_name(self.output_name(&x.to_string_lossy())),
                    None => rel,
                };
                let dest = self.work.join(&rel);
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent).map_err(|x| x.to_string())?;
                }
                written.push(dest.clone());
                let mut out = File::create(&dest).map_err(|x| x.to_string())?;
                bytes += zip::extract(&f, &e, &mut out)?;
                n += 1;
            }
            if n == 0 {
                return Err("archive contains no files".to_string());
            }
            Ok((n, bytes))
        })();
        if r.is_err() {
            for p in &written {
                let _ = fs::remove_file(p);
            }
        }
        r
    }

    // ---------- par2 repair ----------

    fn vread(plan: &Plan, files: &[Option<File>], src: VSrc, off: u64, buf: &mut [u8]) {
        match src {
            VSrc::Out { out, len } => {
                let n = (len.saturating_sub(off) as usize).min(buf.len());
                match &files[out] {
                    Some(f) => read_fill(f, &mut buf[..n], off),
                    None => buf[..n].fill(0),
                }
                buf[n..].fill(0);
            }
            VSrc::Vol { vol } => {
                let vm = &plan.vols[vol];
                let (ds, de) = (vm.data_start, vm.data_start + vm.data_len);
                let end = off + buf.len() as u64;
                let mut pos = off;
                while pos < end && pos < vm.size {
                    let b = &mut buf[(pos - off) as usize..];
                    let n;
                    if pos < ds {
                        n = ((ds - pos) as usize).min(b.len());
                        let h = &vm.head;
                        for k in 0..n {
                            b[k] = h.get(pos as usize + k).copied().unwrap_or(0);
                        }
                    } else if pos < de {
                        n = ((de - pos) as usize).min(b.len());
                        match &files[vm.out] {
                            Some(f) => read_fill(f, &mut b[..n], vm.out_off + pos - ds),
                            None => b[..n].fill(0),
                        }
                    } else {
                        n = ((vm.size - pos) as usize).min(b.len());
                        let t = vm.tail.lock().unwrap();
                        for k in 0..n {
                            b[k] = t.get((pos - de) as usize + k).copied().unwrap_or(0);
                        }
                    }
                    pos += n as u64;
                }
                if pos < end {
                    buf[(pos - off) as usize..].fill(0);
                }
            }
        }
    }

    fn vwrite(plan: &Plan, files: &[Option<File>], src: VSrc, off: u64, data: &[u8]) -> std::io::Result<()> {
        match src {
            VSrc::Out { out, len } => {
                let n = (len.saturating_sub(off) as usize).min(data.len());
                if let Some(f) = &files[out] {
                    f.write_all_at(&data[..n], off)?;
                }
            }
            VSrc::Vol { vol } => {
                let vm = &plan.vols[vol];
                let (ds, de) = (vm.data_start, vm.data_start + vm.data_len);
                let lo = off.max(ds);
                let hi = (off + data.len() as u64).min(de);
                if lo < hi {
                    if let Some(f) = &files[vm.out] {
                        f.write_all_at(&data[(lo - off) as usize..(hi - off) as usize], vm.out_off + lo - ds)?;
                    }
                }
                if off + data.len() as u64 > de {
                    let t0 = off.max(de);
                    let mut t = vm.tail.lock().unwrap();
                    for x in t0..(off + data.len() as u64).min(vm.size) {
                        t[(x - de) as usize] = data[(x - off) as usize];
                    }
                }
            }
        }
        Ok(())
    }

    fn par2_map(&self, plan: &Plan, set: &Par2Set) -> Vec<Option<VSrc>> {
        let n = self.files.len();
        let usable = |i: usize| !matches!(plan.targets[i], Target::Skip);
        let mut used = vec![false; n];
        let mut pick: Vec<Option<usize>> = vec![None; set.recovery_ids.len()];
        for (k, id) in set.recovery_ids.iter().enumerate() {
            let fd = &set.files[id];
            let i = (0..n)
                .find(|&i| usable(i) && !used[i] && plan.ids[i].md5_16k == Some(fd.md5_16k) && plan.ids[i].size == fd.len)
                .or_else(|| (0..n).find(|&i| usable(i) && !used[i] && (plan.ids[i].yname == fd.name || self.files[i].guess == fd.name)));
            if let Some(i) = i {
                used[i] = true;
                pick[k] = Some(i);
            }
        }
        // Files posted under other (obfuscated) names whose first article was lost:
        // pair them by volume number, or by size, when exactly one file fits.
        for (k, id) in set.recovery_ids.iter().enumerate() {
            if pick[k].is_some() {
                continue;
            }
            let fd = &set.files[id];
            let suf = vol_suffix(&fd.name);
            let cands: Vec<usize> = (0..n)
                .filter(|&i| usable(i) && !used[i] && !self.files[i].skip)
                .filter(|&i| plan.ids[i].size == 0 || plan.ids[i].size == fd.len)
                .filter(|&i| match &suf {
                    Some(s) => vol_suffix(&plan.ids[i].yname).as_ref() == Some(s) || vol_suffix(&self.files[i].guess).as_ref() == Some(s),
                    None => fd.len > 0 && plan.ids[i].size == fd.len,
                })
                .collect();
            if cands.len() == 1 {
                used[cands[0]] = true;
                pick[k] = Some(cands[0]);
            }
        }
        pick.iter()
            .enumerate()
            .map(|(k, i)| {
                let fd = &set.files[&set.recovery_ids[k]];
                match plan.targets[(*i)?] {
                    Target::Plain { out } => Some(VSrc::Out { out, len: fd.len }),
                    Target::Rar { vol } => Some(VSrc::Vol { vol }),
                    Target::Skip => None,
                }
            })
            .collect()
    }

    /// Returns (global slice index, file index in recovery set, slice index) of damaged slices.
    fn find_damaged(plan: &Plan, files: &[Option<File>], set: &Par2Set, map: &[Option<VSrc>]) -> (Vec<(usize, usize, u64)>, usize) {
        let s = set.slice;
        let mut all = vec![];
        let mut g = 0;
        for (k, id) in set.recovery_ids.iter().enumerate() {
            let n = set.files[id].len.div_ceil(s);
            for j in 0..n {
                all.push((g, k, j));
                g += 1;
            }
        }
        let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(16);
        let chunk = all.len().div_ceil(threads).max(1);
        let damaged = Mutex::new(vec![]);
        std::thread::scope(|sc| {
            for part in all.chunks(chunk) {
                let damaged = &damaged;
                sc.spawn(move || {
                    let mut buf = vec![0u8; s as usize];
                    for &(g, k, j) in part {
                        let id = &set.recovery_ids[k];
                        let ok = match map[k] {
                            None => false,
                            Some(src) => {
                                Self::vread(plan, files, src, j * s, &mut buf);
                                set.ifsc[id].get(j as usize) == Some(&crc32fast::hash(&buf))
                            }
                        };
                        if !ok {
                            damaged.lock().unwrap().push((g, k, j));
                        }
                    }
                });
            }
        });
        let mut d = damaged.into_inner().unwrap();
        d.sort();
        (d, g)
    }

    fn solve(plan: &Plan, files: &[Option<File>], set: &Par2Set, map: &[Option<VSrc>], damaged: &[(usize, usize, u64)], total: usize) -> Result<(), String> {
        let k = damaged.len();
        let s = set.slice as usize;
        let bases = gf16::input_bases(total);
        // Pick k recovery slices whose matrix is invertible (retry with others if singular).
        let mut chosen: Vec<usize> = (0..k).collect();
        let ainv = loop {
            let mut m = vec![0u16; k * k];
            for (r, &ri) in chosen.iter().enumerate() {
                for (j, d) in damaged.iter().enumerate() {
                    m[r * k + j] = gf16::pow(bases[d.0], set.recv[ri].exp);
                }
            }
            if let Some(inv) = gf16::invert(&mut m, k) {
                break inv;
            }
            let next = chosen.last().unwrap() + 1;
            if next >= set.recv.len() {
                return Err("par2: recovery matrix is singular".into());
            }
            *chosen.last_mut().unwrap() = next;
        };
        let damaged_set: std::collections::HashSet<usize> = damaged.iter().map(|d| d.0).collect();
        // All intact input slices with their coefficients per chosen recovery slice.
        let mut intact = vec![];
        let mut g = 0;
        for (fk, id) in set.recovery_ids.iter().enumerate() {
            let n = set.files[id].len.div_ceil(set.slice);
            for j in 0..n {
                if !damaged_set.contains(&g) {
                    let coefs: Vec<gf16::MulTable> = chosen.iter().map(|&ri| gf16::MulTable::new(gf16::pow(bases[g], set.recv[ri].exp))).collect();
                    intact.push((fk, j, coefs));
                }
                g += 1;
            }
        }
        let ainv: Vec<gf16::MulTable> = ainv.iter().map(|&c| gf16::MulTable::new(c)).collect();
        let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(16);
        let stripe = (s.div_ceil(threads) + 1) & !1;
        // Each pass over the intact slices updates all k accumulators; keep them within L2
        // (k * tile <= ~512 KiB) instead of streaming k full stripes through memory.
        let tile = ((512 << 10) / k.max(1)).clamp(8 << 10, stripe.max(2)) & !63;
        let err = Mutex::new(None);
        std::thread::scope(|sc| {
            for t in 0..threads {
                let (a, b) = ((t * stripe).min(s), ((t + 1) * stripe).min(s));
                if a >= b {
                    continue;
                }
                let (intact, chosen, ainv, err) = (&intact, &chosen, &ainv, &err);
                sc.spawn(move || {
                    let tile = tile.max(2).min(b - a);
                    let mut acc: Vec<Vec<u8>> = vec![vec![0u8; tile]; k];
                    let mut buf = vec![0u8; tile];
                    let mut x = a;
                    while x < b {
                        let y = (x + tile).min(b);
                        let n = y - x;
                        for (r, &ri) in chosen.iter().enumerate() {
                            acc[r][..n].copy_from_slice(&set.recv_data(&set.recv[ri])[x..y]);
                        }
                        for (fk, j, coefs) in intact {
                            let src = map[*fk].expect("intact slice has a source");
                            Self::vread(plan, files, src, j * set.slice + x as u64, &mut buf[..n]);
                            for (r, c) in coefs.iter().enumerate() {
                                gf16::mul_add_t(&mut acc[r][..n], &buf[..n], c);
                            }
                        }
                        for (jd, d) in damaged.iter().enumerate() {
                            buf[..n].fill(0);
                            for r in 0..k {
                                gf16::mul_add_t(&mut buf[..n], &acc[r][..n], &ainv[jd * k + r]);
                            }
                            if let Some(src) = map[d.1] {
                                if let Err(e) = Self::vwrite(plan, files, src, d.2 * set.slice + x as u64, &buf[..n]) {
                                    *err.lock().unwrap() = Some(format!("par2 write: {e}"));
                                }
                            }
                        }
                        x = y;
                    }
                });
            }
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
        Ok(())
    }

    fn try_repair(self: &Arc<Self>, plan: &Plan, allow_fetch: bool) -> RepairOutcome {
        let mut files: Vec<Option<File>> = plan.paths.iter().map(|p| open_rw(&self.cur(p)).ok()).collect();
        let set = self.load_par2(plan);
        if !set.ready() {
            let already: Vec<u32> = self.repair.lock().unwrap().outs.keys().copied().collect();
            let smallest = self
                .files
                .iter()
                .enumerate()
                .filter(|(i, f)| f.skip && !already.contains(&(*i as u32)))
                .min_by_key(|(_, f)| f.segs.len())
                .map(|(i, _)| i);
            match smallest {
                Some(i) if allow_fetch => return self.fetch_recovery(&[i]),
                _ => return RepairOutcome::Failed("par2: no usable index (cannot verify or repair)".into()),
            }
        }
        let mut map = self.par2_map(plan, &set);
        // Files the NZB does not have at all are rebuilt from scratch into new files.
        let aux = self.work.join(".aux");
        for (k, m) in map.iter_mut().enumerate() {
            if m.is_some() {
                continue;
            }
            let fd = &set.files[&set.recovery_ids[k]];
            let dir = if looks_like_rar(&fd.name) {
                aux.join("rar")
            } else if looks_like_7z(&fd.name) {
                aux.join("7z")
            } else if is_par2_name(&fd.name) {
                continue;
            } else {
                self.work.clone()
            };
            let _ = fs::create_dir_all(&dir);
            let p = dir.join(sanitize(&fd.name));
            let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&p).and_then(|f| f.set_len(fd.len).map(|_| f));
            if let Ok(f) = f {
                files.push(Some(f));
                *m = Some(VSrc::Out { out: files.len() - 1, len: fd.len });
            }
        }
        let t0 = Instant::now();
        let (damaged, total) = Self::find_damaged(plan, &files, &set, &map);
        if damaged.is_empty() {
            self.rename_by_par2(plan, &set, &map);
            return RepairOutcome::Repaired(format!("par2: all {total} slices verified"));
        }
        if set.recv.len() >= damaged.len() {
            self.phase.store(PH_REPAIR, Relaxed);
            let t1 = Instant::now();
            if let Err(e) = Self::solve(plan, &files, &set, &map, &damaged, total) {
                return RepairOutcome::Failed(e);
            }
            let (still, _) = Self::find_damaged(plan, &files, &set, &map);
            if !still.is_empty() {
                return RepairOutcome::Failed(format!("par2: {} slices still damaged after repair", still.len()));
            }
            // Leftover archive volumes take the names par2 knows them by, so a set is
            // never split between two naming schemes when it is unpacked.
            self.rename_by_par2(plan, &set, &map);
            return RepairOutcome::Repaired(format!(
                "par2: repaired {}/{total} slices (verify {:.2}s, solve {:.2}s)",
                damaged.len(),
                (t1 - t0).as_secs_f64(),
                t1.elapsed().as_secs_f64()
            ));
        }
        let need = damaged.len() - set.recv.len();
        if !allow_fetch {
            return RepairOutcome::Failed(format!("par2: {} slices damaged, only {} recovery blocks", damaged.len(), set.recv.len()));
        }
        let already: Vec<u32> = self.repair.lock().unwrap().outs.keys().copied().collect();
        let mut cands: Vec<(u32, usize)> = self
            .files
            .iter()
            .enumerate()
            .filter(|(i, f)| f.skip && !already.contains(&(*i as u32)))
            .map(|(i, f)| (f.par2_blocks.unwrap_or(1), i))
            .collect();
        cands.sort();
        let mut chosen = vec![];
        let mut have = 0;
        for (b, i) in cands {
            if have >= need {
                break;
            }
            chosen.push(i);
            have += b as usize;
        }
        if have < need {
            return RepairOutcome::Failed(format!("par2: need {need} more recovery blocks, only {have} available"));
        }
        self.fetch_recovery(&chosen)
    }

    /// Downloads the given recovery files, then re-enters finalize (stage 1).
    fn fetch_recovery(self: &Arc<Self>, chosen: &[usize]) -> RepairOutcome {
        let dir = self.work.join(".aux").join("par2");
        let _ = fs::create_dir_all(&dir);
        let mut work = vec![];
        {
            let mut r = self.repair.lock().unwrap();
            for &i in chosen {
                let p = dir.join(sanitize(&self.files[i].guess));
                if let Ok(o) = OutFile::create(&p, 0, false) {
                    r.outs.insert(i as u32, o);
                    r.paths.push(p);
                    for s in 0..self.files[i].segs.len() {
                        work.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0, bounces: 0, stat: false, skipped: 0 });
                    }
                    self.enc_total.fetch_add(self.files[i].segs.iter().map(|s| s.bytes as u64).sum(), Relaxed);
                }
            }
        }
        self.stage.fetch_add(1, Relaxed);
        self.phase.store(PH_PAR2, Relaxed);
        self.remaining.store(work.len() + 1, Relaxed);
        self.enqueue(QUEUE.get().expect("queue"), work, true);
        self.segment_done();
        RepairOutcome::Fetching
    }

    // ---------- completion ----------

    fn drain_io(self: &Arc<Self>, plan: &Plan) {
        let owner: Arc<dyn IoOwner> = self.clone();
        for o in &plan.outs {
            o.flush_partial(&owner);
        }
        let routs: Vec<Arc<OutFile>> = self.repair.lock().unwrap().outs.values().cloned().collect();
        for o in &routs {
            o.flush_partial(&owner);
        }
        while self.pending_io.load(Relaxed) > 0 {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        for o in &plan.outs {
            o.finish();
        }
    }

    fn finalize(self: Arc<Self>) {
        let plan = self.plan.get().expect("plan");
        self.phase.store(PH_VERIFY, Relaxed);
        self.drain_io(plan);
        let stage = self.stage.load(Relaxed);
        let mut notes = vec![];
        if self.is_cancelled() {
            return self.report(plan, false, vec!["cancelled".into()]);
        }
        if self.is_aborted() {
            let (known, miss) = (self.sample_known.load(Relaxed), self.sample_missing.load(Relaxed));
            let (mb, rec) = (self.missing_bytes.load(Relaxed), self.recoverable.load(Relaxed));
            let msg = if mb <= rec && miss as u64 >= SAMPLE_MIN_MISSING && known > 0 {
                format!(
                    "hopeless: availability sample has {miss} of {known} articles missing (~{:.0}% of {:.0} MB), only {:.1} MB of par2 recovery in NZB",
                    miss as f64 * 100.0 / known as f64,
                    self.want_bytes.load(Relaxed) as f64 / 1e6,
                    rec as f64 / 1e6
                )
            } else {
                format!("hopeless: {:.1} MB missing (or first articles gone), only {:.1} MB of par2 recovery in NZB", mb as f64 / 1e6, rec as f64 / 1e6)
            };
            return self.report(plan, false, vec![msg]);
        }
        let missing = self.missing.load(Relaxed);
        let mut repaired = false;
        let only_extras = {
            let st = self.state.lock().unwrap();
            st.missing_files.iter().all(|&f| {
                let f = f as usize;
                self.files[f].skip || non_essential(&self.files[f].guess) || non_essential(&plan.ids[f].yname)
            })
        };
        if missing > 0 && stage == 0 && only_extras {
            notes.push(format!("{missing} articles missing only in par2/nfo/sfv files (not needed)"));
        } else if missing > 0 || stage >= 1 {
            match self.try_repair(plan, stage < 2) {
                RepairOutcome::Fetching => return,
                RepairOutcome::Repaired(m) => {
                    notes.push(m);
                    repaired = true;
                }
                RepairOutcome::Failed(m) => {
                    notes.push(format!("{missing} articles missing; {m}"));
                    return self.report(plan, false, notes);
                }
            }
        }
        let (mut parts, sets_ok) = self.verify_sets(plan, repaired);
        if sets_ok {
            parts.extend(self.extract_tail_files(plan));
        }
        if !plan.raw_rar.is_empty() {
            self.phase.store(PH_EXTRACT, Relaxed);
        }
        // Volumes whose names carry no order (obfuscated posts) take par2's names first.
        if !plan.par2_outs.is_empty() {
            let unordered = plan.raw_rar.iter().chain(plan.sevenz.iter()).any(|&o| {
                let n = self.cur(&plan.paths[o]).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                rar::base_name(&n).is_empty() && !looks_like_7z(&n)
            });
            if unordered {
                let set = self.load_par2(plan);
                if set.ready() {
                    let map = self.par2_map(plan, &set);
                    let n = self.rename_by_par2(plan, &set, &map);
                    if n > 0 {
                        notes.push(format!("{n} volumes renamed from par2"));
                    }
                }
            }
        }
        let (mut raw, mut raw_ok) = self.extract_raw(plan);
        if !plan.sevenz.is_empty() {
            self.phase.store(PH_EXTRACT, Relaxed);
        }
        let (mut z, mut z_ok) = self.extract_7z(plan);
        // Unpacking can fail although no article was missing: the NZB may lack whole
        // files. Let par2 check (and rebuild) everything, then unpack again.
        let has_par2 = !plan.par2_outs.is_empty() || self.files.iter().any(|f| f.skip);
        if (!raw_ok || !z_ok) && !repaired && has_par2 {
            match self.try_repair(plan, stage < 2) {
                RepairOutcome::Fetching => return,
                RepairOutcome::Repaired(m) => {
                    notes.push(format!("{m} (after a failed unpack)"));
                    self.phase.store(PH_EXTRACT, Relaxed);
                    (raw, raw_ok) = self.extract_raw(plan);
                    (z, z_ok) = self.extract_7z(plan);
                }
                RepairOutcome::Failed(m) => notes.push(m),
            }
        }
        parts.extend(raw);
        parts.extend(z);
        parts.extend(plan.main.iter().cloned());
        parts.extend(notes);
        let write_errors: usize = plan.outs.iter().map(|o| o.write_errors.load(Relaxed)).sum();
        if write_errors > 0 {
            parts.push(format!("{write_errors} write errors"));
        }
        let ok = sets_ok && raw_ok && z_ok && write_errors == 0;
        if ok {
            let n = self.rename_plain_by_par2(plan);
            if n > 0 {
                parts.push(format!("{n} files renamed from par2"));
            }
            let _ = fs::remove_dir_all(self.work.join(".aux"));
            parts.extend(self.tidy_output());
            if let Some(done) = &self.done_dir {
                if let Some(parent) = done.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                let _ = fs::remove_dir_all(done);
                if let Err(e) = fs::rename(&self.work, done) {
                    parts.push(format!("move failed: {e}"));
                }
            }
        }
        self.report(plan, ok, parts);
    }

    fn report(&self, plan: &Plan, ok: bool, mut parts: Vec<String>) {
        parts.extend(plan.notes.iter().take(3).cloned());
        if plan.notes.len() > 3 {
            parts.push(format!("(+{} more notes)", plan.notes.len() - 3));
        }
        self.phase.store(PH_DONE, Relaxed);
        (self.finished)(self, JobResult { ok, summary: parts.join("; "), parts });
    }
}

/// The global work queue, needed to schedule repair downloads from finalize threads.
pub static QUEUE: OnceLock<Arc<Queues>> = OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mkv_titles() {
        let title = "KAOS.S01E02.Episode.2.1080p.NF.WEB-DL-GRiMM";
        let mut info = vec![0x2A, 0xD7, 0xB1, 0x83, 0x0F, 0x42, 0x40, 0x7B, 0xA9, 0x80 | title.len() as u8];
        info.extend_from_slice(title.as_bytes());
        let mut d = vec![0x1A, 0x45, 0xDF, 0xA3, 0x84, 0x42, 0x86, 0x81, 0x01];
        d.extend([0x18, 0x53, 0x80, 0x67, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        d.extend([0x11, 0x4D, 0x9B, 0x74, 0x82, 0xEC, 0x80]);
        d.extend([0x15, 0x49, 0xA9, 0x66, 0x80 | info.len() as u8]);
        d.extend(&info);
        assert_eq!(mkv_title(&d).as_deref(), Some(title));
        assert_eq!(episode_title(&d).as_deref(), Some(title));
        assert!(mkv_title(b"\x1a\x45\xdf\xa3garbage").is_none());
    }
}

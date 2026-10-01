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
use crate::queue::{Queues, Work};
use crate::rar::{self, RarVol};
use crate::yenc::YInfo;
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
}

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
    probes_left: usize,
    probes: Vec<Option<Probe>>,
    pieces: Vec<Piece>,
}

pub struct Job {
    pub id: usize,
    pub name: String,
    pub work: PathBuf,
    pub done_dir: PathBuf,
    pub files: Vec<JFile>,
    state: Mutex<JState>,
    plan: OnceLock<Plan>,
    repair: OnceLock<RepairFetch>,
    remaining: AtomicUsize,
    pending_io: AtomicUsize,
    stage: AtomicU8,
    aborted: AtomicBool,
    missing_bytes: AtomicU64,
    recoverable: AtomicU64,
    pub bytes_total: u64,
    pub bytes_done: AtomicU64,
    pub missing: AtomicUsize,
    pub started: Instant,
    pub finished: Arc<dyn Fn(&Job, JobResult) + Send + Sync>,
}

pub struct JobResult {
    pub ok: bool,
    pub summary: String,
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

fn is_par2_name(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".par2")
}

fn is_par2_volume(name: &str) -> bool {
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

/// Random-looking names (hashes, base62 blobs) carry no information for Plex/*arr.
fn is_obfuscated(name: &str) -> bool {
    let stem = name.rsplit('/').next().unwrap_or(name);
    let stem = stem.rsplit_once('.').map(|(a, _)| a).unwrap_or(stem);
    stem.len() >= 10 && !stem.contains([' ', '.', '_', '-'])
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
        tmp: &Path,
        done: &Path,
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
            work: tmp.join(&nzb.name),
            done_dir: done.join(&nzb.name),
            name: nzb.name,
            state: Mutex::new(JState { probes_left, probes: files.iter().map(|_| None).collect(), pieces: vec![] }),
            files,
            plan: OnceLock::new(),
            repair: OnceLock::new(),
            remaining: AtomicUsize::new(0),
            pending_io: AtomicUsize::new(0),
            stage: AtomicU8::new(0),
            aborted: AtomicBool::new(false),
            missing_bytes: AtomicU64::new(0),
            recoverable: AtomicU64::new(recoverable),
            bytes_total,
            bytes_done: AtomicU64::new(0),
            missing: AtomicUsize::new(0),
            started: Instant::now(),
            finished,
        })
    }

    /// Work items for the probe phase (first article of every wanted file).
    pub fn probe_work(self: &Arc<Self>) -> Vec<Work> {
        let mut v = vec![];
        for (i, f) in self.files.iter().enumerate() {
            if !f.skip {
                v.push(Work { job: self.clone(), file: i as u32, seg: 0, tried: 0 });
            }
        }
        if v.is_empty() {
            (self.finished)(self, JobResult { ok: false, summary: "no downloadable files".into() });
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
        if seg == 0 && self.plan.get().is_none() {
            self.on_probe(file, Some(Probe { info: info.clone(), data: data.to_vec(), crc }), q);
            return;
        }
        self.apply(file, info, data, crc);
        self.segment_done();
    }

    pub fn on_missing(self: &Arc<Self>, file: u32, seg: u32, q: &Queues) {
        self.missing.fetch_add(1, Relaxed);
        let b = self.files[file as usize].segs[seg as usize].bytes as u64;
        let mb = self.missing_bytes.fetch_add(b, Relaxed) + b;
        if self.stage.load(Relaxed) == 0 && mb > self.recoverable.load(Relaxed).saturating_add(1 << 20) {
            self.aborted.store(true, Relaxed);
        }
        self.drop_work(file, seg, q);
    }

    /// Accounts for a work item without downloading it (missing or job aborted).
    pub fn drop_work(self: &Arc<Self>, file: u32, seg: u32, q: &Queues) {
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
                rest.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0 });
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
        if self.missing_bytes.load(Relaxed) > rec.saturating_add(1 << 20) {
            self.aborted.store(true, Relaxed);
            rest.clear();
            remaining = 0;
        }
        // +1 guard so the job cannot finish while probes are still being written.
        self.remaining.store(remaining + 1, Relaxed);
        let _ = self.plan.set(plan);
        for (i, p) in probes.iter().enumerate() {
            if let Some(p) = p {
                self.apply(i as u32, &p.info, &p.data, p.crc);
            }
        }
        q.push_back(rest);
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
                if let Some(out) = self.repair.get().and_then(|r| r.outs.get(&file)) {
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
        let mut ids = vec![FileId::default(); self.files.len()];
        let job_name = self.name.clone();
        let deobf = |name: &str| -> String {
            if is_video(name) && is_obfuscated(name) {
                let ext = name.rsplit('.').next().unwrap_or("mkv");
                format!("{job_name}.{ext}")
            } else {
                name.to_string()
            }
        };
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
        for (i, f) in self.files.iter().enumerate() {
            if f.skip {
                targets.push(Target::Skip);
                continue;
            }
            let Some(p) = &probes[i] else {
                notes.push(format!("first article missing for {}", f.guess));
                let dir = if looks_like_rar(&f.guess) { aux.join("rar") } else { aux.clone() };
                let t = make(&dir, &f.guess, 0, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    if looks_like_rar(&f.guess) {
                        raw_rar.push(out);
                    }
                }
                ids[i] = FileId { yname: f.guess.clone(), size: 0, md5_16k: None };
                targets.push(t);
                continue;
            };
            let name = if p.info.name.is_empty() { f.guess.clone() } else { p.info.name.clone() };
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
                    notes.push(format!("{name}: {} archive, kept as volume", if v.encrypted { "encrypted" } else { "compressed" }));
                }
                let t = make(&aux.join("rar"), &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    raw_rar.push(out);
                }
                targets.push(t);
            } else if p.data.starts_with(b"PAR2\0PKT") {
                let t = make(&aux, &name, p.info.size, &mut outs, &mut paths);
                if let Target::Plain { out } = t {
                    par2_outs.push(out);
                }
                targets.push(t);
            } else {
                let fname = deobf(&name);
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
            if !valid {
                notes.push(format!("{inner}: RAR set incomplete or unordered, kept as volumes"));
                for c in g {
                    let t = make(&aux.join("rar"), &c.3, c.2, &mut outs, &mut paths);
                    if let Target::Plain { out } = t {
                        raw_rar.push(out);
                    }
                    targets[c.0] = t;
                }
                continue;
            }
            let out_name = deobf(&inner);
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
        Plan { targets, outs, paths, vols, sets, notes, main, ids, par2_outs, raw_rar }
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

    /// Extracts stored RAR sets that had to be downloaded as volumes; volumes that
    /// cannot be extracted in-process are kept.
    fn extract_raw(&self, plan: &Plan) -> (Vec<String>, bool) {
        let mut res = vec![];
        let mut ok = true;
        let mut groups: HashMap<String, Vec<(PathBuf, RarVol, u64)>> = HashMap::new();
        let mut keep: Vec<PathBuf> = vec![];
        for &o in &plan.raw_rar {
            let p = &plan.paths[o];
            let mut head = vec![0u8; 65536];
            let n = File::open(p).and_then(|mut f| f.read(&mut head)).unwrap_or(0);
            head.truncate(n);
            match rar::parse_volume(&head) {
                Some(v) if v.stored && !v.encrypted => {
                    let size = fs::metadata(p).map(|m| m.len()).unwrap_or(0);
                    groups.entry(v.inner_name.clone()).or_default().push((p.clone(), v, size));
                }
                _ => keep.push(p.clone()),
            }
        }
        for (inner, mut g) in groups {
            let all5 = g.iter().all(|c| c.1.vol_num.is_some());
            g.sort_by_key(|c| if all5 { c.1.vol_num.unwrap() } else { rar::name_order(&c.0.to_string_lossy()).unwrap_or(u64::MAX) });
            let total: u64 = g.iter().map(|c| c.1.pack_size).sum();
            if total != g[0].1.unp_size || g[0].1.split_before || g.last().unwrap().1.split_after {
                res.push(format!("{inner} FAILED: incomplete RAR set ({} volumes)", g.len()));
                ok = false;
                continue;
            }
            let name = if is_video(&inner) && is_obfuscated(&inner) {
                format!("{}.{}", self.name, inner.rsplit('.').next().unwrap_or("mkv"))
            } else {
                inner.clone()
            };
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
        for p in keep {
            // Compressed/encrypted archives: hand the volumes over as-is.
            if let Some(n) = p.file_name() {
                let _ = fs::rename(&p, self.work.join(n));
            }
            res.push(format!("{} kept (compressed/encrypted RAR)", p.file_name().unwrap_or_default().to_string_lossy()));
        }
        (res, ok)
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
        set.recovery_ids
            .iter()
            .map(|id| {
                let fd = &set.files[id];
                let i = (0..self.files.len())
                    .find(|&i| plan.ids[i].md5_16k == Some(fd.md5_16k) && plan.ids[i].size == fd.len)
                    .or_else(|| (0..self.files.len()).find(|&i| plan.ids[i].yname == fd.name || self.files[i].guess == fd.name))?;
                match plan.targets[i] {
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
                    let coefs: Vec<u16> = chosen.iter().map(|&ri| gf16::pow(bases[g], set.recv[ri].exp)).collect();
                    intact.push((fk, j, coefs));
                }
                g += 1;
            }
        }
        let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(16);
        let stripe = (s.div_ceil(threads) + 1) & !1;
        let err = Mutex::new(None);
        std::thread::scope(|sc| {
            for t in 0..threads {
                let (a, b) = ((t * stripe).min(s), ((t + 1) * stripe).min(s));
                if a >= b {
                    continue;
                }
                let (intact, chosen, ainv, err) = (&intact, &chosen, &ainv, &err);
                sc.spawn(move || {
                    let w = b - a;
                    let mut acc: Vec<Vec<u8>> = chosen.iter().map(|&ri| set.recv_data(&set.recv[ri])[a..b].to_vec()).collect();
                    let mut buf = vec![0u8; w];
                    for (fk, j, coefs) in intact {
                        let src = map[*fk].expect("intact slice has a source");
                        Self::vread(plan, files, src, j * set.slice + a as u64, &mut buf);
                        for (r, c) in coefs.iter().enumerate() {
                            gf16::mul_add(&mut acc[r], &buf, *c);
                        }
                    }
                    for (jd, d) in damaged.iter().enumerate() {
                        buf.fill(0);
                        for r in 0..k {
                            gf16::mul_add(&mut buf, &acc[r], ainv[jd * k + r]);
                        }
                        if let Some(src) = map[d.1] {
                            if let Err(e) = Self::vwrite(plan, files, src, d.2 * set.slice + a as u64, &buf) {
                                *err.lock().unwrap() = Some(format!("par2 write: {e}"));
                            }
                        }
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
        let files: Vec<Option<File>> = plan.paths.iter().map(|p| open_rw(p).ok()).collect();
        let mut set = Par2Set::default();
        for &o in &plan.par2_outs {
            set.add_file(fs::read(&plan.paths[o]).unwrap_or_default());
        }
        if let Some(r) = self.repair.get() {
            for p in &r.paths {
                set.add_file(fs::read(p).unwrap_or_default());
            }
        }
        if !set.ready() {
            return RepairOutcome::Failed("par2: no usable index (cannot verify or repair)".into());
        }
        let map = self.par2_map(plan, &set);
        let t0 = Instant::now();
        let (damaged, total) = Self::find_damaged(plan, &files, &set, &map);
        if damaged.is_empty() {
            return RepairOutcome::Repaired(format!("par2: all {total} slices verified"));
        }
        if set.recv.len() >= damaged.len() {
            let t1 = Instant::now();
            if let Err(e) = Self::solve(plan, &files, &set, &map, &damaged, total) {
                return RepairOutcome::Failed(e);
            }
            let (still, _) = Self::find_damaged(plan, &files, &set, &map);
            if !still.is_empty() {
                return RepairOutcome::Failed(format!("par2: {} slices still damaged after repair", still.len()));
            }
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
        let mut cands: Vec<(u32, usize)> =
            self.files.iter().enumerate().filter(|(_, f)| f.skip).map(|(i, f)| (f.par2_blocks.unwrap_or(1), i)).collect();
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
        let dir = self.work.join(".aux").join("par2");
        let _ = fs::create_dir_all(&dir);
        let mut outs = HashMap::new();
        let mut paths = vec![];
        let mut work = vec![];
        for &i in &chosen {
            let p = dir.join(sanitize(&self.files[i].guess));
            if let Ok(o) = OutFile::create(&p, 0, false) {
                outs.insert(i as u32, o);
                paths.push(p);
                for s in 0..self.files[i].segs.len() {
                    work.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0 });
                }
            }
        }
        let _ = self.repair.set(RepairFetch { outs, paths });
        self.stage.store(1, Relaxed);
        self.remaining.store(work.len() + 1, Relaxed);
        QUEUE.get().expect("queue").push_front(work);
        self.segment_done();
        RepairOutcome::Fetching
    }

    // ---------- completion ----------

    fn drain_io(self: &Arc<Self>, plan: &Plan) {
        let owner: Arc<dyn IoOwner> = self.clone();
        for o in &plan.outs {
            o.flush_partial(&owner);
        }
        if let Some(r) = self.repair.get() {
            for o in r.outs.values() {
                o.flush_partial(&owner);
            }
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
        self.drain_io(plan);
        let stage = self.stage.load(Relaxed);
        let mut notes = vec![];
        if self.is_aborted() {
            let msg = format!(
                "hopeless: {:.1} MB missing, only {:.1} MB of par2 recovery in NZB",
                self.missing_bytes.load(Relaxed) as f64 / 1e6,
                self.recoverable.load(Relaxed) as f64 / 1e6
            );
            return self.report(plan, false, vec![msg]);
        }
        let missing = self.missing.load(Relaxed);
        let mut repaired = false;
        if missing > 0 || stage == 1 {
            match self.try_repair(plan, stage == 0) {
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
        let (raw, raw_ok) = self.extract_raw(plan);
        parts.extend(raw);
        parts.extend(plan.main.iter().cloned());
        parts.extend(notes);
        let write_errors: usize = plan.outs.iter().map(|o| o.write_errors.load(Relaxed)).sum();
        if write_errors > 0 {
            parts.push(format!("{write_errors} write errors"));
        }
        let ok = sets_ok && raw_ok && write_errors == 0;
        if ok {
            let _ = fs::remove_dir_all(self.work.join(".aux"));
            if let Some(parent) = self.done_dir.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::remove_dir_all(&self.done_dir);
            if let Err(e) = fs::rename(&self.work, &self.done_dir) {
                parts.push(format!("move failed: {e}"));
            }
        }
        self.report(plan, ok, parts);
    }

    fn report(&self, plan: &Plan, ok: bool, mut parts: Vec<String>) {
        parts.extend(plan.notes.iter().take(3).cloned());
        if plan.notes.len() > 3 {
            parts.push(format!("(+{} more notes)", plan.notes.len() - 3));
        }
        (self.finished)(self, JobResult { ok, summary: parts.join("; ") });
    }
}

/// The global work queue, needed to schedule repair downloads from finalize threads.
pub static QUEUE: OnceLock<Arc<Queues>> = OnceLock::new();

//! A download job (one NZB). Two phases:
//!  1. Probe: fetch the first article of every file to learn names, sizes and
//!     archive layout.
//!  2. Run: every remaining article is decoded and written directly to its final
//!     location. Stored RAR volumes are never written: their payload goes straight
//!     into the extracted file, and the RAR CRCs are verified by combining the
//!     per-article CRCs.

use crate::nzb::{subject_filename, Nzb, NzbSeg};
use crate::outfile::{IoOwner, OutFile};
use crate::queue::{Queues, Work};
use crate::rar::{self, RarVol};
use crate::yenc::YInfo;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

pub struct JFile {
    pub guess: String,
    pub segs: Vec<NzbSeg>,
    pub skip: bool,
}

struct Probe {
    info: YInfo,
    data: Vec<u8>,
    crc: u32,
}

enum Target {
    Skip,
    Plain { out: usize },
    Rar { vol: usize },
}

struct VolMap {
    out: usize,
    set: usize,
    data_start: u64,
    data_len: u64,
    out_off: u64,
    info: RarVol,
}

struct RarSet {
    name: String,
    vols: Vec<usize>,
    unp_size: u64,
}

struct Plan {
    targets: Vec<Target>,
    outs: Vec<Arc<OutFile>>,
    vols: Vec<VolMap>,
    sets: Vec<RarSet>,
    notes: Vec<String>,
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
    remaining: AtomicUsize,
    pending_io: AtomicUsize,
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

fn is_par2_volume(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.ends_with(".par2") && l.contains(".vol")
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

fn create_out(dir: &Path, rel: &str, size: u64) -> std::io::Result<Arc<OutFile>> {
    let path = dir.join(sanitize(rel));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OutFile::create(&path, size, true)
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
                JFile { guess, segs: f.segs, skip }
            })
            .collect();
        let bytes_total = files.iter().filter(|f| !f.skip).flat_map(|f| f.segs.iter()).map(|s| s.bytes as u64).sum();
        let probes_left = files.iter().filter(|f| !f.skip).count();
        Arc::new(Job {
            id,
            work: tmp.join(&nzb.name),
            done_dir: done.join(&nzb.name),
            name: nzb.name,
            state: Mutex::new(JState { probes_left, probes: files.iter().map(|_| None).collect(), pieces: vec![] }),
            files,
            plan: OnceLock::new(),
            remaining: AtomicUsize::new(0),
            pending_io: AtomicUsize::new(0),
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
        if seg == 0 && self.plan.get().is_none() {
            self.on_probe(file, None, q);
            return;
        }
        self.segment_done();
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
        for (i, t) in plan.targets.iter().enumerate() {
            if matches!(t, Target::Skip) {
                continue;
            }
            for s in 1..self.files[i].segs.len() {
                rest.push(Work { job: self.clone(), file: i as u32, seg: s as u32, tried: 0 });
                remaining += 1;
            }
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
        match &plan.targets[file as usize] {
            Target::Skip => {}
            Target::Plain { out } => plan.outs[*out].write(info.offset(), data, &owner),
            Target::Rar { vol } => {
                let vm = &plan.vols[*vol];
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
                    self.state.lock().unwrap().pieces.push(Piece { vol: *vol as u32, off: lo - ds, len: hi - lo, crc: pcrc });
                }
            }
        }
    }

    fn build_plan(&self, probes: &[Option<Probe>]) -> Plan {
        let aux = self.work.join(".aux");
        let mut targets: Vec<Target> = Vec::with_capacity(self.files.len());
        let mut outs = vec![];
        let mut notes = vec![];
        let mut rar_cands: Vec<(usize, RarVol, u64, String)> = vec![];
        let plain_aux = |dir: &Path, name: &str, size: u64, outs: &mut Vec<Arc<OutFile>>| -> Target {
            match create_out(dir, name, size) {
                Ok(file) => {
                    outs.push(file);
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
                targets.push(plain_aux(&aux, &f.guess, 0, &mut outs));
                continue;
            };
            let name = if p.info.name.is_empty() { f.guess.clone() } else { p.info.name.clone() };
            if rar::is_rar(&p.data) {
                if let Some(v) = rar::parse_volume(&p.data) {
                    if v.stored && !v.encrypted {
                        rar_cands.push((i, v, p.info.size, name));
                        targets.push(Target::Skip); // replaced below
                        continue;
                    }
                    notes.push(format!(
                        "{name}: {} archive, kept as volume",
                        if v.encrypted { "encrypted" } else { "compressed" }
                    ));
                }
                targets.push(plain_aux(&aux.join("rar"), &name, p.info.size, &mut outs));
            } else if p.data.starts_with(b"PAR2\0PKT") {
                targets.push(plain_aux(&aux, &name, p.info.size, &mut outs));
            } else {
                targets.push(plain_aux(&self.work, &name, p.info.size, &mut outs));
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
            let keyed: Option<Vec<u64>> = if all5 {
                Some(g.iter().map(|c| c.1.vol_num.unwrap()).collect())
            } else {
                g.iter().map(|c| rar::name_order(&c.3)).collect()
            };
            let valid = keyed.is_some() && {
                let keys = keyed.unwrap();
                let mut idx: Vec<usize> = (0..g.len()).collect();
                idx.sort_by_key(|&k| keys[k]);
                let mut sorted: Vec<_> = idx.iter().map(|&k| g[k].clone()).collect();
                std::mem::swap(&mut g, &mut sorted);
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
                notes.push(format!("{inner}: RAR set not streamable, kept as volumes"));
                for c in g {
                    targets[c.0] = plain_aux(&aux.join("rar"), &c.3, c.2, &mut outs);
                }
                continue;
            }
            let file = match create_out(&self.work, &inner, g[0].1.unp_size) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("[{}] cannot create {inner}: {e}", self.name);
                    continue;
                }
            };
            outs.push(file);
            let out = outs.len() - 1;
            let set = sets.len();
            let mut off = 0;
            let mut vidx = vec![];
            for c in &g {
                vols.push(VolMap { out, set, data_start: c.1.data_start, data_len: c.1.pack_size, out_off: off, info: c.1.clone() });
                off += c.1.pack_size;
                targets[c.0] = Target::Rar { vol: vols.len() - 1 };
                vidx.push(vols.len() - 1);
            }
            sets.push(RarSet { name: inner.clone(), vols: vidx, unp_size: g[0].1.unp_size });
        }
        Plan { targets, outs, vols, sets, notes }
    }

    fn verify_sets(&self, plan: &Plan) -> Vec<String> {
        let mut pieces = std::mem::take(&mut self.state.lock().unwrap().pieces);
        pieces.sort_by_key(|p| (p.vol, p.off));
        let mut res = vec![];
        for set in &plan.sets {
            let mut full = crc32fast::Hasher::new();
            let mut bad = None;
            let n = set.vols.len();
            for (k, &v) in set.vols.iter().enumerate() {
                let vm = &plan.vols[v];
                let mut part = crc32fast::Hasher::new();
                let mut pos = 0;
                for p in pieces.iter().filter(|p| p.vol as usize == v) {
                    if p.off != pos {
                        bad = Some(format!("gap in volume {k} at {pos}"));
                        break;
                    }
                    let h = crc32fast::Hasher::new_with_initial_len(p.crc, p.len);
                    part.combine(&h);
                    pos += p.len;
                }
                if bad.is_none() && pos != vm.data_len {
                    bad = Some(format!("volume {k} short: {pos}/{}", vm.data_len));
                }
                if bad.is_some() {
                    break;
                }
                full.combine(&part);
                let pc = part.clone().finalize();
                if let Some(want) = vm.info.data_crc {
                    let last = k + 1 == n;
                    if want != pc && !(last && want == full.clone().finalize()) {
                        bad = Some(format!("volume {k} CRC mismatch"));
                        break;
                    }
                }
            }
            res.push(match bad {
                None => format!("{} ({:.2} GB) extracted+verified", set.name, set.unp_size as f64 / 1e9),
                Some(e) => format!("{} FAILED: {e}", set.name),
            });
        }
        res
    }

    fn finalize(self: Arc<Self>) {
        let plan = self.plan.get().expect("plan");
        let owner: Arc<dyn IoOwner> = self.clone();
        for o in &plan.outs {
            o.flush_partial(&owner);
        }
        while self.pending_io.load(Relaxed) > 0 {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let write_errors: usize = plan.outs.iter().map(|o| o.write_errors.load(Relaxed)).sum();
        for o in &plan.outs {
            o.finish();
        }
        let mut parts = self.verify_sets(plan);
        let failed_sets = parts.iter().any(|s| s.contains("FAILED"));
        parts.extend(plan.notes.iter().cloned());
        if write_errors > 0 {
            parts.push(format!("{write_errors} write errors"));
        }
        let missing = self.missing.load(Relaxed);
        let ok = missing == 0 && !failed_sets && write_errors == 0;
        if ok {
            let _ = fs::remove_dir_all(self.work.join(".aux"));
            if let Some(parent) = self.done_dir.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::remove_dir_all(&self.done_dir);
            if let Err(e) = fs::rename(&self.work, &self.done_dir) {
                parts.push(format!("move failed: {e}"));
            }
        } else if missing > 0 {
            parts.push(format!("{missing} articles missing on all servers (needs par2 repair)"));
        }
        (self.finished)(&self, JobResult { ok, summary: parts.join("; ") });
    }
}

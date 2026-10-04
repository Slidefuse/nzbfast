//! PAR2 packet parsing: main, file description, slice checksums (IFSC) and
//! recovery slices, grouped by recovery set. Damaged packets (bad MD5) are skipped.

use md5::{Digest, Md5};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub type Id = [u8; 16];

#[derive(Clone, Debug)]
pub struct FileDesc {
    pub md5_16k: Id,
    pub len: u64,
    pub name: String,
}

pub struct RecvSlice {
    pub exp: u32,
    /// Index into `Par2Set::bufs` and byte offset of the slice data.
    pub buf: usize,
    pub off: usize,
}

/// One recovery set: the files it protects and the recovery slices for them.
#[derive(Default)]
pub struct Par2Set {
    pub slice: u64,
    pub recovery_ids: Vec<Id>,
    pub files: HashMap<Id, FileDesc>,
    pub ifsc: HashMap<Id, Vec<u32>>,
    pub recv: Vec<RecvSlice>,
    pub bufs: Vec<Arc<Vec<u8>>>,
    seen_exp: HashSet<u32>,
}

/// The recovery sets found in a job's par2 files. A post can carry several (say one for
/// the archive and one for the sample), each with its own slice size and recovery data.
#[derive(Default)]
pub struct Par2Sets(HashMap<Id, Par2Set>);

const MAGIC: &[u8; 8] = b"PAR2\0PKT";

fn id(b: &[u8]) -> Id {
    b[..16].try_into().unwrap()
}

/// (recovery set id, offset, length) of every packet in `data` whose MD5 checks out.
fn packets(data: &[u8]) -> Vec<(Id, usize, usize)> {
    let mut out = vec![];
    let mut p = 0;
    while let Some(i) = memchr::memmem::find(&data[p..], MAGIC) {
        let s = p + i;
        if s + 64 > data.len() {
            break;
        }
        let len = u64::from_le_bytes(data[s + 8..s + 16].try_into().unwrap()) as usize;
        if len < 64 || !len.is_multiple_of(4) || s + len > data.len() || Md5::digest(&data[s + 32..s + len]).as_slice() != &data[s + 16..s + 32] {
            p = s + 8;
            continue;
        }
        out.push((id(&data[s + 32..]), s, len));
        p = s + len;
    }
    out
}

impl Par2Sets {
    /// Adds every valid packet in `data` (a whole .par2 file) to its set.
    pub fn add_file(&mut self, data: Vec<u8>) {
        let data = Arc::new(data);
        for (sid, s, len) in packets(&data) {
            self.0.entry(sid).or_default().add_packet(&data, s, len);
        }
    }

    /// Every file description, in any set.
    pub fn files(&self) -> impl Iterator<Item = &FileDesc> {
        self.0.values().flat_map(|s| s.files.values())
    }

    /// The sets that can verify their files, the one protecting the most data first.
    pub fn ready(self) -> Vec<Par2Set> {
        let mut v: Vec<Par2Set> = self
            .0
            .into_values()
            .filter_map(|mut s| {
                let slice = s.slice as usize;
                let bufs = &s.bufs;
                s.recv.retain(|r| r.off + slice <= bufs[r.buf].len());
                s.ready().then_some(s)
            })
            .collect();
        v.sort_by_key(|s| std::cmp::Reverse(s.recovery_ids.iter().map(|i| s.files[i].len).sum::<u64>()));
        v
    }
}

impl Par2Set {
    fn add_packet(&mut self, data: &Arc<Vec<u8>>, s: usize, len: usize) {
        if !self.bufs.last().is_some_and(|b| Arc::ptr_eq(b, data)) {
            self.bufs.push(data.clone());
        }
        let bi = self.bufs.len() - 1;
        let body = &data[s + 64..s + len];
        match &data[s + 48..s + 64] {
            b"PAR 2.0\0Main\0\0\0\0" => {
                self.slice = u64::from_le_bytes(body[..8].try_into().unwrap());
                let n = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
                self.recovery_ids = (0..n).filter_map(|k| body.get(12 + 16 * k..28 + 16 * k)).map(id).collect();
            }
            b"PAR 2.0\0FileDesc" if body.len() >= 56 => {
                let name = String::from_utf8_lossy(body[56..].split(|&c| c == 0).next().unwrap_or(&[])).into_owned();
                self.files.insert(id(body), FileDesc { md5_16k: id(&body[32..]), len: u64::from_le_bytes(body[48..56].try_into().unwrap()), name });
            }
            b"PAR 2.0\0IFSC\0\0\0\0" if body.len() >= 16 => {
                let crcs = body[16..].as_chunks::<20>().0.iter().map(|c| u32::from_le_bytes(c[16..20].try_into().unwrap())).collect();
                self.ifsc.insert(id(body), crcs);
            }
            b"PAR 2.0\0RecvSlic" if body.len() >= 4 => {
                let exp = u32::from_le_bytes(body[..4].try_into().unwrap());
                if self.seen_exp.insert(exp) {
                    self.recv.push(RecvSlice { exp, buf: bi, off: s + 68 });
                }
            }
            _ => {}
        }
    }

    pub fn recv_data(&self, r: &RecvSlice) -> &[u8] {
        &self.bufs[r.buf][r.off..r.off + self.slice as usize]
    }

    fn ready(&self) -> bool {
        self.slice > 0 && !self.recovery_ids.is_empty() && self.recovery_ids.iter().all(|i| self.files.contains_key(i) && self.ifsc.contains_key(i))
    }
}

pub fn md5_16k(data: &[u8]) -> Id {
    Md5::digest(&data[..data.len().min(16384)]).into()
}

/// Recovery block count from a name like `x.vol07+08.par2`.
pub fn vol_blocks(name: &str) -> Option<u32> {
    let l = name.to_ascii_lowercase();
    let i = l.rfind(".vol")?;
    let rest = &l[i + 4..];
    let plus = rest.find('+')?;
    let end = rest[plus + 1..].find(|c: char| !c.is_ascii_digit()).map(|e| plus + 1 + e).unwrap_or(rest.len());
    rest[plus + 1..end].parse().ok()
}

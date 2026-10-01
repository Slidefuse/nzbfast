//! PAR2 packet parsing: main, file description, slice checksums (IFSC) and
//! recovery slices. Damaged packets (bad MD5) are skipped.

use md5::{Digest, Md5};
use std::collections::HashMap;

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

#[derive(Default)]
pub struct Par2Set {
    pub slice: u64,
    pub recovery_ids: Vec<Id>,
    pub files: HashMap<Id, FileDesc>,
    pub ifsc: HashMap<Id, Vec<u32>>,
    pub recv: Vec<RecvSlice>,
    pub bufs: Vec<Vec<u8>>,
    seen_exp: std::collections::HashSet<u32>,
}

const MAGIC: &[u8; 8] = b"PAR2\0PKT";

fn id(b: &[u8]) -> Id {
    b[..16].try_into().unwrap()
}

impl Par2Set {
    /// Adds every valid packet in `data` (a whole .par2 file).
    pub fn add_file(&mut self, data: Vec<u8>) {
        let bi = self.bufs.len();
        let mut p = 0;
        while let Some(i) = memchr::memmem::find(&data[p..], MAGIC) {
            let s = p + i;
            if s + 64 > data.len() {
                break;
            }
            let len = u64::from_le_bytes(data[s + 8..s + 16].try_into().unwrap()) as usize;
            if len < 64 || len % 4 != 0 || s + len > data.len() {
                p = s + 8;
                continue;
            }
            let md5: Id = data[s + 16..s + 32].try_into().unwrap();
            if Md5::digest(&data[s + 32..s + len]).as_slice() != md5 {
                p = s + 8;
                continue;
            }
            let typ = &data[s + 48..s + 64];
            let body = &data[s + 64..s + len];
            match typ {
                b"PAR 2.0\0Main\0\0\0\0" => {
                    self.slice = u64::from_le_bytes(body[..8].try_into().unwrap());
                    let n = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
                    self.recovery_ids = (0..n).map(|k| id(&body[12 + 16 * k..])).collect();
                }
                b"PAR 2.0\0FileDesc" => {
                    let fid = id(body);
                    let name_raw = &body[56..];
                    let name = String::from_utf8_lossy(name_raw.split(|&c| c == 0).next().unwrap_or(&[])).into_owned();
                    self.files.insert(
                        fid,
                        FileDesc {
                            md5_16k: id(&body[32..]),
                            len: u64::from_le_bytes(body[48..56].try_into().unwrap()),
                            name,
                        },
                    );
                }
                b"PAR 2.0\0IFSC\0\0\0\0" => {
                    let fid = id(body);
                    let crcs = body[16..].chunks_exact(20).map(|c| u32::from_le_bytes(c[16..20].try_into().unwrap())).collect();
                    self.ifsc.insert(fid, crcs);
                }
                b"PAR 2.0\0RecvSlic" => {
                    let exp = u32::from_le_bytes(body[..4].try_into().unwrap());
                    if self.seen_exp.insert(exp) {
                        self.recv.push(RecvSlice { exp, buf: bi, off: s + 68 });
                    }
                }
                _ => {}
            }
            p = s + len;
        }
        self.bufs.push(data);
    }

    pub fn recv_data(&self, r: &RecvSlice) -> &[u8] {
        &self.bufs[r.buf][r.off..r.off + self.slice as usize]
    }

    pub fn ready(&self) -> bool {
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

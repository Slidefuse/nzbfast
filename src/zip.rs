//! Minimal ZIP reader for archives found inside downloads (stored and deflated
//! entries, ZIP64; no encryption or multi-disk spanning).

use flate2::read::DeflateDecoder;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

pub struct Entry {
    pub name: String,
    method: u16,
    flags: u16,
    crc: u32,
    csize: u64,
    pub size: u64,
    offset: u64,
}

impl Entry {
    pub fn is_dir(&self) -> bool {
        self.name.ends_with('/')
    }
}

fn u16le(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// Reads the central directory.
pub fn entries(f: &File) -> Result<Vec<Entry>, String> {
    let len = f.metadata().map_err(|e| e.to_string())?.len();
    let n = len.min(22 + 65535 + 20) as usize;
    let mut tail = vec![0u8; n];
    f.read_exact_at(&mut tail, len - n as u64).map_err(|e| e.to_string())?;
    let eocd = (0..=n.saturating_sub(22)).rev().find(|&i| tail[i..i + 4] == [0x50, 0x4b, 0x05, 0x06]).ok_or("not a zip (no end of central directory)")?;
    let mut count = u16le(&tail, eocd + 10) as u64;
    let mut cd_size = u32le(&tail, eocd + 12) as u64;
    let mut cd_off = u32le(&tail, eocd + 16) as u64;
    if u16le(&tail, eocd + 4) != 0 {
        return Err("multi-disk zip is not supported".into());
    }
    // ZIP64: a locator right before the end record points at the 64-bit end record.
    if eocd >= 20 && tail[eocd - 20..eocd - 16] == [0x50, 0x4b, 0x06, 0x07] {
        let z64 = u64le(&tail, eocd - 12);
        let mut r = [0u8; 56];
        f.read_exact_at(&mut r, z64).map_err(|e| e.to_string())?;
        if r[..4] != [0x50, 0x4b, 0x06, 0x06] {
            return Err("bad zip64 end record".into());
        }
        count = u64le(&r, 32);
        cd_size = u64le(&r, 40);
        cd_off = u64le(&r, 48);
    }
    if cd_off + cd_size > len || cd_size > 1 << 30 {
        return Err("central directory out of range".into());
    }
    let mut cd = vec![0u8; cd_size as usize];
    f.read_exact_at(&mut cd, cd_off).map_err(|e| e.to_string())?;
    let mut out = vec![];
    let mut p = 0;
    for _ in 0..count {
        if p + 46 > cd.len() || cd[p..p + 4] != [0x50, 0x4b, 0x01, 0x02] {
            return Err("bad central directory entry".into());
        }
        let (nl, xl, cl) = (u16le(&cd, p + 28) as usize, u16le(&cd, p + 30) as usize, u16le(&cd, p + 32) as usize);
        if p + 46 + nl + xl + cl > cd.len() {
            return Err("truncated central directory".into());
        }
        let mut e = Entry {
            name: String::from_utf8_lossy(&cd[p + 46..p + 46 + nl]).into_owned(),
            flags: u16le(&cd, p + 8),
            method: u16le(&cd, p + 10),
            crc: u32le(&cd, p + 16),
            csize: u32le(&cd, p + 20) as u64,
            size: u32le(&cd, p + 24) as u64,
            offset: u32le(&cd, p + 42) as u64,
        };
        // ZIP64 extra field: only the values that overflowed are present, in this order.
        let mut x = p + 46 + nl;
        let xend = x + xl;
        while x + 4 <= xend {
            let (id, sz) = (u16le(&cd, x), u16le(&cd, x + 2) as usize);
            if id == 1 {
                let mut q = x + 4;
                for v in [&mut e.size, &mut e.csize, &mut e.offset] {
                    if *v == 0xffff_ffff && q + 8 <= x + 4 + sz {
                        *v = u64le(&cd, q);
                        q += 8;
                    }
                }
            }
            x += 4 + sz;
        }
        out.push(e);
        p += 46 + nl + xl + cl;
    }
    Ok(out)
}

/// Writes one entry's data to `out`, checking its CRC. Returns the bytes written.
pub fn extract(f: &File, e: &Entry, out: &mut impl Write) -> Result<u64, String> {
    if e.flags & 1 != 0 {
        return Err(format!("{}: encrypted zip entries are not supported", e.name));
    }
    let mut h = [0u8; 30];
    f.read_exact_at(&mut h, e.offset).map_err(|x| x.to_string())?;
    if h[..4] != [0x50, 0x4b, 0x03, 0x04] {
        return Err(format!("{}: bad local header", e.name));
    }
    let data = e.offset + 30 + u16le(&h, 26) as u64 + u16le(&h, 28) as u64;
    let mut src = f.try_clone().map_err(|x| x.to_string())?;
    src.seek(SeekFrom::Start(data)).map_err(|x| x.to_string())?;
    let raw = src.take(e.csize);
    let mut rd: Box<dyn Read> = match e.method {
        0 => Box::new(raw),
        8 => Box::new(DeflateDecoder::new(raw)),
        m => return Err(format!("{}: compression method {m} is not supported", e.name)),
    };
    let mut crc = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut n = 0u64;
    loop {
        let k = rd.read(&mut buf).map_err(|x| format!("{}: {x}", e.name))?;
        if k == 0 {
            break;
        }
        crc.update(&buf[..k]);
        out.write_all(&buf[..k]).map_err(|x| format!("{}: {x}", e.name))?;
        n += k as u64;
    }
    if n != e.size || crc.finalize() != e.crc {
        return Err(format!("{}: data is corrupt (size or CRC mismatch)", e.name));
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// Builds a zip with one stored and one deflated entry and reads both back.
    #[test]
    fn roundtrip() {
        let a = b"stored entry data".to_vec();
        let b: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let mut enc = flate2::write::DeflateEncoder::new(vec![], flate2::Compression::fast());
        enc.write_all(&b).unwrap();
        let bz = enc.finish().unwrap();
        let mut z = vec![];
        let mut cd = vec![];
        for (name, method, data, plain) in [("a.txt", 0u16, &a, &a), ("d/b.bin", 8, &bz, &b)] {
            let off = z.len() as u32;
            let crc = crc32fast::hash(plain);
            let mut h = vec![0x50, 0x4b, 0x03, 0x04, 20, 0, 0, 0];
            h.extend(method.to_le_bytes());
            h.extend([0u8; 4]);
            h.extend(crc.to_le_bytes());
            h.extend((data.len() as u32).to_le_bytes());
            h.extend((plain.len() as u32).to_le_bytes());
            h.extend((name.len() as u16).to_le_bytes());
            h.extend(0u16.to_le_bytes());
            z.extend(&h);
            z.extend(name.as_bytes());
            z.extend(data.iter());
            let mut c = vec![0x50, 0x4b, 0x01, 0x02, 20, 0, 20, 0, 0, 0];
            c.extend(method.to_le_bytes());
            c.extend([0u8; 4]);
            c.extend(crc.to_le_bytes());
            c.extend((data.len() as u32).to_le_bytes());
            c.extend((plain.len() as u32).to_le_bytes());
            c.extend((name.len() as u16).to_le_bytes());
            c.extend([0u8; 12]);
            c.extend(off.to_le_bytes());
            c.extend(name.as_bytes());
            cd.extend(c);
        }
        let cd_off = z.len() as u32;
        z.extend(&cd);
        z.extend([0x50, 0x4b, 0x05, 0x06, 0, 0, 0, 0, 2, 0, 2, 0]);
        z.extend((cd.len() as u32).to_le_bytes());
        z.extend(cd_off.to_le_bytes());
        z.extend([0, 0]);
        let p = std::env::temp_dir().join(format!("nzbfast-zip-test-{}", std::process::id()));
        std::fs::write(&p, &z).unwrap();
        let f = File::open(&p).unwrap();
        let es = entries(&f).unwrap();
        assert_eq!(es.len(), 2);
        let mut o = vec![];
        extract(&f, &es[0], &mut o).unwrap();
        assert_eq!(o, a);
        o.clear();
        extract(&f, &es[1], &mut o).unwrap();
        assert_eq!(o, b);
        let _ = std::fs::remove_file(&p);
    }
}

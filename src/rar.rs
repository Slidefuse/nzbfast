//! RAR4/RAR5 volume header parsing: just enough to map a stored (uncompressed)
//! multi-volume archive onto the extracted file, so data can be written in place
//! while downloading.

#[derive(Clone, Debug)]
pub struct RarVol {
    pub rar5: bool,
    /// RAR5 volume number from the main header (0 for the first volume); None for RAR4.
    pub vol_num: Option<u64>,
    pub inner_name: String,
    pub unp_size: u64,
    pub pack_size: u64,
    /// Offset of the file data within the volume.
    pub data_start: u64,
    pub data_crc: Option<u32>,
    pub stored: bool,
    pub encrypted: bool,
    pub split_before: bool,
    pub split_after: bool,
}

pub const SIG4: &[u8] = b"Rar!\x1a\x07\x00";
pub const SIG5: &[u8] = b"Rar!\x1a\x07\x01\x00";

pub fn is_rar(b: &[u8]) -> bool {
    b.starts_with(SIG4) || b.starts_with(SIG5)
}

fn u16le(b: &[u8], p: usize) -> Option<u64> {
    Some(u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as u64)
}
fn u32le(b: &[u8], p: usize) -> Option<u64> {
    Some(u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as u64)
}

fn vint(b: &[u8], p: &mut usize) -> Option<u64> {
    let mut r = 0u64;
    let mut s = 0;
    loop {
        let c = *b.get(*p)?;
        *p += 1;
        r |= ((c & 0x7f) as u64) << s;
        if c < 0x80 {
            return Some(r);
        }
        s += 7;
        if s > 63 {
            return None;
        }
    }
}

/// Parses the headers at the start of a volume up to and including the first file header.
pub fn parse_volume(b: &[u8]) -> Option<RarVol> {
    if b.starts_with(SIG5) {
        parse5(b)
    } else if b.starts_with(SIG4) {
        parse4(b)
    } else {
        None
    }
}

fn parse4(b: &[u8]) -> Option<RarVol> {
    let mut p = SIG4.len();
    for _ in 0..16 {
        let typ = *b.get(p + 2)?;
        let flags = u16le(b, p + 3)?;
        let hsize = u16le(b, p + 5)?;
        if hsize < 7 {
            return None;
        }
        match typ {
            0x73 => {
                if flags & 0x0080 != 0 {
                    return Some(encrypted(false));
                }
            }
            0x74 => {
                let h = p;
                let mut pack = u32le(b, h + 7)?;
                let mut unp = u32le(b, h + 11)?;
                let crc = u32le(b, h + 16)? as u32;
                let method = *b.get(h + 25)?;
                let name_size = u16le(b, h + 26)? as usize;
                let mut np = h + 32;
                if flags & 0x100 != 0 {
                    pack |= u32le(b, h + 32)? << 32;
                    unp |= u32le(b, h + 36)? << 32;
                    np += 8;
                }
                let raw = b.get(np..np + name_size)?;
                let raw = if flags & 0x200 != 0 { raw.split(|&c| c == 0).next().unwrap_or(raw) } else { raw };
                return Some(RarVol {
                    rar5: false,
                    vol_num: None,
                    inner_name: String::from_utf8_lossy(raw).replace('\\', "/"),
                    unp_size: unp,
                    pack_size: pack,
                    data_start: (h + hsize as usize) as u64,
                    data_crc: Some(crc),
                    stored: method == 0x30,
                    encrypted: flags & 0x04 != 0,
                    split_before: flags & 0x01 != 0,
                    split_after: flags & 0x02 != 0,
                });
            }
            _ => {}
        }
        let add = if flags & 0x8000 != 0 { u32le(b, p + 7)? } else { 0 };
        p += hsize as usize + add as usize;
    }
    None
}

fn encrypted(rar5: bool) -> RarVol {
    RarVol {
        rar5,
        vol_num: None,
        inner_name: String::new(),
        unp_size: 0,
        pack_size: 0,
        data_start: 0,
        data_crc: None,
        stored: false,
        encrypted: true,
        split_before: false,
        split_after: false,
    }
}

fn parse5(b: &[u8]) -> Option<RarVol> {
    let mut p = SIG5.len();
    let mut vol_num = Some(0);
    for _ in 0..16 {
        let hstart = p;
        p += 4; // header CRC32
        let hsize = vint(b, &mut p)?;
        let body_start = p;
        let hend = body_start + hsize as usize;
        let typ = vint(b, &mut p)?;
        let hflags = vint(b, &mut p)?;
        let extra = if hflags & 1 != 0 { vint(b, &mut p)? } else { 0 };
        let data = if hflags & 2 != 0 { vint(b, &mut p)? } else { 0 };
        match typ {
            1 => {
                let aflags = vint(b, &mut p)?;
                if aflags & 2 != 0 {
                    vol_num = Some(vint(b, &mut p)?);
                }
            }
            4 => return Some(encrypted(true)),
            2 => {
                let fflags = vint(b, &mut p)?;
                let unp = vint(b, &mut p)?;
                let _attr = vint(b, &mut p)?;
                if fflags & 2 != 0 {
                    p += 4;
                }
                let crc = if fflags & 4 != 0 {
                    let c = u32le(b, p)? as u32;
                    p += 4;
                    Some(c)
                } else {
                    None
                };
                let comp = vint(b, &mut p)?;
                let _os = vint(b, &mut p)?;
                let nlen = vint(b, &mut p)? as usize;
                let name = String::from_utf8_lossy(b.get(p..p + nlen)?).into_owned();
                p += nlen;
                // Extra area: look for an encryption record (type 1).
                let mut enc = false;
                let mut ep = hend.checked_sub(extra as usize)?;
                while ep < hend {
                    let rsize = vint(b, &mut ep)? as usize;
                    let rstart = ep;
                    let rtype = vint(b, &mut ep)?;
                    if rtype == 1 {
                        enc = true;
                    }
                    ep = rstart + rsize;
                }
                let _ = hstart;
                return Some(RarVol {
                    rar5: true,
                    vol_num,
                    inner_name: name,
                    unp_size: unp,
                    pack_size: data,
                    data_start: hend as u64,
                    data_crc: crc,
                    stored: (comp >> 7) & 7 == 0,
                    encrypted: enc,
                    split_before: hflags & 0x08 != 0,
                    split_after: hflags & 0x10 != 0,
                });
            }
            _ => {}
        }
        p = hend + data as usize;
    }
    None
}

/// Sort key for volume file names: `.part01.rar` → 1, `.rar` → 0, `.r00` → 1, `.r01` → 2.
pub fn name_order(name: &str) -> Option<u64> {
    let l = name.to_ascii_lowercase();
    if let Some(stem) = l.strip_suffix(".rar") {
        if let Some(i) = stem.rfind(".part") {
            if let Ok(n) = stem[i + 5..].parse::<u64>() {
                return Some(n);
            }
        }
        return Some(0);
    }
    let ext = l.rsplit('.').next()?;
    if ext.len() >= 3 && (ext.starts_with('r') || ext.starts_with('s')) {
        let n: u64 = ext[1..].parse().ok()?;
        let base = if ext.starts_with('s') { 101 } else { 1 };
        return Some(base + n);
    }
    None
}

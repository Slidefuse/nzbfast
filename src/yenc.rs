//! yEnc decoding of raw NNTP article bodies (dot-stuffed, CRLF lines) and encoding
//! (for the mock server).

use memchr::{memchr3, memmem};

#[derive(Clone, Debug, Default)]
pub struct YInfo {
    pub name: String,
    pub size: u64,
    pub part: u32,
    pub total: u32,
    /// 1-based inclusive begin, as in the yEnc header.
    pub begin: u64,
    pub end: u64,
    pub pcrc32: Option<u32>,
    pub crc32: Option<u32>,
}

impl YInfo {
    pub fn offset(&self) -> u64 {
        self.begin.saturating_sub(1)
    }
}

fn kv<'a>(line: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut from = 0;
    while let Some(i) = memmem::find(&line[from..], key) {
        let p = from + i;
        if p == 0 || line[p - 1] == b' ' {
            let v = &line[p + key.len()..];
            let e = memchr::memchr(b' ', v).unwrap_or(v.len());
            return Some(&v[..e]);
        }
        from = p + key.len();
    }
    None
}

fn num(line: &[u8], key: &[u8]) -> Option<u64> {
    std::str::from_utf8(kv(line, key)?).ok()?.trim().parse().ok()
}

fn hex(line: &[u8], key: &[u8]) -> Option<u32> {
    let v = std::str::from_utf8(kv(line, key)?).ok()?.trim();
    u32::from_str_radix(v.trim_start_matches("0x"), 16).ok()
}

#[derive(Debug)]
pub enum YErr {
    NoHeader,
    NoTrailer,
    SizeMismatch { want: u64, got: u64 },
    Crc { want: u32, got: u32 },
}

/// Decodes one article body (the bytes between the status line and the terminating
/// `.\r\n`). Returns the header info and CRC32 of the decoded data in `out`.
pub fn decode(body: &[u8], out: &mut Vec<u8>) -> Result<(YInfo, u32), YErr> {
    let hb = memmem::find(body, b"=ybegin ").ok_or(YErr::NoHeader)?;
    let le = hb + memchr::memchr(b'\n', &body[hb..]).ok_or(YErr::NoHeader)?;
    let hline = trim_cr(&body[hb..le]);
    let mut info = YInfo {
        size: num(hline, b"size=").unwrap_or(0),
        part: num(hline, b"part=").unwrap_or(0) as u32,
        total: num(hline, b"total=").unwrap_or(0) as u32,
        ..Default::default()
    };
    if let Some(p) = memmem::find(hline, b" name=") {
        info.name = String::from_utf8_lossy(&hline[p + 6..]).trim().to_string();
    }
    let mut start = le + 1;
    if body[start..].starts_with(b"=ypart ") {
        let pe = start + memchr::memchr(b'\n', &body[start..]).ok_or(YErr::NoHeader)?;
        let pline = trim_cr(&body[start..pe]);
        info.begin = num(pline, b"begin=").unwrap_or(1);
        info.end = num(pline, b"end=").unwrap_or(info.size);
        start = pe + 1;
    } else {
        info.begin = 1;
        info.end = info.size;
    }
    let te = memmem::rfind(body, b"\n=yend").ok_or(YErr::NoTrailer)?;
    let tline_end = memchr::memchr(b'\n', &body[te + 1..]).map(|i| te + 1 + i).unwrap_or(body.len());
    let tline = trim_cr(&body[te + 1..tline_end]);
    info.pcrc32 = hex(tline, b"pcrc32=");
    info.crc32 = hex(tline, b"crc32=");
    let part_size = num(tline, b"size=");

    let end = if te > 0 && body[te - 1] == b'\r' { te - 1 } else { te };
    crate::ysimd::decode(&body[start..end.max(start)], out);

    let want = if info.part > 0 || info.begin != 1 || info.end != info.size {
        info.end + 1 - info.begin
    } else {
        info.size
    };
    let got = out.len() as u64;
    if want != got || part_size.is_some_and(|s| s != got) {
        return Err(YErr::SizeMismatch { want, got });
    }
    let crc = crc32fast::hash(out);
    let expect = info.pcrc32.or(if info.part <= 1 && info.total <= 1 { info.crc32 } else { None });
    if let Some(w) = expect {
        if w != crc {
            return Err(YErr::Crc { want: w, got: crc });
        }
    }
    Ok((info, crc))
}

fn trim_cr(l: &[u8]) -> &[u8] {
    if l.last() == Some(&b'\r') {
        &l[..l.len() - 1]
    } else {
        l
    }
}

/// Decodes yEnc data in `b[start..]` (dot-stuffed CRLF lines) into `out`.
#[inline(never)]
pub fn decode_data(b: &[u8], start: usize, out: &mut Vec<u8>) {
    out.clear();
    out.reserve(b.len().saturating_sub(start));
    let base = out.as_mut_ptr();
    let mut o = 0usize;
    let end = b.len();
    let mut i = start;
    if i < end && b[i] == b'.' {
        i += 1;
    }
    // SAFETY: decoded output is never longer than the input region; capacity reserved above.
    unsafe {
        while i < end {
            match memchr3(b'=', b'\r', b'\n', &b[i..end]) {
                None => {
                    sub42(&b[i..end], base.add(o));
                    o += end - i;
                    break;
                }
                Some(k) => {
                    sub42(&b[i..i + k], base.add(o));
                    o += k;
                    let j = i + k;
                    match b[j] {
                        b'=' => {
                            if j + 1 < end {
                                *base.add(o) = b[j + 1].wrapping_sub(106);
                                o += 1;
                            }
                            i = j + 2;
                        }
                        b'\r' => i = j + 1,
                        _ => {
                            i = j + 1;
                            if i < end && b[i] == b'.' {
                                i += 1;
                            }
                        }
                    }
                }
            }
        }
        out.set_len(o);
    }
}

#[inline(always)]
unsafe fn sub42(src: &[u8], dst: *mut u8) {
    for (k, &c) in src.iter().enumerate() {
        *dst.add(k) = c.wrapping_sub(42);
    }
}

/// Encodes `data` as a complete NNTP BODY response (status line, dot-stuffed yEnc, terminator).
pub fn encode_article(
    msgid: &str,
    name: &str,
    file_size: u64,
    part: u32,
    total: u32,
    begin: u64,
    data: &[u8],
    out: &mut Vec<u8>,
) {
    use std::io::Write;
    let _ = write!(out, "222 0 {msgid}\r\n");
    let _ = write!(out, "=ybegin part={part} total={total} line=128 size={file_size} name={name}\r\n");
    let _ = write!(out, "=ypart begin={} end={}\r\n", begin + 1, begin + data.len() as u64);
    let mut col = 0;
    for &b in data {
        let c = b.wrapping_add(42);
        let esc = matches!(c, 0 | b'\n' | b'\r' | b'=') || (col == 0 && (c == b'\t' || c == b' '));
        if col == 0 && c == b'.' {
            out.push(b'.');
        }
        if esc {
            out.push(b'=');
            out.push(c.wrapping_add(64));
            col += 2;
        } else {
            out.push(c);
            col += 1;
        }
        if col >= 128 {
            out.extend_from_slice(b"\r\n");
            col = 0;
        }
    }
    if col > 0 {
        out.extend_from_slice(b"\r\n");
    }
    let _ = write!(out, "=yend size={} part={part} pcrc32={:08x}\r\n.\r\n", data.len(), crc32fast::hash(data));
}

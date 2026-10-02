//! SIMD yEnc decoding of dot-stuffed CRLF data.
//!
//! Per block: drop CR, LF, '=' and the stuffing '.' that starts a line; subtract 42
//! from every kept byte and another 64 from the byte following '='. Carries between
//! blocks: "previous byte was '='" and "previous byte was LF".

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::sync::OnceLock;

#[derive(Clone, Copy, Default)]
pub struct Carry {
    pub esc: bool,
    pub after_lf: bool,
}

/// Scalar reference decoder; returns bytes written.
pub unsafe fn scalar(src: &[u8], dst: *mut u8, c: &mut Carry) -> usize {
    let mut o = 0;
    for &b in src {
        if c.esc {
            c.esc = false;
            c.after_lf = false;
            *dst.add(o) = b.wrapping_sub(106);
            o += 1;
            continue;
        }
        let was_lf = c.after_lf;
        c.after_lf = false;
        match b {
            b'=' => c.esc = true,
            b'\r' => {}
            b'\n' => c.after_lf = true,
            b'.' if was_lf => {}
            _ => {
                *dst.add(o) = b.wrapping_sub(42);
                o += 1;
            }
        }
    }
    o
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vbmi2,popcnt")]
unsafe fn avx512(src: &[u8], dst: *mut u8, c: &mut Carry) -> (usize, usize) {
    let n = src.len() / 64 * 64;
    let (eqv, crv, lfv, dotv) = (_mm512_set1_epi8(b'=' as i8), _mm512_set1_epi8(b'\r' as i8), _mm512_set1_epi8(b'\n' as i8), _mm512_set1_epi8(b'.' as i8));
    let k42 = _mm512_set1_epi8(42);
    let k64 = _mm512_set1_epi8(64);
    let mut esc = c.esc as u64;
    let mut lf = c.after_lf as u64;
    let mut o = 0usize;
    let mut i = 0;
    while i < n {
        let v = _mm512_loadu_si512(src.as_ptr().add(i) as *const _);
        let m_eq = _mm512_cmpeq_epi8_mask(v, eqv);
        let m_cr = _mm512_cmpeq_epi8_mask(v, crv);
        let m_lf = _mm512_cmpeq_epi8_mask(v, lfv);
        let m_dot = _mm512_cmpeq_epi8_mask(v, dotv);
        // Escapes: a '=' that is itself escaped cannot occur, but "==" chains are handled
        // conservatively by not treating an escaped byte as an escape start.
        let esc_next = (m_eq << 1) | esc;
        let real_eq = m_eq & !esc_next;
        let esc_next = (real_eq << 1) | esc;
        let lf_prev = ((m_lf & !esc_next) << 1) | lf;
        let drop = (real_eq | ((m_cr | m_lf | (lf_prev & m_dot)) & !esc_next)) ;
        let keep = !drop;
        let mut d = _mm512_sub_epi8(v, k42);
        d = _mm512_mask_sub_epi8(d, esc_next, d, k64);
        let packed = _mm512_maskz_compress_epi8(keep, d);
        _mm512_storeu_si512(dst.add(o) as *mut _, packed);
        o += keep.count_ones() as usize;
        esc = (real_eq >> 63) & 1;
        lf = ((m_lf & !esc_next) >> 63) & 1;
        i += 64;
    }
    c.esc = esc != 0;
    c.after_lf = lf != 0;
    (n, o)
}

static LUT: OnceLock<Vec<[u8; 16]>> = OnceLock::new();

fn lut() -> &'static [[u8; 16]] {
    LUT.get_or_init(|| {
        (0..256u32)
            .map(|m| {
                let mut t = [0x80u8; 16];
                let mut k = 0;
                for bit in 0..8 {
                    if m & (1 << bit) != 0 {
                        t[k] = bit as u8;
                        t[8 + k] = bit as u8 + 8;
                        k += 1;
                    }
                }
                t
            })
            .collect()
    })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3,sse4.1,popcnt")]
unsafe fn ssse3(src: &[u8], dst: *mut u8, c: &mut Carry, lut: &[[u8; 16]]) -> (usize, usize) {
    let n = src.len() / 16 * 16;
    let (eqv, crv, lfv, dotv) = (_mm_set1_epi8(b'=' as i8), _mm_set1_epi8(b'\r' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'.' as i8));
    let k42 = _mm_set1_epi8(42);
    let mut esc = c.esc as u32;
    let mut lf = c.after_lf as u32;
    let mut o = 0usize;
    let mut i = 0;
    while i < n {
        let v = _mm_loadu_si128(src.as_ptr().add(i) as *const _);
        let m_eq = _mm_movemask_epi8(_mm_cmpeq_epi8(v, eqv)) as u32;
        let m_cr = _mm_movemask_epi8(_mm_cmpeq_epi8(v, crv)) as u32;
        let m_lf = _mm_movemask_epi8(_mm_cmpeq_epi8(v, lfv)) as u32;
        let m_dot = _mm_movemask_epi8(_mm_cmpeq_epi8(v, dotv)) as u32;
        let esc_next0 = ((m_eq << 1) | esc) & 0xffff;
        let real_eq = m_eq & !esc_next0;
        let esc_next = ((real_eq << 1) | esc) & 0xffff;
        let lf_prev = (((m_lf & !esc_next) << 1) | lf) & 0xffff;
        let drop = real_eq | ((m_cr | m_lf | (lf_prev & m_dot)) & !esc_next);
        let keep = !drop & 0xffff;
        // Expand esc_next bits to a byte mask: bytes with bit set get an extra -64.
        let bits = _mm_set1_epi16(esc_next as i16);
        let sel = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1);
        let bytes = _mm_shuffle_epi8(bits, sel);
        let bitpos = _mm_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128);
        let is_esc = _mm_cmpeq_epi8(_mm_and_si128(bytes, bitpos), bitpos);
        let d = _mm_sub_epi8(_mm_sub_epi8(v, k42), _mm_and_si128(is_esc, _mm_set1_epi8(64)));
        let lo = (keep & 0xff) as usize;
        let hi = (keep >> 8) as usize;
        let sh_lo = _mm_loadu_si128(lut[lo].as_ptr() as *const _);
        let sh_hi = _mm_loadu_si128(lut[hi].as_ptr() as *const _);
        let plo = _mm_shuffle_epi8(d, sh_lo);
        _mm_storel_epi64(dst.add(o) as *mut _, plo);
        o += lo.count_ones() as usize;
        // High half: shuffle indices from the upper table half (bit + 8).
        let phi = _mm_shuffle_epi8(d, _mm_unpackhi_epi64(sh_hi, sh_hi));
        _mm_storel_epi64(dst.add(o) as *mut _, phi);
        o += hi.count_ones() as usize;
        esc = (real_eq >> 15) & 1;
        lf = ((m_lf & !esc_next) >> 15) & 1;
        i += 16;
    }
    c.esc = esc != 0;
    c.after_lf = lf != 0;
    (n, o)
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Kind {
    Scalar,
    Ssse3,
    Avx512,
}

/// Whether the CPU can run the given implementation.
pub fn supported(kind: Kind) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        match kind {
            Kind::Scalar => true,
            Kind::Ssse3 => is_x86_feature_detected!("ssse3") && is_x86_feature_detected!("sse4.1"),
            Kind::Avx512 => is_x86_feature_detected!("avx512vbmi2") && is_x86_feature_detected!("avx512bw"),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        kind == Kind::Scalar
    }
}

pub fn best() -> Kind {
    static K: OnceLock<Kind> = OnceLock::new();
    *K.get_or_init(|| {
        if let Ok(v) = std::env::var("NZBFAST_YENC") {
            return match v.as_str() {
                "scalar" => Kind::Scalar,
                "ssse3" => Kind::Ssse3,
                _ => Kind::Avx512,
            };
        }
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512vbmi2") && is_x86_feature_detected!("avx512bw") {
                return Kind::Avx512;
            }
            if is_x86_feature_detected!("ssse3") && is_x86_feature_detected!("sse4.1") {
                return Kind::Ssse3;
            }
        }
        Kind::Scalar
    })
}

/// Decodes `src` (starting at a line start) into `out` using the given implementation.
pub fn decode_with(kind: Kind, src: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(src.len() + 64);
    let dst = out.as_mut_ptr();
    let mut c = Carry { esc: false, after_lf: true };
    unsafe {
        let (used, mut o) = match kind {
            #[cfg(target_arch = "x86_64")]
            Kind::Avx512 => avx512(src, dst, &mut c),
            #[cfg(target_arch = "x86_64")]
            Kind::Ssse3 => ssse3(src, dst, &mut c, lut()),
            _ => (0, 0),
        };
        o += scalar(&src[used..], dst.add(o), &mut c);
        out.set_len(o);
    }
}

pub fn decode(src: &[u8], out: &mut Vec<u8>) {
    decode_with(best(), src, out)
}

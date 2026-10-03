//! GF(2^16) arithmetic for PAR2 (generator polynomial 0x1100B) and the
//! region multiply-accumulate used by Reed-Solomon repair.

use std::sync::OnceLock;

const POLY: u32 = 0x1100B;
pub const LIMIT: u32 = 65535;

struct Tables {
    exp: Vec<u16>,
    log: Vec<u16>,
}

fn tables() -> &'static Tables {
    static T: OnceLock<Tables> = OnceLock::new();
    T.get_or_init(|| {
        let mut exp = vec![0u16; 2 * LIMIT as usize];
        let mut log = vec![0u16; 65536];
        let mut x: u32 = 1;
        for i in 0..LIMIT {
            exp[i as usize] = x as u16;
            log[x as usize] = i as u16;
            x <<= 1;
            if x & 0x10000 != 0 {
                x ^= POLY;
            }
        }
        for i in LIMIT..2 * LIMIT {
            exp[i as usize] = exp[(i - LIMIT) as usize];
        }
        Tables { exp, log }
    })
}

pub fn mul(a: u16, b: u16) -> u16 {
    if a == 0 || b == 0 {
        return 0;
    }
    let t = tables();
    t.exp[t.log[a as usize] as usize + t.log[b as usize] as usize]
}

pub fn inv(a: u16) -> u16 {
    assert!(a != 0);
    let t = tables();
    t.exp[(LIMIT - t.log[a as usize] as u32) as usize % LIMIT as usize]
}

pub fn pow(a: u16, e: u32) -> u16 {
    if e == 0 {
        return 1;
    }
    if a == 0 {
        return 0;
    }
    let t = tables();
    t.exp[((t.log[a as usize] as u64 * e as u64) % LIMIT as u64) as usize]
}

/// PAR2 input-slice constants: 2^n for the n coprime to 65535, in order.
pub fn input_bases(n: usize) -> Vec<u16> {
    fn gcd(mut a: u32, mut b: u32) -> u32 {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    }
    let t = tables();
    let mut v = Vec::with_capacity(n);
    let mut logbase = 0u32;
    while v.len() < n {
        while gcd(LIMIT, logbase) != 1 {
            logbase += 1;
        }
        v.push(t.exp[logbase as usize]);
        logbase += 1;
    }
    v
}

/// Multiplication by a constant `c`, split by input nibble: `lo[k][x]` / `hi[k][x]` are the
/// low / high bytes of `c * (x << 4k)`. The product of a word is the XOR over its 4 nibbles,
/// which maps onto 16-entry byte shuffles (the technique par2cmdline-turbo uses).
#[derive(Clone)]
pub struct MulTable {
    lo: [[u8; 16]; 4],
    hi: [[u8; 16]; 4],
    c: u16,
}

impl MulTable {
    pub fn new(c: u16) -> MulTable {
        let mut t = MulTable { lo: [[0; 16]; 4], hi: [[0; 16]; 4], c };
        for k in 0..4 {
            for x in 0..16u16 {
                let p = mul(c, x << (4 * k));
                t.lo[k][x as usize] = p as u8;
                t.hi[k][x as usize] = (p >> 8) as u8;
            }
        }
        t
    }

    fn word(&self, w: u16) -> u16 {
        let mut p = 0u16;
        for k in 0..4 {
            let x = ((w >> (4 * k)) & 15) as usize;
            p ^= self.lo[k][x] as u16 | (self.hi[k][x] as u16) << 8;
        }
        p
    }
}

/// dst ^= c * src, over little-endian 16-bit words. `src.len()` must be even.
#[cfg(test)]
pub fn mul_add(dst: &mut [u8], src: &[u8], c: u16) {
    if c == 0 {
        return;
    }
    mul_add_t(dst, src, &MulTable::new(c));
}

/// dst ^= t.c * src, over little-endian 16-bit words.
pub fn mul_add_t(dst: &mut [u8], src: &[u8], t: &MulTable) {
    let n = dst.len().min(src.len()) & !1;
    let (dst, src) = (&mut dst[..n], &src[..n]);
    match t.c {
        0 => return,
        1 => {
            for (d, s) in dst.iter_mut().zip(src) {
                *d ^= *s;
            }
            return;
        }
        _ => {}
    }
    #[cfg(target_arch = "x86_64")]
    let done = if is_x86_feature_detected!("avx2") { unsafe { mul_add_avx2(dst, src, t) } } else { 0 };
    #[cfg(not(target_arch = "x86_64"))]
    let done = 0;
    for (dw, sw) in dst[done..].as_chunks_mut::<2>().0.iter_mut().zip(src[done..].as_chunks::<2>().0) {
        *dw = (u16::from_le_bytes(*dw) ^ t.word(u16::from_le_bytes(*sw))).to_le_bytes();
    }
}

/// 64 bytes (32 words) per step: split words into low/high byte vectors, look up each
/// nibble with `vpshufb`, then re-interleave the product bytes. Returns bytes processed.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn mul_add_avx2(dst: &mut [u8], src: &[u8], t: &MulTable) -> usize {
    use std::arch::x86_64::*;
    let tbl = |b: &[u8; 16]| _mm256_broadcastsi128_si256(_mm_loadu_si128(b.as_ptr() as *const __m128i));
    let (l0, l1, l2, l3) = (tbl(&t.lo[0]), tbl(&t.lo[1]), tbl(&t.lo[2]), tbl(&t.lo[3]));
    let (h0, h1, h2, h3) = (tbl(&t.hi[0]), tbl(&t.hi[1]), tbl(&t.hi[2]), tbl(&t.hi[3]));
    let mask = _mm256_set1_epi8(0x0f);
    // Per 128-bit lane: even bytes (low halves of words) first, then odd bytes.
    let deint = _mm256_setr_epi8(0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15, 0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15);
    let n = dst.len() & !63;
    let (d, s) = (dst.as_mut_ptr(), src.as_ptr());
    let mut i = 0;
    while i < n {
        let a = _mm256_shuffle_epi8(_mm256_loadu_si256(s.add(i) as *const __m256i), deint);
        let b = _mm256_shuffle_epi8(_mm256_loadu_si256(s.add(i + 32) as *const __m256i), deint);
        let lo = _mm256_unpacklo_epi64(a, b);
        let hi = _mm256_unpackhi_epi64(a, b);
        let n0 = _mm256_and_si256(lo, mask);
        let n1 = _mm256_and_si256(_mm256_srli_epi16(lo, 4), mask);
        let n2 = _mm256_and_si256(hi, mask);
        let n3 = _mm256_and_si256(_mm256_srli_epi16(hi, 4), mask);
        let plo = _mm256_xor_si256(
            _mm256_xor_si256(_mm256_shuffle_epi8(l0, n0), _mm256_shuffle_epi8(l1, n1)),
            _mm256_xor_si256(_mm256_shuffle_epi8(l2, n2), _mm256_shuffle_epi8(l3, n3)),
        );
        let phi = _mm256_xor_si256(
            _mm256_xor_si256(_mm256_shuffle_epi8(h0, n0), _mm256_shuffle_epi8(h1, n1)),
            _mm256_xor_si256(_mm256_shuffle_epi8(h2, n2), _mm256_shuffle_epi8(h3, n3)),
        );
        // unpacklo/hi_epi8 undo the epi64 unpack: words 0..15 and 16..31 in source order.
        let pa = _mm256_unpacklo_epi8(plo, phi);
        let pb = _mm256_unpackhi_epi8(plo, phi);
        let da = d.add(i) as *mut __m256i;
        let db = d.add(i + 32) as *mut __m256i;
        _mm256_storeu_si256(da, _mm256_xor_si256(_mm256_loadu_si256(da), pa));
        _mm256_storeu_si256(db, _mm256_xor_si256(_mm256_loadu_si256(db), pb));
        i += 64;
    }
    n
}

/// Inverts a k×k matrix (row-major; `m` is destroyed). `None` if singular.
pub fn invert(m: &mut [u16], k: usize) -> Option<Vec<u16>> {
    let mut inv = vec![0u16; k * k];
    for i in 0..k {
        inv[i * k + i] = 1;
    }
    for col in 0..k {
        let piv = (col..k).find(|&r| m[r * k + col] != 0)?;
        if piv != col {
            for j in 0..k {
                m.swap(piv * k + j, col * k + j);
                inv.swap(piv * k + j, col * k + j);
            }
        }
        let f = inv_or_zero(m[col * k + col]);
        for j in 0..k {
            m[col * k + j] = mul(m[col * k + j], f);
            inv[col * k + j] = mul(inv[col * k + j], f);
        }
        for r in 0..k {
            if r != col && m[r * k + col] != 0 {
                let g = m[r * k + col];
                for j in 0..k {
                    m[r * k + j] ^= mul(g, m[col * k + j]);
                    inv[r * k + j] ^= mul(g, inv[col * k + j]);
                }
            }
        }
    }
    Some(inv)
}

fn inv_or_zero(a: u16) -> u16 {
    if a == 0 {
        0
    } else {
        inv(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mul_add_matches_word_by_word() {
        let mut seed = 0x1234_5678u32;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed
        };
        for len in [0usize, 2, 62, 64, 66, 128, 1000, 4096 + 6] {
            for c in [0u16, 1, 2, 0x100B, 0x8000, 0xffff, rnd() as u16] {
                let src: Vec<u8> = (0..len).map(|_| rnd() as u8).collect();
                let dst0: Vec<u8> = (0..len).map(|_| rnd() as u8).collect();
                let mut want = dst0.clone();
                for (d, s) in want.as_chunks_mut::<2>().0.iter_mut().zip(src.as_chunks::<2>().0) {
                    *d = (u16::from_le_bytes(*d) ^ mul(c, u16::from_le_bytes(*s))).to_le_bytes();
                }
                let mut got = dst0.clone();
                mul_add(&mut got, &src, c);
                assert_eq!(got, want, "len {len} c {c:#x}");
            }
        }
    }
}

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

/// dst ^= c * src, over little-endian 16-bit words. `src.len()` must be even.
pub fn mul_add(dst: &mut [u8], src: &[u8], c: u16) {
    if c == 0 {
        return;
    }
    if c == 1 {
        for (d, s) in dst.iter_mut().zip(src) {
            *d ^= *s;
        }
        return;
    }
    let mut lo = [0u16; 256];
    let mut hi = [0u16; 256];
    for x in 0..256u16 {
        lo[x as usize] = mul(c, x);
        hi[x as usize] = mul(c, x << 8);
    }
    let n = dst.len().min(src.len()) & !1;
    let (d, s) = (&mut dst[..n], &src[..n]);
    for (dw, sw) in d.chunks_exact_mut(2).zip(s.chunks_exact(2)) {
        let p = lo[sw[0] as usize] ^ hi[sw[1] as usize];
        let cur = u16::from_le_bytes([dw[0], dw[1]]) ^ p;
        dw.copy_from_slice(&cur.to_le_bytes());
    }
}

/// Inverts a k×k matrix in place (row-major). Returns false if singular.
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

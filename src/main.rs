mod config;
mod conn;
mod job;
mod mock;
mod nzb;
mod outfile;
mod queue;
mod rar;
mod yenc;
mod ysimd;

use conn::{ConnCtx, SStats};
use job::{Job, JobResult};
use queue::Queues;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

fn usage() -> ! {
    eprintln!(
        "usage:
  nzbfast get [--sab-ini FILE] [--server SPEC]... [--only a,b] [--tmp DIR] [--done DIR]
              [--active N] [--depth N] [--nic IF] [--limit N] NZB|DIR...
  nzbfast mock-gen --out DIR RELEASE_DIR...
  nzbfast mock-serve --dir DIR [--port P] [--tls]
  nzbfast bench"
    );
    std::process::exit(2)
}

fn nic_rx(nic: &str) -> u64 {
    std::fs::read_to_string(format!("/sys/class/net/{nic}/statistics/rx_bytes"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn collect_nzbs(p: &Path, out: &mut Vec<PathBuf>) {
    if p.is_dir() {
        let mut v: Vec<_> = std::fs::read_dir(p).map(|r| r.flatten().map(|e| e.path()).collect()).unwrap_or_default();
        v.sort();
        for e in v {
            collect_nzbs(&e, out);
        }
    } else {
        let s = p.to_string_lossy();
        if s.ends_with(".nzb") || s.ends_with(".nzb.gz") {
            out.push(p.to_path_buf());
        }
    }
}

fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first() else { usage() };
    let rest = &args[1..];
    match cmd.as_str() {
        "get" => get(rest),
        "mock-gen" => {
            let mut out = None;
            let mut rels = vec![];
            let mut it = rest.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--out" => out = it.next().cloned(),
                    _ => rels.push(a.clone()),
                }
            }
            mock::gen(Path::new(&out.unwrap_or_else(|| usage())), &rels).unwrap();
        }
        "mock-serve" => {
            let (mut dir, mut port, mut tls) = (None, 1119u16, false);
            let mut it = rest.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--dir" => dir = it.next().cloned(),
                    "--port" => port = it.next().unwrap().parse().unwrap(),
                    "--tls" => tls = true,
                    _ => usage(),
                }
            }
            mock::serve(Path::new(&dir.unwrap_or_else(|| usage())), port, tls).unwrap();
        }
        "bench" => bench(),
        "rarcheck" => {
            for f in rest {
                let b = std::fs::read(f).unwrap();
                let size: u64 = std::fs::read_to_string(format!("{f}.size")).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
                match rar::parse_volume(&b) {
                    Some(v) => println!(
                        "{} rar5={} vol={:?} order={:?} inner={} unp={} pack={} start={} trailing={} crc={:08x?} stored={} enc={} split_before={} split_after={}",
                        Path::new(f).file_name().unwrap().to_string_lossy(),
                        v.rar5, v.vol_num, rar::name_order(f), v.inner_name, v.unp_size, v.pack_size, v.data_start,
                        size as i64 - (v.data_start + v.pack_size) as i64, v.data_crc, v.stored, v.encrypted, v.split_before, v.split_after
                    ),
                    None => println!("{f}: not parseable"),
                }
            }
        }
        _ => usage(),
    }
}

fn get(args: &[String]) {
    let mut servers = vec![];
    let mut only: Vec<String> = vec![];
    let mut tmp = PathBuf::from("/root/nzbfast/tmp");
    let mut done = PathBuf::from("/root/nzbfast/done");
    let mut active_max = 6usize;
    let mut depth = 8usize;
    let mut nic = "ens18".to_string();
    let mut limit = usize::MAX;
    let mut inputs = vec![];
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut v = || it.next().cloned().unwrap_or_else(|| usage());
        match a.as_str() {
            "--sab-ini" => servers.extend(config::from_sab_ini(&v()).unwrap()),
            "--server" => servers.push(config::from_spec(&v()).unwrap()),
            "--only" => only = v().split(',').map(|s| s.to_lowercase()).collect(),
            "--tmp" => tmp = v().into(),
            "--done" => done = v().into(),
            "--active" => active_max = v().parse().unwrap(),
            "--depth" => depth = v().parse().unwrap(),
            "--nic" => nic = v(),
            "--limit" => limit = v().parse().unwrap(),
            s if s.starts_with("--") => usage(),
            _ => collect_nzbs(Path::new(a), &mut inputs),
        }
    }
    if !only.is_empty() {
        servers.retain(|s| only.iter().any(|o| s.name.to_lowercase().contains(o) || s.host.contains(o)));
    }
    if servers.is_empty() || inputs.is_empty() {
        usage();
    }
    inputs.truncate(limit);
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::create_dir_all(&done).unwrap();

    outfile::start_io(8);
    let min_prio = servers.iter().map(|s| s.priority).min().unwrap();
    let primary: Vec<bool> = servers.iter().map(|s| s.priority == min_prio).collect();
    let mut order: Vec<usize> = (0..servers.len()).collect();
    order.sort_by_key(|&i| (servers[i].priority, i));
    let q = Arc::new(Queues::new(primary, order));
    let stats: Vec<Arc<SStats>> = servers.iter().map(|_| Arc::new(SStats::default())).collect();
    let mut total_conns = 0;
    eprintln!("servers:");
    for (i, s) in servers.iter().enumerate() {
        let d = if s.depth > 0 { s.depth } else { depth };
        eprintln!("  {:<18} {}:{} tls={} conns={} depth={d} prio={}", s.name, s.host, s.port, s.tls, s.conns, s.priority);
        let tls = conn::tls_config(s.insecure);
        for _ in 0..s.conns {
            let ctx = ConnCtx { idx: i, cfg: s.clone(), depth: d, tls: tls.clone(), q: q.clone(), st: stats[i].clone() };
            std::thread::Builder::new().stack_size(256 << 10).spawn(move || conn::run(ctx)).unwrap();
        }
        total_conns += s.conns * d;
    }

    let active = Arc::new(AtomicUsize::new(0));
    let done_ok = Arc::new(AtomicUsize::new(0));
    let done_bad = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel::<String>();
    let finished = {
        let (active, done_ok, done_bad) = (active.clone(), done_ok.clone(), done_bad.clone());
        let tx = std::sync::Mutex::new(tx);
        Arc::new(move |j: &Job, r: JobResult| {
            let secs = j.started.elapsed().as_secs_f64();
            let gb = j.bytes_done.load(Relaxed) as f64 / 1e9;
            if r.ok { done_ok.fetch_add(1, Relaxed) } else { done_bad.fetch_add(1, Relaxed) };
            let _ = tx.lock().unwrap().send(format!(
                "{} job {} [{}] {:.2} GB in {:.1}s ({:.2} Gbit/s): {}",
                if r.ok { "DONE" } else { "FAIL" },
                j.id,
                j.name,
                gb,
                secs,
                gb * 8.0 / secs.max(0.001),
                r.summary
            ));
            active.fetch_sub(1, Relaxed);
        }) as Arc<dyn Fn(&Job, JobResult) + Send + Sync>
    };

    // Reporter.
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let rep = {
        let (stats, q, active, done_ok, done_bad, stop, nic) =
            (stats.clone(), q.clone(), active.clone(), done_ok.clone(), done_bad.clone(), stop.clone(), nic.clone());
        let names: Vec<String> = servers.iter().map(|s| s.name.clone()).collect();
        std::thread::spawn(move || {
            let mut prev: Vec<u64> = vec![0; stats.len()];
            let mut prev_nic = nic_rx(&nic);
            let mut sec = 0u64;
            while !stop.load(Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                sec += 1;
                let mut tot = 0u64;
                let mut parts = String::new();
                for (i, s) in stats.iter().enumerate() {
                    let b = s.bytes.load(Relaxed);
                    let d = b - prev[i];
                    prev[i] = b;
                    tot += d;
                    parts.push_str(&format!(" | {} {:.2}/{}", names[i], d as f64 * 8.0 / 1e9, s.live.load(Relaxed)));
                }
                let n = nic_rx(&nic);
                let nr = (n - prev_nic) as f64 * 8.0 / 1e9;
                prev_nic = n;
                eprintln!(
                    "t={sec:>4}s {:6.2} Gbit/s (nic {nr:6.2}) q={} chunks={} active={} ok={} fail={}{parts}",
                    tot as f64 * 8.0 / 1e9,
                    q.len(),
                    outfile::CHUNKS_LIVE.load(Relaxed),
                    active.load(Relaxed),
                    done_ok.load(Relaxed),
                    done_bad.load(Relaxed)
                );
            }
        })
    };

    // Feeder: keep the queue deep enough to saturate all connections, finishing jobs roughly in order.
    let low_water = (total_conns * 6).max(2000);
    let mut next = 0;
    let n_inputs = inputs.len();
    let mut printer = |rx: &mpsc::Receiver<String>| {
        while let Ok(m) = rx.try_recv() {
            println!("{m}");
        }
    };
    while next < n_inputs || active.load(Relaxed) > 0 {
        printer(&rx);
        if next < n_inputs && active.load(Relaxed) < active_max && q.len() < low_water {
            let path = inputs[next].to_string_lossy().to_string();
            next += 1;
            match nzb::load(&path) {
                Ok(n) => {
                    active.fetch_add(1, Relaxed);
                    let job = Job::new(next, n, &tmp, &done, finished.clone());
                    q.push_front(job.probe_work());
                }
                Err(e) => eprintln!("skip {path}: {e}"),
            }
            continue;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    printer(&rx);
    q.close();
    stop.store(true, Relaxed);
    let _ = rep.join();
    let el = t0.elapsed().as_secs_f64();
    let bytes: u64 = stats.iter().map(|s| s.bytes.load(Relaxed)).sum();
    println!("\n=== summary ===");
    for (i, s) in servers.iter().enumerate() {
        let st = &stats[i];
        println!(
            "{:<18} {:>8.2} GB ok={} missing={} crc_err={} conn_err={}",
            s.name,
            st.bytes.load(Relaxed) as f64 / 1e9,
            st.ok.load(Relaxed),
            st.missing.load(Relaxed),
            st.crc_errors.load(Relaxed),
            st.errors.load(Relaxed)
        );
    }
    println!(
        "jobs ok={} failed={} | {:.2} GB in {:.1}s = {:.2} Gbit/s average",
        done_ok.load(Relaxed),
        done_bad.load(Relaxed),
        bytes as f64 / 1e9,
        el,
        bytes as f64 * 8.0 / el / 1e9
    );
    let _ = AtomicU64::new(0);
}

fn bench() {
    let mut seed: u64 = 0x1234_5678_9abc_def0;
    let data: Vec<u8> = (0..mock::ARTICLE)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let mut art = vec![];
    yenc::encode_article("<x@y>", "f.bin", data.len() as u64, 1, 1, 0, &data, &mut art);
    let body_start = memchr::memchr(b'\n', &art).unwrap() + 1;
    let body = &art[body_start..art.len() - 3];
    let mut out = vec![];
    let (info, _) = yenc::decode(body, &mut out).unwrap();
    assert_eq!(out, data, "roundtrip");
    let _ = info;
    let n = 3000;
    let t = Instant::now();
    for _ in 0..n {
        yenc::decode_data(body, 0, &mut out);
    }
    let s = t.elapsed().as_secs_f64();
    println!("yEnc decode: {:.2} GB/s per core (encoded input)", (body.len() * n) as f64 / s / 1e9);
    let t = Instant::now();
    let mut x = 0;
    for _ in 0..n {
        x ^= crc32fast::hash(&data);
    }
    let s = t.elapsed().as_secs_f64();
    println!("crc32: {:.2} GB/s per core ({x})", (data.len() * n) as f64 / s / 1e9);
    // Randomized cross-check of SIMD decoders against the original data.
    let kinds = [ysimd::Kind::Scalar, ysimd::Kind::Ssse3, ysimd::Kind::Avx512];
    let avail: Vec<_> = kinds
        .iter()
        .copied()
        .filter(|k| *k != ysimd::Kind::Avx512 || std::is_x86_feature_detected!("avx512vbmi2"))
        .collect();
    for t in 0..2000u64 {
        let len = (t * 7919 % 5000) as usize + (t % 3) as usize;
        let mut d: Vec<u8> = (0..len)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        // Bias toward bytes that become special after +42 (escapes, '.', CR, LF).
        for (k, b) in d.iter_mut().enumerate() {
            if k % 11 == (t % 11) as usize {
                *b = [214u8, 224, 227, 19, 4, 238][k % 6];
            }
        }
        let mut a = vec![];
        yenc::encode_article("<t@t>", "t", d.len() as u64, 1, 1, 0, &d, &mut a);
        let b0 = memchr::memchr(b'\n', &a).unwrap() + 1;
        let body = &a[b0..a.len() - 3];
        for &k in &avail {
            let s = memchr::memmem::find(body, b"=ypart").unwrap();
            let ds = s + memchr::memchr(b'\n', &body[s..]).unwrap() + 1;
            let de = memchr::memmem::rfind(body, b"\r\n=yend").unwrap().max(ds);
            ysimd::decode_with(k, &body[ds..de], &mut out);
            assert_eq!(out, d, "decoder {k:?} mismatch on case {t} len {len}");
        }
    }
    println!("SIMD decoders verified on 2000 random cases: {avail:?}");
    let s = memchr::memmem::find(body, b"=ypart").unwrap();
    let ds = s + memchr::memchr(b'\n', &body[s..]).unwrap() + 1;
    let de = memchr::memmem::rfind(body, b"\r\n=yend").unwrap();
    for &k in &avail {
        let t = Instant::now();
        for _ in 0..n {
            ysimd::decode_with(k, &body[ds..de], &mut out);
        }
        let s = t.elapsed().as_secs_f64();
        println!("yEnc {k:?}: {:.2} GB/s per core", ((de - ds) * n) as f64 / s / 1e9);
    }
    let fin = memchr::memmem::Finder::new(b"\r\n.\r\n");
    let t = Instant::now();
    let mut c = 0;
    for _ in 0..n {
        c += fin.find(body).is_some() as usize;
    }
    let s = t.elapsed().as_secs_f64();
    println!("terminator scan: {:.2} GB/s per core ({c})", (body.len() * n) as f64 / s / 1e9);
}

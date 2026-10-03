mod api;
mod config;
mod conn;
mod engine;
mod gf16;
mod http;
mod job;
mod mock;
mod nzb;
mod outfile;
mod par2;
mod queue;
mod rar;
mod sabimport;
mod stats;
mod svccfg;
mod yenc;
mod ysimd;
mod zip;

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
  nzbfast serve [--config FILE]     (default /etc/nzbfast/nzbfast.toml)
  nzbfast import-sab [--config FILE] --sab-url URL --sab-incomplete DIR [--dry-run]
  nzbfast get [--sab-ini FILE] [--server SPEC]... [--only a,b] [--tmp DIR] [--done DIR]
              [--active N] [--depth N] [--nic IF] [--limit N] NZB|DIR...
  nzbfast tidy [--name NAME] [--password PW] DIR   (join splits, name by content, unpack nested)
  nzbfast mock-gen --out DIR RELEASE_DIR...
  nzbfast mock-serve --dir DIR [--port P] [--tls] [--drop SUBSTR:EVERY[:CODE]]... [--miss-ms N] [--conn-mbs N]
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
        "serve" => serve(rest),
        "import-sab" => {
            let (mut path, mut url, mut inc, mut dry) = ("/etc/nzbfast/nzbfast.toml".to_string(), None, None, false);
            let mut it = rest.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--config" => path = it.next().cloned().unwrap_or_else(|| usage()),
                    "--sab-url" => url = it.next().cloned(),
                    "--sab-incomplete" => inc = it.next().cloned(),
                    "--dry-run" => dry = true,
                    _ => usage(),
                }
            }
            let (Some(url), Some(inc)) = (url, inc) else { usage() };
            let l = svccfg::load(&path).unwrap_or_else(|e| {
                eprintln!("config: {e}");
                std::process::exit(1)
            });
            if let Err(e) = sabimport::run(l, &url, Path::new(&inc), dry) {
                eprintln!("import-sab: {e}");
                std::process::exit(1);
            }
        }
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
            let (mut dir, mut port, mut tls, mut miss_ms, mut conn_mbs) = (None, 1119u16, false, 0, 0);
            let mut drops = vec![];
            let mut it = rest.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--dir" => dir = it.next().cloned(),
                    "--port" => port = it.next().unwrap().parse().unwrap(),
                    "--tls" => tls = true,
                    "--drop" => drops.push(it.next().unwrap().clone()),
                    "--miss-ms" => miss_ms = it.next().unwrap().parse().unwrap(),
                    "--conn-mbs" => conn_mbs = it.next().unwrap().parse().unwrap(),
                    _ => usage(),
                }
            }
            mock::serve(Path::new(&dir.unwrap_or_else(|| usage())), port, tls, &drops, miss_ms, conn_mbs).unwrap();
        }
        "tidy" => {
            let (mut name, mut pw, mut dir) = (None, None, None);
            let mut it = rest.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--name" => name = it.next().cloned(),
                    "--password" => pw = it.next().cloned(),
                    _ => dir = Some(a.clone()),
                }
            }
            let dir = PathBuf::from(dir.unwrap_or_else(|| usage()));
            let name = name.unwrap_or_else(|| dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default());
            for m in job::Job::tidy_dir(&dir, &name, pw) {
                println!("{m}");
            }
        }
        "bench" => bench(),
        "fetch" => {
            // fetch SAB_INI SERVER_SUBSTR MSGID OUT
            let servers = config::from_sab_ini(&rest[0]).unwrap();
            let mut cfg = servers.into_iter().find(|s| s.name.to_lowercase().contains(&rest[1].to_lowercase())).expect("server");
            if rest.len() > 4 {
                cfg.host = rest[4].clone();
            }
            let body = conn::fetch_raw(&cfg, &rest[2]).unwrap();
            std::fs::write(&rest[3], &body).unwrap();
            let mut out = vec![];
            println!("{} bytes; decode: {:?}", body.len(), yenc::decode(&body, &mut out).map(|(i, c)| (i, c, out.len())));
        }
        "par2check" => {
            // par2check DIR: verify the files under DIR against the par2 set found there.
            fn walk(d: &Path, out: &mut Vec<PathBuf>) {
                for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        walk(&p, out)
                    } else {
                        out.push(p)
                    }
                }
            }
            let mut all = vec![];
            walk(Path::new(&rest[0]), &mut all);
            let mut set = par2::Par2Set::default();
            for p in all.iter().filter(|p| p.to_string_lossy().to_lowercase().ends_with(".par2")) {
                set.add_file(std::fs::read(p).unwrap_or_default());
            }
            let byname: std::collections::HashMap<String, PathBuf> =
                all.iter().map(|p| (p.file_name().unwrap().to_string_lossy().into_owned(), p.clone())).collect();
            println!("slice {} recovery files {} recovery blocks {}", set.slice, set.recovery_ids.len(), set.recv.len());
            let (mut total, mut damaged) = (0u64, 0u64);
            let mut buf = vec![0u8; set.slice as usize];
            for id in &set.recovery_ids {
                let Some(fd) = set.files.get(id) else { continue };
                let n = fd.len.div_ceil(set.slice);
                let f = byname.get(&fd.name).and_then(|p| std::fs::File::open(p).ok());
                let mut bad = 0;
                for j in 0..n {
                    let ok = match &f {
                        Some(f) => {
                            use std::os::unix::fs::FileExt;
                            let want = ((fd.len - j * set.slice) as usize).min(buf.len());
                            let mut got = 0;
                            while got < want {
                                match f.read_at(&mut buf[got..want], j * set.slice + got as u64) {
                                    Ok(0) | Err(_) => break,
                                    Ok(k) => got += k,
                                }
                            }
                            buf[got..].fill(0);
                            set.ifsc.get(id).and_then(|c| c.get(j as usize)) == Some(&crc32fast::hash(&buf))
                        }
                        None => false,
                    };
                    if !ok {
                        bad += 1;
                    }
                }
                total += n;
                damaged += bad;
                if bad > 0 {
                    println!("  {:<60} {bad}/{n} damaged{}", fd.name, if f.is_none() { " (not found)" } else { "" });
                }
            }
            println!("total slices {total}, damaged {damaged}, recovery blocks {}", set.recv.len());
        }
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

fn serve(args: &[String]) {
    let mut path = "/etc/nzbfast/nzbfast.toml".to_string();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => path = it.next().cloned().unwrap_or_else(|| usage()),
            _ => usage(),
        }
    }
    let mut l = svccfg::load(&path).unwrap_or_else(|e| {
        eprintln!("config: {e}");
        std::process::exit(1)
    });
    if l.cfg.api_key.is_empty() {
        // No key configured or imported: generate one and keep it across restarts.
        let kf = l.cfg.state_dir.join("api_key");
        l.cfg.api_key = std::fs::read_to_string(&kf).map(|s| s.trim().to_string()).unwrap_or_default();
        if l.cfg.api_key.is_empty() {
            let mut b = [0u8; 16];
            use std::io::Read;
            let _ = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b));
            l.cfg.api_key = b.iter().map(|x| format!("{x:02x}")).collect();
            let _ = std::fs::create_dir_all(&l.cfg.state_dir);
            let _ = std::fs::write(&kf, &l.cfg.api_key);
            eprintln!("generated API key (stored in {})", kf.display());
        }
    }
    let listen = l.cfg.listen.clone();
    let eng = engine::Engine::start(l).unwrap_or_else(|e| {
        eprintln!("start: {e}");
        std::process::exit(1)
    });
    let hub = stats::Hub::start(eng.clone());
    let handler = api::handler(eng.clone(), hub);
    // Several addresses may be given, e.g. loopback plus a Docker bridge gateway.
    for addr in listen.split(',').map(str::trim).filter(|a| !a.is_empty()) {
        if let Err(e) = http::serve(addr, handler.clone()) {
            eprintln!("listen {addr}: {e}");
            std::process::exit(1);
        }
        eprintln!("nzbfast {} serving SABnzbd API + UI on http://{addr}/", env!("CARGO_PKG_VERSION"));
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn get(args: &[String]) {
    let mut servers = vec![];
    let mut only: Vec<String> = vec![];
    let mut tmp = PathBuf::from("/root/nzbfast/tmp");
    let mut done = PathBuf::from("/root/nzbfast/done");
    let mut active_max = 256usize;
    let mut depth = 8usize;
    let mut nic = "ens18".to_string();
    let mut limit = usize::MAX;
    let mut inputs = vec![];
    let mut sets: Vec<String> = vec![];
    let mut list_only = false;
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
            "--set" => sets.push(v()),
            "--list-servers" => list_only = true,
            s if s.starts_with("--") => usage(),
            _ => collect_nzbs(Path::new(a), &mut inputs),
        }
    }
    // --set name:key=value (key: host, port, conns, prio, depth, enable)
    for spec in &sets {
        let (name, kv) = spec.split_once(':').unwrap_or_else(|| usage());
        let (k, val) = kv.split_once('=').unwrap_or_else(|| usage());
        let name = name.to_lowercase();
        for s in servers.iter_mut().filter(|s| s.name.to_lowercase().contains(&name) || s.host.contains(&name)) {
            match k {
                "host" => s.host = val.to_string(),
                "port" => s.port = val.parse().unwrap(),
                "conns" => s.conns = val.parse().unwrap(),
                "prio" => s.priority = val.parse().unwrap(),
                "depth" => s.depth = val.parse().unwrap(),
                "enable" => {
                    if val == "0" {
                        s.conns = 0
                    }
                }
                _ => usage(),
            }
        }
    }
    servers.retain(|s| s.conns > 0);
    if list_only {
        for s in &servers {
            println!("{:<22} {}:{} tls={} conns={} prio={} user_set={}", s.name, s.host, s.port, s.tls, s.conns, s.priority, !s.user.is_empty());
        }
        return;
    }
    if !only.is_empty() {
        servers.retain(|s| only.iter().any(|o| s.name.to_lowercase().contains(o) || s.host.contains(o)));
    }
    if servers.is_empty() || inputs.is_empty() {
        usage();
    }
    let _ = &list_only;
    inputs.truncate(limit);
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::create_dir_all(&done).unwrap();

    outfile::start_io(8);
    let slots = servers.iter().map(|s| (s.conns * if s.depth > 0 { s.depth } else { depth }) as u32).collect();
    let q = Arc::new(Queues::new(servers.iter().map(|s| s.priority).collect(), slots));
    let _ = job::QUEUE.set(q.clone());
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
                for w in q.sweep() {
                    w.missing(&q);
                }
                let mut tot = 0u64;
                let mut parts = String::new();
                for (i, s) in stats.iter().enumerate() {
                    let b = s.bytes.load(Relaxed);
                    let d = b - prev[i];
                    prev[i] = b;
                    tot += d;
                    parts.push_str(&format!(
                        " | {} {:.2}/{} {:.0}%/{:.0}% {:.0}ms",
                        names[i],
                        d as f64 * 8.0 / 1e9,
                        s.live.load(Relaxed),
                        q.hit_rate(i) * 100.0,
                        q.retry_rate(i) * 100.0,
                        q.miss_ms(i)
                    ));
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
    let low_water = (total_conns * 20).max(4000);
    let mut next = 0;
    let n_inputs = inputs.len();
    let mut printer = |rx: &mpsc::Receiver<String>| {
        while let Ok(m) = rx.try_recv() {
            println!("{m}");
        }
    };
    while next < n_inputs || active.load(Relaxed) > 0 {
        printer(&rx);
        if next < n_inputs && active.load(Relaxed) < active_max && q.main_len() < low_water {
            let path = inputs[next].to_string_lossy().to_string();
            next += 1;
            match nzb::load(&path) {
                Ok(n) => {
                    active.fetch_add(1, Relaxed);
                    let (work, fin) = (tmp.join(&n.name), done.join(&n.name));
                    let job = Job::new(next, n, work, Some(fin), finished.clone());
                    let w = job.probe_work();
                    job.enqueue(&q, w, true);
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
        .filter(|k| match k {
            ysimd::Kind::Scalar => true,
            _ => ysimd::supported(*k),
        })
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

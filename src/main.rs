mod api;
mod config;
mod conn;
mod engine;
mod gf16;
mod http;
mod job;
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
use job::{Job, JobResult, OnFinish};
use queue::Queues;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

const DEFAULT_CONFIG: &str = "/etc/nzbfast/nzbfast.toml";

fn usage() -> ! {
    eprintln!(
        "nzbfast {}

usage:
  nzbfast serve [--config FILE]
      Run the service: SABnzbd-compatible API and web UI (default config {DEFAULT_CONFIG}).

  nzbfast get [--config FILE | --sab-ini FILE | --server SPEC]... [--out DIR] NZB|DIR...
      Download NZB files (or every NZB in a folder) into DIR (default: current folder).
      Servers come from the service config unless --sab-ini or --server is given.
      SPEC: host=HOST,port=563,tls=1,user=USER,pass=PASS,conns=20[,prio=0][,name=NAME]

  nzbfast import-sab [--config FILE] --sab-url URL --sab-incomplete DIR [--dry-run]
      Take over a SABnzbd queue and history (stop the nzbfast service first).",
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(2)
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

fn load_config(path: &str) -> svccfg::Loaded {
    svccfg::load(path).unwrap_or_else(|e| {
        eprintln!("config: {e}");
        std::process::exit(1)
    })
}

fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first() else { usage() };
    let rest = &args[1..];
    match cmd.as_str() {
        "serve" => serve(rest),
        "get" => get(rest),
        "import-sab" => {
            let (mut path, mut url, mut inc, mut dry) = (DEFAULT_CONFIG.to_string(), None, None, false);
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
            if let Err(e) = sabimport::run(load_config(&path), &url, Path::new(&inc), dry) {
                eprintln!("import-sab: {e}");
                std::process::exit(1);
            }
        }
        "--version" | "-V" | "version" => println!("nzbfast {}", env!("CARGO_PKG_VERSION")),
        _ => usage(),
    }
}

fn serve(args: &[String]) {
    let mut path = DEFAULT_CONFIG.to_string();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => path = it.next().cloned().unwrap_or_else(|| usage()),
            _ => usage(),
        }
    }
    let mut l = load_config(&path);
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
    let mut config = None;
    let mut out = PathBuf::from(".");
    let mut inputs = vec![];
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut v = || it.next().cloned().unwrap_or_else(|| usage());
        match a.as_str() {
            "--config" => config = Some(v()),
            "--sab-ini" => servers.extend(config::from_sab_ini(&v()).unwrap_or_else(|e| {
                eprintln!("{e}");
                std::process::exit(1)
            })),
            "--server" => servers.push(config::from_spec(&v()).unwrap_or_else(|e| {
                eprintln!("--server: {e}");
                std::process::exit(1)
            })),
            "--out" => out = v().into(),
            s if s.starts_with("--") => usage(),
            _ => collect_nzbs(Path::new(a), &mut inputs),
        }
    }
    let mut depth = 8;
    if servers.is_empty() || config.is_some() {
        let l = load_config(config.as_deref().unwrap_or(DEFAULT_CONFIG));
        depth = l.cfg.depth;
        servers.extend(l.servers);
    }
    servers.retain(|s| s.conns > 0);
    if servers.is_empty() || inputs.is_empty() {
        usage();
    }
    let incomplete = out.join(".incomplete");
    if let Err(e) = std::fs::create_dir_all(&incomplete) {
        eprintln!("{}: {e}", incomplete.display());
        std::process::exit(1);
    }

    outfile::start_io(8);
    let slots = servers.iter().map(|s| (s.conns * if s.depth > 0 { s.depth } else { depth }) as u32).collect();
    let q = Arc::new(Queues::new(servers.iter().map(|s| s.priority).collect(), slots));
    let _ = job::QUEUE.set(q.clone());
    let stats: Vec<Arc<SStats>> = servers.iter().map(|_| Arc::new(SStats::default())).collect();
    let mut total_slots = 0;
    for (i, s) in servers.iter().enumerate() {
        let d = if s.depth > 0 { s.depth } else { depth };
        let tls = conn::tls_config(s.insecure);
        for _ in 0..s.conns {
            let ctx = ConnCtx { idx: i, cfg: s.clone(), depth: d, tls: tls.clone(), q: q.clone(), st: stats[i].clone() };
            std::thread::Builder::new().stack_size(256 << 10).spawn(move || conn::run(ctx)).unwrap();
        }
        total_slots += s.conns * d;
    }

    let active = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel::<(bool, String)>();
    let finished = {
        let active = active.clone();
        let tx = std::sync::Mutex::new(tx);
        Arc::new(move |j: &Job, r: JobResult| {
            let secs = j.started.elapsed().as_secs_f64();
            let gb = j.bytes_done.load(Relaxed) as f64 / 1e9;
            let line = format!("{} {} ({gb:.2} GB in {secs:.1} s): {}", if r.ok { "done" } else { "FAILED" }, j.name, r.summary);
            let _ = tx.lock().unwrap().send((r.ok, line));
            active.fetch_sub(1, Relaxed);
        }) as OnFinish
    };

    let t0 = Instant::now();
    let (mut ok, mut failed) = (0, 0);
    let mut prev = 0u64;
    let mut last_report = Instant::now();
    // Keep enough first attempts queued to fill every pipeline; jobs finish roughly in order.
    let low_water = (total_slots * 20).max(4000);
    let mut next = 0;
    while next < inputs.len() || active.load(Relaxed) > 0 {
        while let Ok((good, line)) = rx.try_recv() {
            if good {
                ok += 1
            } else {
                failed += 1
            }
            println!("{line}");
        }
        if last_report.elapsed() >= Duration::from_secs(1) {
            last_report = Instant::now();
            for w in q.sweep() {
                w.missing(&q);
            }
            let bytes: u64 = stats.iter().map(|s| s.bytes.load(Relaxed)).sum();
            eprintln!("{:6.2} Gbit/s  {} downloading, {} queued", (bytes - prev) as f64 * 8.0 / 1e9, active.load(Relaxed), inputs.len() - next);
            prev = bytes;
        }
        if next < inputs.len() && active.load(Relaxed) < 64 && q.main_len() < low_water {
            let path = inputs[next].to_string_lossy().to_string();
            next += 1;
            match nzb::load(&path) {
                Ok(n) => {
                    active.fetch_add(1, Relaxed);
                    let (work, fin) = (incomplete.join(&n.name), out.join(&n.name));
                    let job = Job::new(next, n, work, Some(fin), finished.clone());
                    let w = job.probe_work();
                    job.enqueue(&q, w, true);
                }
                Err(e) => {
                    eprintln!("skip {path}: {e}");
                    failed += 1;
                }
            }
            continue;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    while let Ok((good, line)) = rx.try_recv() {
        if good {
            ok += 1
        } else {
            failed += 1
        }
        println!("{line}");
    }
    q.close();
    let _ = std::fs::remove_dir(&incomplete);
    let el = t0.elapsed().as_secs_f64();
    let bytes: u64 = stats.iter().map(|s| s.bytes.load(Relaxed)).sum();
    for (s, st) in servers.iter().zip(&stats) {
        eprintln!(
            "{:<20} {:>8.2} GB  articles ok {}  missing {}  crc errors {}  connection errors {}",
            s.name,
            st.bytes.load(Relaxed) as f64 / 1e9,
            st.ok.load(Relaxed),
            st.missing.load(Relaxed),
            st.crc_errors.load(Relaxed),
            st.errors.load(Relaxed)
        );
    }
    eprintln!("{ok} done, {failed} failed: {:.2} GB in {el:.1} s ({:.2} Gbit/s)", bytes as f64 / 1e9, bytes as f64 * 8.0 / el / 1e9);
    if failed > 0 {
        std::process::exit(1);
    }
}

//! NNTP test server for the benchmarks. `gen` encodes release folders into yEnc
//! articles plus one NZB per release; `serve` holds the articles in RAM and serves
//! them on one or more ports, each behaving like a different kind of provider.

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ARTICLE: usize = 716_800;

fn usage() -> ! {
    eprintln!(
        "usage:
  nntp-mock gen --out DIR RELEASE_DIR...
  nntp-mock serve --dir DIR [--tls] [--stats FILE] [--epoch FILE] --listen SPEC [--listen SPEC]...

SPEC is PORT followed by comma-separated options:
  drop=SUBSTR:EVERY[:CODE]  message-ids containing SUBSTR whose part number is a multiple
                            of EVERY answer CODE (default 430); EVERY=0 drops every match,
                            ~EVERY drops all but those. Repeatable.
  miss-ms=N                 delay before every \"not found\" reply
  conn-mbs=N                per-connection bandwidth cap in MB/s
  max-conns=N               connections beyond N get \"502 too many connections\"
  login-ms=N                delay before the greeting (a server that stalls logins)
  reset-every=N             drop the connection after every N-th article, mid-body
  outage=A-B                refuse and drop connections from A to B seconds after the time
                            (unix seconds) last written to the --epoch file"
    );
    std::process::exit(2)
}

fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(cmd), rest) = (args.first(), args.get(1..).unwrap_or_default()) else { usage() };
    let mut it = rest.iter();
    match cmd.as_str() {
        "gen" => {
            let (mut out, mut rels) = (None, vec![]);
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--out" => out = it.next().cloned(),
                    _ => rels.push(a.clone()),
                }
            }
            gen(Path::new(&out.unwrap_or_else(|| usage())), &rels).unwrap();
        }
        "serve" => {
            let (mut dir, mut tls, mut stats, mut epoch, mut specs) = (None, false, None, None, vec![]);
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--dir" => dir = it.next().cloned(),
                    "--tls" => tls = true,
                    "--stats" => stats = it.next().cloned(),
                    "--epoch" => epoch = it.next().cloned(),
                    "--listen" => specs.push(it.next().cloned().unwrap_or_else(|| usage())),
                    _ => usage(),
                }
            }
            if specs.is_empty() {
                usage();
            }
            if let Some(f) = epoch {
                std::thread::spawn(move || loop {
                    if let Some(t) = fs::read_to_string(&f).ok().and_then(|s| s.trim().parse::<f64>().ok()) {
                        EPOCH_MS.store((t * 1000.0) as u64, Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                });
            }
            serve(Path::new(&dir.unwrap_or_else(|| usage())), tls, stats, &specs).unwrap();
        }
        _ => usage(),
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_article(msgid: &str, name: &str, file_size: u64, part: u32, total: u32, begin: u64, data: &[u8], out: &mut Vec<u8>) {
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

/// Writes `articles.bin` + `index.tsv` (message-id, offset, length) and `<release>.nzb`.
fn gen(out: &Path, releases: &[String]) -> io::Result<()> {
    fs::create_dir_all(out)?;
    let mut bin = BufWriter::new(fs::File::create(out.join("articles.bin"))?);
    let mut idx = BufWriter::new(fs::File::create(out.join("index.tsv"))?);
    let mut off = 0u64;
    let mut art = Vec::with_capacity(ARTICLE * 2);
    for rel in releases {
        let rel_path = Path::new(rel);
        let rname = rel_path.file_name().unwrap().to_string_lossy().to_string();
        let mut files: Vec<_> = fs::read_dir(rel_path)?.flatten().map(|e| e.path()).filter(|p| p.is_file()).collect();
        files.sort();
        let mut nzb = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n");
        for (fi, f) in files.iter().enumerate() {
            let fname = f.file_name().unwrap().to_string_lossy().to_string();
            let data = fs::read(f)?;
            let total = data.len().div_ceil(ARTICLE).max(1);
            nzb.push_str(&format!(
                "<file poster=\"mock\" date=\"0\" subject=\"[{}/{}] - &quot;{}&quot; yEnc (1/{total})\">\n<groups><group>alt.binaries.test</group></groups>\n<segments>\n",
                fi + 1,
                files.len(),
                fname.replace('&', "&amp;")
            ));
            for p in 0..total {
                let a = p * ARTICLE;
                let b = (a + ARTICLE).min(data.len());
                let msgid = format!("<{rname}.{fi}.{p}@mock>");
                art.clear();
                encode_article(&msgid, &fname, data.len() as u64, p as u32 + 1, total as u32, a as u64, &data[a..b], &mut art);
                bin.write_all(&art)?;
                writeln!(idx, "{msgid}\t{off}\t{}", art.len())?;
                nzb.push_str(&format!("<segment bytes=\"{}\" number=\"{}\">{}</segment>\n", art.len(), p + 1, &msgid[1..msgid.len() - 1]));
                off += art.len() as u64;
            }
            nzb.push_str("</segments>\n</file>\n");
        }
        nzb.push_str("</nzb>\n");
        fs::write(out.join(format!("{rname}.nzb")), nzb)?;
        eprintln!("encoded {rname}: {} files", files.len());
    }
    bin.flush()?;
    idx.flush()
}

struct Store {
    data: Vec<u8>,
    idx: HashMap<String, (usize, usize)>,
}

#[derive(Default)]
struct Profile {
    port: u16,
    drop: HashMap<String, String>,
    miss_ms: u64,
    conn_rate: u64,
    max_conns: usize,
    login_ms: u64,
    reset_every: u64,
    outage: Option<(f64, f64)>,
    conns: AtomicUsize,
    sent: AtomicU64,
}

/// Unix time in ms read from the --epoch file (when the benchmark run started).
static EPOCH_MS: AtomicU64 = AtomicU64::new(0);

fn unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

impl Profile {
    fn in_outage(&self) -> bool {
        let Some((a, b)) = self.outage else { return false };
        let t = unix_ms().saturating_sub(EPOCH_MS.load(Relaxed)) as f64 / 1000.0;
        t >= a && t < b
    }
}

fn part_number(id: &str) -> usize {
    id.rsplit('.').next().and_then(|t| t.split('@').next()).and_then(|n| n.parse().ok()).unwrap_or(1)
}

fn profile(spec: &str, store: &Store) -> Profile {
    let mut opts = spec.split(',');
    let mut p = Profile { port: opts.next().and_then(|s| s.parse().ok()).unwrap_or_else(|| usage()), ..Default::default() };
    for o in opts {
        let (k, v) = o.split_once('=').unwrap_or_else(|| usage());
        let n = || v.parse::<u64>().unwrap_or_else(|_| usage());
        match k {
            "miss-ms" => p.miss_ms = n(),
            "conn-mbs" => p.conn_rate = n() * 1_000_000,
            "max-conns" => p.max_conns = n() as usize,
            "login-ms" => p.login_ms = n(),
            "reset-every" => p.reset_every = n(),
            "outage" => {
                let (a, b) = v.split_once('-').unwrap_or_else(|| usage());
                p.outage = Some((a.parse().unwrap_or_else(|_| usage()), b.parse().unwrap_or_else(|_| usage())));
            }
            "drop" => {
                let mut it = v.splitn(3, ':');
                let sub = it.next().unwrap_or("");
                let (inv, every) = match it.next().unwrap_or("0") {
                    e if e.starts_with('~') => (true, e[1..].parse::<usize>().unwrap_or(0)),
                    e => (false, e.parse().unwrap_or(0)),
                };
                let code = it.next().unwrap_or("430");
                for id in store.idx.keys() {
                    if id.contains(sub) && (every == 0 || (part_number(id) % every == every - 1) != inv) {
                        p.drop.insert(id.clone(), code.to_string());
                    }
                }
            }
            _ => usage(),
        }
    }
    p
}

fn serve(dir: &Path, tls: bool, stats: Option<String>, specs: &[String]) -> io::Result<()> {
    let data = fs::read(dir.join("articles.bin"))?;
    let mut idx = HashMap::new();
    for line in fs::read_to_string(dir.join("index.tsv"))?.lines() {
        let mut it = line.split('\t');
        let (Some(id), Some(o), Some(l)) = (it.next(), it.next(), it.next()) else {
            continue;
        };
        idx.insert(id.to_string(), (o.parse().unwrap(), l.parse().unwrap()));
    }
    let store = Arc::new(Store { data, idx });
    let tls_cfg = tls.then(|| {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::try_from(ck.key_pair.serialize_der()).unwrap();
        Arc::new(rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(vec![ck.cert.der().clone()], key).unwrap())
    });
    let profiles: Vec<Arc<Profile>> = specs.iter().map(|s| Arc::new(profile(s, &store))).collect();
    for p in &profiles {
        let l = TcpListener::bind(("0.0.0.0", p.port))?;
        eprintln!("port {}: {} articles answer an error", p.port, p.drop.len());
        let (store, p, tls_cfg) = (store.clone(), p.clone(), tls_cfg.clone());
        std::thread::spawn(move || {
            for s in l.incoming().flatten() {
                let (store, p, tls_cfg) = (store.clone(), p.clone(), tls_cfg.clone());
                std::thread::spawn(move || accept(s, &store, &p, tls_cfg));
            }
        });
    }
    eprintln!("{} articles, {:.1} GB in RAM, tls={tls}", store.idx.len(), store.data.len() as f64 / 1e9);
    // Bytes sent per port every 100 ms: "unix_time bytes_port1 bytes_port2 ...".
    let mut out = stats.map(|f| BufWriter::new(fs::File::create(f).unwrap()));
    if let Some(o) = out.as_mut() {
        let _ = writeln!(o, "# ports {}", profiles.iter().map(|p| p.port.to_string()).collect::<Vec<_>>().join(" "));
    }
    loop {
        std::thread::sleep(Duration::from_millis(100));
        if let Some(o) = out.as_mut() {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
            let _ = write!(o, "{:.1}", now.as_secs_f64());
            for p in &profiles {
                let _ = write!(o, " {}", p.sent.load(Relaxed));
            }
            let _ = writeln!(o);
            let _ = o.flush();
        }
    }
}

fn accept(s: TcpStream, store: &Store, p: &Profile, tls: Option<Arc<rustls::ServerConfig>>) {
    if p.in_outage() {
        return;
    }
    let _ = s.set_nodelay(true);
    let n = p.conns.fetch_add(1, Relaxed) + 1;
    if p.login_ms > 0 {
        std::thread::sleep(Duration::from_millis(p.login_ms));
    }
    let full = p.max_conns > 0 && n > p.max_conns;
    let _ = match tls {
        Some(c) => handle(rustls::StreamOwned::new(rustls::ServerConnection::new(c).unwrap(), s), store, p, full),
        None => handle(s, store, p, full),
    };
    p.conns.fetch_sub(1, Relaxed);
}

fn miss<S: Write>(s: &mut S, p: &Profile, reply: &str) -> io::Result<()> {
    if p.miss_ms > 0 {
        s.flush()?;
        std::thread::sleep(Duration::from_millis(p.miss_ms));
    }
    s.write_all(reply.as_bytes())
}

fn handle<S: Read + Write>(mut s: S, st: &Store, p: &Profile, full: bool) -> io::Result<()> {
    if full {
        s.write_all(b"502 too many connections\r\n")?;
        return s.flush();
    }
    s.write_all(b"200 mock ready\r\n")?;
    s.flush()?;
    let mut buf = vec![0u8; 65536];
    let mut have = 0;
    // Bandwidth cap as a token bucket holding up to 0.25 s of transfer.
    let (mut free_at, mut bodies) = (Instant::now(), 0u64);
    loop {
        let n = s.read(&mut buf[have..])?;
        if n == 0 {
            return Ok(());
        }
        have += n;
        let mut consumed = 0;
        while let Some(i) = memchr::memchr(b'\n', &buf[consumed..have]) {
            let line = String::from_utf8_lossy(&buf[consumed..consumed + i]).trim().to_string();
            consumed += i + 1;
            let up = line.to_ascii_uppercase();
            if up.starts_with("BODY ") || up.starts_with("STAT ") {
                if p.in_outage() {
                    return Ok(());
                }
                let id = line[5..].trim();
                match (p.drop.get(id), st.idx.get(id)) {
                    (Some(code), _) => miss(&mut s, p, &format!("{code} no such article\r\n"))?,
                    (None, None) => miss(&mut s, p, "430 no such article\r\n")?,
                    (None, Some(_)) if up.starts_with("STAT") => s.write_all(format!("223 0 {id}\r\n").as_bytes())?,
                    (None, Some(&(o, l))) => {
                        if p.conn_rate > 0 {
                            let now = Instant::now();
                            free_at = free_at.max(now - Duration::from_millis(250)) + Duration::from_secs_f64(l as f64 / p.conn_rate as f64);
                            if free_at > now {
                                s.flush()?;
                                std::thread::sleep(free_at - now);
                            }
                        }
                        bodies += 1;
                        if p.reset_every > 0 && bodies % p.reset_every == 0 {
                            s.write_all(&st.data[o..o + l / 2])?;
                            s.flush()?;
                            return Ok(());
                        }
                        s.write_all(&st.data[o..o + l])?;
                        p.sent.fetch_add(l as u64, Relaxed);
                    }
                }
            } else if up.starts_with("AUTHINFO USER") {
                s.write_all(b"381 more\r\n")?;
            } else if up.starts_with("AUTHINFO PASS") {
                s.write_all(b"281 ok\r\n")?;
            } else if up.starts_with("QUIT") {
                s.write_all(b"205 bye\r\n")?;
                return s.flush();
            } else {
                s.write_all(b"500 what\r\n")?;
            }
        }
        s.flush()?;
        buf.copy_within(consumed..have, 0);
        have -= consumed;
        if have == buf.len() {
            return Ok(());
        }
    }
}

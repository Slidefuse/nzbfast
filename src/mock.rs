//! Mock NNTP server for local testing: pre-encodes releases into yEnc articles held
//! in RAM and serves them (optionally over TLS) with pipelining support.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

pub const ARTICLE: usize = 716_800;

/// Encodes each release directory into articles.bin/index.tsv and writes one NZB per release.
pub fn gen(out: &Path, releases: &[String]) -> io::Result<()> {
    fs::create_dir_all(out)?;
    let mut bin = io::BufWriter::new(fs::File::create(out.join("articles.bin"))?);
    let mut idx = io::BufWriter::new(fs::File::create(out.join("index.tsv"))?);
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
                crate::yenc::encode_article(&msgid, &fname, data.len() as u64, p as u32 + 1, total as u32, a as u64, &data[a..b], &mut art);
                bin.write_all(&art)?;
                writeln!(idx, "{msgid}\t{off}\t{}", art.len())?;
                nzb.push_str(&format!("<segment bytes=\"{}\" number=\"{}\">{}</segment>\n", art.len(), p + 1, &msgid[1..msgid.len() - 1]));
                off += art.len() as u64;
            }
            nzb.push_str("</segments>\n</file>\n");
        }
        nzb.push_str("</nzb>\n");
        fs::write(out.join(format!("{rname}.nzb")), nzb)?;
        eprintln!("encoded release {rname}: {} files", files.len());
    }
    bin.flush()?;
    idx.flush()?;
    Ok(())
}

struct Store {
    data: Vec<u8>,
    idx: HashMap<String, (usize, usize)>,
    /// Message-ids answered with an error (simulated missing articles) and the reply.
    drop: HashMap<String, String>,
    /// Delay before each "not found" reply (some providers take ~1 s to say 430).
    miss_ms: u64,
    /// Per-connection bandwidth cap in bytes/s (0 = unlimited).
    conn_rate: u64,
}

/// `drop`: list of `substring:every[:code]` rules; ids containing `substring` whose part
/// number is divisible by `every` are reported missing (every=0 drops all matches;
/// `~every` drops all but those),
/// with reply `code` (default 430).
pub fn serve(dir: &Path, port: u16, tls: bool, drop_rules: &[String], miss_ms: u64, conn_mbs: u64) -> io::Result<()> {
    let data = fs::read(dir.join("articles.bin"))?;
    let mut idx = HashMap::new();
    for line in fs::read_to_string(dir.join("index.tsv"))?.lines() {
        let mut it = line.split('\t');
        let (Some(id), Some(o), Some(l)) = (it.next(), it.next(), it.next()) else { continue };
        idx.insert(id.to_string(), (o.parse().unwrap(), l.parse().unwrap()));
    }
    let mut drop = HashMap::new();
    for id in idx.keys() {
        for r in drop_rules {
            let mut it = r.splitn(3, ':');
            let sub = it.next().unwrap_or("");
            // A leading '~' inverts the rule: drop all matches except every `every`-th.
            let (inv, every) = match it.next().unwrap_or("0") {
                e if e.starts_with('~') => (true, e[1..].parse::<usize>().unwrap_or(0)),
                e => (false, e.parse().unwrap_or(0)),
            };
            let code = it.next().unwrap_or("430");
            let part: usize = id.rsplit('.').next().and_then(|t| t.split('@').next()).and_then(|n| n.parse().ok()).unwrap_or(1);
            if id.contains(sub) && (every == 0 || (part % every == every - 1) != inv) {
                drop.insert(id.clone(), code.to_string());
            }
        }
    }
    eprintln!("mock: dropping {} articles", drop.len());
    eprintln!("mock: {} articles, {:.1} GB in RAM, port {port}, tls={tls}", idx.len(), data.len() as f64 / 1e9);
    let store = Arc::new(Store { data, idx, drop, miss_ms, conn_rate: conn_mbs * 1_000_000 });
    let server_cfg = if tls {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = ck.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::try_from(ck.key_pair.serialize_der()).unwrap();
        Some(Arc::new(rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(vec![cert], key).unwrap()))
    } else {
        None
    };
    let l = TcpListener::bind(("0.0.0.0", port))?;
    for s in l.incoming() {
        let Ok(s) = s else { continue };
        let _ = s.set_nodelay(true);
        let store = store.clone();
        let cfg = server_cfg.clone();
        std::thread::spawn(move || {
            let r = match cfg {
                Some(c) => {
                    let conn = rustls::ServerConnection::new(c).unwrap();
                    handle(rustls::StreamOwned::new(conn, s), &store)
                }
                None => handle(s, &store),
            };
            let _ = r;
        });
    }
    Ok(())
}

fn miss<S: Write>(s: &mut S, st: &Store, reply: &str) -> io::Result<()> {
    if st.miss_ms > 0 {
        s.flush()?;
        std::thread::sleep(std::time::Duration::from_millis(st.miss_ms));
    }
    s.write_all(reply.as_bytes())
}

fn handle<S: Read + Write>(mut s: S, st: &Store) -> io::Result<()> {
    s.write_all(b"200 mock ready\r\n")?;
    s.flush()?;
    let mut buf = vec![0u8; 65536];
    let mut have = 0;
    let (t0, mut sent) = (std::time::Instant::now(), 0u64);
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
            if up.starts_with("BODY ") {
                let id = line[5..].trim();
                match (st.drop.get(id), st.idx.get(id)) {
                    (Some(code), _) => miss(&mut s, st, &format!("{code} simulated failure\r\n"))?,
                    (None, Some(&(o, l))) => {
                        if st.conn_rate > 0 {
                            sent += l as u64;
                            let due = std::time::Duration::from_secs_f64(sent as f64 / st.conn_rate as f64);
                            if let Some(w) = due.checked_sub(t0.elapsed()) {
                                std::thread::sleep(w);
                            }
                        }
                        s.write_all(&st.data[o..o + l])?
                    }
                    (None, None) => miss(&mut s, st, "430 no such article\r\n")?,
                }
            } else if up.starts_with("STAT ") {
                let id = line[5..].trim();
                match (st.drop.get(id), st.idx.get(id)) {
                    (Some(code), _) => miss(&mut s, st, &format!("{code} simulated failure\r\n"))?,
                    (None, Some(_)) => s.write_all(format!("223 0 {id}\r\n").as_bytes())?,
                    (None, None) => miss(&mut s, st, "430 no such article\r\n")?,
                }
            } else if up.starts_with("AUTHINFO USER") {
                s.write_all(b"381 more\r\n")?;
            } else if up.starts_with("AUTHINFO PASS") {
                s.write_all(b"281 ok\r\n")?;
            } else if up.starts_with("QUIT") {
                s.write_all(b"205 bye\r\n")?;
                s.flush()?;
                return Ok(());
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

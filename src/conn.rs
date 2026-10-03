//! One NNTP connection per thread: blocking I/O, pipelined BODY requests,
//! decode + write on the same thread (no hand-offs on the hot path).

use crate::config::ServerCfg;
use crate::queue::{Queues, Work};
use crate::yenc;
use memchr::memmem;
use rustls::pki_types::ServerName;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct SStats {
    pub bytes: AtomicU64,
    pub ok: AtomicU64,
    pub missing: AtomicU64,
    pub errors: AtomicU64,
    pub live: AtomicU64,
    /// Connections being opened or logging in.
    pub opening: AtomicU64,
    pub crc_errors: AtomicU64,
    /// Connection attempts that failed in a row (reset by any successful login).
    pub consec_fail: AtomicU64,
    /// Unusual per-article replies seen (logged for the first few).
    pub odd_replies: AtomicU64,
    pub last_error: std::sync::Mutex<String>,
}

/// Global download speed limit (GCRA over received bytes; 0 = unlimited).
pub struct Limiter {
    pub rate: AtomicU64,
    tat: AtomicU64,
}

pub static LIMIT: Limiter = Limiter { rate: AtomicU64::new(0), tat: AtomicU64::new(0) };

fn mono_ns() -> u64 {
    static T0: OnceLock<Instant> = OnceLock::new();
    T0.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

impl Limiter {
    /// Accounts for `n` received bytes, sleeping when the connection runs ahead of the limit.
    /// Not reading makes TCP flow control slow the sender down.
    pub fn consume(&self, n: u64) {
        let rate = self.rate.load(Relaxed);
        if rate == 0 {
            return;
        }
        let now = mono_ns();
        let cost = n.saturating_mul(1_000_000_000) / rate;
        let mut tat = self.tat.load(Relaxed);
        let new = loop {
            let new = tat.max(now) + cost;
            match self.tat.compare_exchange_weak(tat, new, Relaxed, Relaxed) {
                Ok(_) => break new,
                Err(t) => tat = t,
            }
        };
        const BURST_NS: u64 = 50_000_000;
        if new > now + BURST_NS {
            std::thread::sleep(Duration::from_nanos(new - now - BURST_NS));
        }
    }
}

pub trait Stream: Read + Write + Send {}
impl<T: Read + Write + Send> Stream for T {}

mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};
    #[derive(Debug)]
    pub struct NoVerify;
    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(&self, _: &[u8], _: &CertificateDer<'_>, _: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(&self, _: &[u8], _: &CertificateDer<'_>, _: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }
}

pub fn tls_config(insecure: bool) -> Arc<rustls::ClientConfig> {
    let cfg = if insecure {
        rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(danger::NoVerify)).with_no_client_auth()
    } else {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()
    };
    Arc::new(cfg)
}

/// Socket wrapper that receives in large chunks: rustls asks for at most a few KB
/// per read, which would otherwise cost one syscall per 4 KB at multi-Gbit rates.
struct BigSock {
    s: TcpStream,
    buf: Box<[u8]>,
    pos: usize,
    end: usize,
}

impl Read for BigSock {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.end {
            // Large caller buffers bypass the staging copy.
            if out.len() >= self.buf.len() {
                return self.s.read(out);
            }
            self.end = self.s.read(&mut self.buf)?;
            self.pos = 0;
        }
        let n = out.len().min(self.end - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl Write for BigSock {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.s.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.s.flush()
    }
}

fn connect(cfg: &ServerCfg, tls: &Arc<rustls::ClientConfig>) -> io::Result<Box<dyn Stream>> {
    let addrs: Vec<_> = match &cfg.connect {
        Some(c) => c.to_socket_addrs()?.collect(),
        None => (cfg.host.as_str(), cfg.port).to_socket_addrs()?.collect(),
    };
    let addr = *addrs.first().ok_or_else(|| io::Error::other("no address"))?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(15))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(90)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    if !cfg.tls {
        return Ok(Box::new(tcp));
    }
    let name = ServerName::try_from(cfg.host.clone()).map_err(io::Error::other)?;
    let conn = rustls::ClientConnection::new(tls.clone(), name).map_err(io::Error::other)?;
    let sock = BigSock { s: tcp, buf: vec![0u8; 1 << 20].into_boxed_slice(), pos: 0, end: 0 };
    Ok(Box::new(rustls::StreamOwned::new(conn, sock)))
}

struct Rd {
    s: Box<dyn Stream>,
    buf: Vec<u8>,
    start: usize,
    end: usize,
    /// Counts bytes as they arrive, so throughput reflects the wire rather than
    /// when articles happen to finish.
    rx: Option<Arc<SStats>>,
}

fn err(msg: String) -> io::Error {
    io::Error::other(msg)
}

impl Rd {
    fn fill(&mut self) -> io::Result<()> {
        if self.start > 0 && (self.end == self.buf.len() || self.start > self.buf.len() / 2) {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.end == self.buf.len() {
            let n = self.buf.len() * 2;
            self.buf.resize(n, 0);
        }
        let n = self.s.read(&mut self.buf[self.end..])?;
        if n == 0 {
            return Err(err("connection closed".into()));
        }
        if let Some(st) = &self.rx {
            st.bytes.fetch_add(n as u64, Relaxed);
        }
        self.end += n;
        Ok(())
    }

    fn line(&mut self) -> io::Result<String> {
        let mut scanned = 0;
        loop {
            if let Some(i) = memchr::memchr(b'\n', &self.buf[self.start + scanned..self.end]) {
                let e = self.start + scanned + i;
                let l = String::from_utf8_lossy(&self.buf[self.start..e]).trim_end().to_string();
                self.start = e + 1;
                return Ok(l);
            }
            scanned = self.end - self.start;
            self.fill()?;
        }
    }

    /// Returns the absolute range of a dot-terminated block (valid until next fill).
    fn block(&mut self, fin: &memmem::Finder) -> io::Result<(usize, usize)> {
        let mut scanned = 0usize;
        loop {
            let avail = &self.buf[self.start..self.end];
            if avail.starts_with(b".\r\n") {
                let s = self.start;
                self.start += 3;
                return Ok((s, s));
            }
            let from = scanned.saturating_sub(4);
            if let Some(i) = fin.find(&avail[from..]) {
                let p = from + i;
                let s = self.start;
                self.start = s + p + 5;
                return Ok((s, s + p + 2));
            }
            scanned = avail.len();
            self.fill()?;
        }
    }
}

fn send(rd: &mut Rd, s: &str) -> io::Result<()> {
    rd.s.write_all(s.as_bytes())?;
    rd.s.flush()
}

fn login(rd: &mut Rd, cfg: &ServerCfg) -> io::Result<()> {
    let g = rd.line()?;
    if !(g.starts_with("200") || g.starts_with("201")) {
        return Err(err(format!("greeting: {}", &g[..g.len().min(80)])));
    }
    if cfg.user.is_empty() {
        return Ok(());
    }
    send(rd, &format!("AUTHINFO USER {}\r\n", cfg.user))?;
    let r = rd.line()?;
    if r.starts_with("381") {
        send(rd, &format!("AUTHINFO PASS {}\r\n", cfg.pass))?;
        let r2 = rd.line()?;
        if !r2.starts_with("281") {
            return Err(err(format!("auth: {}", &r2[..r2.len().min(80)])));
        }
    } else if !r.starts_with("281") {
        return Err(err(format!("auth: {}", &r[..r.len().min(80)])));
    }
    Ok(())
}

pub struct ConnCtx {
    pub idx: usize,
    pub cfg: ServerCfg,
    pub depth: usize,
    pub tls: Arc<rustls::ClientConfig>,
    pub q: Arc<Queues>,
    pub st: Arc<SStats>,
}

/// Pops the next work item, discarding items of aborted (hopeless) jobs.
fn pop_live(ctx: &ConnCtx, wait: Option<Duration>) -> Option<Work> {
    loop {
        let w = ctx.q.pop(ctx.idx, wait)?;
        if w.job.is_aborted() {
            w.job.route.settle(ctx.idx);
            w.dropped(&ctx.q);
            continue;
        }
        return Some(w);
    }
}

const PIPELINE_SECS: f64 = 1.0;
const ARTICLE_EST: f64 = 750_000.0;

/// Sizes a connection's pipeline: the requests in flight cover what the connection
/// delivered in its last responses over PIPELINE_SECS (at least one, at most `max`).
/// Requests cannot be taken back once sent, so a slow server must not sit on articles
/// that faster ones could fetch meanwhile. Throughput is summed over several responses,
/// so an occasional slow article does not shorten the pipeline of a fast server.
struct Pace {
    max: usize,
    window: usize,
    /// (bytes, seconds) of recent responses that followed another one.
    recent: [(f64, f64); 16],
    n: usize,
    mark: std::time::Instant,
    lone: bool,
}

impl Pace {
    fn new(max: usize) -> Self {
        Pace { max, window: 1, recent: [(0.0, 0.0); 16], n: 0, mark: std::time::Instant::now(), lone: true }
    }

    /// Requests are being sent while none are outstanding.
    fn start(&mut self) {
        (self.mark, self.lone) = (std::time::Instant::now(), true);
    }

    /// A response arrived with `bytes` of article data (0 for a status-only reply).
    fn answered(&mut self, bytes: f64) {
        let secs = self.mark.elapsed().as_secs_f64();
        self.mark = std::time::Instant::now();
        let fit = |b: f64, s: f64| ((b / s.max(1e-6) * PIPELINE_SECS / ARTICLE_EST) as usize).clamp(1, self.max).min(2 * self.window);
        if std::mem::replace(&mut self.lone, false) {
            // A lone request also waited a round trip: only a reason to lengthen the pipeline.
            if bytes > 0.0 {
                self.window = self.window.max(fit(bytes, secs));
            }
            return;
        }
        self.recent[self.n % self.recent.len()] = (bytes, secs);
        self.n += 1;
        let (b, s) = self.recent.iter().take(self.n).fold((0.0, 0.0), |a, r| (a.0 + r.0, a.1 + r.1));
        if b > 0.0 {
            self.window = fit(b, s);
        }
    }
}

/// Connection failures in a row, with no session live, before a server is marked down.
/// While other connections are still logging in it takes one failure per connection:
/// a server at its account's connection limit refuses the extra ones but is not down.
const DOWN_AFTER: u64 = 3;

fn note_error(ctx: &ConnCtx, e: &io::Error) {
    let n = ctx.st.errors.fetch_add(1, Relaxed);
    if n < 5 || n.is_multiple_of(100) {
        eprintln!("[{}] {e}", ctx.cfg.name);
    }
    *ctx.st.last_error.lock().unwrap() = e.to_string();
}

/// Connects and logs in without any work, to find out whether a down server is back.
fn probe(ctx: &ConnCtx) -> io::Result<()> {
    let s = connect(&ctx.cfg, &ctx.tls)?;
    let mut rd = Rd { s, buf: vec![0; 64 << 10], start: 0, end: 0, rx: None };
    login(&mut rd, &ctx.cfg)?;
    let _ = send(&mut rd, "QUIT\r\n");
    Ok(())
}

/// Replies to BODY that mean "not available here" rather than a broken session:
/// any 4xx/5xx except service/auth/connection-level codes. Some servers answer
/// malformed message-ids with 412 or 501, for example.
fn article_unavailable(code: &str) -> bool {
    (code.starts_with('4') || code.starts_with('5')) && !matches!(code, "400" | "401" | "403" | "480" | "481" | "482" | "502" | "503")
}

pub fn run(ctx: ConnCtx) {
    let mut backoff = 1;
    loop {
        if ctx.q.is_down(ctx.idx) {
            backoff = 1;
            std::thread::sleep(Duration::from_millis(200));
            if !ctx.q.is_down(ctx.idx) || !ctx.q.claim_probe(ctx.idx) {
                continue;
            }
            match probe(&ctx) {
                Ok(()) => {
                    ctx.q.login_ok(ctx.idx);
                    eprintln!("[{}] reachable again", ctx.cfg.name);
                    ctx.st.consec_fail.store(0, Relaxed);
                    ctx.q.set_down(ctx.idx, false);
                }
                Err(e) => note_error(&ctx, &e),
            }
            continue;
        }
        // Only connect when there is work, but take it only once logged in: a server that
        // stalls the login (some answer 502 only after ~20 s) must not sit on articles
        // that healthy servers could fetch meanwhile.
        if !ctx.q.wait_work(ctx.idx, Duration::from_secs(5)) {
            if ctx.q.is_closed() {
                return;
            }
            continue;
        }
        let mut inflight: VecDeque<Work> = VecDeque::new();
        let mut worked = false;
        match session(&ctx, &mut inflight, &mut worked) {
            Ok(()) => backoff = 1,
            // A session that was answering requests and broke off reconnects right away.
            Err(e) if worked => {
                note_error(&ctx, &e);
                for w in ctx.q.give_back(ctx.idx, inflight.drain(..).collect()) {
                    w.missing(&ctx.q);
                }
                backoff = 1;
            }
            Err(e) => {
                note_error(&ctx, &e);
                ctx.q.login_failed(ctx.idx, ctx.st.live.load(Relaxed));
                for w in ctx.q.give_back(ctx.idx, inflight.drain(..).collect()) {
                    w.missing(&ctx.q);
                }
                let fails = ctx.st.consec_fail.fetch_add(1, Relaxed) + 1;
                let settled = ctx.st.opening.load(Relaxed) == 0 || fails >= ctx.cfg.conns as u64;
                if fails >= DOWN_AFTER && ctx.st.live.load(Relaxed) == 0 && settled && !ctx.q.is_down(ctx.idx) {
                    eprintln!("[{}] marked down after {fails} failed connection attempts", ctx.cfg.name);
                    ctx.q.set_down(ctx.idx, true);
                }
                // Once the server is marked down, the shared probe takes over.
                let until = std::time::Instant::now() + Duration::from_secs(backoff);
                backoff = (backoff * 2).min(30);
                while std::time::Instant::now() < until && !ctx.q.is_down(ctx.idx) {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
}

fn session(ctx: &ConnCtx, inflight: &mut VecDeque<Work>, worked: &mut bool) -> io::Result<()> {
    struct Count<'a>(&'a AtomicU64);
    impl<'a> Count<'a> {
        fn new(c: &'a AtomicU64) -> Self {
            c.fetch_add(1, Relaxed);
            Count(c)
        }
    }
    impl Drop for Count<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Relaxed);
        }
    }
    ctx.q.connecting(ctx.idx);
    let opening = Count::new(&ctx.st.opening);
    let s = connect(&ctx.cfg, &ctx.tls)?;
    let mut rd = Rd { s, buf: vec![0; 8 << 20], start: 0, end: 0, rx: Some(ctx.st.clone()) };
    login(&mut rd, &ctx.cfg)?;
    ctx.q.login_ok(ctx.idx);
    ctx.st.consec_fail.store(0, Relaxed);
    let _live = Count::new(&ctx.st.live);
    drop(opening);
    if ctx.q.is_down(ctx.idx) {
        eprintln!("[{}] reachable again", ctx.cfg.name);
        ctx.q.set_down(ctx.idx, false);
    }
    let fin = memmem::Finder::new(b"\r\n.\r\n");
    let mut out: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut req = String::with_capacity(4096);
    let mut sent = 0usize; // inflight[..sent] have been requested
    let mut last_resp = std::time::Instant::now();
    let mut pace = Pace::new(ctx.depth);
    loop {
        while inflight.len() < pace.window {
            match pop_live(ctx, None) {
                Some(w) => inflight.push_back(w),
                None => break,
            }
        }
        if inflight.is_empty() {
            match pop_live(ctx, Some(Duration::from_secs(20))) {
                Some(w) => inflight.push_back(w),
                None => {
                    let _ = send(&mut rd, "QUIT\r\n");
                    return Ok(());
                }
            }
        }
        if sent < inflight.len() {
            req.clear();
            for w in inflight.iter().skip(sent) {
                req.push_str(if w.stat { "STAT " } else { "BODY " });
                req.push_str(w.job.msgid(w.file, w.seg));
                req.push_str("\r\n");
            }
            if sent == 0 {
                pace.start();
            }
            sent = inflight.len();
            send(&mut rd, &req)?;
        }
        let status = rd.line()?;
        *worked = true;
        let code = status.get(..3).unwrap_or("");
        let since = last_resp.elapsed();
        last_resp = std::time::Instant::now();
        if code != "222" {
            pace.answered(0.0);
        }
        match code {
            "222" => {
                let (s, e) = rd.block(&fin)?;
                let w = inflight.pop_front().unwrap();
                sent -= 1;
                pace.answered((e - s) as f64);
                LIMIT.consume((e - s) as u64);
                ctx.q.record(ctx.idx, true, w.tried != 0);
                w.job.route.record(ctx.idx, true);
                match yenc::decode(&rd.buf[s..e], &mut out) {
                    Ok((info, crc)) => {
                        ctx.st.ok.fetch_add(1, Relaxed);
                        w.job.on_article(w.file, w.seg, &info, &out, crc, &ctx.q);
                    }
                    Err(e) => {
                        if ctx.st.crc_errors.fetch_add(1, Relaxed) < 3 {
                            eprintln!("[{}] decode error ({e}) for {}", ctx.cfg.name, w.job.msgid(w.file, w.seg));
                        }
                        if let Some(w) = ctx.q.retry_elsewhere(ctx.idx, w) {
                            w.missing(&ctx.q);
                        }
                    }
                }
            }
            "223" => {
                // STAT: the article exists.
                let w = inflight.pop_front().unwrap();
                sent -= 1;
                ctx.q.record(ctx.idx, true, w.tried != 0);
                w.job.route.record(ctx.idx, true);
                w.job.on_stat(Some(true));
            }
            c if article_unavailable(c) => {
                let w = inflight.pop_front().unwrap();
                sent -= 1;
                if w.stat && !matches!(c, "430" | "423") {
                    // A server that does not do STAT says nothing about availability.
                    w.job.route.settle(ctx.idx);
                    w.job.on_stat(None);
                    continue;
                }
                if !matches!(c, "430" | "423" | "451") && ctx.st.odd_replies.fetch_add(1, Relaxed) < 5 {
                    eprintln!("[{}] {} for {} (treated as missing)", ctx.cfg.name, &status[..status.len().min(60)], w.job.msgid(w.file, w.seg));
                }
                ctx.st.missing.fetch_add(1, Relaxed);
                ctx.q.record(ctx.idx, false, w.tried != 0);
                w.job.route.record(ctx.idx, false);
                ctx.q.record_miss_time(ctx.idx, since.as_micros() as u64);
                if let Some(w) = ctx.q.retry_elsewhere(ctx.idx, w) {
                    w.missing(&ctx.q);
                }
            }
            _ => return Err(err(format!("unexpected: {}", &status[..status.len().min(80)]))),
        }
    }
}

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
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
pub struct SStats {
    pub bytes: AtomicU64,
    pub ok: AtomicU64,
    pub missing: AtomicU64,
    pub errors: AtomicU64,
    pub live: AtomicU64,
    pub crc_errors: AtomicU64,
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
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }
}

pub fn tls_config(insecure: bool) -> Arc<rustls::ClientConfig> {
    let cfg = if insecure {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(danger::NoVerify))
            .with_no_client_auth()
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
    let addr = (cfg.host.as_str(), cfg.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no address"))?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(15))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(90)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    if !cfg.tls {
        return Ok(Box::new(tcp));
    }
    let name = ServerName::try_from(cfg.host.clone()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let conn = rustls::ClientConnection::new(tls.clone(), name).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let sock = BigSock { s: tcp, buf: vec![0u8; 1 << 20].into_boxed_slice(), pos: 0, end: 0 };
    Ok(Box::new(rustls::StreamOwned::new(conn, sock)))
}

struct Rd {
    s: Box<dyn Stream>,
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

fn err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
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
            w.job.drop_work(w.file, w.seg, &ctx.q);
            continue;
        }
        return Some(w);
    }
}

pub fn run(ctx: ConnCtx) {
    let mut backoff = 1;
    loop {
        // Only connect when there is work.
        let Some(first) = pop_live(&ctx, Some(Duration::from_secs(5))) else {
            if ctx.q.is_closed() {
                return;
            }
            continue;
        };
        let mut inflight: VecDeque<Work> = VecDeque::new();
        inflight.push_back(first);
        match session(&ctx, &mut inflight) {
            Ok(()) => backoff = 1,
            Err(e) => {
                let n = ctx.st.errors.fetch_add(1, Relaxed);
                if n < 5 || n % 100 == 0 {
                    eprintln!("[{}] {e}", ctx.cfg.name);
                }
                ctx.q.give_back(ctx.idx, inflight.drain(..).collect());
                std::thread::sleep(Duration::from_secs(backoff));
                backoff = (backoff * 2).min(30);
            }
        }
    }
}

fn session(ctx: &ConnCtx, inflight: &mut VecDeque<Work>) -> io::Result<()> {
    let s = connect(&ctx.cfg, &ctx.tls)?;
    let mut rd = Rd { s, buf: vec![0; 8 << 20], start: 0, end: 0 };
    login(&mut rd, &ctx.cfg)?;
    ctx.st.live.fetch_add(1, Relaxed);
    struct Live<'a>(&'a AtomicU64);
    impl Drop for Live<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Relaxed);
        }
    }
    let _live = Live(&ctx.st.live);
    let fin = memmem::Finder::new(b"\r\n.\r\n");
    let mut out: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut req = String::with_capacity(4096);
    let mut sent = 0usize; // inflight[..sent] have been requested
    loop {
        // Top up the pipeline.
        while inflight.len() < ctx.depth {
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
                req.push_str("BODY ");
                req.push_str(w.job.msgid(w.file, w.seg));
                req.push_str("\r\n");
            }
            sent = inflight.len();
            send(&mut rd, &req)?;
        }
        let status = rd.line()?;
        let code = status.get(..3).unwrap_or("");
        match code {
            "222" => {
                let (s, e) = rd.block(&fin)?;
                let w = inflight.pop_front().unwrap();
                sent -= 1;
                ctx.st.bytes.fetch_add((e - s) as u64, Relaxed);
                ctx.q.record(ctx.idx, true, w.tried != 0);
                match yenc::decode(&rd.buf[s..e], &mut out) {
                    Ok((info, crc)) => {
                        ctx.st.ok.fetch_add(1, Relaxed);
                        w.job.on_article(w.file, w.seg, &info, &out, crc, &ctx.q);
                    }
                    Err(e) => {
                        if ctx.st.crc_errors.fetch_add(1, Relaxed) < 3 {
                            eprintln!("[{}] decode error {:?} for {}", ctx.cfg.name, e, w.job.msgid(w.file, w.seg));
                        }
                        if let Some(w) = ctx.q.retry_elsewhere(ctx.idx, w) {
                            w.job.on_missing(w.file, w.seg, &ctx.q);
                        }
                    }
                }
            }
            "430" | "423" | "451" => {
                let w = inflight.pop_front().unwrap();
                sent -= 1;
                ctx.st.missing.fetch_add(1, Relaxed);
                ctx.q.record(ctx.idx, false, w.tried != 0);
                if let Some(w) = ctx.q.retry_elsewhere(ctx.idx, w) {
                    w.job.on_missing(w.file, w.seg, &ctx.q);
                }
            }
            _ => return Err(err(format!("unexpected: {}", &status[..status.len().min(80)]))),
        }
    }
}

/// Debug helper: fetches one article body (raw, as sent by the server).
pub fn fetch_raw(cfg: &ServerCfg, msgid: &str) -> io::Result<Vec<u8>> {
    let tls = tls_config(cfg.insecure);
    let s = connect(cfg, &tls)?;
    let mut rd = Rd { s, buf: vec![0; 8 << 20], start: 0, end: 0 };
    login(&mut rd, cfg)?;
    send(&mut rd, &format!("BODY {msgid}\r\n"))?;
    let status = rd.line()?;
    if !status.starts_with("222") {
        return Err(err(format!("status: {status}")));
    }
    let fin = memmem::Finder::new(b"\r\n.\r\n");
    let (a, b) = rd.block(&fin)?;
    Ok(rd.buf[a..b].to_vec())
}

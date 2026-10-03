//! Minimal HTTP/1.1 server (one thread per connection; the clients are a few
//! *arr apps and browsers) and a small client for `addurl`.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

const MAX_HEADER: usize = 64 << 10;
const MAX_BODY: usize = 256 << 20;
const MAX_CONNS: usize = 512;

pub struct Request {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    /// Header names are lowercase.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .filter(|(k, _)| k == "cookie")
            .flat_map(|(_, v)| v.split(';'))
            .filter_map(|c| c.trim().split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v)
    }
}

pub type Streamer = Box<dyn FnOnce(&mut dyn Write) + Send>;

pub enum Body {
    Bytes(Vec<u8>),
    /// Written after the headers; the connection closes afterwards (used for SSE).
    Stream(Streamer),
}

pub struct Response {
    pub status: u16,
    pub ctype: String,
    pub headers: Vec<(String, String)>,
    pub body: Body,
}

impl Response {
    pub fn new(status: u16, ctype: &str, body: Vec<u8>) -> Response {
        Response { status, ctype: ctype.into(), headers: vec![], body: Body::Bytes(body) }
    }

    pub fn json(v: &serde_json::Value) -> Response {
        Response::new(200, "application/json; charset=utf-8", serde_json::to_vec(v).unwrap_or_default())
    }

    pub fn text(status: u16, s: &str) -> Response {
        Response::new(status, "text/plain; charset=utf-8", s.as_bytes().to_vec())
    }

    pub fn with_header(mut self, k: &str, v: &str) -> Response {
        self.headers.push((k.into(), v.into()));
        self
    }
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    }
}

pub fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16) {
                Ok(v) => {
                    out.push(v);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn parse_form(s: &str) -> Vec<(String, String)> {
    s.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (url_decode(k), url_decode(v))
        })
        .collect()
}

pub struct Part {
    pub name: String,
    pub filename: Option<String>,
    pub data: Vec<u8>,
}

fn disposition_param(h: &str, key: &str) -> Option<String> {
    for p in h.split(';').skip(1) {
        let (k, v) = p.trim().split_once('=')?;
        if k.trim().eq_ignore_ascii_case(key) {
            let v = v.trim();
            return Some(if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') { v[1..v.len() - 1].replace("\\\"", "\"") } else { v.to_string() });
        }
    }
    None
}

/// Parses a multipart/form-data body.
pub fn multipart(body: &[u8], ctype: &str) -> Vec<Part> {
    let Some(b) = ctype.split(';').filter_map(|p| p.trim().strip_prefix("boundary=")).next() else {
        return vec![];
    };
    let b = b.trim_matches('"');
    let delim = format!("--{b}");
    let next_delim = format!("\r\n--{b}");
    let fin = memchr::memmem::Finder::new(next_delim.as_bytes());
    let Some(mut pos) = memchr::memmem::find(body, delim.as_bytes()).map(|p| p + delim.len()) else {
        return vec![];
    };
    let mut parts = vec![];
    loop {
        if body[pos..].starts_with(b"--") {
            break;
        }
        // Skip the CRLF after the delimiter.
        let Some(he) = memchr::memmem::find(&body[pos..], b"\r\n\r\n") else {
            break;
        };
        let head = String::from_utf8_lossy(&body[pos..pos + he]).into_owned();
        let ds = pos + he + 4;
        let Some(de) = fin.find(&body[ds..]).map(|e| ds + e) else {
            break;
        };
        let mut name = String::new();
        let mut filename = None;
        for line in head.split("\r\n") {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("content-disposition") {
                    name = disposition_param(v, "name").unwrap_or_default();
                    filename = disposition_param(v, "filename");
                }
            }
        }
        parts.push(Part { name, filename, data: body[ds..de].to_vec() });
        pos = de + next_delim.len();
    }
    parts
}

fn read_more(s: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<usize> {
    let old = buf.len();
    buf.resize(old + 65536, 0);
    let n = s.read(&mut buf[old..]);
    buf.truncate(old + *n.as_ref().unwrap_or(&0));
    n
}

fn read_body(s: &mut TcpStream, buf: &mut Vec<u8>, start: usize, headers: &[(String, String)]) -> io::Result<(Vec<u8>, usize)> {
    let h = |n: &str| headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.as_str());
    if h("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked")) {
        let mut body = vec![];
        let mut p = start;
        loop {
            let le = loop {
                if let Some(i) = memchr::memmem::find(&buf[p..], b"\r\n") {
                    break p + i;
                }
                if read_more(s, buf)? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            };
            let size_s = String::from_utf8_lossy(&buf[p..le]);
            let size = usize::from_str_radix(size_s.split(';').next().unwrap_or("").trim(), 16).map_err(|_| io::Error::other("bad chunk size"))?;
            p = le + 2;
            if size == 0 {
                // Trailers end with an empty line.
                loop {
                    if let Some(i) = memchr::memmem::find(&buf[p..], b"\r\n") {
                        let empty = i == 0;
                        p += i + 2;
                        if empty {
                            return Ok((body, p));
                        }
                        continue;
                    }
                    if read_more(s, buf)? == 0 {
                        return Ok((body, buf.len()));
                    }
                }
            }
            if body.len() + size > MAX_BODY {
                return Err(io::Error::other("body too large"));
            }
            while buf.len() < p + size + 2 {
                if read_more(s, buf)? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            body.extend_from_slice(&buf[p..p + size]);
            p += size + 2;
        }
    }
    let len: usize = h("content-length").and_then(|v| v.trim().parse().ok()).unwrap_or(0);
    if len > MAX_BODY {
        return Err(io::Error::other("body too large"));
    }
    while buf.len() < start + len {
        if read_more(s, buf)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok((buf[start..start + len].to_vec(), start + len))
}

pub type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

pub fn serve(addr: &str, h: Handler) -> io::Result<()> {
    let l = TcpListener::bind(addr)?;
    let live = Arc::new(AtomicUsize::new(0));
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(s) = s else { continue };
            if live.load(Relaxed) >= MAX_CONNS {
                continue;
            }
            let (h, live) = (h.clone(), live.clone());
            live.fetch_add(1, Relaxed);
            let _ = std::thread::Builder::new().stack_size(512 << 10).spawn(move || {
                let _ = connection(s, &h);
                live.fetch_sub(1, Relaxed);
            });
        }
    });
    Ok(())
}

fn connection(mut s: TcpStream, h: &Handler) -> io::Result<()> {
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(120)))?;
    s.set_write_timeout(Some(Duration::from_secs(60)))?;
    let mut buf: Vec<u8> = Vec::with_capacity(16384);
    loop {
        let he = loop {
            if let Some(i) = memchr::memmem::find(&buf, b"\r\n\r\n") {
                break i;
            }
            if buf.len() > MAX_HEADER {
                return write_simple(&mut s, 431);
            }
            if read_more(&mut s, &mut buf)? == 0 {
                return Ok(());
            }
        };
        let head = String::from_utf8_lossy(&buf[..he]).into_owned();
        let mut lines = head.split("\r\n");
        let rl = lines.next().unwrap_or("");
        let mut it = rl.split(' ');
        let (method, target, version) = (it.next().unwrap_or(""), it.next().unwrap_or("/"), it.next().unwrap_or("HTTP/1.0"));
        let headers: Vec<(String, String)> =
            lines.filter_map(|l| l.split_once(':')).map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())).collect();
        let hv = |n: &str| headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.to_ascii_lowercase());
        if hv("expect").is_some_and(|v| v == "100-continue") {
            s.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        }
        let (body, consumed) = match read_body(&mut s, &mut buf, he + 4, &headers) {
            Ok(v) => v,
            Err(e) if e.to_string().contains("too large") => return write_simple(&mut s, 413),
            Err(e) => return Err(e),
        };
        buf.drain(..consumed.min(buf.len()));
        let keep = match hv("connection") {
            Some(c) if c.contains("close") => false,
            Some(c) if c.contains("keep-alive") => true,
            _ => version == "HTTP/1.1",
        };
        let (path, qs) = target.split_once('?').unwrap_or((target, ""));
        let req = Request { method: method.to_string(), path: url_decode(path), query: parse_form(qs), headers, body };
        let head_only = req.method == "HEAD";
        let resp = h(req);
        let mut out = format!("HTTP/1.1 {} {}\r\nContent-Type: {}\r\n", resp.status, reason(resp.status), resp.ctype);
        for (k, v) in &resp.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        match resp.body {
            Body::Bytes(b) => {
                out.push_str(&format!("Content-Length: {}\r\nConnection: {}\r\n\r\n", b.len(), if keep { "keep-alive" } else { "close" }));
                let mut w = out.into_bytes();
                if !head_only {
                    w.extend_from_slice(&b);
                }
                s.write_all(&w)?;
                if !keep {
                    return Ok(());
                }
            }
            Body::Stream(f) => {
                out.push_str("Cache-Control: no-cache\r\nConnection: close\r\nX-Accel-Buffering: no\r\n\r\n");
                s.write_all(out.as_bytes())?;
                s.set_write_timeout(Some(Duration::from_secs(10)))?;
                f(&mut s);
                return Ok(());
            }
        }
    }
}

fn write_simple(s: &mut TcpStream, code: u16) -> io::Result<()> {
    let r = reason(code);
    s.write_all(format!("HTTP/1.1 {code} {r}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{r}", r.len()).as_bytes())
}

// ---------- client ----------

pub struct Fetched {
    pub data: Vec<u8>,
    /// File name from Content-Disposition, if any.
    pub filename: Option<String>,
}

/// GET with redirects, chunked transfer and gzip support (used by `addurl`).
pub fn get(url: &str) -> Result<Fetched, String> {
    let mut url = url.to_string();
    for _ in 0..6 {
        let (scheme, rest) = url.split_once("://").ok_or("bad url")?;
        let tls = match scheme.to_ascii_lowercase().as_str() {
            "http" => false,
            "https" => true,
            _ => return Err(format!("unsupported scheme {scheme}")),
        };
        let (hostport, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) if !h.contains(']') || h.ends_with(']') => (h.trim_matches(['[', ']']), p.parse().map_err(|_| "bad port")?),
            _ => (hostport, if tls { 443 } else { 80 }),
        };
        let addr = (host, port).to_socket_addrs().map_err(|e| e.to_string())?.next().ok_or("no address")?;
        let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(15)).map_err(|e| e.to_string())?;
        tcp.set_read_timeout(Some(Duration::from_secs(60))).ok();
        let req = format!("GET {path} HTTP/1.1\r\nHost: {hostport}\r\nUser-Agent: nzbfast\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n");
        let mut raw = vec![];
        if tls {
            let name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| e.to_string())?;
            let conn = rustls::ClientConnection::new(crate::conn::tls_config(false), name).map_err(|e| e.to_string())?;
            let mut st = rustls::StreamOwned::new(conn, tcp);
            st.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
            read_all_lenient(&mut st, &mut raw)?;
        } else {
            let mut st = tcp;
            st.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
            read_all_lenient(&mut st, &mut raw)?;
        }
        let he = memchr::memmem::find(&raw, b"\r\n\r\n").ok_or("bad response")?;
        let head = String::from_utf8_lossy(&raw[..he]).into_owned();
        let mut lines = head.split("\r\n");
        let status: u16 = lines.next().and_then(|l| l.split(' ').nth(1)).and_then(|c| c.parse().ok()).ok_or("bad status")?;
        let headers: Vec<(String, String)> =
            lines.filter_map(|l| l.split_once(':')).map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())).collect();
        let h = |n: &str| headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
        if (300..400).contains(&status) {
            let loc = h("location").ok_or("redirect without location")?;
            url = if loc.contains("://") {
                loc
            } else if loc.starts_with('/') {
                format!("{scheme}://{hostport}{loc}")
            } else {
                format!("{scheme}://{hostport}/{loc}")
            };
            continue;
        }
        if status != 200 {
            return Err(format!("HTTP {status}"));
        }
        let mut body = raw[he + 4..].to_vec();
        if h("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked")) {
            body = dechunk(&body)?;
        }
        if h("content-encoding").is_some_and(|v| v.contains("gzip")) || body.starts_with(&[0x1f, 0x8b]) {
            let mut d = vec![];
            flate2::read::GzDecoder::new(&body[..]).read_to_end(&mut d).map_err(|e| e.to_string())?;
            body = d;
        }
        let filename = h("content-disposition").and_then(|v| disposition_param(&format!("x;{}", v.split_once(';').map(|x| x.1).unwrap_or("")), "filename"));
        return Ok(Fetched { data: body, filename });
    }
    Err("too many redirects".into())
}

fn read_all_lenient<R: Read>(r: &mut R, out: &mut Vec<u8>) -> Result<(), String> {
    let mut b = [0u8; 65536];
    loop {
        match r.read(&mut b) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                out.extend_from_slice(&b[..n]);
                if out.len() > MAX_BODY {
                    return Err("response too large".into());
                }
            }
            // Servers that close without TLS close_notify.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && !out.is_empty() => return Ok(()),
            Err(e) => return Err(e.to_string()),
        }
    }
}

fn dechunk(b: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = vec![];
    let mut p = 0;
    loop {
        let le = p + memchr::memmem::find(&b[p..], b"\r\n").ok_or("bad chunk")?;
        let size = usize::from_str_radix(String::from_utf8_lossy(&b[p..le]).split(';').next().unwrap_or("").trim(), 16).map_err(|_| "bad chunk size")?;
        p = le + 2;
        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(b.get(p..p + size).ok_or("truncated chunk")?);
        p += size + 2;
    }
}

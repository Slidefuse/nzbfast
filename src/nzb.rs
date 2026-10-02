//! Minimal, fast NZB parser (plain or gzip-compressed).

use std::io::Read;

#[derive(Clone, Debug)]
pub struct NzbSeg {
    pub number: u32,
    pub bytes: u32,
    pub msgid: Box<str>,
}

#[derive(Debug)]
pub struct NzbFile {
    pub subject: String,
    pub segs: Vec<NzbSeg>,
}

#[derive(Debug)]
pub struct Nzb {
    pub name: String,
    pub files: Vec<NzbFile>,
    /// Archive password from `<head><meta type="password">`.
    pub password: Option<String>,
}

pub fn xml_unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(j) = rest.find(';') else { break };
        let ent = &rest[1..j];
        match ent {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ if ent.starts_with("#x") => {
                if let Some(c) = u32::from_str_radix(&ent[2..], 16).ok().and_then(char::from_u32) {
                    out.push(c)
                }
            }
            _ if ent.starts_with('#') => {
                if let Some(c) = ent[1..].parse().ok().and_then(char::from_u32) {
                    out.push(c)
                }
            }
            _ => out.push_str(&rest[..=j]),
        }
        rest = &rest[j + 1..];
    }
    out.push_str(rest);
    out
}

fn attr<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let mut from = 0;
    while let Some(i) = tag[from..].find(name) {
        let p = from + i;
        let before_ok = p == 0 || tag.as_bytes()[p - 1].is_ascii_whitespace();
        let rest = &tag[p + name.len()..];
        let rest_trim = rest.trim_start();
        if before_ok && rest_trim.starts_with('=') {
            let v = rest_trim[1..].trim_start();
            let q = v.chars().next()?;
            if q == '"' || q == '\'' {
                let end = v[1..].find(q)?;
                return Some(&v[1..1 + end]);
            }
        }
        from = p + name.len();
    }
    None
}

pub fn parse(text: &str, name: String) -> Result<Nzb, String> {
    let mut files = vec![];
    let mut pos = 0;
    while let Some(i) = text[pos..].find("<file") {
        let start = pos + i;
        let tag_end = start + text[start..].find('>').ok_or("unterminated <file>")?;
        let tag = &text[start..tag_end];
        let subject = xml_unescape(attr(tag, "subject").unwrap_or(""));
        let close = tag_end + text[tag_end..].find("</file>").ok_or("missing </file>")?;
        let body = &text[tag_end..close];
        let mut segs = vec![];
        let mut sp = 0;
        while let Some(j) = body[sp..].find("<segment") {
            let s0 = sp + j;
            let te = s0 + body[s0..].find('>').ok_or("unterminated <segment>")?;
            let stag = &body[s0..te];
            if body[s0..].starts_with("<segments") {
                sp = te;
                continue;
            }
            let ce = te + body[te..].find("</segment>").ok_or("missing </segment>")?;
            let id = xml_unescape(body[te + 1..ce].trim());
            let number = attr(stag, "number").and_then(|v| v.parse().ok()).unwrap_or(0);
            let bytes = attr(stag, "bytes").and_then(|v| v.parse().ok()).unwrap_or(0);
            if !id.is_empty() {
                let id = if id.starts_with('<') { id } else { format!("<{id}>") };
                segs.push(NzbSeg { number, bytes, msgid: id.into_boxed_str() });
            }
            sp = ce;
        }
        segs.sort_by_key(|s| s.number);
        segs.dedup_by_key(|s| s.number);
        if !segs.is_empty() {
            files.push(NzbFile { subject, segs });
        }
        pos = close;
    }
    if files.is_empty() {
        return Err("NZB has no files".into());
    }
    Ok(Nzb { name, files, password: head_meta(text, "password") })
}

/// Value of `<meta type="KIND">value</meta>` inside the NZB `<head>`.
fn head_meta(text: &str, kind: &str) -> Option<String> {
    let h = text.find("<head")?;
    let end = h + text[h..].find("</head>")?;
    let head = &text[h..end];
    let mut pos = 0;
    while let Some(i) = head[pos..].find("<meta") {
        let s = pos + i;
        let te = s + head[s..].find('>')?;
        let ce = te + head[te..].find("</meta>")?;
        if attr(&head[s..te], "type").is_some_and(|t| t.eq_ignore_ascii_case(kind)) {
            let v = xml_unescape(head[te + 1..ce].trim());
            return (!v.is_empty()).then_some(v);
        }
        pos = ce;
    }
    None
}

pub fn load(path: &str) -> Result<Nzb, String> {
    let raw = std::fs::read(path).map_err(|e| format!("{path}: {e}"))?;
    let data = if raw.starts_with(&[0x1f, 0x8b]) {
        let mut s = Vec::new();
        flate2::read::GzDecoder::new(&raw[..]).read_to_end(&mut s).map_err(|e| format!("{path}: {e}"))?;
        s
    } else {
        raw
    };
    let text = String::from_utf8_lossy(&data);
    let base = std::path::Path::new(path).file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let name = base.trim_end_matches(".gz").trim_end_matches(".nzb").to_string();
    parse(&text, name)
}

/// Best-effort filename from a subject line: the first `"quoted"` part, else the subject.
pub fn subject_filename(subject: &str) -> String {
    if let Some(a) = subject.find('"') {
        if let Some(b) = subject[a + 1..].find('"') {
            return subject[a + 1..a + 1 + b].to_string();
        }
    }
    subject.to_string()
}

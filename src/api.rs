//! HTTP routes: the SABnzbd-compatible API (what Sonarr, Radarr, Prowlarr and
//! friends speak), the web UI page and its 10 Hz event stream.

use crate::engine::{self, Engine, PRIO_FORCE, PRIO_HIGH, PRIO_LOW, PRIO_NORMAL, PRIO_PAUSED};
use crate::http::{self, Body, Request, Response};
use crate::stats::{self, Hub};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, OnceLock};

/// Version reported to clients; Sonarr/Radarr require a modern SABnzbd.
pub const SAB_VERSION: &str = "4.3.3";

const UI_HTML: &str = include_str!("ui.html");
const UI_COOKIE: &str = "nzbfast_ui";

/// Link speed of the NIC in bytes/s (10 Gbit/s if unknown).
pub fn line_speed(nic: &str) -> u64 {
    let mbit: u64 = std::fs::read_to_string(format!("/sys/class/net/{nic}/speed")).ok().and_then(|s| s.trim().parse().ok()).filter(|v| *v > 0).unwrap_or(10_000);
    mbit * 125_000
}

fn ui_token(eng: &Engine) -> String {
    let d = Md5::digest(format!("{}:nzbfast-ui", eng.cfg.api_key).as_bytes());
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn ui_authed(eng: &Engine, req: &Request) -> bool {
    !eng.cfg.ui_auth || req.cookie(UI_COOKIE).is_some_and(|c| c == ui_token(eng))
}

fn cookie_header(eng: &Engine) -> String {
    format!("{UI_COOKIE}={}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000", ui_token(eng))
}

pub fn handler(eng: Arc<Engine>, hub: Arc<Hub>) -> http::Handler {
    Arc::new(move |req: Request| route(&eng, &hub, req))
}

fn route(eng: &Arc<Engine>, hub: &Arc<Hub>, req: Request) -> Response {
    let path = req.path.trim_end_matches('/');
    if path.ends_with("/api") || path == "api" {
        // Browser calls from the UI carry the session cookie plus a custom header
        // (which cross-site pages cannot send without CORS approval).
        let ui = req.header("x-nzbfast-ui").is_some() && ui_authed(eng, &req);
        return sab(eng, &req, ui);
    }
    match path {
        "" | "/index.html" | "/sabnzbd" => {
            let key = req.query.iter().find(|(k, _)| k == "apikey").map(|(_, v)| v.as_str());
            if key.is_some_and(|k| k == eng.cfg.api_key) {
                return Response::new(302, "text/plain", vec![]).with_header("Location", "/").with_header("Set-Cookie", &cookie_header(eng));
            }
            ui_page(&req)
        }
        "/ui/login" => {
            let form = http::parse_form(&String::from_utf8_lossy(&req.body));
            let key = form.iter().find(|(k, _)| k == "apikey").map(|(_, v)| v.as_str()).unwrap_or("");
            if req.method == "POST" && !eng.cfg.api_key.is_empty() && key == eng.cfg.api_key {
                Response::json(&json!({"ok": true})).with_header("Set-Cookie", &cookie_header(eng))
            } else {
                std::thread::sleep(std::time::Duration::from_millis(500));
                Response::new(401, "application/json", br#"{"ok":false}"#.to_vec())
            }
        }
        "/ui/session" => Response::json(&json!({"auth": ui_authed(eng, &req), "version": env!("CARGO_PKG_VERSION")})),
        "/events" => {
            if !ui_authed(eng, &req) {
                return Response::text(401, "unauthorized");
            }
            let (eng, hub) = (eng.clone(), hub.clone());
            Response { status: 200, ctype: "text/event-stream".into(), headers: vec![], body: Body::Stream(Box::new(move |w| hub.stream(&eng, w))) }
        }
        "/favicon.ico" => Response::new(204, "image/x-icon", vec![]),
        _ => Response::text(404, "not found"),
    }
}

fn ui_page(req: &Request) -> Response {
    static GZ: OnceLock<Vec<u8>> = OnceLock::new();
    let gz_ok = req.header("accept-encoding").is_some_and(|v| v.contains("gzip"));
    let r = if gz_ok {
        let body = GZ.get_or_init(|| {
            use std::io::Write;
            let mut e = flate2::write::GzEncoder::new(vec![], flate2::Compression::best());
            let _ = e.write_all(UI_HTML.as_bytes());
            e.finish().unwrap_or_default()
        });
        Response::new(200, "text/html; charset=utf-8", body.clone()).with_header("Content-Encoding", "gzip")
    } else {
        Response::new(200, "text/html; charset=utf-8", UI_HTML.as_bytes().to_vec())
    };
    r.with_header("Cache-Control", "no-cache").with_header("X-Frame-Options", "DENY").with_header("Referrer-Policy", "no-referrer")
}

// ---------- formatting helpers (SABnzbd conventions) ----------

fn fmt_bytes(b: f64) -> String {
    const U: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut v = b;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{v:.0} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

fn fmt_speed(b: f64) -> String {
    // SABnzbd style: "12.3 M" (per second, 1024-based).
    let mut v = b;
    for u in ["", "K", "M", "G", "T"] {
        if v < 1024.0 {
            return format!("{v:.1} {u}").trim_end().to_string();
        }
        v /= 1024.0;
    }
    format!("{v:.1} P")
}

fn fmt_left(secs: u64) -> String {
    format!("{}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

fn mb(b: u64) -> String {
    format!("{:.2}", b as f64 / 1048576.0)
}

fn prio_name(p: i32) -> &'static str {
    match p {
        PRIO_FORCE => "Force",
        PRIO_HIGH => "High",
        PRIO_LOW => "Low",
        _ => "Normal",
    }
}

fn parse_prio(v: &str) -> Option<i32> {
    match v.trim().to_ascii_lowercase().as_str() {
        "force" => Some(PRIO_FORCE),
        "high" => Some(PRIO_HIGH),
        "normal" => Some(PRIO_NORMAL),
        "low" => Some(PRIO_LOW),
        "paused" | "stop" => Some(PRIO_PAUSED),
        "default" => Some(-100),
        s => s.parse().ok(),
    }
}

fn age(added: u64) -> String {
    let s = engine::unix_now().saturating_sub(added);
    if s >= 86400 {
        format!("{}d", s / 86400)
    } else if s >= 3600 {
        format!("{}h", s / 3600)
    } else {
        format!("{}m", s / 60)
    }
}

fn err(msg: &str) -> Response {
    Response::json(&json!({"status": false, "error": msg}))
}

fn ok() -> Response {
    Response::json(&json!({"status": true}))
}

/// Request parameters: query string, url-encoded body and multipart fields.
struct Params {
    kv: Vec<(String, String)>,
    files: Vec<http::Part>,
}

impl Params {
    fn get(&self, k: &str) -> Option<&str> {
        self.kv.iter().find(|(a, _)| a == k).map(|(_, v)| v.as_str())
    }

    fn ids(&self, k: &str) -> Vec<String> {
        self.get(k).map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default()
    }

    fn num(&self, k: &str) -> Option<i64> {
        self.get(k).and_then(|v| v.trim().parse().ok())
    }
}

fn params(req: &Request) -> Params {
    let mut kv = req.query.clone();
    let mut files = vec![];
    let ct = req.header("content-type").unwrap_or("");
    if ct.to_ascii_lowercase().starts_with("multipart/form-data") {
        for p in http::multipart(&req.body, ct) {
            if p.filename.is_some() || p.name == "nzbfile" {
                files.push(p);
            } else {
                kv.push((p.name, String::from_utf8_lossy(&p.data).into_owned()));
            }
        }
    } else if ct.to_ascii_lowercase().starts_with("application/x-www-form-urlencoded") {
        kv.extend(http::parse_form(&String::from_utf8_lossy(&req.body)));
    }
    Params { kv, files }
}

fn sab(eng: &Arc<Engine>, req: &Request, ui: bool) -> Response {
    let p = params(req);
    let mode = p.get("mode").unwrap_or("").to_string();
    match mode.as_str() {
        "version" => return Response::json(&json!({"version": SAB_VERSION})),
        "auth" => return Response::json(&json!({"auth": "apikey"})),
        _ => {}
    }
    let key = p.get("apikey").or(req.header("x-api-key"));
    let full = ui || key.is_some_and(|k| !eng.cfg.api_key.is_empty() && k == eng.cfg.api_key);
    if !full {
        let add_only = key.is_some_and(|k| !eng.cfg.nzb_key.is_empty() && k == eng.cfg.nzb_key);
        if !(add_only && matches!(mode.as_str(), "addfile" | "addurl")) {
            return err(if key.is_none() { "API Key Required" } else { "API Key Incorrect" });
        }
    }
    let name = p.get("name").unwrap_or("");
    match mode.as_str() {
        "queue" => match name {
            "" => queue_list(eng, &p),
            "delete" => {
                let ids = eng.delete_queue(&p.ids("value"));
                Response::json(&json!({"status": true, "nzo_ids": ids}))
            }
            "purge" => {
                let ids = eng.delete_queue(&["all".to_string()]);
                Response::json(&json!({"status": true, "nzo_ids": ids}))
            }
            "pause" => {
                let ids = eng.pause_jobs(&p.ids("value"), true);
                Response::json(&json!({"status": true, "nzo_ids": ids}))
            }
            "resume" => {
                let ids = eng.pause_jobs(&p.ids("value"), false);
                Response::json(&json!({"status": true, "nzo_ids": ids}))
            }
            "priority" => match p.get("value2").and_then(parse_prio) {
                Some(pr) => {
                    let pr = if pr == -100 { PRIO_NORMAL } else { pr };
                    match eng.set_priority(&p.ids("value"), pr) {
                        Some(pos) => Response::json(&json!({"position": pos})),
                        None => Response::json(&json!({"position": -1})),
                    }
                }
                None => err("invalid priority"),
            },
            "rename" => {
                if eng.rename(p.get("value").unwrap_or(""), p.get("value2").unwrap_or("")) {
                    ok()
                } else {
                    err("cannot rename (unknown or already downloading)")
                }
            }
            "sort" | "change_complete_action" => ok(),
            _ => err("not implemented"),
        },
        "history" => match name {
            "" => history_list(eng, &p),
            "delete" => {
                let ids = eng.delete_history(&p.ids("value"), p.get("del_files") == Some("1"));
                Response::json(&json!({"status": true, "nzo_ids": ids}))
            }
            _ => err("not implemented"),
        },
        "addfile" | "addlocalfile" | "addurl" => add(eng, &mode, &p),
        "pause" => {
            eng.set_paused(true);
            ok()
        }
        "resume" => {
            eng.set_paused(false);
            ok()
        }
        "switch" => {
            let to = p.num("value2").unwrap_or(0).max(0) as usize;
            match eng.move_job(p.get("value").unwrap_or(""), to) {
                Some((pos, pr)) => Response::json(&json!({"result": {"position": pos, "priority": pr}})),
                None => Response::json(&json!({"result": {"position": -1, "priority": 0}})),
            }
        }
        "change_cat" => {
            eng.change_cat(&p.ids("value"), p.get("value2").unwrap_or("*"));
            ok()
        }
        "retry" => {
            let id = p.get("value").unwrap_or("");
            if eng.retry(id) {
                Response::json(&json!({"status": true, "nzo_id": id}))
            } else {
                err("cannot retry")
            }
        }
        "config" => match name {
            "speedlimit" => {
                let v = p.get("value").unwrap_or("").trim().to_ascii_uppercase();
                let line = line_speed(&eng.cfg.nic);
                let bytes = if let Some(n) = v.strip_suffix('K') {
                    n.trim().parse::<f64>().map(|x| x * 1024.0).unwrap_or(0.0) as u64
                } else if let Some(n) = v.strip_suffix('M') {
                    n.trim().parse::<f64>().map(|x| x * 1048576.0).unwrap_or(0.0) as u64
                } else if let Some(n) = v.strip_suffix('G') {
                    n.trim().parse::<f64>().map(|x| x * 1073741824.0).unwrap_or(0.0) as u64
                } else {
                    // Plain number: percentage of the line speed.
                    match v.parse::<f64>() {
                        Ok(pct) if pct > 0.0 && pct < 100.0 => (line as f64 * pct / 100.0) as u64,
                        _ => 0,
                    }
                };
                eng.set_limit(bytes);
                ok()
            }
            _ => ok(),
        },
        "get_config" => get_config(eng, &p),
        "fullstatus" | "status" => full_status(eng),
        "server_stats" => server_stats(eng),
        "get_cats" => Response::json(&json!({"categories": eng.cats.iter().map(|c| c.name.clone()).collect::<Vec<_>>()})),
        "get_scripts" => Response::json(&json!({"scripts": ["None"]})),
        "warnings" => {
            let w: Vec<String> = eng
                .stats
                .iter()
                .enumerate()
                .filter(|(i, _)| eng.q.is_down(*i))
                .map(|(i, s)| format!("{} unreachable: {}", eng.servers[i].name, s.last_error.lock().unwrap()))
                .collect();
            Response::json(&json!({"warnings": w}))
        }
        "get_files" => get_files(eng, &p),
        "shutdown" | "restart" | "set_config" | "set_config_default" | "rss_now" | "watched_now" | "reset_quota" => ok(),
        _ => err("not implemented"),
    }
}

fn add(eng: &Engine, mode: &str, p: &Params) -> Response {
    let cat = p.get("cat");
    let prio = p.get("priority").and_then(parse_prio);
    let nzbname = p.get("nzbname");
    let mut ids = vec![];
    let mut errors = vec![];
    match mode {
        "addfile" => {
            if p.files.is_empty() {
                return err("no file given");
            }
            for f in &p.files {
                let fname = f.filename.clone().unwrap_or_else(|| "upload.nzb".into());
                match eng.add_nzb(&f.data, &fname, nzbname, cat, prio) {
                    Ok(id) => ids.push(id),
                    Err(e) => errors.push(e),
                }
            }
        }
        "addlocalfile" => {
            let path = p.get("name").unwrap_or("");
            match std::fs::read(path) {
                Ok(data) => match eng.add_nzb(&data, path, nzbname, cat, prio) {
                    Ok(id) => ids.push(id),
                    Err(e) => errors.push(e),
                },
                Err(e) => errors.push(format!("{path}: {e}")),
            }
        }
        _ => {
            let url = p.get("name").unwrap_or("");
            match http::get(url) {
                Ok(f) => {
                    let fname = f.filename.unwrap_or_else(|| {
                        let last = url.split('?').next().unwrap_or("").rsplit('/').next().unwrap_or("download");
                        http::url_decode(last)
                    });
                    match eng.add_nzb(&f.data, &fname, nzbname, cat, prio) {
                        Ok(id) => ids.push(id),
                        Err(e) => errors.push(e),
                    }
                }
                Err(e) => errors.push(format!("fetching {url}: {e}")),
            }
        }
    }
    if ids.is_empty() {
        return Response::json(&json!({"status": false, "nzo_ids": [], "error": errors.join("; ")}));
    }
    Response::json(&json!({"status": true, "nzo_ids": ids}))
}

fn queue_list(eng: &Engine, p: &Params) -> Response {
    let start = p.num("start").unwrap_or(0).max(0) as usize;
    let limit = p.num("limit").unwrap_or(0).max(0) as usize;
    let cat = p.get("cat").or(p.get("category")).filter(|c| !c.is_empty());
    let search = p.get("search").filter(|s| !s.is_empty()).map(|s| s.to_lowercase());
    let only: Vec<String> = p.ids("nzo_ids");
    let rate = eng.rate5.load(Relaxed) as f64;
    let paused = eng.paused.load(Relaxed);
    let st = eng.store.lock().unwrap();
    let (mut total_b, mut left_b) = (0u64, 0u64);
    let mut cum = 0u64;
    let mut slots = vec![];
    let mut matched = 0usize;
    for (i, e) in st.queue.iter().enumerate() {
        let (t, d) = e.progress();
        total_b += t;
        left_b += t - d;
        if !e.m.paused {
            cum += t - d;
        }
        if cat.is_some_and(|c| !e.m.cat.eq_ignore_ascii_case(c))
            || search.as_ref().is_some_and(|s| !e.m.name.to_lowercase().contains(s))
            || (!only.is_empty() && !only.contains(&e.m.nzo))
        {
            continue;
        }
        matched += 1;
        if matched <= start || (limit > 0 && slots.len() >= limit) {
            continue;
        }
        let status = if e.m.paused {
            "Paused"
        } else if let Some(r) = &e.run {
            if r.job.phase.load(Relaxed) == crate::job::PH_PAR2 {
                "Fetching"
            } else {
                "Downloading"
            }
        } else {
            "Queued"
        };
        let tl = if rate > 0.0 && !e.m.paused && !paused { fmt_left((cum as f64 / rate) as u64) } else { "0:00:00".into() };
        let missing = e.run.as_ref().map(|r| r.job.missing.load(Relaxed)).unwrap_or(0);
        let phase = e.run.as_ref().map(|r| stats::phase_name(r.job.phase.load(Relaxed))).unwrap_or("queued");
        slots.push(json!({
            "index": i,
            "nzo_id": e.m.nzo,
            "unpackopts": "3",
            "priority": prio_name(e.m.prio),
            "script": "None",
            "filename": e.m.name,
            "labels": [],
            "password": "",
            "cat": e.m.cat,
            "mbleft": mb(t - d),
            "mb": mb(t),
            "size": fmt_bytes(t as f64),
            "sizeleft": fmt_bytes((t - d) as f64),
            "percentage": format!("{}", (d * 100).checked_div(t).unwrap_or(0)),
            "mbmissing": "0.0",
            "direct_unpack": null,
            "status": status,
            "timeleft": tl,
            "avg_age": age(e.m.added),
            "time_added": e.m.added,
            "nzbfast_phase": phase,
            "nzbfast_missing_articles": missing,
        }));
    }
    let n = st.queue.len();
    drop(st);
    let (free, total) = disk(&eng.cfg.complete_dir);
    let status = if paused {
        "Paused"
    } else if n == 0 {
        "Idle"
    } else {
        "Downloading"
    };
    let lim = crate::conn::LIMIT.rate.load(Relaxed);
    let line = line_speed(&eng.cfg.nic);
    Response::json(&json!({"queue": {
        "version": SAB_VERSION,
        "paused": paused,
        "paused_all": paused,
        "pause_int": "0",
        "status": status,
        "speedlimit": if lim == 0 { "100".to_string() } else { format!("{}", (lim * 100 / line.max(1)).clamp(1, 100)) },
        "speedlimit_abs": if lim == 0 { String::new() } else { lim.to_string() },
        "have_warnings": "0",
        "finishaction": null,
        "quota": "0 ",
        "have_quota": false,
        "left_quota": "0 ",
        "cache_art": "0",
        "cache_size": "0 B",
        "kbpersec": format!("{:.2}", rate / 1024.0),
        "speed": fmt_speed(rate),
        "mbleft": mb(left_b),
        "mb": mb(total_b),
        "sizeleft": fmt_bytes(left_b as f64),
        "size": fmt_bytes(total_b as f64),
        "noofslots_total": n,
        "noofslots": matched,
        "start": start,
        "limit": limit,
        "finish": 0,
        "timeleft": if rate > 0.0 { fmt_left((left_b as f64 / rate) as u64) } else { "0:00:00".into() },
        "diskspace1": format!("{:.2}", free as f64 / 1073741824.0),
        "diskspace2": format!("{:.2}", free as f64 / 1073741824.0),
        "diskspacetotal1": format!("{:.2}", total as f64 / 1073741824.0),
        "diskspacetotal2": format!("{:.2}", total as f64 / 1073741824.0),
        "diskspace1_norm": fmt_bytes(free as f64),
        "diskspace2_norm": fmt_bytes(free as f64),
        "my_home": eng.map_path(&eng.cfg.complete_dir),
        "slots": slots,
    }}))
}

fn disk(path: &str) -> (u64, u64) {
    let Ok(c) = std::ffi::CString::new(path) else { return (0, 0) };
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return (0, 0);
    }
    (s.f_bavail as u64 * s.f_frsize as u64, s.f_blocks as u64 * s.f_frsize as u64)
}

fn history_list(eng: &Engine, p: &Params) -> Response {
    let start = p.num("start").unwrap_or(0).max(0) as usize;
    let limit = p.num("limit").unwrap_or(0).max(0) as usize;
    let cat = p.get("category").or(p.get("cat")).filter(|c| !c.is_empty());
    let search = p.get("search").filter(|s| !s.is_empty()).map(|s| s.to_lowercase());
    let failed_only = p.get("failed_only") == Some("1");
    let only: Vec<String> = p.ids("nzo_ids");
    let status_filter: Vec<String> = p.ids("status").into_iter().map(|s| s.to_lowercase()).collect();
    let st = eng.store.lock().unwrap();
    let mut slots = vec![];
    let mut matched = 0usize;
    let mut pp = 0;
    for (i, h) in st.history.iter().enumerate() {
        if h.status != "Completed" && h.status != "Failed" {
            pp += 1;
        }
        if cat.is_some_and(|c| !h.m.cat.eq_ignore_ascii_case(c))
            || search.as_ref().is_some_and(|s| !h.m.name.to_lowercase().contains(s))
            || (failed_only && h.status != "Failed")
            || (!only.is_empty() && !only.contains(&h.m.nzo))
            || (!status_filter.is_empty() && !status_filter.contains(&h.status.to_lowercase()))
        {
            continue;
        }
        matched += 1;
        if matched <= start || (limit > 0 && slots.len() >= limit) {
            continue;
        }
        let storage = if h.status == "Completed" { eng.map_path(&h.storage) } else { String::new() };
        slots.push(json!({
            "id": i,
            "completed": h.completed,
            "name": h.m.name,
            "nzb_name": h.m.nzb_name,
            "category": h.m.cat,
            "pp": "D",
            "script": "None",
            "report": "",
            "url": "",
            "status": h.status,
            "nzo_id": h.m.nzo,
            "storage": storage,
            "path": eng.map_path(&eng.cfg.staging_dir.join(&h.m.nzo).to_string_lossy()),
            "script_log": "",
            "script_line": "",
            "download_time": h.download_time,
            "postproc_time": h.postproc_time,
            "stage_log": [{"name": "Download", "actions": h.log}],
            "downloaded": h.downloaded,
            "completeness": null,
            "fail_message": h.fail_message,
            "url_info": "",
            "bytes": h.m.bytes,
            "meta": null,
            "series": null,
            "md5sum": "",
            "password": null,
            "duplicate_key": "",
            "archive": false,
            "time_added": h.m.added,
            "size": fmt_bytes(h.m.bytes as f64),
            "loaded": false,
            "retry": (h.status == "Failed") as u8,
            "action_line": "",
        }));
    }
    let n = st.history.len();
    drop(st);
    let size = |d: u64| fmt_bytes(eng.server_totals(d).values().sum::<u64>() as f64);
    Response::json(&json!({"history": {
        "noofslots": matched,
        "noofslots_total": n,
        "ppslots": pp,
        "day_size": size(1),
        "week_size": size(7),
        "month_size": size(30),
        "total_size": size(0),
        "last_history_update": eng.hist_ver.load(Relaxed),
        "version": SAB_VERSION,
        "slots": slots,
    }}))
}

fn get_config(eng: &Engine, p: &Params) -> Response {
    let misc = json!({
        "complete_dir": eng.map_path(&eng.cfg.complete_dir),
        "download_dir": eng.map_path(&eng.cfg.staging_dir.to_string_lossy()),
        "tv_categories": [],
        "enable_tv_sorting": 0,
        "movie_categories": [],
        "enable_movie_sorting": 0,
        "date_categories": [],
        "enable_date_sorting": 0,
        "pre_check": 0,
        "history_retention": "",
        "history_retention_option": "all",
        "history_retention_number": 1,
        "api_key": eng.cfg.api_key,
        "nzb_key": eng.cfg.nzb_key,
        "port": eng.cfg.listen.rsplit(':').next().unwrap_or(""),
        "bandwidth_max": "",
        "enable_https": 0,
        "folder_rename": 1,
    });
    let cats: Vec<Value> = eng
        .cats
        .iter()
        .enumerate()
        .map(|(i, c)| json!({"name": c.name, "order": i, "pp": c.pp, "script": if c.script.is_empty() { "None" } else { &c.script }, "dir": c.dir, "newzbin": "", "priority": c.priority}))
        .collect();
    let servers: Vec<Value> = eng
        .servers
        .iter()
        .map(|s| {
            json!({"name": s.name, "displayname": s.name, "host": s.host, "port": s.port, "username": if s.user.is_empty() { "" } else { "**********" },
                   "password": if s.pass.is_empty() { "" } else { "**********" }, "connections": s.conns, "ssl": s.tls as u8, "enable": 1, "priority": s.priority})
        })
        .collect();
    match p.get("section") {
        Some("misc") => match p.get("keyword") {
            Some(k) => Response::json(&json!({"config": {"misc": {k: misc.get(k).cloned().unwrap_or(Value::Null)}}})),
            None => Response::json(&json!({"config": {"misc": misc}})),
        },
        Some("categories") => Response::json(&json!({"config": {"categories": cats}})),
        Some("servers") => Response::json(&json!({"config": {"servers": servers}})),
        _ => Response::json(&json!({"config": {"misc": misc, "categories": cats, "servers": servers, "sorters": []}})),
    }
}

fn full_status(eng: &Engine) -> Response {
    let rate = eng.rate5.load(Relaxed) as f64;
    let (free, total) = disk(&eng.cfg.complete_dir);
    Response::json(&json!({"status": {
        "version": SAB_VERSION,
        "paused": eng.paused.load(Relaxed),
        "completedir": eng.map_path(&eng.cfg.complete_dir),
        "downloaddir": eng.map_path(&eng.cfg.staging_dir.to_string_lossy()),
        "completedirspace": format!("{:.2}", free as f64 / 1073741824.0),
        "completedirspacetotal": format!("{:.2}", total as f64 / 1073741824.0),
        "kbpersec": format!("{:.2}", rate / 1024.0),
        "speed": fmt_speed(rate),
        "speedlimit_abs": crate::conn::LIMIT.rate.load(Relaxed).to_string(),
        "noofslots_total": eng.store.lock().unwrap().queue.len(),
        "servers": eng.servers.iter().enumerate().map(|(i, s)| json!({
            "servername": s.name,
            "serveractiveconn": eng.stats[i].live.load(Relaxed),
            "servertotalconn": s.conns,
            "serverssl": s.tls as u8,
            "serveractive": !eng.q.is_down(i),
            "servererror": if eng.q.is_down(i) { eng.stats[i].last_error.lock().unwrap().clone() } else { String::new() },
            "serverpriority": s.priority,
        })).collect::<Vec<_>>(),
        "warnings": [],
    }}))
}

fn server_stats(eng: &Engine) -> Response {
    let (d1, d7, d30, all) = (eng.server_totals(1), eng.server_totals(7), eng.server_totals(30), eng.server_totals(0));
    let daily = eng.daily.lock().unwrap().clone();
    let mut servers = serde_json::Map::new();
    for s in &eng.servers {
        let g = |m: &std::collections::BTreeMap<String, u64>| m.get(&s.name).copied().unwrap_or(0);
        servers.insert(
            s.name.clone(),
            json!({"total": g(&all), "month": g(&d30), "week": g(&d7), "day": g(&d1), "daily": daily.get(&s.name).cloned().unwrap_or_default(),
                   "articles_tried": {}, "articles_success": {}}),
        );
    }
    let sum = |m: &std::collections::BTreeMap<String, u64>| m.values().sum::<u64>();
    Response::json(&json!({"total": sum(&all), "month": sum(&d30), "week": sum(&d7), "day": sum(&d1), "servers": servers}))
}

fn get_files(eng: &Engine, p: &Params) -> Response {
    let id = p.get("value").unwrap_or("");
    let st = eng.store.lock().unwrap();
    let Some(e) = st.queue.iter().find(|e| e.m.nzo == id) else { return Response::json(&json!({"files": []})) };
    let (t, d) = e.progress();
    Response::json(&json!({"files": [{"filename": e.m.name, "mb": mb(t), "mbleft": mb(t - d), "bytes": t, "age": age(e.m.added), "nzf_id": e.m.nzo, "status": if e.run.is_some() { "active" } else { "queued" }}]}))
}

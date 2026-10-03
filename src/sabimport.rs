//! Takes over a SABnzbd queue and history with the same job ids, so Sonarr/Radarr
//! keep tracking their downloads when their SABnzbd client is pointed at nzbfast.
//!
//! Run while the nzbfast service is stopped (and SABnzbd is paused):
//!   nzbfast import-sab --sab-url http://127.0.0.1:8080 --sab-incomplete /path/to/incomplete

use crate::engine::{self, Hist, Meta, PRIO_FORCE, PRIO_HIGH, PRIO_LOW, PRIO_NORMAL};
use crate::job;
use crate::nzb;
use crate::svccfg::Loaded;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};

fn sab_get(url: &str, key: &str, query: &str) -> Result<Value, String> {
    let u = format!("{}/api?output=json&apikey={key}&{query}", url.trim_end_matches('/'));
    let f = crate::http::get(&u)?;
    serde_json::from_slice(&f.data).map_err(|e| format!("SABnzbd {query}: {e}"))
}

fn s(v: &Value, k: &str) -> String {
    match &v[k] {
        Value::String(x) => x.clone(),
        Value::Null => String::new(),
        x => x.to_string(),
    }
}

fn n(v: &Value, k: &str) -> u64 {
    match &v[k] {
        Value::Number(x) => x.as_u64().unwrap_or(0),
        Value::String(x) => x.parse().unwrap_or(0),
        _ => 0,
    }
}

/// The NZB SABnzbd saved for a job: `<incomplete>/<job>/__ADMIN__/*.nzb.gz`.
fn find_nzb(incomplete: &Path, folder: &str) -> Option<PathBuf> {
    let admin = incomplete.join(folder).join("__ADMIN__");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&admin)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let l = p.to_string_lossy().to_lowercase();
            l.ends_with(".nzb.gz") || l.ends_with(".nzb")
        })
        .collect();
    found.sort();
    found.into_iter().next()
}

pub fn run(l: Loaded, sab_url: &str, incomplete: &Path, dry_run: bool) -> Result<(), String> {
    let cfg = &l.cfg;
    let cat_name = |c: &str| l.categories.iter().find(|x| x.name.eq_ignore_ascii_case(c)).map(|x| x.name.clone()).unwrap_or_else(|| "*".into());
    let q = sab_get(sab_url, &cfg.api_key, "mode=queue&start=0&limit=0")?;
    let h = sab_get(sab_url, &cfg.api_key, "mode=history&start=0&limit=0")?;
    let mut qslots: Vec<Value> = q["queue"]["slots"].as_array().cloned().unwrap_or_default();
    qslots.sort_by_key(|x| n(x, "index"));
    let hslots: Vec<Value> = h["history"]["slots"].as_array().cloned().unwrap_or_default();
    eprintln!("SABnzbd: {} queued jobs, {} history entries", qslots.len(), hslots.len());

    let nzb_dir = cfg.state_dir.join("nzb");
    if !dry_run {
        std::fs::create_dir_all(&nzb_dir).map_err(|e| e.to_string())?;
    }
    let mut metas = vec![];
    let mut skipped = vec![];
    let now = engine::unix_now();
    for x in &qslots {
        let nzo = s(x, "nzo_id");
        let folder = s(x, "filename");
        let Some(path) = find_nzb(incomplete, &folder) else {
            skipped.push(format!("{folder}: NZB not found"));
            continue;
        };
        let raw = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut text = vec![];
        if raw.starts_with(&[0x1f, 0x8b]) {
            if let Err(e) = flate2::read::GzDecoder::new(&raw[..]).read_to_end(&mut text) {
                skipped.push(format!("{folder}: {e}"));
                continue;
            }
        } else {
            text = raw.clone();
        }
        let parsed = match nzb::parse(&String::from_utf8_lossy(&text), folder.clone()) {
            Ok(p) => p,
            Err(e) => {
                skipped.push(format!("{folder}: {e}"));
                continue;
            }
        };
        let (mut bytes, mut bytes_par2) = (0u64, 0u64);
        for f in &parsed.files {
            let b: u64 = f.segs.iter().map(|s| s.bytes as u64).sum();
            if job::is_par2_volume(&nzb::subject_filename(&f.subject)) {
                bytes_par2 += b;
            } else {
                bytes += b;
            }
        }
        let prio = match s(x, "priority").to_ascii_lowercase().as_str() {
            "force" | "2" => PRIO_FORCE,
            "high" | "1" => PRIO_HIGH,
            "low" | "-1" => PRIO_LOW,
            _ => PRIO_NORMAL,
        };
        let password = Some(s(x, "password")).filter(|p| !p.is_empty() && p != "None");
        let added = Some(n(x, "time_added")).filter(|t| *t > 0).unwrap_or(now);
        if !dry_run {
            // Stored exactly as SABnzbd had it; nzbfast reads plain or gzip NZBs.
            let ext = if raw.starts_with(&[0x1f, 0x8b]) { "nzb.gz" } else { "nzb" };
            let dst = nzb_dir.join(format!("{nzo}.nzb.gz"));
            let data = if ext == "nzb.gz" {
                raw
            } else {
                use std::io::Write;
                let mut gz = flate2::write::GzEncoder::new(vec![], flate2::Compression::fast());
                gz.write_all(&raw).map_err(|e| e.to_string())?;
                gz.finish().map_err(|e| e.to_string())?
            };
            std::fs::write(&dst, data).map_err(|e| format!("{}: {e}", dst.display()))?;
        }
        metas.push(Meta {
            nzo,
            name: folder,
            nzb_name: path.file_name().map(|f| f.to_string_lossy().trim_end_matches(".gz").to_string()).unwrap_or_default(),
            cat: cat_name(&s(x, "cat")),
            prio,
            paused: s(x, "status") == "Paused",
            added,
            bytes,
            bytes_par2,
            files: parsed.files.len() as u32,
            password,
        });
    }

    let mut hists = vec![];
    let mut busy = 0;
    for x in &hslots {
        let status = s(x, "status");
        if status != "Completed" && status != "Failed" {
            busy += 1;
            continue;
        }
        let log: Vec<String> = x["stage_log"]
            .as_array()
            .map(|st| st.iter().flat_map(|e| e["actions"].as_array().cloned().unwrap_or_default()).filter_map(|a| a.as_str().map(String::from)).collect())
            .unwrap_or_default();
        hists.push(Hist {
            m: Meta {
                nzo: s(x, "nzo_id"),
                name: s(x, "name"),
                nzb_name: s(x, "nzb_name"),
                cat: cat_name(&s(x, "category")),
                prio: PRIO_NORMAL,
                paused: false,
                added: n(x, "time_added"),
                bytes: n(x, "bytes"),
                bytes_par2: 0,
                files: 0,
                password: None,
            },
            status,
            fail_message: s(x, "fail_message"),
            storage: s(x, "storage"),
            tmp_dest: String::new(),
            completed: n(x, "completed"),
            download_time: n(x, "download_time"),
            postproc_time: n(x, "postproc_time"),
            downloaded: n(x, "downloaded"),
            log,
        });
    }
    for m in skipped.iter().take(20) {
        eprintln!("  skipped {m}");
    }
    if busy > 0 {
        eprintln!("  note: {busy} SABnzbd jobs are still post-processing and were not imported (let them finish first)");
    }
    let total: u64 = metas.iter().map(|m| m.bytes).sum();
    eprintln!(
        "importing {} queued jobs ({:.2} TB), {} history entries, {} skipped{}",
        metas.len(),
        total as f64 / 1e12,
        hists.len(),
        skipped.len(),
        if dry_run { " [dry run]" } else { "" }
    );
    if dry_run {
        return Ok(());
    }
    let (nq, nh) = engine::import_state(cfg, metas, hists).map_err(|e| e.to_string())?;
    eprintln!("nzbfast state now holds {nq} queued jobs and {nh} history entries");
    Ok(())
}

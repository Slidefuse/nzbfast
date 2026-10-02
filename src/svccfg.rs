//! Service configuration (TOML). Servers, categories, API keys and the completed
//! folder can be imported from an existing SABnzbd ini, so nzbfast can take over
//! from SABnzbd without reconfiguring Sonarr/Radarr beyond host and port.

use crate::config::{self, ServerCfg};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize, Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct SvcCfg {
    /// HTTP listen address(es) for the API and the web UI, comma-separated.
    pub listen: String,
    /// Full-access API key (taken from the SABnzbd ini when empty).
    pub api_key: String,
    /// Add-only key, as in SABnzbd.
    pub nzb_key: String,
    /// Optional SABnzbd ini to import servers, categories, keys and folders from.
    pub sab_ini: String,
    /// Queue, history and stored NZBs.
    pub state_dir: PathBuf,
    /// Fast scratch area (RAM or NVMe) where jobs are assembled.
    pub staging_dir: PathBuf,
    /// Final destination root; category folders live below it.
    pub complete_dir: String,
    /// Upper bound for data held in staging (active downloads + jobs waiting to move).
    pub staging_limit_gb: f64,
    pub active_jobs: usize,
    pub depth: usize,
    pub nic: String,
    pub io_threads: usize,
    /// Jobs moved to `complete_dir` concurrently, and copy threads per file.
    pub mover_jobs: usize,
    pub mover_threads: usize,
    pub history_keep: usize,
    /// Require the API key (once, then a cookie) for the web UI.
    pub ui_auth: bool,
    /// Initial speed limit in MB/s (0 = unlimited).
    pub speed_limit_mbs: f64,
    pub servers: Vec<SrvOverride>,
    pub categories: Vec<Category>,
    /// Rewrites reported paths for clients that see the folders under another
    /// prefix (e.g. Sonarr in a container): `{ from = "/mnt/data", to = "/data" }`.
    pub path_map: Vec<PathMap>,
}

impl Default for SvcCfg {
    fn default() -> Self {
        SvcCfg {
            listen: "0.0.0.0:8085".into(),
            api_key: String::new(),
            nzb_key: String::new(),
            sab_ini: String::new(),
            state_dir: "/var/lib/nzbfast".into(),
            staging_dir: "/dev/shm/nzbfast".into(),
            complete_dir: String::new(),
            staging_limit_gb: 32.0,
            active_jobs: 256,
            depth: 8,
            nic: "eth0".into(),
            io_threads: 8,
            mover_jobs: 2,
            mover_threads: 4,
            history_keep: 20000,
            ui_auth: true,
            speed_limit_mbs: 0.0,
            servers: vec![],
            categories: vec![],
            path_map: vec![],
        }
    }
}

/// Changes to an imported server (matched by name or host), or a new server when
/// `match` is empty.
#[derive(Deserialize, Clone, Debug, Default)]
#[serde(default, deny_unknown_fields)]
pub struct SrvOverride {
    #[serde(rename = "match")]
    pub matches: String,
    pub name: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    /// Dial this `addr:port` instead (e.g. a TCP relay); TLS still verifies `host`.
    pub connect: Option<String>,
    pub tls: Option<bool>,
    pub insecure: Option<bool>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub conns: Option<usize>,
    pub prio: Option<u32>,
    pub depth: Option<usize>,
    pub enable: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Category {
    pub name: String,
    #[serde(default)]
    pub dir: String,
    /// SABnzbd priority (-100 = default, -1 low, 0 normal, 1 high, 2 force).
    #[serde(default = "default_prio")]
    pub priority: i32,
    #[serde(default)]
    pub pp: String,
    #[serde(default)]
    pub script: String,
}

fn default_prio() -> i32 {
    -100
}

#[derive(Deserialize, Clone, Debug)]
pub struct PathMap {
    pub from: String,
    pub to: String,
}

pub struct Loaded {
    pub cfg: SvcCfg,
    pub servers: Vec<ServerCfg>,
    pub categories: Vec<Category>,
}

fn apply(s: &mut ServerCfg, o: &SrvOverride) {
    if let Some(v) = &o.name {
        s.name = v.clone();
    }
    if let Some(v) = &o.host {
        s.host = v.clone();
    }
    if let Some(v) = o.port {
        s.port = v;
    }
    if let Some(v) = &o.connect {
        s.connect = if v.is_empty() { None } else { Some(v.clone()) };
    }
    if let Some(v) = o.tls {
        s.tls = v;
    }
    if let Some(v) = o.insecure {
        s.insecure = v;
    }
    if let Some(v) = &o.user {
        s.user = v.clone();
    }
    if let Some(v) = &o.pass {
        s.pass = v.clone();
    }
    if let Some(v) = o.conns {
        s.conns = v;
    }
    if let Some(v) = o.prio {
        s.priority = v;
    }
    if let Some(v) = o.depth {
        s.depth = v;
    }
    if o.enable == Some(false) {
        s.conns = 0;
    }
}

pub fn load(path: &str) -> Result<Loaded, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut cfg: SvcCfg = toml::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
    let mut servers = vec![];
    let mut categories: Vec<Category> = vec![];
    if !cfg.sab_ini.is_empty() {
        let ini = config::parse_sab_ini(&cfg.sab_ini)?;
        servers = config::servers_from_ini(&ini);
        let misc = |k: &str| ini.misc.get(k).cloned().unwrap_or_default();
        if cfg.api_key.is_empty() {
            cfg.api_key = misc("api_key");
        }
        if cfg.nzb_key.is_empty() {
            cfg.nzb_key = misc("nzb_key");
        }
        if cfg.complete_dir.is_empty() {
            cfg.complete_dir = misc("complete_dir");
        }
        let mut cats: Vec<(i64, Category)> = ini
            .categories
            .iter()
            .filter_map(|c| {
                let g = |k: &str| c.get(k).cloned().unwrap_or_default();
                let name = g("name");
                if name.is_empty() {
                    return None;
                }
                let order = g("order").parse().unwrap_or(0);
                let priority = g("priority").parse().unwrap_or(-100);
                Some((order, Category { name, dir: g("dir"), priority, pp: g("pp"), script: g("script") }))
            })
            .collect();
        cats.sort_by_key(|c| c.0);
        categories = cats.into_iter().map(|c| c.1).collect();
    }
    for o in &cfg.servers {
        if o.matches.is_empty() {
            let mut s = ServerCfg {
                name: String::new(),
                host: String::new(),
                port: 563,
                tls: true,
                insecure: false,
                user: String::new(),
                pass: String::new(),
                conns: 8,
                priority: 0,
                depth: 0,
                connect: None,
            };
            apply(&mut s, o);
            if s.name.is_empty() {
                s.name = s.host.clone();
            }
            if s.host.is_empty() {
                return Err("server entry without `match` needs a host".into());
            }
            servers.push(s);
            continue;
        }
        let m = o.matches.to_lowercase();
        let mut hit = false;
        for s in servers.iter_mut().filter(|s| s.name.to_lowercase().contains(&m) || s.host.to_lowercase().contains(&m)) {
            apply(s, o);
            hit = true;
        }
        if !hit {
            return Err(format!("server override `{}` matches no server", o.matches));
        }
    }
    servers.retain(|s| s.conns > 0);
    if servers.len() > 64 {
        return Err("at most 64 servers are supported".into());
    }
    // Explicit categories replace imported ones with the same name.
    for c in &cfg.categories {
        categories.retain(|x| x.name != c.name);
        categories.push(c.clone());
    }
    if !categories.iter().any(|c| c.name == "*") {
        categories.insert(0, Category { name: "*".into(), dir: String::new(), priority: 0, pp: "3".into(), script: "None".into() });
    }
    if cfg.complete_dir.is_empty() {
        return Err("complete_dir is not set (and not found in sab_ini)".into());
    }
    Ok(Loaded { cfg, servers, categories })
}

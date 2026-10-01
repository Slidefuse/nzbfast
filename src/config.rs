//! Server configuration: read from a SABnzbd ini (so existing setups work as-is)
//! or from a compact command-line spec.

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ServerCfg {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub insecure: bool,
    pub user: String,
    pub pass: String,
    pub conns: usize,
    pub priority: u32,
    pub depth: usize,
}

fn unquote(v: &str) -> String {
    let v = v.trim();
    if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
        v[1..v.len() - 1].to_string()
    } else {
        v.to_string()
    }
}

/// Parses enabled servers from the `[servers]` section of a SABnzbd ini.
pub fn from_sab_ini(path: &str) -> Result<Vec<ServerCfg>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut in_servers = false;
    let mut sections: Vec<HashMap<String, String>> = vec![];
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("[[") {
            if in_servers {
                sections.push(HashMap::new());
            }
            continue;
        }
        if t.starts_with('[') {
            in_servers = t == "[servers]";
            continue;
        }
        if !in_servers {
            continue;
        }
        if let (Some((k, v)), Some(s)) = (t.split_once('='), sections.last_mut()) {
            s.insert(k.trim().to_string(), unquote(v));
        }
    }
    let get = |s: &HashMap<String, String>, k: &str| s.get(k).cloned().unwrap_or_default();
    Ok(sections
        .iter()
        .filter(|s| get(s, "enable") == "1")
        .map(|s| ServerCfg {
            name: s.get("displayname").or(s.get("name")).cloned().unwrap_or_default(),
            host: get(s, "host"),
            port: get(s, "port").parse().unwrap_or(563),
            tls: get(s, "ssl") == "1",
            insecure: false,
            user: get(s, "username"),
            pass: get(s, "password"),
            conns: get(s, "connections").parse().unwrap_or(8),
            priority: get(s, "priority").parse().unwrap_or(0),
            depth: 0,
        })
        .collect())
}

/// Parses `name=x,host=h,port=p,tls=1,conns=n,user=u,pass=p,prio=0,insecure=1,depth=d`.
pub fn from_spec(spec: &str) -> Result<ServerCfg, String> {
    let mut m = HashMap::new();
    for kv in spec.split(',') {
        let (k, v) = kv.split_once('=').ok_or_else(|| format!("bad server spec item {kv}"))?;
        m.insert(k.trim().to_string(), v.trim().to_string());
    }
    let g = |k: &str| m.get(k).cloned().unwrap_or_default();
    Ok(ServerCfg {
        name: m.get("name").cloned().unwrap_or_else(|| g("host")),
        host: g("host"),
        port: g("port").parse().map_err(|_| "port required")?,
        tls: g("tls") == "1",
        insecure: g("insecure") == "1",
        user: g("user"),
        pass: g("pass"),
        conns: g("conns").parse().unwrap_or(8),
        priority: g("prio").parse().unwrap_or(0),
        depth: g("depth").parse().unwrap_or(0),
    })
}

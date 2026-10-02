# nzbfast

A Usenet downloader built to run at the limit of the network and disks, for
large Plex libraries fed by Sonarr/Radarr.

- Pipelined NNTP over TLS, one blocking thread per connection, SIMD yEnc decoding.
- Stored RAR sets are written straight into the extracted file while downloading
  (no volumes on disk); RAR CRCs are verified from the per-article CRCs.
- Compressed, encrypted, obfuscated and multi-file RAR sets are unpacked in-process
  with the UnRAR library (statically linked; no external binaries).
- par2 verification and Reed–Solomon repair in-process, fetching only the recovery
  volumes a repair needs.
- Adaptive routing across providers; unreachable providers are detected and routed
  around; hopeless jobs fail fast, so Sonarr/Radarr can pick another release.

## Service mode

```
nzbfast serve --config /etc/nzbfast/nzbfast.toml
```

See `deploy/nzbfast.example.toml` and `deploy/nzbfast.service`.

Jobs are assembled in a fast staging area (RAM or NVMe) and then moved to the
completed folder (e.g. NFS) by a mover. A staging budget makes a slow final tier
throttle new downloads instead of exhausting RAM.

### Sonarr / Radarr / Prowlarr

nzbfast implements the SABnzbd API, so the *arr apps use their built-in
**SABnzbd** download client unchanged: host, port, API key and category.
With `sab_ini` pointing at an existing SABnzbd config, the API key, categories,
servers and completed folder carry over.

Supported modes: `version`, `auth`, `get_config`, `fullstatus`, `status`,
`server_stats`, `queue` (list, `delete`, `pause`, `resume`, `priority`, `rename`,
`purge`), `history` (list with `category`/`search`/`failed_only`/`nzo_ids`,
`delete` with `del_files`), `addfile`, `addurl`, `addlocalfile`, `pause`, `resume`,
`switch`, `change_cat`, `retry`, `get_cats`, `get_scripts`, `warnings`,
`get_files`, `config&name=speedlimit`.

Tested end to end with Sonarr 4.0.20 and Radarr 6.4.4: client test, grab,
download, import, failed-download blocklisting, and removal after import.

Archive passwords are taken from `<meta type="password">` in the NZB or from
a `Name{{password}}` job name.

### Web UI

`http://host:port/` shows live throughput (stacked per provider, with NIC rate),
provider health, active jobs, queue and history, updated 10 times per second over
a single Server-Sent Events stream. Sign in with the API key (or open
`/?apikey=KEY` once). Jobs can be added by drag and drop, paused, reprioritised,
moved, retried and deleted.

## Other commands

```
nzbfast get [--sab-ini FILE] [--server SPEC]... NZB|DIR...   # one-shot download
nzbfast mock-gen --out DIR RELEASE_DIR...                     # test fixtures
nzbfast mock-serve --dir DIR [--port P] [--tls] [--drop SUB:N]
nzbfast bench                                                 # decoder benchmarks
```

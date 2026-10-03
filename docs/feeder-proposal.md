# Proposal: a "feeder" that keeps nzbfast's queue full

Status: draft for review (2026-10-02). Nothing described here is built or running yet.

## Problem

Prod (`root@tuner.down.lol`) has a large backlog to (re)download, but nzbfast's queue sits
empty most of the time. nzbfast finishes a typical grab within seconds of receiving it
(e.g. a 5.4 GB movie in 9 s), so throughput is bounded by how fast Radarr/Sonarr hand it
work, not by Usenet or the NAS.

Backlog at the time of writing:

| App | Wanted | Notes |
|---|---|---|
| Radarr | 1,126 monitored missing movies (was 1,973 before re-importing 946 downloads that had obfuscated, extensionless filenames) + 109 cutoff-unmet | Profiles: "Balanced 1080p" (7,666 movies), "Balanced 2160p" (17) |
| Sonarr | 139,850 missing episodes in 7,557 monitored seasons | 7,257 seasons (135,771 episodes, 97%) have **no** episodes at all; 300 are partial. Profiles: "Balanced 1080p" (1,981 series), two 2160p profiles (3 series) |

Capacity: NAS `/mediapool` has 303 TB free of 340 TB. nzbfast sustains ~1.2–1.6 GB/s
(download link ~9.4 Gbit/s; NAS writes ~1.6 GB/s since the 20 Gbit upgrade), so the
whole backlog (rough estimate ~200 TB) is ~1.5–2 days of download time. To stay busy,
nzbfast needs a new grab every ~1–4 seconds.

## Measurements: where the time goes

### Through the *arr stack today

- Radarr's `MissingMoviesSearch` processes one movie at a time, ~4 indexer queries per
  movie (IMDb-ID query + title query, on each of 2 indexers). Observed: **2–6 movies per
  minute** (~12 s per movie).
- Prowlarr's history shows each indexer answering in 0.2–1.0 s (median ~0.4–0.5 s), but
  consecutive queries to the same indexer are spaced **~2–8 s apart**.
- Test: 20 IMDb-ID queries through Prowlarr's per-indexer endpoint
  (`/prowlarr/{id}/api?t=movie&imdbid=…`), serial vs 20 in parallel:

  | Indexer | Serial | 20 parallel |
  |---|---|---|
  | NZBgeek (id 1) | 46.2 s | 40.0 s |
  | NzbPlanet (id 5) | 64.5 s | 42.0 s |

  So the path through Prowlarr is paced at roughly one query per ~2 s per indexer, and
  running requests in parallel does not help. (Sonarr's episode search batches were
  competing for the same indexers during this test.) I have not confirmed in source
  whether this spacing comes from Prowlarr, from Radarr/Sonarr, or both. Reviewer: worth
  checking.
- Sonarr was searching episode by episode (`EpisodeSearch`, 1,415 episodes in 15 batches
  of 100). It used ~75% of the query budget (284 of the last 375 Prowlarr queries). 12
  queued batches (1,115 episodes) have since been cancelled and saved to
  `/root/sonarr-search-paused.jsonl` on prod; 3 already-started batches could not be
  cancelled (HTTP 409).

### Indexers queried directly (from the dev box)

100 popular movies by IMDb ID, `t=movie&extended=1&limit=100`, 20 concurrent requests:

| Indexer | Wall time | Rate | Latency median / p90 | HTTP codes | Movies with results |
|---|---|---|---|---|---|
| NZBgeek | 2.1 s | **48 queries/s** | 0.34 s / 0.58 s | 100 × 200 | 89/100 (5,543 releases) |
| NzbPlanet | 0.9 s | **112 queries/s** | 0.15 s / 0.20 s | 100 × 200 | 92/100 (4,748 releases) |
| Nzb.su | — | — | — | 429 even when serial (4 of 20); 20/20 when parallel | — |

Direct search capacity is therefore **at least 100× higher** than what Radarr/Sonarr get.
No usage limits are published: `t=caps` returns only `<limits max="100" default="100"/>`
(results per query); no rate-limit headers. Prowlarr has no query/grab limit configured
for NZBgeek and a 20,000/day query limit for NzbPlanet. Lifetime Prowlarr counters:
NZBgeek 31,483 queries / 14,602 grabs, NzbPlanet 26,443 / 4,205, all with zero failures.

Note: the 20 *missing* movies used in the first direct test returned zero results on both
indexers. The remaining Radarr backlog is skewed toward obscure titles (Top Gear specials,
etc.), so the movie hit rate will be much lower than the 89–92% above. TV, especially
whole-season packs, is where most of the volume is.

## Proposal

A small service (the "feeder") that does the searching itself, but leaves the
**grab decision to Radarr/Sonarr** so quality profiles, custom formats, the blocklist and
queue de-duplication all still apply.

1. **Wanted lists.** Periodically read:
   - Radarr: `wanted/missing` (monitored) and `wanted/cutoff`.
   - Sonarr: `series` statistics → per monitored season, whether it is entirely missing
     (→ season search, prefer packs) or partial (→ episode searches for the missing
     episodes only).
2. **Search directly.** Query NZBgeek and NzbPlanet in parallel (~10–20 concurrent per
   indexer): `t=movie&imdbid=` for movies, `t=tvsearch&tvdbid=&season=` (and `&ep=` for
   partial seasons). Skip Nzb.su for now because it returns 429s. Back off globally on any
   429/5xx.
3. **Pick candidates locally, decide in the *arr.** Parse titles, drop ones obviously
   outside the item's profile (resolution/source), rank the rest (prefer the profile
   cutoff quality, then season packs for whole seasons, then sensible size), and hand the
   best one to Radarr/Sonarr via `POST /api/v3/release/push` (title, downloadUrl, size,
   publishDate, indexer, protocol=usenet). Radarr/Sonarr evaluate it exactly as if it came
   from RSS and grab it if approved. If they reject it, push the next candidate (max 3).
   Push one item at a time per movie/season so an approved grab is not followed by a
   second "upgrade" grab for the same item.
4. **Pace by queue depth, not by search speed.** Keep nzbfast a bounded amount ahead (for
   example ~300 jobs or a few TB queued, read from nzbfast's queue API). This avoids
   fetching NZBs long before download (DMCA takedowns make old NZBs fail), keeps grab
   counts per indexer reasonable, and leaves room for RSS grabs.
5. **State.** Remember per item: last searched, outcome (pushed / rejected with reason / no
   results), so items with no results are retried on a slow schedule (e.g. daily), not
   on every pass.
6. **Deployment.** Run on the prod host as a systemd service with its own root-only config
   (indexer API keys, Radarr/Sonarr URLs and keys). Start in **dry-run mode**, which only
   logs what it would push, and turn on pushing after review.

Expected effect: search no longer limits throughput. The full backlog can be searched in
minutes. Grabs are paced at download speed (~1 every 1–4 s ≈ 20–30k/day), so nzbfast
stays at line rate until the backlog is gone or the indexers run out of results.

### Cheaper interim step (no new code)

Replace Sonarr's per-episode searches with `SeasonSearch` per entirely-missing season
(7,257 seasons) plus `EpisodeSearch` for the 300 partial seasons. That is ~18× fewer
searches inside the existing stack, though still at ~0.5 queries/s per indexer
(~10–16 h for TV alone).

## Risks and open questions for the reviewer

1. **Undisclosed account limits.** NZBgeek and NzbPlanet publish no daily API/grab caps;
   20–30k grabs/day may exceed what the accounts allow. Mitigations: split grabs across
   both indexers, stop on the first 429, and maybe start with a conservative daily grab
   budget. *Does the owner know the plan limits?*
2. **Push semantics.** Confirm `release/push` behaviour on current Radarr (v6.x) and
   Sonarr (v4.x): does a pushed release that is approved always get grabbed immediately,
   and does a later push for the same item get rejected while the first is queued, or
   treated as an upgrade?
3. **Duplicating the profile logic.** Local ranking is only a pre-filter; the *arr must
   stay the authority. Is a pre-filter even needed, or is pushing the top N by a simple
   score and letting the *arr reject enough?
4. **Season packs vs the *arr's preference.** Sonarr's own SeasonSearch prefers packs;
   make sure pushed single-episode releases do not pre-empt a better pack for the same
   season (push packs first; only fall back to episodes when no acceptable pack exists).
5. **Bypassing Prowlarr** means its stats, limits and history no longer see these
   queries. Alternative: query through Prowlarr's per-indexer endpoint if the ~2 s spacing
   turns out to be configurable there; that would be preferable if so.
6. **Load on Radarr/Sonarr** from many pushes and imports (the import side handled ~200
   imports in 2 minutes earlier without issue).
7. **Old NZBs.** A significant share of grabs fail with missing articles ("hopeless")
   today (78 Radarr download failures in one hour earlier). Those trigger the *arr's own
   re-search, which goes back through the slow path. The feeder should also notice failed
   items and retry with the next candidate directly.

## Context: related fixes already made today

- nzbfast now names extensionless obfuscated video files from their container header
  (`<job name>.mkv` etc.). 946 existing downloads were renamed (log:
  `/root/nzbfast-deobf-renames.tsv` on prod) and imported via `DownloadedMoviesScan`.
- nzbfast now decodes HTML-escaped job names (`&amp;` → `&`, including double-escaped).
- Credentials for prod *arr apps and indexers are stored on the dev box at
  `/root/.config/nzbfast-prod/creds.json` (mode 600). They are not included in this
  document.

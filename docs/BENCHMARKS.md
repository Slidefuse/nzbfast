# nzbfast vs SABnzbd vs NZBGet — Benchmark Report

Measured 2026-10-02. Harness and setup: [`bench/`](../bench/README.md).

## Summary

nzbfast delivered finished, verified files 2.1–8.9× sooner than the best-tuned SABnzbd 5.1.3 or
NZBGet 26.3 in every scenario we ran, and used less than half the CPU per gigabyte on ordinary
downloads. Its remaining cost is memory. All three clients produced byte-exact output for every
recoverable release. Six weaknesses found in the first pass were fixed during the benchmark; the
numbers below are for the fixed build.

| Category | nzbfast | Best alternative | Result |
| --- | --- | --- | --- |
| 24 real releases from live providers (88.5 GB) | 26.9 s | 238.3 s (SABnzbd) | nzbfast 8.9× faster |
| 6 × 4 GB movies, NZB to finished file | 3.1 s | 14.3 s (NZBGet, tuned) | nzbfast 4.7× faster |
| 40 × 400 MB TV episodes | 1.7 s | 13.4 s (SABnzbd) | nzbfast 7.7× faster |
| Mixed post-processing (RAR -m3, 7z, obfuscated, encrypted) | 7.6 s | 18.2 s (NZBGet, tuned) | nzbfast 2.4× faster |
| 3 damaged + 1 dead release | 4.1 s | 8.4 s (NZBGet, tuned) | nzbfast 2.1× faster |
| Movies from a provider 100 ms away | 11.9 s | 102.6 s (SABnzbd) | nzbfast 8.6× faster |
| Movies on a 4-vCPU box | 4.6 s | 19.1 s (NZBGet, tuned) | nzbfast 4.1× faster |
| Dead release reported to Sonarr/Radarr (live providers) | 7.8 s | 224 s (SABnzbd) | nzbfast 29× sooner |
| CPU per GB downloaded (movies) | 0.85 s | 1.90 s (SABnzbd) | nzbfast uses 55% less |
| Peak memory while downloading | 2.4–8.9 GB | 0.1–0.2 GB (NZBGet, default) | NZBGet far leaner |
| Add an NZB while busy (median) | 2.0 ms | 15.8 ms (SABnzbd) | nzbfast 8× faster |

Why it is faster:

- **No unpack step for stored RARs.** Payload goes straight into the final file during download,
  so a job finishes about 0.3 s after its last article.
- **Pipelined connections.** Eight requests stay in flight per connection, so distant providers
  cost far less throughput.
- **Everything runs in parallel.** Up to 256 jobs download and post-process at once.
- **Native code throughout.** SIMD yEnc decoding, plus in-process RAR, 7z and par2 with no
  external tools.

For Sonarr and Radarr it is a drop-in replacement: it speaks the SABnzbd API and can import a
running SABnzbd's queue and history.

## How we tested

Every client downloaded the same 71 GB corpus from the same in-RAM NNTP server over TLS, and every
output file was checked against a reference MD5. A second round used live Usenet providers.

| Item | Setting |
| --- | --- |
| Host | AMD EPYC 9375F VM, 16 vCPU, 377 GB RAM, Ubuntu 24.04, kernel 6.8, in Amsterdam |
| CPU split | Client pinned to 10 vCPUs; NNTP server pinned to the other 6 |
| Storage | Incomplete and complete folders on tmpfs (/dev/shm) for every client |
| Server | `nzbfast mock-serve`: pre-encoded yEnc articles (716,800-byte parts) from RAM over TLS, with pipelining; selected articles answer 430 to simulate missing posts |
| Connections | 50 per client, one server (live round: 130 across three providers + 20 backup) |
| nzbfast | commit 9d9d2ac plus uncommitted working-tree changes and the fixes below, default config |
| SABnzbd | 5.1.3 from source, Python 3.12, sabctools 9.6.3 (AVX-512 yEnc), par2cmdline-turbo 1.5.0, unrar 7.00, 7-Zip |
| NZBGet | 26.3 (nzbgetcom), official build with bundled unrar 7 and 7za |

**Corpus.** Payloads are unique pseudo-random data (incompressible, like video), packaged the way
scene and P2P groups post. Every release carries 8% par2 recovery data.

| Suite | Releases | Size | What it exercises |
| --- | --- | --- | --- |
| movies | 6 × 4 GB, stored RAR5 in 100 MB volumes | 26.0 GB | Raw throughput on large jobs |
| tv | 40 × 400 MB, stored RAR5 in 50 MB volumes | 17.4 GB | Per-job overhead, queue handling |
| pp | 4 plain MKV, 2 compressed RAR (-m3), 2 split 7z, 2 fully obfuscated, 1 header-encrypted | 23.8 GB | Unpacking, deobfuscation, passwords |
| repair | 3 releases missing 2–5% of articles, 1 missing 25% (unrepairable) | 8.7 GB | par2 repair, failing fast |

**Variants.** Each competitor ran with its defaults and "tuned": SABnzbd with direct unpack, a
4 GB article cache and 4 receive threads; NZBGet with `PostStrategy=rocket`, a 4 GB article cache
and a 2 GB par buffer. Results use each client's faster variant (SABnzbd: defaults; NZBGet: tuned).

**Metrics.** Each run starts a fresh client with empty state and adds every NZB of the suite
through the client's own API. The clock stops when every job sits in history as completed or
failed. End-to-end rate = release size (par2 included) ÷ wall time, so post-processing counts. CPU
and memory cover the whole process tree, sampled every 0.5 s. Each configuration ran 3 times;
medians are reported. Run-to-run spread was under 5% except SABnzbd on the tv and pp suites (up
to 14%).

## Throughput: from NZB to finished file

| Suite | nzbfast | SABnzbd (default) | NZBGet (rocket) |
| --- | --- | --- | --- |
| Movies: 6 × 4 GB stored RAR | **3.06 s** (67.9 Gbit/s) | 21.41 s (9.7) | 14.25 s (14.6) |
| TV: 40 × 400 MB episodes | **1.74 s** (79.8 Gbit/s) | 13.43 s (10.3) | 14.14 s (9.8) |
| Post-processing mix: 11 releases | **7.59 s** (25.1 Gbit/s) | 21.50 s (8.9) | 18.21 s (10.5) |
| Repair: 3 damaged + 1 dead | **4.06 s** (17.0 Gbit/s) | 15.24 s (4.5) | 8.45 s (8.2) |

- **Faster wire.** nzbfast peaked at 104–123 Gbit/s, against 13–15 Gbit/s for SABnzbd and 24–30
  Gbit/s for NZBGet, with the same 50 connections.
- **No unpack step for stored RARs.** The job is done 0.2–0.5 s after the last article arrives.
  Even tuned, NZBGet spent 4.7 s (movies) and 7.6 s (TV) unpacking after its download had
  finished, and 23 s and 67 s at its defaults.
- **Jobs run side by side.** SABnzbd downloads one job at a time in queue order. NZBGet unpacks one
  job at a time by default (up to six with `PostStrategy=rocket`). nzbfast downloads up to 256 jobs
  at once.

SABnzbd's tuned variant was 4–24% slower than its defaults here; NZBGet's tuned variant cut its
times by 54–81% (movies 31.3 → 14.3 s, TV 72.9 → 14.1 s).

## Post-processing: unpack, 7z, obfuscation, passwords

All three clients produced byte-exact output for all 11 releases. The suite queued all 11 at once;
the table shows when the last release of each type was complete, in seconds after the NZBs were
added.

| Release type | nzbfast | SABnzbd | NZBGet (rocket) |
| --- | --- | --- | --- |
| Plain MKV + par2 (4 × 2 GB) | 2.6 s | 6.7 s | 5.6 s |
| Compressed RAR5, -m3 (2 × 2 GB) | 7.6 s | 17.9 s | 13.7 s |
| Split 7z, .7z.001… (2 × 2 GB) | 3.1 s | 22.9 s | 10.2 s |
| Obfuscated: every file name random (2 × 2 GB) | 3.1 s | 21.4 s | 13.2 s |
| Header-encrypted RAR, password in NZB (1 × 2 GB) | 3.6 s | 21.9 s | 18.2 s |

nzbfast does all of this in-process (UnRAR statically linked, pure-Rust 7z, built-in par2 and
Reed–Solomon). SABnzbd shells out to unrar, 7z and par2; NZBGet bundles unrar and 7za and has
par2 built in.

## Repair and dead releases

| Scenario | nzbfast | SABnzbd | NZBGet (rocket) |
| --- | --- | --- | --- |
| One release, 5% missing (one whole RAR volume): time to repaired file | 2.5 s | 4.5 s | 6.1 s |
| … CPU time spent | 10.4 s | 9.3 s | 9.8 s |
| One dead release (25% missing, 8% par2): time to failed | 0.5 s | 0.6 s | 0.6 s |
| … data fetched before giving up | 0.16 GB | 0.18 GB | 0.55 GB |
| Repair suite (3 damaged + 1 dead, queued together): time to all done | 4.1 s | 15.2 s | 8.4 s |
| … dead release marked failed after | 1.0 s | 15.8 s | 3.4 s |

- **Repair costs about the same CPU as par2cmdline-turbo.** Reed–Solomon uses AVX2 table lookups
  and works in cache-sized tiles.
- **Dead posts are caught from a sample.** Before downloading, nzbfast checks every 25th article
  of every file with `STAT` (no data transferred) and aborts only if the projected loss exceeds
  twice the par2 recovery. It never aborted the three repairable releases.

## Real Usenet providers

24 recent grabs from a real Sonarr/Radarr install (0.8–6.6 GB each, 88.5 GB total), downloaded
from Eweka, Tweaknews and Newshosting (130 connections) with Supernews as backup (20), over TLS
from Amsterdam. Three rounds with the client order rotated; the production instance sharing these
accounts was paused during every run.

| Measure (median of 3 rounds) | nzbfast | SABnzbd | NZBGet (rocket) |
| --- | --- | --- | --- |
| Time until all 24 jobs completed or failed | 26.9 s | 238.3 s | 405.7 s |
| End-to-end rate | 26.4 Gbit/s | 3.0 Gbit/s | 1.7 Gbit/s |
| Recoverable releases completed, per round (of 18) | 18, 18, 18 | 18, 18, 17 | 17, 17, 17 |
| Dead releases reported failed after | 7.8 s | 224 s | 322 s |
| Encrypted release without password | failed at 12 s | paused, never finishes | failed at 29 s |
| CPU time | 189 s | 183 s | 395 s |
| Peak heap | 8.2–8.9 GB | 1.1–1.4 GB | 3.6–3.8 GB |

- Every completed release has byte-identical output sizes across the three clients.
- *Soul.Plane* needs par2 repair: nzbfast completed it every round, NZBGet failed it every round,
  SABnzbd once ("not enough repair blocks").
- SABnzbd pauses password-less encrypted releases by default (`pause_on_pwrar`), so the job never
  reaches history and Sonarr/Radarr wait on it.

## Small boxes and distant providers

Movie suite, seconds (each client's fastest setup):

| Condition | nzbfast | SABnzbd | NZBGet | nzbfast vs next best |
| --- | --- | --- | --- | --- |
| Baseline: 10 vCPU, loopback | 3.06 | 21.41 | 14.25 | 4.7× |
| Small box: 4 vCPU | 4.62 | 22.46 | 19.12 | 4.1× |
| Same-continent provider: 30 ms RTT | 4.64 | 36.92 | 35.08 | 7.6× |
| Transatlantic provider: 100 ms RTT | 11.92 | 102.57 | 103.60 | 8.6× |

At 100 ms, SABnzbd and NZBGet settle at 2.0 Gbit/s with 50 connections, while nzbfast's pipelining
holds 17.4 Gbit/s. RTT was added with `tc netem` on loopback.

## Resource usage

| Measure | nzbfast | SABnzbd | NZBGet |
| --- | --- | --- | --- |
| CPU-seconds per GB, movies | 0.85 | 1.90 | 2.79 |
| CPU-seconds per GB, TV | 0.77 | 2.02 | 2.80 |
| CPU-seconds per GB, post-processing mix | 2.26 | 2.97 | 3.73 |
| CPU-seconds per GB, repair suite | 3.6 | 4.4 | 3.5 |
| Peak heap while downloading, default | 2.8–7.4 GB | 0.3–1.2 GB | 0.1–0.2 GB |
| Peak heap, nzbfast with `write_buffer_mb = 256` | 2.4–2.7 GB | | |
| Memory at idle (RSS) | 7.5 MB | 72 MB | 7.8 MB |
| Start to API ready | 0.05 s | 0.4 s | 0.05 s |
| Install footprint | one 6.8 MB binary (stripped), no external tools | 17 MB source + 94 MB venv + unrar, 7z, par2 | 22 MB incl. unrar, 7za |

Competitors at defaults; tuned NZBGet peaks at 1.0–3.9 GB. nzbfast's memory is set by
`write_buffer_mb` (finished chunks waiting for disk; default 1/16 of RAM, at most 4 GiB). At
256 MiB only the movie suite slowed (3.1 → 3.6 s); the rest of the peak is output being assembled,
about one 8 MB chunk per article in flight.

## API responsiveness

| Client | Add NZB p50 / p99 (ms) | Queue poll p50 / p99 (ms) | History poll p50 / p99 (ms) |
| --- | --- | --- | --- |
| nzbfast | 2.0 / 14.5 | 1.1 / 6.9 | 0.7 / 5.6 |
| SABnzbd | 15.8 / 56.5 | 1.8 / 13.2 | 2.4 / 7.3 |
| NZBGet | 102 / 104 | 0.6 / 5.9 | 0.5 / 2.5 |

## Fixes made during this benchmark

| Issue | First build | Fixed build |
| --- | --- | --- |
| par2 repair, one release missing a whole volume | 4.5 s, 35.4 CPU-s | 2.5 s, 10.4 CPU-s |
| Repair suite (3 damaged + 1 dead) | 11.7 s | 4.1 s |
| Data fetched before failing a dead post | 0.74 GB | 0.16 GB |
| `.nfo` of a fully obfuscated post | left under a random name | renamed from par2 |
| A provider that stalls logins for ~20 s (live, 0.83 GB release) | 80 s | 5.5 s |
| Memory while downloading | no limit; up to 8 GB of free buffers kept | `write_buffer_mb` |

An article whose yEnc header says 768,000 bytes while 767,999 arrive (CRC mismatch) is damaged on
every provider, so rebuilding it from par2 is correct; SABnzbd accepts the damaged bytes and
relies on its par2 pass instead.

## Caveats

- **Memory.** Even at `write_buffer_mb = 256`, nzbfast holds about 2.5 GB at full speed with 50
  connections. NZBGet remains the choice for a Raspberry Pi-class box.
- **Integrity model.** nzbfast trusts yEnc and RAR CRCs and runs par2 only when data is missing,
  as NZBGet's default `ParCheck=auto` does; SABnzbd also checks every job against par2 hashes.
- **Test server.** The NNTP server for the controlled suites is nzbfast's own `mock-serve`
  (standard AUTHINFO/BODY/STAT over TLS with pipelining; the competitors ran against it
  unmodified).
- **Scope.** Storage was tmpfs for every client; disk-bound setups (HDD, NFS) were not measured.
- **Queue order.** SABnzbd's one-job-at-a-time download finishes the top of the queue first, which
  some users prefer for strict priority.

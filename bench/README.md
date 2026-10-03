# Benchmarks

Reproduces [docs/BENCHMARKS.md](../docs/BENCHMARKS.md): nzbfast, SABnzbd and NZBGet
download the same corpus from the same in-RAM NNTP test server, every output is
checked against reference MD5s, and `plot.py` draws the graphs.

## Setup

Everything lives under `$BENCH_ROOT` (default `/root/bench`; the clients' configs use
`/dev/shm/b` for their folders). You need about 80 GB of RAM for the test server and
70 GB of tmpfs for the downloads, 16 vCPUs (clients on 0-9, server on 10-15), Python 3
with matplotlib, and root (the harness drops page caches; `rtt` uses `tc netem`).

```
$BENCH_ROOT/bin/          nzbfast, nntp-mock (cargo build --release -p nzbfast -p nntp-mock),
                          rar, par2 (par2cmdline-turbo), plus 7z on PATH
$BENCH_ROOT/SABnzbd-5.1.3/  SABnzbd source, with its venv in $BENCH_ROOT/sabvenv
$BENCH_ROOT/nzbget/       NZBGet 26.3 (nzbget-26.3-bin-linux.run --destdir)
$BENCH_ROOT/cfg/          nzbfast.toml, sab.ini.tmpl and nzbget.conf.tmpl, made with
                          sed "s|BENCH_ROOT|$BENCH_ROOT|" cfg/nzbget.sed | sed -f - nzbget/nzbget.conf
```

Then build the corpus (71 GB of releases, encoded into 73 GB of articles):

```
./mkcorpus.sh
```

## Running

```
./run.sh mock        # start the test server (keep it running)
./run.sh core        # movies, tv, pp, repair: every client and variant, 3 runs each
./run.sh providers   # provider scenarios, each client's faster variant
./run.sh small       # 4 vCPUs
./run.sh rtt         # 30 and 100 ms round trips (tc netem on lo)
python3 plot.py      # graphs into docs/img, tables on stdout
```

Each run is one fresh client: `harness.py CLIENT SUITE_OR_SCENARIO TAG [--variant
default|tuned]` adds every NZB of the suite through the client's API, waits until all
of them are in its history, verifies the output and writes
`$BENCH_ROOT/results/SUITE.CLIENT.VARIANT.TAG.json` (wall time, CPU, memory, per-job
results, a 0.5 s throughput series, bytes served per test-server port and API latency).

`run.sh real` runs the NZBs in `$BENCH_ROOT/real` against real providers, with server
blocks for each client in `$BENCH_ROOT/prov/{nzbfast-servers.toml,sab-servers.ini,nzbget-servers.conf}`
(keep them mode 600). Other users of the same accounts skew the results, since
connection limits are shared.

## The test server

`nntp-mock serve` holds every article in RAM and answers `BODY`/`STAT` with pipelining
over TLS. Each `--listen` port behaves like a different provider:

| Port | Behaviour |
| --- | --- |
| 5563 | Everything, except the damaged releases' missing articles |
| 5570 | Everything (backup provider) |
| 5571 | Half the TV episodes taken down; "430 not found" takes 500 ms |
| 5572 | 2 MB/s per connection |
| 5573 | At most 20 connections; more get "502 too many connections" |
| 5574 | Accepts connections but stalls the login for 60 s |
| 5575 | Drops the connection mid-article every 50 articles; 412 for 2.5% of articles |
| 5576 | Refuses and drops all connections from 1 s to 6 s into each run |

# Benchmark harness

Reproduces the comparison in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md): nzbfast, SABnzbd and
NZBGet download the same corpus from the same in-RAM TLS NNTP server (`nzbfast mock-serve`), and
every output is checked against reference MD5s.

Layout (override the root with `BENCH_ROOT`; the shell scripts assume `/root/bench`):

```
$BENCH_ROOT/bin/        nzbfast (release build), par2 (par2cmdline-turbo), rar
$BENCH_ROOT/SABnzbd-5.1.3/  + sabvenv/ (pip install -r requirements.txt)
$BENCH_ROOT/nzbget/     NZBGet 26.3 (nzbget-26.3-bin-linux.run --destdir)
$BENCH_ROOT/cfg/        sab.ini.tmpl, nzbfast.toml, nzbget.conf.tmpl (sed -f cfg/nzbget.sed on the stock nzbget.conf)
$BENCH_ROOT/corpus/     made by mkcorpus.sh (71 GB; staging in /dev/shm)
$BENCH_ROOT/mock/       nzbfast mock-gen --out mock corpus/*  (then add the password meta to Enc.E01's NZB)
```

Run:

```
taskset -c 10-15 nzbfast mock-serve --dir mock --port 5563 --tls \
  --drop "Repair.R01.1080p-BENCH.:33" --drop "Repair.R02.1080p-BENCH.3.:0" \
  --drop "Repair.R03.1080p-BENCH.:50" --drop "Dead.D01.1080p-BENCH.:4"
./mock.sh        # (re)start the mock server with the drop rules above
./runall.sh      # movies, tv, pp, repair x {nzbfast, sab, nzbget} x {default, tuned} x 3
./extras.sh      # isolated repair/dead, 4 vCPU, 30/100 ms RTT (tc netem on lo)
./extras2.sh     # the same scenarios for NZBGet tuned
./nfv2.sh        # nzbfast only, every scenario (used after the fixes)
./realrun.sh     # live providers: NZBs in $BENCH_ROOT/real, provider configs in $BENCH_ROOT/prov
```

Live providers need `$BENCH_ROOT/prov/{nzbfast-servers.toml,sab-servers.ini,nzbget-servers.conf}`
(server blocks for each client; keep them mode 600). If a production instance shares the
accounts, pause it around runs (see `prod.sh.example`): shared connection limits and relay
traffic otherwise skew results.

One run: `taskset -c 0-9 python3 harness.py CLIENT SUITE TAG [--variant default|tuned]`.
Results land in `$BENCH_ROOT/results/SUITE.CLIENT.VARIANT.TAG.json` (wall time, CPU, memory,
per-job status and MD5 check, 0.5 s throughput series, API latencies).

#!/bin/bash
# Runs the benchmark matrix; each result lands in $BENCH_ROOT/results and existing
# results are kept, so an interrupted run resumes where it stopped.
#
#   run.sh mock        (re)start the NNTP test server on CPUs 10-15
#   run.sh core        movies, tv, pp, repair suites; every client and variant
#   run.sh providers   provider scenarios (takedown, slow, connection limit, dead, flaky, outage)
#   run.sh small       movies and tv on 4 vCPUs
#   run.sh rtt         movies with 30 and 100 ms round-trip time (tc netem on lo)
#   run.sh real        NZBs in $BENCH_ROOT/real against the providers in $BENCH_ROOT/prov
#
# CLIENTS limits the clients (default "nzbfast sab nzbget"), RUNS the repetitions (3),
# TAG the result tag prefix (r).
R=${BENCH_ROOT:-/root/bench}
H=$(dirname "$(readlink -f "$0")")
RUNS=${RUNS:-3}
TAG=${TAG:-r}
CLIENTS=${CLIENTS:-nzbfast sab nzbget}
export BENCH_ROOT=$R

# The variants each client runs in: nzbfast has no tuning knobs that matter here.
variants() { case $1 in nzbfast) echo default ;; sab) echo default tuned ;; nzbget) echo default tuned ;; esac; }
# Each client's faster variant, used for the scenario runs.
best() { case $1 in nzbget) echo tuned ;; *) echo default ;; esac; }

one() { # client variant suite tag cpus
  [ -f "$R/results/$3.$1.$2.$4.json" ] && return
  rm -rf /dev/shm/b
  timeout 4000 taskset -c "$5" python3 "$H/harness.py" "$1" "$3" "$4" --variant "$2" 2>&1 | tail -1
  pkill -x nzbget; sleep 3
}

odd_episodes() { for i in $(seq -w 1 2 40); do printf ',drop=Show.S01E%s.:0' "$i"; done; }

case ${1:-} in
mock)
  pkill -x nntp-mock; sleep 2; rm -f "$R/mock.log"
  setsid nohup taskset -c 10-15 "$R/bin/nntp-mock" serve --dir "$R/mock" --tls --stats "$R/mock-stats.tsv" --epoch "$R/mock-epoch" \
    --listen 5563,drop=Repair.R01.1080p-BENCH.:33,drop=Repair.R02.1080p-BENCH.3.:0,drop=Repair.R03.1080p-BENCH.:50,drop=Dead.D01.1080p-BENCH.:4 \
    --listen 5570 \
    --listen "5571,miss-ms=500$(odd_episodes)" \
    --listen 5572,conn-mbs=2 \
    --listen 5573,max-conns=20 \
    --listen 5574,login-ms=60000 \
    --listen 5575,reset-every=50,drop=Movie.:40:412 \
    --listen 5576,outage=1-6 \
    > "$R/mock.log" 2>&1 < /dev/null &
  until grep -qs "in RAM" "$R/mock.log"; do sleep 2; done
  cat "$R/mock.log" ;;
core)
  for run in $(seq 1 $RUNS); do for suite in movies tv pp repair rep1 dead1; do for c in $CLIENTS; do for v in $(variants $c); do
    one $c $v $suite $TAG$run 0-9
  done; done; done; done ;;
providers)
  for run in $(seq 1 $RUNS); do for s in takedown slowprov connlimit deadprov flaky outage; do for c in $CLIENTS; do
    one $c $(best $c) $s $TAG$run 0-9
  done; done; done ;;
small)
  for run in $(seq 1 $RUNS); do for c in $CLIENTS; do
    one $c $(best $c) movies cpu4-$TAG$run 0-3; one $c $(best $c) tv cpu4-$TAG$run 0-3
  done; done ;;
rtt)
  for rtt in 30 100; do
    tc qdisc add dev lo root netem delay $((rtt / 2))ms limit 1000000
    for run in $(seq 1 $RUNS); do for c in $CLIENTS; do one $c $(best $c) movies rtt$rtt-$TAG$run 0-9; done; done
    tc qdisc del dev lo root
  done ;;
real)
  # Client order rotates per round so provider-side caching favours no one.
  for run in $(seq 1 $RUNS); do
    set -- $CLIENTS; for _ in $(seq 2 $run); do set -- "${@:2}" "$1"; done
    for c in "$@"; do one $c $(best $c) real $TAG$run 0-15; sleep 30; done
  done ;;
*) sed -n '2,15p' "$0"; exit 2 ;;
esac

#!/bin/bash
# Follow-up scenarios: isolated repair/dead, 4-core box, WAN RTT via netem on loopback.
cd /root/bench
CL=("nzbfast default" "sab default" "nzbget default")
one() { [ -f results/$2.$1.$3.$4.json ] || { timeout 4000 taskset -c $5 python3 scripts/harness.py $1 $2 $4 --variant $3 2>&1 | tail -1; pkill -x nzbget; sleep 3; }; }
for run in r1 r2 r3; do
  for cv in "${CL[@]}"; do set -- $cv; one $1 rep1 $2 $run 0-9; one $1 dead1 $2 $run 0-9; done
done
for run in r1 r2; do
  for cv in "${CL[@]}" "sab tuned"; do set -- $cv; one $1 movies $2 cpu4-$run 0-3; one $1 tv $2 cpu4-$run 0-3; done
done
for rtt in 30 100; do
  tc qdisc add dev lo root netem delay $((rtt/2))ms limit 1000000
  for run in r1 r2; do
    for cv in "${CL[@]}"; do set -- $cv; one $1 movies $2 rtt$rtt-$run 0-9; done
  done
  tc qdisc del dev lo root
done
echo EXTRASDONE

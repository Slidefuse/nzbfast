#!/bin/bash
cd /root/bench
one() { [ -f results/$2.$1.$3.$4.json ] || { timeout 4000 taskset -c $5 python3 scripts/harness.py $1 $2 $4 --variant $3 2>&1 | tail -1; pkill -x nzbget; sleep 3; }; }
while ! grep -q ALLDONE runall2.log; do sleep 5; done
for run in r1 r2 r3; do one nzbget rep1 tuned $run 0-9; one nzbget dead1 tuned $run 0-9; done
for run in r1 r2; do one nzbget movies tuned cpu4-$run 0-3; one nzbget tv tuned cpu4-$run 0-3; done
for rtt in 30 100; do
  tc qdisc add dev lo root netem delay $((rtt/2))ms limit 1000000
  for run in r1 r2; do one nzbget movies tuned rtt$rtt-$run 0-9; done
  tc qdisc del dev lo root
done
echo EXTRAS2DONE

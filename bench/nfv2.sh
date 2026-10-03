#!/bin/bash
# Patched nzbfast: every mock scenario again (prod paused for the batch).
cd /root/bench
scripts/prod.sh pause
one() { [ -f results/$2.nzbfast.default.$3.json ] || { rm -rf /dev/shm/b; timeout 900 taskset -c $1 python3 scripts/harness.py nzbfast $2 $3 2>&1 | tail -1; }; }
for r in 1 2 3; do for s in movies tv pp repair rep1 dead1; do one 0-9 $s n2-r$r; done; done
for r in 1 2; do one 0-3 movies n2cpu4-r$r; one 0-3 tv n2cpu4-r$r; done
for rtt in 30 100; do
  tc qdisc add dev lo root netem delay $((rtt/2))ms limit 1000000
  for r in 1 2; do one 0-9 movies n2rtt$rtt-r$r; done
  tc qdisc del dev lo root
done
# low-memory setting, for the report
cp cfg/nzbfast.toml cfg/nzbfast.toml.orig
sed "s/^ui_auth = false/ui_auth = false\nwrite_buffer_mb = 256/" cfg/nzbfast.toml.orig > cfg/nzbfast.toml
for s in movies tv pp; do one 0-9 $s n2wb256-r1; done
cp cfg/nzbfast.toml.orig cfg/nzbfast.toml
scripts/prod.sh resume
echo NFV2DONE

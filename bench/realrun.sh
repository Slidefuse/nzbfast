#!/bin/bash
# Real-provider round: all NZBs in $BENCH_ROOT/real, 3 rounds, client order rotated so
# provider-side caching does not systematically favour one client. Prod shares the
# provider accounts and relays through this host, so it is paused for each run.
cd /root/bench
run=0
for order in "nzbfast:default sab:default nzbget:tuned" "sab:default nzbget:tuned nzbfast:default" "nzbget:tuned nzbfast:default sab:default"; do
  run=$((run+1))
  for cv in $order; do
    c=${cv%%:*}; v=${cv##*:}
    [ -f results/real.$c.$v.r$run.json ] && continue
    scripts/prod.sh pause
    rm -rf /dev/shm/b
    timeout 3600 python3 scripts/harness.py $c real r$run --variant $v 2>&1 | tail -1
    pkill -x nzbget
    scripts/prod.sh resume
    sleep 60   # give prod a minute between runs
  done
done
rm -rf /dev/shm/b
echo REALDONE

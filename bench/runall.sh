#!/bin/bash
cd /root/bench
for run in r1 r2 r3; do
  for suite in movies tv pp repair; do
    for cv in "nzbfast default" "sab default" "sab tuned" "nzbget default" "nzbget tuned"; do
      set -- $cv
      [ -f results/$suite.$1.$2.$run.json ] && continue
      timeout 4000 taskset -c 0-9 python3 scripts/harness.py $1 $suite $run --variant $2 2>&1 | tail -1
      pkill -x nzbget; sleep 3
    done
  done
done
echo ALLDONE

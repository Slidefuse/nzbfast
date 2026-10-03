#!/bin/bash
# (Re)start the benchmark NNTP server on CPUs 10-15.
cd /root/bench
for p in $(pgrep -x nzbfast); do grep -qa mock-serve /proc/$p/cmdline && kill $p; done
sleep 2
setsid nohup taskset -c 10-15 bin/nzbfast mock-serve --dir /root/bench/mock --port 5563 --tls \
  --drop "Repair.R01.1080p-BENCH.:33" --drop "Repair.R02.1080p-BENCH.3.:0" \
  --drop "Repair.R03.1080p-BENCH.:50" --drop "Dead.D01.1080p-BENCH.:4" > mock.log 2>&1 < /dev/null &
for i in $(seq 1 60); do grep -q "in RAM" mock.log && break; sleep 3; done
cat mock.log

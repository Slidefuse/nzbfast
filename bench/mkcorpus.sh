#!/bin/bash
# Build the benchmark corpus: unique random payloads packaged like real Usenet posts.
set -e
B=/root/bench/bin; C=/root/bench/corpus; W=/dev/shm/corpus-work
rnd() { openssl enc -aes-128-ctr -pass pass:"$1" -nosalt -pbkdf2 </dev/zero 2>/dev/null | head -c "$2"; }
par() { (cd "$1" && $B/par2 c -q -q -r${3:-8} -n7 "$2.par2" * >/dev/null); }
mkrel() { # name payload_bytes kind vol
  local n=$1 sz=$2 kind=$3 vol=${4:-100m}; local d=$C/$n; rm -rf $d $W/$n; mkdir -p $d $W/$n
  rnd "$n" "$sz" > $W/$n/$n.mkv
  case $kind in
    rar)  (cd $W/$n && $B/rar a -idq -m0 -ma5 -v$vol $d/$n.rar $n.mkv) ;;
    rarc) (cd $W/$n && $B/rar a -idq -m3 -ma5 -v$vol $d/$n.rar $n.mkv) ;;
    rarp) (cd $W/$n && $B/rar a -idq -m0 -ma5 -hpbenchpass -v$vol $d/$n.rar $n.mkv) ;;
    7z)   (cd $W/$n && 7z a -bd -mx1 -v$vol $d/$n.7z $n.mkv >/dev/null) ;;
    plain) mv $W/$n/$n.mkv $d/ ;;
  esac
  echo "nfo $n" > $d/$n.nfo
  par $d $n 8
  if [ "$kind" = obf ]; then :; fi
  rm -rf $W/$n
}
obfuscate() { # rename every file in release to random hex (names recoverable from par2)
  local d=$C/$1; for f in $d/*; do mv "$f" "$d/$(openssl rand -hex 12)"; done
}
G=1000000000
for i in 1 2 3 4 5 6; do mkrel Movie.M0$i.2160p.WEB-DL-BENCH $((4*G)) rar 100m & done; wait
for i in $(seq -w 1 40); do mkrel Show.S01E$i.1080p.WEB-BENCH $((400*1000000)) rar 50m & [ $((10#$i % 10)) = 0 ] && wait; done; wait
for i in 1 2 3 4; do mkrel Plain.P0$i.1080p.WEB-BENCH $((2*G)) plain & done; wait
for i in 1 2; do mkrel Comp.C0$i.1080p-BENCH $((2*G)) rarc 100m & done
for i in 1 2; do mkrel SevenZ.Z0$i.1080p-BENCH $((2*G)) 7z 100m & done
for i in 1 2; do mkrel Obf.O0$i.1080p-BENCH $((2*G)) rar 100m & done
mkrel Enc.E01.1080p-BENCH $((2*G)) rarp 100m &
for i in 1 2 3; do mkrel Repair.R0$i.1080p-BENCH $((2*G)) rar 100m & done
mkrel Dead.D01.1080p-BENCH $((2*G)) rar 100m &
wait
obfuscate Obf.O01.1080p-BENCH; obfuscate Obf.O02.1080p-BENCH
# Reference checksums of payloads (regenerated deterministically)
for d in $C/*; do n=$(basename $d); echo "$(rnd $n $(case $n in Movie*) echo $((4*G));; Show*) echo 400000000;; *) echo $((2*G));; esac) | md5sum | cut -d' ' -f1)  $n"; done > /root/bench/corpus.md5
du -sh $C

#!/bin/bash
# One-time cutover of prod to the direct-grab feeder:
#  - deploys feeder.py (direct grab into nzbfast + ManualImport; no release/push for missing items) and ui.html
#  - adds nzbfast categories feeder-series / feeder-movies (not watched by Radarr/Sonarr), restarts nzbfast
#  - removes the NZB cache config and its three ufw rules (port 18087), which are no longer needed
#  - starts the feeder, with its status UI on 127.0.0.1:18088, proxied by nginx at /feeder/ (same login as nzbfast)
set -euo pipefail
cd "$(dirname "$0")"
H=root@tuner.down.lol
ssh $H '! ss -ltn | grep -q ":18088 "' || { echo "port 18088 already in use on prod"; exit 1; }
scp -q feeder.py ui.html $H:/usr/local/lib/nzbfast-feeder/
ssh $H bash -s <<'REMOTE'
set -e
systemctl stop nzbfast-feeder

T=/etc/nzbfast/nzbfast.toml
if ! grep -q "feeder-series" $T; then
  cp $T $T.pre-feeder
  cat >> $T <<'EOF'

# nzbfast-feeder grabs: categories Radarr/Sonarr do not watch (the feeder imports them itself)
[[categories]]
name = "feeder-series"
dir = "FeederSeries"

[[categories]]
name = "feeder-movies"
dir = "FeederMovies"
EOF
fi
systemctl restart nzbfast
for i in $(seq 60); do ss -ltn | grep -q "127.0.0.1:18086 " && break; sleep 1; done
K=$(sed -n "s/^api_key *= *//p" /mnt/mfast/sabnzbd/config/sabnzbd.ini | head -1)
curl -s "http://127.0.0.1:18086/api?mode=get_cats&output=json&apikey=$K"; echo

python3 - <<'PY'
import json
p = "/etc/nzbfast/feeder.json"
c = json.load(open(p))
c.pop("nzb_cache", None); c.pop("push_concurrency", None)
c["categories"] = {"radarr": "feeder-movies", "sonarr": "feeder-series"}
c.setdefault("ui_listen", "127.0.0.1:18088")
json.dump(c, open(p, "w"), indent=1)
PY
chmod 600 /etc/nzbfast/feeder.json

for ip in 172.18.0.1 172.17.0.1 192.168.88.7; do
  ufw delete allow in on br-350dfac8ed81 to $ip port 18087 proto tcp || true
done
rm -rf /var/lib/nzbfast-feeder/nzb
systemctl start nzbfast-feeder; sleep 2; systemctl is-active nzbfast-feeder

N=/etc/nginx/sites-available/nzbfast
if ! grep -q "location /feeder/" $N; then
  cp $N /root/nginx-nzbfast.pre-feeder
  python3 - $N <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
loc = """    location = /feeder { return 301 /feeder/; }
    location /feeder/ {
        proxy_pass http://127.0.0.1:18088/;
        proxy_http_version 1.1;
        proxy_read_timeout 60;
    }
    location / {"""
open(p, "w").write(s.replace("    location / {", loc, 1))
PY
  if nginx -t 2>/dev/null; then systemctl reload nginx
  else cp /root/nginx-nzbfast.pre-feeder $N; echo "nginx test failed; restored"; nginx -t || true; fi
fi
sleep 3
curl -s -o /dev/null -w "feeder UI %{http_code}\n" http://127.0.0.1:18088/
REMOTE

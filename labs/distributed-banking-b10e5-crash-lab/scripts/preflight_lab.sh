#!/usr/bin/env bash
# Read-only host checks except for initializing a new LAB-only .env file.
set -euo pipefail
cd "$(dirname "$0")/.."
test "$(basename "$PWD")" = distributed-banking-b10e5-crash-lab || { echo "WRONG_LAB_DIRECTORY"; exit 1; }
command -v docker >/dev/null
command -v python3 >/dev/null
if ! docker image inspect "${LAB_APP_IMAGE:-distributed-banking-platform:phase8a-20261107}" >/dev/null 2>&1; then
  echo "MISSING_EXISTING_PHASE8A_IMAGE - do not build or pull automatically"; exit 1
fi
docker image inspect postgres:17-alpine >/dev/null 2>&1 || { echo "MISSING_POSTGRES_17_ALPINE_IMAGE"; exit 1; }
if [ ! -f .env ]; then
  # Never invent credentials for an existing Compose project or its durable volumes.
  if docker ps -a --filter label=com.docker.compose.project=distributed-banking-b10e5-crash \
       --format '{{.ID}}' | grep -q .; then
    echo 'EXISTING_LAB_WITHOUT_ENV_REFUSE'; exit 1
  fi
  if docker volume ls --filter label=com.docker.compose.project=distributed-banking-b10e5-crash \
       --format '{{.Name}}' | grep -q .; then
    echo 'EXISTING_LAB_VOLUMES_WITHOUT_ENV_REFUSE'; exit 1
  fi
  umask 077
  python3 - <<'PY'
import secrets
from pathlib import Path
p=Path('.env')
with p.open('x', encoding='ascii') as f:
    for shard in ('A','B','C'):
        f.write(f'LAB_{shard}_PASSWORD={secrets.token_hex(24)}\n')
    f.write('LAB_ROUTING_EPOCH=1\nLAB_B_LAST=2000\nLAB_GATEWAY_PORT=18082\n')
print('BONUS10E5_NEW_ISOLATED_ENV_CREATED')
PY
fi
chmod 600 .env
if [ -f .env ]; then
  grep -qx 'LAB_ROUTING_EPOCH=1' .env || { echo "ENV_ALREADY_AT_POST_CUTOVER_STAGE"; exit 1; }
fi
python3 - <<'PY'
import json
from pathlib import Path
cfg=json.loads(Path('config/active_map.json').read_text())
assert cfg['version']==1 and len(cfg['shards'])==2, 'Active map was already switched'
print('BONUS10E5_MAP1_VERIFIED')
PY
if docker compose -f compose.b10e5.yaml config --format json | python3 -c 'import sys,json; data=json.load(sys.stdin); assert data["name"]=="distributed-banking-b10e5-crash"; print("BONUS10E5_EXCLUSIVE_PROJECT_OK")'; then :; else exit 1; fi
python3 - <<'PY'
from pathlib import Path
memory=Path('/proc/meminfo').read_text()
line=next(x for x in memory.splitlines() if x.startswith('MemAvailable:'))
avail=int(line.split()[1]); print('MEM_AVAILABLE_MIB=',avail//1024)
assert avail>=1536*1024, 'Insufficient available RAM for isolated integrated lab'
import shutil
free=shutil.disk_usage('/').free
print('DISK_FREE_MIB=',free//1048576)
assert free>=1200*1048576, 'Insufficient free disk for isolated experiment'
PY
if command -v ss >/dev/null; then
  if ss -lnt "( sport = :18082 )" | grep -q ':18082'; then
    echo "LAB_GATEWAY_PORT_IN_USE"; exit 1
  fi
fi
curl -fsS --max-time 5 http://127.0.0.1:8001/ready >/dev/null || { echo "ORIGINAL_GATEWAY_NOT_READY"; exit 1; }
python3 -m compileall -q scripts/lab_operator.py bank_platform
printf '\nBONUS10E4D_READ_ONLY_SAFETY_PREFLIGHT_PASS\n'

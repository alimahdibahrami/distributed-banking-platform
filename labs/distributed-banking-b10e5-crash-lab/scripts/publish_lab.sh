#!/usr/bin/env bash
# This is an explicitly operator-invoked, isolated, controlled restart.
# DO NOT use for live production: this mode has a short global restart window.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || exit 1
DC=(docker compose --env-file .env -f compose.b10e5.yaml)
# Allow idempotent invocation ONLY if already approved. Avoid proceeding
# from an empty C, missing original source fence, or dangling 2PC.
env -u B10E5_FAULT_POINT bash scripts/operator.sh prepublish
if grep -qx 'LAB_ROUTING_EPOCH=1' .env; then
  python3 - <<'PY'
from pathlib import Path
p=Path('config/active_map.json')
s=Path('config/shard_map_3shard.example.json').read_bytes()
import json
assert json.loads(s)['version']==2 and len(json.loads(s)['shards'])==3
q=p.with_suffix('.json.next')
q.write_bytes(s)
q.replace(p)  # atomic rename inside a bind-mounted *directory*
e=Path('.env')
t=e.read_text()
assert t.count('LAB_ROUTING_EPOCH=1\n')==1 and t.count('LAB_B_LAST=2000\n')==1
e.write_text(t.replace('LAB_ROUTING_EPOCH=1\n','LAB_ROUTING_EPOCH=2\n').replace('LAB_B_LAST=2000\n','LAB_B_LAST=1500\n'))
PY
elif grep -qx 'LAB_ROUTING_EPOCH=2' .env; then
  echo 'LAB_MAP_ALREADY_VERSION2 - reconciliation path'
else
  echo 'INVALID_ROUTING_EPOCH'; exit 1
fi
# Crash after BOTH durable files are replaced but before any service roll-out.
# Simulates operator crash between publishing config and restarting services.
if [ "${B10E5_FAULT_POINT:-}" = 'PUBLISH_AFTER_MAP' ]; then
  echo 'BONUS10E5_EXPECTED_CRASH: PUBLISH_AFTER_MAP' >&2
  exit 86
elif [ -n "${B10E5_FAULT_POINT:-}" ]; then
  echo 'INVALID_PUBLISH_CRASH_POINT' >&2
  exit 2
fi
# Re-evaluate .env interpolation only after file replacement.
DC=(docker compose --env-file .env -f compose.b10e5.yaml)
"${DC[@]}" --profile post up -d --no-deps --force-recreate \
  bank_a1 bank_a2 bank_b1 bank_b2 bank_c1 bank_c2 coordinator
"${DC[@]}" up -d --no-deps --force-recreate gateway
"${DC[@]}" --profile post ps
READY=0
for attempt in $(seq 1 45); do
  if response=$(curl -fsS --max-time 3 http://127.0.0.1:18082/ready 2>/dev/null); then
    if printf '%s\n' "$response" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d.get("banking_ready") is True; assert d.get("routing_epoch")==2; assert d.get("epoch_coherent") is True; assert d.get("healthy_replicas")=={"A":2,"B":2,"C":2}' 2>/dev/null; then
      READY=1; echo "$response"; break
    fi
  fi
  sleep 2
done
[ "$READY" = 1 ] || { echo 'B10E5_GATEWAY_NOT_READY_AFTER_PUBLISH'; exit 1; }
bash scripts/operator.sh verify-live
curl -fsS --max-time 5 http://127.0.0.1:8001/ready; echo
systemctl is-active distributed-banking-selfheal
echo 'BONUS10E5_CRASH_LAB_PUBLICATION_RESUMED_PASS'

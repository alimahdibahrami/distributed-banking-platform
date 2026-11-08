#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo 'Run preflight_lab.sh first'; exit 1; }
grep -qx 'LAB_ROUTING_EPOCH=1' .env || { echo 'NOT_FRESH_PRE_CUTOVER_LAB'; exit 1; }
DC=(docker compose --env-file .env -f compose.b10e5.yaml)
"${DC[@]}" config --quiet
# All containers/volumes are namespaced by the dedicated Compose project.
"${DC[@]}" up -d --wait shard_a_db shard_b_db shard_c_db
"${DC[@]}" up -d shard_a_init shard_b_init shard_c_init coordinator_state_init migration_state_init
for service in shard_a_init shard_b_init shard_c_init coordinator_state_init migration_state_init; do
  id=$("${DC[@]}" ps -aq "$service")
  [ -n "$id" ] || { echo "INIT_CONTAINER_MISSING:$service"; exit 1; }
  result=$(docker wait "$id")
  [ "$result" = 0 ] || { echo "INIT_FAILED:$service:$result"; "${DC[@]}" logs --tail 80 "$service"; exit 1; }
done
"${DC[@]}" up -d --wait bank_a1 bank_a2 bank_b1 bank_b2 coordinator gateway
"${DC[@]}" ps
for attempt in $(seq 1 40); do
  if response=$(curl -fsS --max-time 3 http://127.0.0.1:18082/ready 2>/dev/null); then
    if printf '%s\n' "$response" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["banking_ready"] is True; assert d["routing_epoch"]==1; assert d["healthy_replicas"]=={"A":2,"B":2}' 2>/dev/null; then
      echo "$response"
      break
    fi
  fi
  if [ "$attempt" -eq 40 ]; then echo "B10E5_GATEWAY_BOOTSTRAP_NOT_READY"; exit 1; fi
  sleep 2
done
"${DC[@]}" --profile ops run -T --rm --no-deps operator status </dev/null
echo 'BONUS10E4D_ISOLATED_2SHARD_BASELINE_PASS'

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "No isolated .env"; exit 1; }
[ "$#" -eq 1 ] || { echo "usage: bash scripts/operator.sh {status|capture|snapshot|exercise|fence|promote|prepublish|verify-live|post-2pc}"; exit 1; }
case "$1" in status|capture|snapshot|exercise|fence|promote|prepublish|verify-live|post-2pc) ;; *) exit 2;; esac
OPTS=()
if [ -n "${B10E5_FAULT_POINT:-}" ]; then
  case "$B10E5_FAULT_POINT" in
    CAPTURE_AFTER_COMMIT|SNAPSHOT_AFTER_COMMIT|FENCE_AFTER_COMMIT|PROMOTE_AFTER_C_ACTIVATION|PROMOTE_AFTER_B_CLEANUP) ;;
    *) echo "BAD_FAULT_POINT" >&2; exit 2;;
  esac
  case "$1:$B10E5_FAULT_POINT" in
    capture:CAPTURE_AFTER_COMMIT|snapshot:SNAPSHOT_AFTER_COMMIT|fence:FENCE_AFTER_COMMIT|promote:PROMOTE_AFTER_C_ACTIVATION|promote:PROMOTE_AFTER_B_CLEANUP) ;;
    *) echo "FAULT_ACTION_MISMATCH" >&2; exit 2;;
  esac
  OPTS=(-e "B10E5_FAULT_POINT=$B10E5_FAULT_POINT")
fi
docker compose --env-file .env -f compose.b10e5.yaml --profile ops run -T --rm --no-deps "${OPTS[@]}" operator "$1" </dev/null

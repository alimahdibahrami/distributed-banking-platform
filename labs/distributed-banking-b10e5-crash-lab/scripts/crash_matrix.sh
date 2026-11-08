#!/usr/bin/env bash
# Destructive-to-LAB-only crash injection; never invoke against a completed or production project.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo 'RUN_PREFLIGHT_FIRST'; exit 1; }
grep -qx 'LAB_ROUTING_EPOCH=1' .env || { echo 'NOT_FRESH_LAB_EPOCH1'; exit 1; }
python3 - <<'PY'
import json
from pathlib import Path
assert json.loads(Path('config/active_map.json').read_text())['version']==1
PY

python3 scripts/check_fresh.py

# One single new isolated lab, each step is retried only after a verified injected crash.
expect_crash() {
  local point="$1" action="$2" rc=0
  echo "========== INJECT $point =========="
  set +e
  B10E5_FAULT_POINT="$point" bash scripts/operator.sh "$action"
  rc=$?
  set -e
  [ "$rc" -eq 86 ] || { echo "UNEXPECTED_OPERATOR_EXIT:$point:$rc"; exit 1; }
  echo "B10E5_EXPECTED_CRASH_EXIT_86:$point"
  bash scripts/operator.sh status
}
resume() {
  echo "========== RECOVER $1 =========="
  bash scripts/operator.sh "$1"
  bash scripts/operator.sh status
}
bash scripts/operator.sh status
expect_crash CAPTURE_AFTER_COMMIT capture
resume capture
expect_crash SNAPSHOT_AFTER_COMMIT snapshot
resume snapshot
# Financial traffic once only: no retries of unknown outcome
bash scripts/operator.sh exercise
expect_crash FENCE_AFTER_COMMIT fence
resume fence
expect_crash PROMOTE_AFTER_C_ACTIVATION promote
resume promote
bash scripts/operator.sh prepublish
rc=0
set +e
B10E5_FAULT_POINT=PUBLISH_AFTER_MAP bash scripts/publish_lab.sh
rc=$?
set -e
[ "$rc" -eq 86 ] || { echo "UNEXPECTED_PUBLISH_EXIT:$rc"; exit 1; }
echo 'B10E5_EXPECTED_CRASH_EXIT_86:PUBLISH_AFTER_MAP'
bash scripts/operator.sh status
# The next step publishes only once, checks gateway health, and measures availability
bash scripts/measure_publish.sh
bash scripts/operator.sh status
bash scripts/operator.sh post-2pc
bash scripts/operator.sh status
python3 scripts/verify_final.py
echo 'BONUS10E5_INTEGRATED_CRASH_RESUME_MATRIX_PASS'

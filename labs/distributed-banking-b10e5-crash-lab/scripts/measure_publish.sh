#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
grep -qx 'LAB_ROUTING_EPOCH=2' .env
python3 scripts/publish_availability.py

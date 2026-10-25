#!/bin/sh
# Read-only VPS checks before first deployment. Does not touch Assignment 1.
set -eu
printf '\n== Docker versions ==\n'
docker --version
docker compose version
printf '\n== Existing containers ==\n'
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}'
printf '\n== Relevant TCP listeners ==\n'
if command -v ss >/dev/null 2>&1; then
  ss -lnt | grep -E '(:8000|:8001|State)' || true
else
  printf '%s\n' 'ss unavailable; inspect docker ps above'
fi
printf '\n== Disk space ==\n'
df -h .
printf '\n== Existing Compose projects ==\n'
docker compose ls
printf '\nREAD-ONLY PREFLIGHT COMPLETE. No container or volume was changed.\n'

#!/bin/sh
set -eu
# Run only after unpacking this source into an empty/new GitHub repository.
# This does not push or set up a remote automatically.
git add .
git commit -m "chore: initialize distributed banking scaffold"

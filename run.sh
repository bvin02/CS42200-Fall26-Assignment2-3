#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker build -t cs422-a2-a3 .
exec docker run --rm --network host \
  -v "$PWD/inputs:/work/inputs" \
  -v "$PWD/results:/work/results" \
  cs422-a2-a3 "$@"

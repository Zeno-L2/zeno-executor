#!/usr/bin/env bash
# Runs the devnet integration test against a freshly wiped go-zenon devnet built from ../go-zenon,
# the checkout go.mod's replace directive names. The devnet runs as its own Docker Compose project,
# so a wipe removes only that project's containers, network and volumes. The devnet is wiped again
# when the test passes and left running for inspection when it fails.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
zenon=$root/../go-zenon
project=go-executor-devnet

compose() {
  docker compose -f "$zenon/docker-compose.yml" -p "$project" "$@"
}

# wipe removes this project's containers, network and volumes, and nothing else.
wipe() {
  if [ -z "$project" ]; then
    echo "refusing to remove volumes: no Docker Compose project name" >&2
    exit 1
  fi
  compose down -v
}

wipe
compose up -d --build pillar pillar2 pillar3 rpc observer

status=0
(cd "$root" && go test -tags devnet -count=1 -timeout 60m -v ./internal/devnet/) || status=$?

if [ "$status" -eq 0 ]; then
  wipe
else
  echo "devnet left running for inspection as Docker Compose project $project; the next run of this script wipes it" >&2
fi
exit "$status"

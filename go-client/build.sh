#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p bin
NAME=agent-benchmark-go
if [ "$(go env GOOS)" = windows ]; then NAME="$NAME.exe"; fi
go build -trimpath -o "bin/$NAME" .
printf 'Built %s/bin/%s\n' "$PWD" "$NAME"

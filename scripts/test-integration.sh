#!/usr/bin/env bash
# Builds the integration tests once to a fixed path and runs them from there.
# The Windows firewall remembers "Allow access" per program path. Plain
# `go test` builds into a new temp folder on every run, so it asks every time.
#
# Usage: scripts/test-integration.sh [test binary flags]
#   scripts/test-integration.sh -test.run TestPubSub -test.v
set -euo pipefail
cd "$(dirname "$0")/.."

exe=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) exe=".exe" ;; esac
bin="bin/integration.test${exe}"

mkdir -p bin
go test -c -o "$bin" ./test/integration
cd test/integration
exec "../../$bin" -test.count=1 -test.timeout=300s "$@"

#!/usr/bin/env bash
#
# Runs psc.go. If Go is installed it is compiled first; otherwise the prebuilt
# bin/psc_go.<os>-<arch> from build_binaries.bash is used (e.g. on a VM).
#
# Credentials come from the environment - never hardcode them here, this file
# is version controlled. Export them first, e.g. from a cluster env file:
#
#   source ../sc_api/cluster-<id>.env     # sets USERNAME/PASSWORD, used below
#
# or set them directly:
#
#   export SCYLLA_PASSWORD='...'
#   ./run_psc_go.bash

set -euo pipefail
cd "$(dirname "$0")"

export SCYLLA_PSC_DNS="${SCYLLA_PSC_DNS:-scylla-psc-nr67594-df67f06f.clusters.scylla.cloud}"
export SCYLLA_PSC_CONN_ID="${SCYLLA_PSC_CONN_ID:-dfb43f71-ee86-51a8-b18d-0b59023aef17}"
# Discovery port: 9000-9002 reach the cluster for discovery; the driver then
# follows system.client_routes to the per-node ports (9003-9005).
export SCYLLA_PSC_PORT="${SCYLLA_PSC_PORT:-9000}"

export SCYLLA_USER="${SCYLLA_USER:-${USERNAME:-scylla}}"
SCYLLA_PASSWORD="${SCYLLA_PASSWORD:-${PASSWORD:-}}"
: "${SCYLLA_PASSWORD:?set SCYLLA_PASSWORD before running (see the comments at the top of this file)}"
export SCYLLA_PASSWORD

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  aarch64|arm64) arch=arm64 ;;
  x86_64)        arch=amd64 ;;
  *)             arch=$(uname -m) ;;
esac
bin="bin/psc_go.${os}-${arch}"

if command -v go >/dev/null; then
  mkdir -p bin
  go build -o "$bin" psc.go
fi
[[ -x "$bin" ]] || { echo "$bin not found - run ./build_binaries.bash ${os}/${arch} and rsync bin/" >&2; exit 1; }
"./$bin" "$@"

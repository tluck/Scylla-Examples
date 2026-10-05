#!/usr/bin/env bash
#
# Compiles psc.go (into ./psc_go) and runs it.
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

go build -o psc_go psc.go
./psc_go "$@"

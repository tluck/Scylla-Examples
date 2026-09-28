#!/usr/bin/env bash
#
# Credentials come from the environment - never hardcode them here, this file
# is version controlled.
#
#   export SCYLLA_PASSWORD='...'
#   ./run_benchmark_psc.bash <connection_id>

connection_id=${1:-1}

SCYLLA_NODES=${SCYLLA_NODES:-endpoint.cluster-1.scylladb.com:9001}
SCYLLA_USER=${SCYLLA_USER:-scylla}
: "${SCYLLA_PASSWORD:?set SCYLLA_PASSWORD before running}"

set -x
scylla-bench \
  -workload uniform \
  -mode mixed \
  -nodes "$SCYLLA_NODES" \
  -username "$SCYLLA_USER" \
  -password "$SCYLLA_PASSWORD" \
  -replication-factor 3 \
  -concurrency 512 \
  -duration 300s \
  -client-routes-connection-ids "${connection_id}"

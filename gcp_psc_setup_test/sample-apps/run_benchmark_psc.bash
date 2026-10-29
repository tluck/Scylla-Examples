#!/usr/bin/env bash
#
# Credentials come from the environment - never hardcode them here, this file
# is version controlled.
#
#   export SCYLLA_PASSWORD='...'
#   ./run_benchmark_psc.bash <connection_id>
#
# Uses bin/scylla-bench.<os>-<arch> from build_binaries.bash, falling back to
# scylla-bench on the PATH.

cd "$(dirname "$0")"

connection_id=${1:-1}

SCYLLA_NODES=${SCYLLA_NODES:-endpoint.cluster-1.scylladb.com:9000}
SCYLLA_USER=${SCYLLA_USER:-scylla}
: "${SCYLLA_PASSWORD:?set SCYLLA_PASSWORD before running}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  aarch64|arm64) arch=arm64 ;;
  x86_64)        arch=amd64 ;;
  *)             arch=$(uname -m) ;;
esac
scylla_bench="bin/scylla-bench.${os}-${arch}"
if [[ ! -x "$scylla_bench" ]]; then
  scylla_bench=$(command -v scylla-bench) ||
    { echo "no scylla-bench found - run ./build_binaries.bash ${os}/${arch} and rsync bin/" >&2; exit 1; }
fi

set -x
"$scylla_bench" \
  -workload uniform \
  -mode mixed \
  -nodes "$SCYLLA_NODES" \
  -username "$SCYLLA_USER" \
  -password "$SCYLLA_PASSWORD" \
  -replication-factor 3 \
  -concurrency 512 \
  -duration 300s \
  -client-routes-connection-ids "${connection_id}"

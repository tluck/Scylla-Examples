#!/usr/bin/env bash
#
# Runs scylla-bench through the PSC endpoint. Settings and credentials: see psc_env.bash.
#
#   PSC_ENV=psc-<connection_name>.env ./run_benchmark_psc.bash [extra scylla-bench flags]
#
# TLS: scylla-bench has no server-name option, so it encrypts with the CA but does not verify the server.
# Uses bin/scylla-bench.<os>-<arch> from build_binaries.bash, falling back to
# scylla-bench on the PATH.

set -euo pipefail
cd "$(dirname "$0")"
source ./psc_env.bash

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

extra=()
[[ -n ${SCYLLA_CA_FILE} ]] && extra+=(-tls -tls-ca-cert-file "${SCYLLA_CA_FILE}")
[[ -n ${SCYLLA_DC} ]] && extra+=(-datacenter "${SCYLLA_DC}")

set -x
"$scylla_bench" \
  -workload uniform \
  -mode mixed \
  -nodes "${SCYLLA_PSC_DNS}:${SCYLLA_PSC_PORT}" \
  -username "$SCYLLA_USER" \
  -password "$SCYLLA_PASSWORD" \
  -replication-factor 3 \
  -concurrency 512 \
  -duration 300s \
  -client-routes-connection-ids "${SCYLLA_PSC_CONN_ID}" \
  ${extra[@]+"${extra[@]}"} "$@"

#!/usr/bin/env bash
#
# Runs psc.go. If Go is installed it is compiled first; otherwise the prebuilt
# bin/psc_go.<os>-<arch> from build_binaries.bash is used (e.g. on a VM).
# Settings and credentials: see psc_env.bash.
#
#   PSC_ENV=psc-<connection_name>.env ./run_sample_go.bash

set -euo pipefail
cd "$(dirname "$0")"
source ./psc_env.bash

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

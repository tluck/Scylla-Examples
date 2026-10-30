#!/usr/bin/env bash
#
# Builds static scylla-bench and psc_go binaries into ./bin for the VMs these
# folders get rsynced to (which may have no internet or Go toolchain).
# Binaries are git-ignored - build them locally, then rsync.
#
#   ./build_binaries.bash                            # linux/arm64 (default)
#   ./build_binaries.bash linux/arm64 linux/amd64    # one or more os/arch targets
#
# Output: bin/scylla-bench.<os>-<arch> and bin/psc_go.<os>-<arch>

set -euo pipefail
cd "$(dirname "$0")"

SCYLLA_BENCH_VERSION="${SCYLLA_BENCH_VERSION:-v1.3.3}"
targets=("${@:-linux/arm64}")

mkdir -p bin
out="$PWD/bin"

# Fetch scylla-bench into the Go module cache and build it from there, so its
# own go.mod (including the scylladb/gocql replace) is honored.
sb_dir=$(go mod download -json "github.com/scylladb/scylla-bench@${SCYLLA_BENCH_VERSION}" |
  sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')

for target in "${targets[@]}"; do
  os=${target%/*}
  arch=${target#*/}
  echo "building ${os}/${arch} (scylla-bench ${SCYLLA_BENCH_VERSION})"
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -o "$out/psc_go.${os}-${arch}" psc.go
  (cd "$sb_dir" && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -o "$out/scylla-bench.${os}-${arch}" .)
done

ls -l bin

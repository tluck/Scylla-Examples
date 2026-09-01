#!/bin/bash
# Install Go, defaulting to the current stable release.
#
#   ./install_go.bash              # latest stable, auto-detected OS/arch
#   GO_VERSION=go1.25.7 ./install_go.bash   # pin a specific version
#
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/usr/local}"

# ------
# Latest stable version, straight from go.dev (e.g. "go1.27.1").
# Override with GO_VERSION to pin a release.
GO_VERSION="${GO_VERSION:-$(curl -fsSL 'https://go.dev/VERSION?m=text' | head -n1)}"
if [[ -z "$GO_VERSION" ]]; then
    echo "Could not determine the latest Go version; set GO_VERSION and retry." >&2
    exit 1
fi

# Confirm architecture — uname's names differ from Go's (aarch64 -> arm64).
echo "Detected: $(uname -s) $(uname -m)"
case "$(uname -s)" in
    Linux)  GOOS=linux  ;;
    Darwin) GOOS=darwin ;;
    *)      echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
    x86_64|amd64)   GOARCH=amd64  ;;
    aarch64|arm64)  GOARCH=arm64  ;;
    armv6l|armv7l)  GOARCH=armv6l ;;
    *)              echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

TARBALL="${GO_VERSION}.${GOOS}-${GOARCH}.tar.gz"
echo "Installing ${GO_VERSION} (${GOOS}-${GOARCH}) into ${INSTALL_DIR}/go"

# Runs as root in a container, so no sudo: fail early if that is not the case
if [[ ! -w "$INSTALL_DIR" ]]; then
    echo "${INSTALL_DIR} is not writable by $(id -un); run as root or set INSTALL_DIR." >&2
    exit 1
fi

# Download (see go.dev/dl for the full list of builds)
TMPDIR_GO="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_GO"' EXIT
curl -fLo "${TMPDIR_GO}/${TARBALL}" "https://go.dev/dl/${TARBALL}"

# Verify the download against the checksum go.dev publishes for this build
if command -v jq >/dev/null 2>&1 && command -v sha256sum >/dev/null 2>&1; then
    want="$(curl -fsSL 'https://go.dev/dl/?mode=json&include=all' \
        | jq -r --arg f "$TARBALL" '.[].files[] | select(.filename == $f) | .sha256')"
    got="$(sha256sum "${TMPDIR_GO}/${TARBALL}" | cut -d' ' -f1)"
    if [[ -n "$want" && "$want" != "$got" ]]; then
        echo "Checksum mismatch for ${TARBALL}: expected ${want}, got ${got}" >&2
        exit 1
    fi
    echo "Checksum OK"
else
    echo "Skipping checksum verification (needs jq and sha256sum)"
fi

# Extract — the old tree must go first, Go does not support extracting over it
rm -rf "${INSTALL_DIR}/go"
tar -C "$INSTALL_DIR" -xzf "${TMPDIR_GO}/${TARBALL}"

# Add to PATH — prepended, so this install wins over any older go already there
export PATH="${INSTALL_DIR}/go/bin:$PATH"

# Verify, calling the new binary directly so an older go on PATH cannot answer
"${INSTALL_DIR}/go/bin/go" version
echo "Add this to your shell profile to make it permanent:"
echo "  export PATH=${INSTALL_DIR}/go/bin:\$PATH"
# ------

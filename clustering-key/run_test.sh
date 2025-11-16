#!/bin/bash
set -euo pipefail

# Build and run the Rust clustering key IN query test

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "Building Rust clustering test..."
cargo build --release

echo -e "${GREEN}✓ Build successful! Running test...${NC}"
echo "Usage: ./target/release/large-clustering-in [OPTIONS]"
echo ""
echo "Example runs:"
echo "  # Basic test (10 seconds, 50 concurrency, 50 keys per IN query)"
echo "  ./target/release/large-clustering-in"
echo ""
echo "  # High performance test"
echo "  ./target/release/large-clustering-in --duration 30 --concurrency 200 --num-rows 10000"
echo ""
echo "  # Quick test"
echo "  ./target/release/large-clustering-in --duration 5 --concurrency 20 --num-rows 1000"
echo ""
echo "Available options:"
./target/release/large-clustering-in --help

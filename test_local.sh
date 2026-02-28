#!/bin/bash
# Local test script for macOS/Linux
# Usage: ./test_local.sh [file_or_dir] [pybbt_args...]
# Examples:
#   ./test_local.sh                    # Run all tests
#   ./test_local.sh cases/rdb.py       # Run specific test file

set -e

echo "=== Building redis-shake ==="
sh build.sh

echo ""
echo "=== Running unit tests ==="
go test ./... -v

echo ""
echo "=== Running black box tests ==="
cd tests/

# Check if redis-server is available
if ! command -v redis-server &> /dev/null; then
    echo "Error: redis-server not found in PATH"
    echo "Install with: brew install redis (macOS) or apt install redis (Linux)"
    exit 1
fi

# Show Redis version
REDIS_VERSION=$(redis-server --version 2>&1 | head -1)
echo "Redis server: $REDIS_VERSION"

# Run tests without modules flag (suitable for Homebrew/system Redis)
# Default to running all cases if no argument provided
TEST_TARGET="${1:-cases}"
shift 2>/dev/null || true

pybbt "$TEST_TARGET" --verbose "$@"

#!/bin/bash
# Local test script for macOS/Linux
# Usage: ./test_local.sh [pytest_args]
# Examples:
#   ./test_local.sh                    # Run all tests
#   ./test_local.sh cases/rdb.py       # Run specific test file
#   ./test_local.sh -k "rdb"           # Run tests matching pattern

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
if [ $# -eq 0 ]; then
    pybbt cases --verbose
else
    pybbt cases --verbose "$@"
fi

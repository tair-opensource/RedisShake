#!/bin/bash

set -e

echo "[ BUILD RELEASE ]"
BIN_DIR=$(pwd)/bin/
rm -rf "$BIN_DIR"
mkdir -p "$BIN_DIR"

# build the current platform
echo "try build for current platform"
go build -v -trimpath -o "$BIN_DIR/redis-shake" "./cmd/redis-shake"
echo "build success"


if [ "$1" == "dist" ]; then
  echo "[ DIST ]"
  for g in "linux" "darwin"; do
    for a in "amd64" "arm64"; do
      echo "try build GOOS=$g GOARCH=$a"
      export GOOS=$g
      export GOARCH=$a
      go build -v -trimpath -o "$BIN_DIR/redis-shake-$g-$a" "./cmd/redis-shake"
      unset GOOS
      unset GOARCH
      echo "build success"
    done
  done
  cp sync.toml "$BIN_DIR"
  cp restore.toml "$BIN_DIR"
  cp -r filters "$BIN_DIR"
  cp -r scripts/cluster_helper "$BIN_DIR"
  cd "$BIN_DIR"
  tar -czvf ./redis-shake.tar.gz ./sync.toml ./restore.toml ./redis-shake-* ./filters ./cluster_helper
  rm -rf ./filters
  cd ..
fi

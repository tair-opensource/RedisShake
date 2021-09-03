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

cp redis-shake.toml "$BIN_DIR"

if [ "$1" == "dist" ]; then
  echo "[ DIST ]"
  cd bin
  cp -r ../filters ./
  tar -czvf ./redis-shake.tar.gz ./redis-shake.toml ./redis-shake-* ./filters
  rm -rf ./filters
  cd ..
fi

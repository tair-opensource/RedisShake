#!/bin/bash

set -o errexit

# make sure we're in the directory where the project lives
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

MODULE_NAME=$(grep module src/go.mod |cut -d ' ' -f 2)

# go version >=1.6
go_version=$(go version | awk -F' ' '{print $3;}')
bigVersion=$(echo $go_version | awk -F'[o.]' '{print $2}')
midVersion=$(echo $go_version | awk -F'[o.]' '{print $3}')
if [ $bigVersion -lt "1" -o $bigVersion -eq "1" -a $midVersion -lt "6" ]; then
    echo "go version[$go_version] must >= 1.6"
    exit 1
fi

# older version Git don't support --short !
if [ -d ".git" ];then
    branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
    cid=$(git rev-parse HEAD)
else
    branch="unknown"
    cid="0.0"
fi
branch=$branch","$cid
info="$MODULE_NAME/redis-shake/common.Version=$branch"

# golang version
info=$info","$go_version

t=$(date "+%Y-%m-%d_%H:%M:%S")
info=$info","$t

echo "[ BUILD RELEASE ]"
BIN_DIR=$(pwd)/bin/
cd src
goos=(linux darwin windows)
for g in "linux" "darwin" "windows";
do
    echo "try build GOOS=$g"
    export GOOS=$g
    go build -v -ldflags "-X $info" -o "$BIN_DIR/redis-shake.$g" "$MODULE_NAME/redis-shake/main"
    unset GOOS
    echo "build $g successfully!"
done

cd $PROJECT_DIR
cp conf/redis-shake.conf $BIN_DIR


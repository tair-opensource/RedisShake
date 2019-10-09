#!/bin/bash

if [ "Linux" != "$(uname -s)" ];then
    echo "only support Linux"
    exit -1
fi

set -o errexit

# run unit test
curPath=$(cd "$(dirname "$0")"; pwd)
export GOPATH=$curPath/..
cd $curPath/../src/redis-shake
go test -tags integration ./...
if [ $? -ne 0 ]; then
    echo "run ut failed"
else
    echo "run ut successfully"
fi

# run integration test
cd $curPath


#!/bin/bash

set -o errexit

# start an standalone redis with given port
if [ $# != 2 ] ; then
    echo "USAGE: $0 [port] [start/stop]"
    exit 1
elif [ $2 != "start" -a $2 != "stop" ]; then
    echo "parameter illegal"
    exit 2
fi

port=$1
path="standalone-$port"
curPath=$(cd "$(dirname "$0")"; pwd)
echo $curPath
subPath="$curPath/$path"

if [ $2 == "start" ]; then
    # start
    mkdir -p $subPath 
    $curPath/../tools/redis-server --port $port --pidfile $subPath/$port.pid --logfile $subPath/$port.log 1>/dev/null 2>&1 &
else
    # stop
    kill -9 $(cat $subPath/$port.pid)
    cd $curPath
    rm -rf $subPath
fi

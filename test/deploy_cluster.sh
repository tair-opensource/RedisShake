#!/bin/bash

set -o errexit

# start an standalone redis with given port
if [ $# != 3 ] ; then
    echo "USAGE: $0 [beginning port] [start/stop] [number of node]"
    exit 1
elif [ $2 != "start" -a $2 != "stop" ]; then
    echo "parameter illegal"
    exit 2
fi

port=$1
path="cluster-$port"
curPath=$(cd "$(dirname "$0")"; pwd)
subPath="$curPath/$path"

if [ $2 == "start" ]; then
    # start
    mkdir -p $subPath 
    cd $subPath
    cnt=0
    for ((i=$port;i<$port+100&&cnt<$3;i++));
    do
        inUse=$(lsof -i:$i | wc -l)
        if [ $inUse -gt 1 ]; then
            echo "port[$i] in using"
            continue
        fi
        cnt=$cnt+1
        echo "build node with port[$i]"

        nodePath="$i"
        mkdir -p $nodePath
        cd $nodePath
        ../../../tools/redis-server --port $i --pidfile $i.pid --cluster-enabled yes --cluster-node-timeout 5000 1>$i.log 2>&1 &
        nodeList=$nodeList" 127.0.0.1:$i"
        cd ..
        sleep 1
    done
    #../../tools/redis-trib.rb create --replicas 1 $nodeList 
    expect -f ../../tools/build-cluster.expect ../../tools/redis-trib.rb $nodeList
else
    # stop
    cd $curPath
    for i in $subPath/*
    do
        echo "try kill $i"
        nodePath=$(basename $i)
        kill -9 $(cat $subPath/$nodePath/$nodePath.pid)
    done
    rm -rf $subPath
fi

#!/bin/bash

set -o errexit
curPath=$(cd "$(dirname "$0")"; pwd)

function write {
    ret=$()
}

function testIncr {
    src=$1 # source redis address
    dst=$2 # target redis address
    fatherPath=$3
    sonPath=$4

    # 1. write redis-shake.conf
    echo "source.address = $1" > redis-shake.conf
    echo "source.type = standalone" >> redis-shake.conf
    echo "target.address= $2" >> redis-shake.conf
    echo "target.type = standalone" >> redis-shake.conf
    echo "log.file = $fatherPath/$sonPath/redis-shake.log"

    # 2. start redis-shake
    ../../bin/redis-shake -conf=redis-shake.conf -type=sync &

    # 3. write & test

}

function TestNormal {
    src=$1 # source redis address
    dst=$2 # target redis address
    
    # mkdir a path
    cd $curPath
    sub="incr"
    mkdir -p $sub && cd $sub

    # test incr
    testIncr $1 $2 $curPath $sub

    # test full
}

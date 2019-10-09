#!/usr/bin/env bash

catalog=$(dirname "$0")

cd "${catalog}" || exit 1

if [ $# != 2 ] ; then
	echo "USAGE: $0 [conf] [mode]"
	exit 0
fi

name="redis-shake"

if [ "Linux" != "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOs doesn't supply to use this script, please use \"./%s -conf=config_file_name\" manual command to run\\n" "$name"
    exit 1
fi

if [ "sync" == $2 ]; then
    ./hypervisor --daemon --exec="./$name.linux -conf=$1 -type=$2 1>>$name.output 2>&1" 1>>hypervisor.output 2>&1
else
    ./$name.linux -conf=$1 -type=$2 1>>$name.output 2>&1 &
fi

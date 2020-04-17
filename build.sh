#!/bin/bash

set -o errexit

# older version Git don't support --short !
if [ -d ".git" ];then
    #branch=`git symbolic-ref --short -q HEAD`
    branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
    cid=$(git rev-parse HEAD)
else
    branch="unknown"
    cid="0.0"
fi
branch=$branch","$cid

output=./bin/
integration_test=./bin/integration-test
rm -rf ${output}

# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GOPATH=$(pwd)
export GOPATH

info="redis-shake/common.Version=$branch"
# golang version
goversion=$(go version | awk -F' ' '{print $3;}')
info=$info","$goversion
bigVersion=$(echo $goversion | awk -F'[o.]' '{print $2}')
midVersion=$(echo $goversion | awk -F'[o.]' '{print $3}')
if  [ $bigVersion -lt "1" -o $bigVersion -eq "1" -a $midVersion -lt "6" ]; then
    echo "go version[$goversion] must >= 1.6"
    exit 1
fi

t=$(date "+%Y-%m-%d_%H:%M:%S")
info=$info","$t

echo "[ BUILD RELEASE ]"
run_builder='go build -v'

goos=(linux darwin windows)
for g in "${goos[@]}"; do
    export GOOS=$g
    echo "try build goos=$g"

    build_dir="src/redis-shake/$i/main"
    all_files=""
    for j in $(ls $build_dir); do
        all_files="$all_files $build_dir/$j "
    done

    $run_builder -ldflags "-X $info" -o "${output}/redis-shake.$g" $all_files
    echo "build $g successfully!"
done
unset GOOS

# build integration test
$run_builder -o "${integration_test}/integration-test" "./src/integration-test/main/main.go"

# copy scripts
cp scripts/start.sh ${output}/
cp scripts/stop.sh ${output}/
#cp scripts/run_direct.py ${output}/
cp -r tools ${integration_test}/
cp -r test ${integration_test}/

if [ "Linux" == "$(uname -s)" ];then
	# hypervisor
	gcc -Wall -O3 scripts/hypervisor.c -o ${output}/hypervisor -lpthread
elif [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOS doesn't supply hypervisor\\n"
fi

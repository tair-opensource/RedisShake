#!/bin/bash

set -o errexit

MODULE_NAME=$(grep module src/go.mod |cut -d ' ' -f 2)

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

export GOBIN=$(pwd)/bin/
integration_test=$GOBIN/integration-test


# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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

main_package="$MODULE_NAME/redis-shake/main"

cd src
goos=(linux darwin windows)
for g in "${goos[@]}"; do
    export GOOS=$g
    echo "try build goos=$g"

    $run_builder -ldflags "-X $info" -o "$GOBIN/redis-shake.$g" $main_package
    echo "build $g successfully!"
done
unset GOOS

# build integration test
$run_builder -o "${integration_test}/integration-test" "$MODULE_NAME/integration-test/main"
cd ..
# copy scripts
cp scripts/start.sh ${GOBIN}/
cp scripts/stop.sh ${GOBIN}/
#cp scripts/run_direct.py ${output}/
cp -r tools ${integration_test}/
cp -r test ${integration_test}/

if [ "Linux" == "$(uname -s)" ];then
	# hypervisor
	gcc -Wall -O3 scripts/hypervisor.c -o ${output}/hypervisor -lpthread
elif [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOS doesn't supply hypervisor\\n"
fi

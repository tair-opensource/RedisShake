#!/bin/bash
set -e

# unit test
go test ./... -v

# black box test
cd tests/
echo "sssss"
echo $moduleSupported
pybbt cases
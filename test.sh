#!/bin/bash
set -e

# unit test
go test ./... -v

# black box test
cd tests/
pybbt cases --verbose --flags modules
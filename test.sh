#!/bin/bash
set -e

# unit test
go test ./... -v

# black box test - use local pybbt module
cd tests/
export PYTHONPATH="$(pwd):$PYTHONPATH"
python -m pybbt cases --verbose --flags modules
#!/bin/bash

cd src

PKGS=$(go list ./... | grep -v /vendor/)
go test $PKGS

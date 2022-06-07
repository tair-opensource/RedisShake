#!/bin/bash

cd src

PKGS=$(go list ./...)
go test $PKGS

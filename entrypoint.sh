#!/bin/sh
if [ "$SYNC" = "true" ]; then
    ./redis-shake shake_sync_env.toml
elif [ "$SCAN" = "true" ]; then
    ./redis-shake shake_scan_env.toml
else
    echo "Error: Neither SYNC nor SCAN environment variable is set to true"
    exit 1
fi
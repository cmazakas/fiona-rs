#!/bin/bash

set -ex

clear

# export BSAN_OPTIONS=stacktrace_max_len=32

cargo +bsan \
    bsan test \
    -Z build-std \
    --target x86_64-unknown-linux-gnu \
    --features bsan \
    "$@"

#!/bin/bash

set -ex

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"

clear

CARGO_FLAGS=(--target x86_64-unknown-linux-gnu -Zbuild-std)

RUSTFLAGS='-Zsanitizer=address' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1 --nocapture
RUSTFLAGS='-Zsanitizer=thread' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1 --nocapture
cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1 --nocapture

RUSTFLAGS='-Zsanitizer=address' cargo test "${CARGO_FLAGS[@]}" --profile=lto -- --test-threads=1 --nocapture
RUSTFLAGS='-Zsanitizer=thread' cargo test "${CARGO_FLAGS[@]}" --profile=lto -- --test-threads=1 --nocapture
cargo test "${CARGO_FLAGS[@]}" --profile=lto -- --test-threads=1 --nocapture

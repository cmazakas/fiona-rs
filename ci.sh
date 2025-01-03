#!/bin/bash

set -ex

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"

clear

CARGO_FLAGS=(--target x86_64-unknown-linux-gnu -Zbuild-std)

RUSTFLAGS='-Zsanitizer=address' PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-asan-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-asan-install/lib" cargo test "${CARGO_FLAGS[@]}"

RUSTFLAGS='-Zsanitizer=thread' PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-install/lib" cargo test "${CARGO_FLAGS[@]}"

PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-install/lib" cargo test "${CARGO_FLAGS[@]}"

RUSTFLAGS='-Zsanitizer=address' PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-asan-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-asan-install/lib" cargo test "${CARGO_FLAGS[@]}" --release

RUSTFLAGS='-Zsanitizer=thread' PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-install/lib" cargo test "${CARGO_FLAGS[@]}" --release

PKG_CONFIG_PATH="/home/exbigboss/cpp/liburing-install/lib/pkgconfig" LD_LIBRARY_PATH="/home/exbigboss/cpp/liburing-install/lib" cargo test "${CARGO_FLAGS[@]}" --release

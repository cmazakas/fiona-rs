# fiona-rs

Requires Linux kernel version 6.8 and up.

A Rust port of some C++ I had written which was a port of some Rust code I originally worked on.

Aims to be a competitively fast I/O runtime using io_uring and all of its features.

## Dev Scripts

For local dev testing, a script like this is useful:

```bash
#!/bin/bash

set -ex

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"

clear

CARGO_FLAGS=(--target x86_64-unknown-linux-gnu -Zbuild-std)

export RUSTFLAGS='-Zsanitizer=address'
cargo test "${CARGO_FLAGS[@]}" "$@"

# export RUSTFLAGS='-Zsanitizer=thread'
# cargo test "${CARGO_FLAGS[@]}" "$@"

# cargo test "${CARGO_FLAGS[@]}" "$@"
```

and for a more comprehensive coverage suite:

```bash
#!/bin/bash

set -ex

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-19
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"

clear

CARGO_FLAGS=(--target x86_64-unknown-linux-gnu -Zbuild-std)

export RUSTFLAGS='-Zsanitizer=address'
cargo test "${CARGO_FLAGS[@]}"

export RUSTFLAGS='-Zsanitizer=thread'
cargo test "${CARGO_FLAGS[@]}"

cargo test "${CARGO_FLAGS[@]}"

export RUSTFLAGS='-Zsanitizer=address'
cargo test "${CARGO_FLAGS[@]}" --release

export RUSTFLAGS='-Zsanitizer=thread'
cargo test "${CARGO_FLAGS[@]}" --release

cargo test "${CARGO_FLAGS[@]}" --release
```

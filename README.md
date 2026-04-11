# fiona-rs

Building this crate successfully may require the following in your `.cargo/config.toml`:

```toml
[env]
CLANG_PATH = "/usr/bin/clang-20"
LIBCLANG_PATH = "/usr/lib/llvm-20/lib"
```

Requires Linux kernel version 6.8 and up.

A Rust port of some C++ I had written which was a port of some Rust code I originally worked on.

Aims to be a competitively fast I/O runtime using io_uring and all of its features.

## Working Around RLIMIT_MEMLOCK Limits

By default, Fiona uses io_uring's zero-copy TCP send. In order to do this, the kernel has to lock pages of memory in order to perform the direct memory access.

Linux distributions will oftentimes limit the amount of memory a user can lock. This is done for myriad reasons but it can prevent Fiona from scaling properly, as TCP sends will be returning `ENOMEM`. The limit used by most distributions is relatively small, and can be verified locally by using `ulimit -l`.

Fiona is actively developed primarily on Ubuntu machines. The following steps seem to be sufficient for modern installs of Ubuntu 25.04/25.10:

1. Edit `/etc/security/limits.conf` by appending:
   ```bash
   <yourusername> soft memlock unlimited
   <yourusername> hard memlock unlimited
   ```
2. Make sure the following two files:
   ```
   /etc/pam.d/common-session
   /etc/pam.d/common-session-noninteractive
   ```
   contain the following line:
   ```
   session required pam_limits.so
   ```
3. Reboot. Upon logging in, `ulimit -l` should now show `unlimited`.

Note, the above commands permit the user to potentially `mlock` _all_ available memory which can be undesireable. `unlimited` which can instead be replaced with a numeric value which is KB.

## Running Benchmarks

Right now Fiona has one main benchmark: `echo2`. This benchmark simply spawns a number of concurrent clients and sends 1 MiB both directions, hashing the entirety of the message and comparing it against a known sentinel value. To run benchmarks with Fiona it's recommended to use two physical machines connected by a high-quality ethernet cable.

To run the server:
```bash
cargo bench --bench echo2 -- --ipv4-addr 192.168.10.12 --port 8016 --fiona --server --nr-files 6000
```

To run the client:
```bash
cargo bench --bench echo2 -- --ipv4-addr 192.168.10.12 --port 8016 --fiona --client --nr-files 6000
```

`--fiona` can be replaced with `--tokio` or `--compio` to use those runtimes instead.

## Dev Scripts

For local dev testing, a script like this is useful:

```bash
#!/bin/bash

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-20
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-20
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"
export RUSTFLAGS="-Zsanitizer=address"

set -ex

clear

cargo test -Z build-std --target x86_64-unknown-linux-gnu --profile release-with-debug "$@"
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

RUSTFLAGS='-Zsanitizer=address' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1
RUSTFLAGS='-Zsanitizer=thread' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1
cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1

RUSTFLAGS='-Zsanitizer=address -C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
RUSTFLAGS='-Zsanitizer=thread -C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
RUSTFLAGS='-C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
```

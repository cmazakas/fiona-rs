# fiona-rs

An io_uring runtime that aims to leverage all of its unique features to experiment and see what kinds of new APIs are possible.

## Build Requirements

Requires Linux kernel version 7.0 and up.

Building this crate successfully may require the following in your `.cargo/config.toml`:

```toml
[env]
CLANG_PATH = "/usr/bin/clang-20"
LIBCLANG_PATH = "/usr/lib/llvm-20/lib"
```

## Preliminary Benchmarks

Benchmarks were build against commit [ff37219f55b8c32bf9541dbfd476db4d1824e4f3](https://github.com/cmazakas/fiona-rs/tree/ff37219f55b8c32bf9541dbfd476db4d1824e4f3).

Benchmarks were run using two physical machines, a gaming desktop with a 2.5 Gb NIC and a Dell XPS 17 laptop. A 2.5 Gb ethernet cable was used to connect the two machines. The Dell XPS functioned as the server, the gaming desktop functioned as the client.

The server machine uses:
```
cargo bench --bench echo2 -- --ipv4-addr 192.168.10.12 --port 8015 --tokio --server --nr-files 5000
```

The client machine uses:
```
cargo bench --bench echo2 -- --ipv4-addr 192.168.10.12 --port 8015 --tokio --client --nr-files 5000
```

`--tokio` can be substituted for `--fiona`. All benchmarks were run back-to-back as fast as human input allows. Tokio was relegated to port 8015, fiona-rs used port 8016.

Current benchmark data applies to [echo2](benches/echo2.rs).

| Number of Connections | fiona-rs (Total client loop time) | Tokio (Total client loop time) | fiona-rs (Average client duration) | Tokio (Average client duration) |
| --------------------- | --------------------------------- | ------------------------------ | ---------------------------------- | ------------------------------- |
| 1000                  | 5.78s                             | 6.45s                          | 4.79s                              | 5.55s                           |
| 2000                  | 11.95s                            | 13.59s                         | 9.99s                              | 10.41s                          |
| 3000                  | 18.79s                            | 21.82s                         | 15.72s                             | 16.40s                          |
| 4000                  | 27.69s                            | 36.73s                         | 23.35s                             | 22.64s                          |
| 5000                  | 37.40s                            | 47.45s                         | 31.64s                             | 31.23s                          |
| 6000                  | 48.48s                            | 58.10s                         | 39.17s                             | 39.24s                          |
| 7000                  | 68.62s                            | 75.54s                         | 46.93s                             | 46.84s                          |
| 8000                  | 67.96s                            | 102.84s                        | 54.28s                             | 54.45s                          |
| 9000                  | 76.64s                            | 108.29s                        | 61.22s                             | 61.04s                          |
| 10000                 | 112.34s                           | Timed out                      | 68.07s                             | Timed out                       |

The benchmarks also track strong statistical outliers. No outliers meeting the current critertia were detected for either runtime during the benchmark runs.

Note: results are preliminary and are subject to noise and are not absolutely conclusive of overall performance.

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

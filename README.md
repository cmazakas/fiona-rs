An io_uring runtime that aims to leverage all of its unique features to experiment and see what kinds of new APIs are possible.

- [Build Requirements](#build-requirements)
- [Preliminary Benchmarks](#preliminary-benchmarks)
- [Working Around RLIMIT\_MEMLOCK Limits](#working-around-rlimit_memlock-limits)
- [Running Benchmarks](#running-benchmarks)
- [Dev Scripts](#dev-scripts)
- [Borrow Sanitizer](#borrow-sanitizer)


# Build Requirements

Requires Linux kernel version 7.0 and up.

Building this crate successfully may require the following in your `.cargo/config.toml`:

```toml
[env]
CLANG_PATH = "/usr/bin/clang-20"
LIBCLANG_PATH = "/usr/lib/llvm-20/lib"
```

# Preliminary Benchmarks

Benchmarks were build against commit [c1936ffedf9542e3f70878c855b92eb96c88cfb5](https://github.com/cmazakas/fiona-rs/tree/c1936ffedf9542e3f70878c855b92eb96c88cfb5).

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

Kernel version:
```
❯ uname -a
Linux pleiades 7.0.0-30-generic #30-Ubuntu SMP PREEMPT_DYNAMIC Fri Jul 31 18:22:54 UTC 2026 x86_64 GNU/Linux
```

| Number of Connections | fiona-rs (Total client loop time) | Tokio (Total client loop time) | fiona-rs (Average client duration) | Tokio (Average client duration) |
| --------------------- | --------------------------------- | ------------------------------ | ---------------------------------- | ------------------------------- |
| 1000                  | 5.89s                             | 6.50s                          | 4.99s                              | 5.54s                           |
| 2000                  | 11.78s                            | 12.82s                         | 9.62s                              | 10.45s                          |
| 3000                  | 19.06s                            | 19.59s                         | 16.80s                             | 16.08s                          |
| 4000                  | 26.98s                            | 32.40s                         | 23.64s                             | 22.19s                          |
| 5000                  | 39.93s                            | 46.48s                         | 31.36s                             | 31.01s                          |
| 6000                  | 44.71s                            | 57.36s                         | 38.64s                             | 38.54s                          |
| 7000                  | 52.83s                            | 71.47s                         | 45.94s                             | 45.43s                          |
| 8000                  | 66.25s                            | 94.25s                         | 53.39s                             | 50.06s                          |
| 9000                  | 75.56s                            | 131.53s                        | 61.28s                             | 59.02s                          |
| 10000                 | 95.46s                            | Timed out                      | 67.68s                             | Timed out                       |

The benchmarks also track strong statistical outliers. Only one statistical outlier was detected for Tokio with 9000 connections.

Note: results are preliminary and are subject to noise and are not absolutely conclusive of overall performance.

# Working Around RLIMIT_MEMLOCK Limits

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

# Running Benchmarks

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

# Dev Scripts

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

# Borrow Sanitizer

Fiona has been partially tested under [Borrow Sanitizer](https://borrowsanitizer.com/).

To run Fiona's test suite, follow the setup instructions here: https://github.com/BorrowSanitizer/bsan#usage.

Because docker disables io_uring by default, you'll need to run something like this:
```
docker run -it --security-opt seccomp=unconfined ghcr.io/borrowsanitizer/bsan:latest
```
or use a custom security profile depending on your level of comfort and expertise with docker.

Not all of Fiona's tests can feasibly be run under bsan, so a script like the following is useful:

```
#!/bin/bash

set -ex

clear

export BSAN_OPTIONS=stacktrace_max_len=32

rm /tmp/fiona* || echo 0

cargo +bsan \
    bsan test \
    -Z build-std \
    --target x86_64-unknown-linux-gnu \
    -- \
    --test-threads=1 \
    --nocapture \
    --skip await_double_wake \
    --skip await_rayon_stress_test \
    --skip await_rayon_tasks \
    --skip await_stress_test \
    --skip file_offset_out_of_bounds \
    --skip fixed_bufs_get_bufs \
    --skip tcp_concurrent_send_recv \
    --skip tcp_connection_stress_test_cq_overflow \
    --skip tcp_connection_stress_test_no_cq_overflow \
    --skip tcp_ephemeral_port_exhaustion_panic \
    --skip tcp_multiple_accepts \
    --skip tcp_socket_reuse \
    --skip tcp_fixed_send_random_bytes \
    --skip tcp_stress_panicking \
    --skip slotmap_stable_submit \
    --skip time \
    --skip tls \
    "$@"
```

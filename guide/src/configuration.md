# Configuration

This chapter covers how to configure the Fiona runtime, Cargo build profiles,
sanitizer support, and environment setup.

## Builder Options

`IoContext::new()` creates a ring with default parameters. For control over
ring sizing, use the builder:

```rust
let mut io = IoContext::builder()
    .sq_entries(256)
    .cq_entries(4096)
    .num_files(12000)
    .build()
    .unwrap();
```

The builder accepts the following options:

| Method | Controls | Default |
|--------|----------|---------|
| `sq_entries(n)` | Submission queue depth - how many I/O operations can be queued before the ring must be submitted | Ring default |
| `cq_entries(n)` | Completion queue depth - how many completed operations can accumulate before the kernel stalls | Ring default |
| `num_files(n)` | Fixed-file table capacity - the number of direct file descriptors the ring can manage | Ring default |

The `num_files` setting matters for high connection counts. Every accepted TCP
connection consumes a slot in the fixed-file table. If you expect thousands of
concurrent connections, set this accordingly (e.g. `num_files(12000)` for a
10,000-connection benchmark).

Fiona automatically sets `DEFER_TASKRUN`, `COOP_TASKRUN`, and `SINGLE_ISSUER`
on every ring. These flags are not configurable - they are required for Fiona's
single-threaded completion processing model.

## Build Requirements

Fiona requires Linux kernel version 7.0 or later.

Building the crate requires Clang for the liburing C bindings. Add the
following to your `.cargo/config.toml`:

```toml
[env]
CLANG_PATH = "/usr/bin/clang-20"
LIBCLANG_PATH = "/usr/lib/llvm-20/lib"
```

Adjust the version suffix to match your installed LLVM version.

## Cargo Profiles

Fiona ships three custom Cargo profiles in addition to the built-in `dev` and
`release` profiles.

### release-with-debug

Release optimizations with full debug symbols. Useful for profiling and
sanitizer runs where you want optimized code but readable stack traces.

```toml
[profile.release-with-debug]
inherits = "release"
debug = "full"
```

```bash
cargo test --profile release-with-debug
```

### lto

Release with thin LTO and a single codegen unit for maximum optimization.
Use this for final production builds.

```toml
[profile.lto]
inherits = "release"
lto = "thin"
codegen-units = 1
```

```bash
cargo build --profile lto
```

### bench

The bench profile also enables thin LTO and single codegen unit so that
benchmark binaries reflect production-grade optimization.

```toml
[profile.bench]
inherits = "release"
lto = "thin"
codegen-units = 1
```

## Sanitizer Support

Fiona supports AddressSanitizer (ASAN) and ThreadSanitizer (TSAN) through a
`sanitizers` feature flag. This flag propagates to the underlying `axboe-liburing`
C binding so that the C code is also built with sanitizer instrumentation.

### Basic Usage

```bash
RUSTFLAGS="-Zsanitizer=address" cargo test --features sanitizers
RUSTFLAGS="-Zsanitizer=thread" cargo test --features sanitizers
```

### Full ASAN Setup

For thorough leak detection and stricter checking, set the following
environment variables before running tests:

```bash
export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-20
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"
```

Then run tests with the `release-with-debug` profile for readable stack traces
in optimized code:

```bash
RUSTFLAGS="-Zsanitizer=address" \
    cargo test -Z build-std \
    --target x86_64-unknown-linux-gnu \
    --profile release-with-debug \
    -- --test-threads=1
```

### Full TSAN Setup

ThreadSanitizer uses the same symbolizer and profile:

```bash
RUSTFLAGS="-Zsanitizer=thread" \
    cargo test -Z build-std \
    --target x86_64-unknown-linux-gnu \
    --profile release-with-debug \
    -- --test-threads=1
```

### Comprehensive Dev Script

This script runs the full test matrix - ASAN, TSAN, and plain - across both
`release-with-debug` and LTO release profiles:

```bash
#!/bin/bash

set -ex

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-20
export MSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-20
export ASAN_OPTIONS="detect_leaks=1:detect_invalid_pointer_pairs=2:strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1"
export LSAN_OPTIONS="suppressions=suppr.txt"

CARGO_FLAGS=(--target x86_64-unknown-linux-gnu -Zbuild-std)

RUSTFLAGS='-Zsanitizer=address' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1
RUSTFLAGS='-Zsanitizer=thread' cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1
cargo test "${CARGO_FLAGS[@]}" --profile=release-with-debug -- --test-threads=1

RUSTFLAGS='-Zsanitizer=address -C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
RUSTFLAGS='-Zsanitizer=thread -C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
RUSTFLAGS='-C embed-bitcode -C lto' cargo test "${CARGO_FLAGS[@]}" --release -- --test-threads=1
```

## Borrow Sanitizer

Fiona has been partially tested under [Borrow Sanitizer](https://borrowsanitizer.com/).
Follow the setup instructions at the
[bsan repository](https://github.com/BorrowSanitizer/bsan#usage), then run:

```bash
export BSAN_OPTIONS=stacktrace_max_len=32
cargo +bsan bsan test \
    -Z build-std \
    --target x86_64-unknown-linux-gnu \
    -- --test-threads=1 --nocapture
```

Not all tests are compatible with bsan. The full script skips tests that are
infeasible under borrow sanitization (stress tests, timing-sensitive tests,
TLS tests). See the `README.md` for the complete skip list.

Borrow Sanitizer runs inside Docker. Because Docker blocks io_uring by
default, you need the `seccomp=unconfined` flag (see the Docker section below).

## Docker

Docker's default seccomp profile blocks `io_uring_setup` and related syscalls.
Any Fiona program running inside a container will fail unless you disable the
seccomp filter:

```bash
docker run -it --security-opt seccomp=unconfined your-image
```

For the Borrow Sanitizer Docker image specifically:

```bash
docker run -it --security-opt seccomp=unconfined ghcr.io/borrowsanitizer/bsan:latest
```

If you need tighter security, create a custom seccomp profile that allows the
`io_uring_setup`, `io_uring_enter`, and `io_uring_register` syscalls rather
than disabling seccomp entirely.

## RLIMIT_MEMLOCK

Fiona uses io_uring's zero-copy TCP send by default. The kernel locks pages of
memory to perform direct memory access for these sends. Linux distributions
typically limit how much memory a user can lock, and the default limit is small.
At scale, TCP sends will fail with `ENOMEM` if the limit is too low.

Check your current limit:

```bash
ulimit -l
```

To set memlock to unlimited (tested on Ubuntu 25.04/25.10):

**1. Edit `/etc/security/limits.conf`**

Append the following lines, replacing `<yourusername>` with your login name:

```text
<yourusername> soft memlock unlimited
<yourusername> hard memlock unlimited
```

**2. Ensure PAM loads the limits**

Verify that both of these files contain the line `session required pam_limits.so`:

- `/etc/pam.d/common-session`
- `/etc/pam.d/common-session-noninteractive`

If the line is missing, add it.

**3. Reboot**

After rebooting, confirm the change:

```bash
ulimit -l
# should print: unlimited
```

Setting memlock to `unlimited` allows the user to lock all available physical
memory. If that is undesirable, replace `unlimited` with a numeric value in
kilobytes that is sufficient for your workload.

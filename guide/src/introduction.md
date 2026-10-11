# Introduction

Fiona is an io_uring-based async runtime for Rust that replaces Tokio on Linux.
Where Tokio wraps epoll behind a reactor, Fiona submits I/O directly to the
kernel ring. Zero-copy TCP sends are the default, multishot accepts handle
connection storms in a single submission, and bundled receives deliver data
without copying it out of kernel buffers. If you want raw io_uring performance
with an async/await API, Fiona gives you that without hand-rolling SQEs.

Fiona's binding layer talks to the kernel through axboe-liburing. TLS is
provided by rustls 0.23. The runtime is single-threaded per ring, with
cross-thread wakeups via `io_uring_prep_msg_ring` when you need multi-threaded
designs. In-flight I/O cancels automatically on drop - there is no manual
cancellation ceremony.

## Requirements

### Linux kernel 7.0+

Fiona requires Linux kernel 7.0 or later. The ring flags it sets
(`DEFER_TASKRUN`, `COOP_TASKRUN`, `SINGLE_ISSUER`) and the multishot/bundled
features it depends on are not available in older kernels.

### Nightly Rust

Fiona builds on the nightly toolchain only. Stable and beta are not supported.

### CLANG_PATH

Building the crate requires clang for the liburing C bindings. Add the
following to your `.cargo/config.toml`:

```toml
[env]
CLANG_PATH = "/usr/bin/clang-20"
LIBCLANG_PATH = "/usr/lib/llvm-20/lib"
```

Adjust the version suffix to match whatever clang you have installed.

### Docker

io_uring syscalls are blocked by Docker's default seccomp profile. If you are
running inside a container, disable it:

```bash
docker run -it --security-opt seccomp=unconfined your-image
```

## Performance

The benchmark suite (`echo2`) measures single-threaded TCP echo throughput,
spawning concurrent clients that each send 1 MiB in both directions. The test
was run on two physical machines connected by a 2.5 Gb ethernet cable.

| Connections | fiona (total) | Tokio (total) | fiona (avg per client) | Tokio (avg per client) |
|-------------|---------------|---------------|------------------------|------------------------|
| 1000        | 5.78s         | 6.45s         | 4.79s                  | 5.55s                  |
| 2000        | 11.95s        | 13.59s        | 9.99s                  | 10.41s                 |
| 3000        | 18.79s        | 21.82s        | 15.72s                 | 16.40s                 |
| 4000        | 27.69s        | 36.73s        | 23.35s                 | 22.64s                 |
| 5000        | 37.40s        | 47.45s        | 31.64s                 | 31.23s                 |
| 6000        | 48.48s        | 58.10s        | 39.17s                 | 39.24s                 |
| 7000        | 68.62s        | 75.54s        | 46.93s                 | 46.84s                 |
| 8000        | 67.96s        | 102.84s       | 54.28s                 | 54.45s                 |
| 9000        | 76.64s        | 108.29s       | 61.22s                 | 61.04s                 |
| 10000       | 112.34s       | Timed out     | 68.07s                 | Timed out              |

At low connection counts the two runtimes trade punches on per-client latency.
The gap opens as connection count rises: by 8000 connections Fiona finishes the
full run in 66% of Tokio's time, and at 10000 connections Tokio times out
entirely while Fiona completes in under two minutes.

These results are preliminary - hardware, kernel version, and network topology
all matter - but the trend is consistent: io_uring's submission model scales
better than epoll under connection pressure.

## What this guide covers

Each chapter focuses on one part of the runtime:

- **[The Runtime](runtime.md)** - Creating an `IoContext`, spawning tasks, and
  driving the event loop with `run()`.
- **[TCP Networking](tcp.md)** - Listeners, multishot accept, zero-copy sends,
  bundled receives, connect with timeout, and stream lifecycle.
- **[Timers and Timeouts](timers.md)** - Kernel-level sleeps, reusable timer
  objects, and per-stream inactivity timeouts.
- **[Buffer Management](buffers.md)** - Buffer groups for zero-copy receives
  and fixed buffers for sends and file writes.
- **[TLS](tls.md)** - Client and server handshakes over rustls, read/write
  semantics, flush control, and graceful shutdown.
- **[File I/O](file-io.md)** - O_DIRECT file operations with fixed buffers.
- **[Concurrency](concurrency.md)** - Cancel-on-drop semantics, multi-threaded
  operation with one ring per thread, and shared ownership via cloning.
- **[Configuration](configuration.md)** - Builder options, Cargo profiles,
  sanitizer support, RLIMIT_MEMLOCK tuning, and Docker setup.
- **[Benchmarking](benchmarking.md)** - Running the echo2, echo_mt, timer, and
  ring_msg benchmarks against Tokio and Compio.

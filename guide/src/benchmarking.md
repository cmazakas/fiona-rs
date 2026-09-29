# Benchmarking

Fiona ships benchmark binaries that test networking and timing primitives, with
built-in support for comparing against Tokio and Compio on identical workloads.

## Benchmark binaries

| Binary | Description |
|--------|-------------|
| `echo2` | Single-threaded TCP echo server/client. Spawns N concurrent clients, sends 1 MiB in both directions, and hashes the entire payload with BLAKE2b-512 to verify data integrity. |
| `echo_mt` | Multi-threaded TCP echo. Same workload as `echo2` but distributes connections across multiple threads using `SO_REUSEPORT` load balancing. |
| `timer` | Timer operation benchmarks. Spawns concurrent tasks that each execute 10,000 one-millisecond timer waits to measure timer scheduling throughput. |
| `ring_msg` | Ring-to-ring messaging benchmarks. Measures cross-thread wakeup performance using `io_uring_prep_msg_ring` to send messages between io_uring instances on separate threads. |

## Two-machine setup

Benchmarks should be run on two separate physical machines connected by an
ethernet cable. One machine acts as the server, the other as the client.

Using two machines avoids the kernel's loopback fast path, which bypasses the
NIC and most of the network stack. Loopback results do not reflect real-world
performance because they skip interrupt handling, DMA, and driver-level
batching. Two-machine tests exercise the full network path and produce
measurements that are representative of production deployments.

The published results were collected using a gaming desktop with a 2.5 Gb NIC
(client) and a Dell XPS 17 laptop (server), connected by a 2.5 Gb ethernet
cable.

## CLI reference

### echo2

```text
--server          Run in server mode
--client          Run in client mode (mutually exclusive with --server)
--fiona           Use the Fiona runtime
--tokio           Use the Tokio runtime
--compio          Use the Compio runtime
--ipv4-addr ADDR  IPv4 address to bind/connect (required)
--port PORT       TCP port to bind/connect (required)
--nr-files N      Number of concurrent connections (default: 5000)
```

The runtime flags (`--fiona`, `--tokio`, `--compio`) are mutually exclusive.
Switching between them runs the exact same workload on a different async
runtime, providing a direct comparison.

### echo_mt

```text
--server          Run in server mode
--client          Run in client mode (mutually exclusive with --server)
--fiona           Use the Fiona runtime
--tokio           Use the Tokio runtime
--ipv4-addr ADDR  IPv4 address to bind/connect (required)
--port PORT       TCP port to bind/connect (required)
--num-files N     Number of connections per thread (default: 5000)
--num-threads N   Number of worker threads (default: 16)
```

The total connection count is `num-files * num-threads`. Fiona spawns one
io_uring instance per thread; Tokio uses its built-in multi-threaded scheduler.

### timer and ring_msg

These benchmarks have no CLI flags. They run a fixed workload and print timing
results.

## Running benchmarks

Start the server on one machine, then start the client on the other.

Server:

```bash
cargo bench --bench echo2 -- --server --fiona --ipv4-addr 192.168.10.12 --port 8016 --nr-files 6000
```

Client (from the second machine):

```bash
cargo bench --bench echo2 -- --client --fiona --ipv4-addr 192.168.10.12 --port 8016 --nr-files 6000
```

To compare runtimes, re-run with `--tokio` or `--compio` in place of `--fiona`.
Use a different port for each runtime to avoid conflicts when running back to
back:

```bash
# Fiona on port 8016
cargo bench --bench echo2 -- --server --fiona --ipv4-addr 192.168.10.12 --port 8016 --nr-files 5000

# Tokio on port 8015
cargo bench --bench echo2 -- --server --tokio --ipv4-addr 192.168.10.12 --port 8015 --nr-files 5000
```

For multi-threaded benchmarks:

```bash
cargo bench --bench echo_mt -- --server --fiona --ipv4-addr 192.168.10.12 --port 8016 --num-files 5000 --num-threads 16
```

For timer and ring_msg, run locally on a single machine:

```bash
cargo bench --bench timer
cargo bench --bench ring_msg
```

## Benchmark results

The table below shows `echo2` results collected on the two-machine setup
described above. Each row is a complete run at the given connection count.

| Connections | Fiona (total client loop) | Tokio (total client loop) | Fiona (avg client duration) | Tokio (avg client duration) |
|-------------|---------------------------|---------------------------|-----------------------------|-----------------------------|
| 1000        | 5.78s                     | 6.45s                     | 4.79s                       | 5.55s                       |
| 2000        | 11.95s                    | 13.59s                    | 9.99s                       | 10.41s                      |
| 3000        | 18.79s                    | 21.82s                    | 15.72s                      | 16.40s                      |
| 4000        | 27.69s                    | 36.73s                    | 23.35s                      | 22.64s                      |
| 5000        | 37.40s                    | 47.45s                    | 31.64s                      | 31.23s                      |
| 6000        | 48.48s                    | 58.10s                    | 39.17s                      | 39.24s                      |
| 7000        | 68.62s                    | 75.54s                    | 46.93s                      | 46.84s                      |
| 8000        | 67.96s                    | 102.84s                   | 54.28s                      | 54.45s                      |
| 9000        | 76.64s                    | 108.29s                   | 61.22s                      | 61.04s                      |
| 10000       | 112.34s                   | Timed out                 | 68.07s                      | Timed out                   |

**Total client loop time** is wall-clock time from spawning the first client to
the last client completing. **Average client duration** is the mean time a
single client takes to connect, send 1 MiB, receive 1 MiB, verify the hash,
and close.

Fiona's total loop time advantage grows with connection count. At 8000
connections, Fiona completes in 68s where Tokio needs 103s. At 10,000
connections Tokio times out entirely. The average per-client durations remain
close between the two runtimes, which indicates the total-time gap comes from
how each runtime schedules and dispatches I/O at high concurrency rather than
per-connection overhead.

The benchmarks also track statistical outliers (clients taking more than 2x
the average duration). No outliers meeting this criterion were detected for
either runtime during these runs.

These results are preliminary and subject to noise. They are not conclusive of
overall performance across all workloads.

## Cargo profiles for benchmarking

The `[profile.bench]` section in `Cargo.toml` enables thin LTO and sets
`codegen-units` to 1 for maximum optimization:

```toml
[profile.bench]
inherits = "release"
lto = "thin"
codegen-units = 1
```

An additional `[profile.lto]` profile provides the same settings for general
use outside the bench harness:

```toml
[profile.lto]
inherits = "release"
lto = "thin"
codegen-units = 1
```

Invoke it with `cargo build --profile lto`.

## RLIMIT_MEMLOCK

Fiona uses io_uring's zero-copy TCP send by default, which requires the kernel
to lock pages of memory for DMA. At high connection counts, the default
`memlock` limit is too small and sends will fail with `ENOMEM`.

Before running benchmarks at scale, set memlock to unlimited. See the
[Configuration](configuration.md) chapter for instructions.

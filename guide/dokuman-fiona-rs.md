# Fiona

Fiona is an io_uring-based async runtime for Rust that replaces Tokio on Linux. Where Tokio wraps epoll behind a reactor, Fiona submits I/O directly to the kernel ring - zero-copy TCP sends are the default, multishot accepts handle connection storms in a single submission, and bundled receives deliver data without copying it out of kernel buffers. If you want raw io_uring performance with an async/await API, Fiona gives you that without hand-rolling SQEs.

## The Runtime

Everything starts with an `IoContext`. You create one, spawn async tasks onto its executor, and call `run()` to drive the event loop until all tasks complete.

````rust
use fiona::IoContext;

fn main() {
    let mut io = IoContext::new().unwrap();
    let ex = io.get_executor();

    let h = ex.spawn(async move {
        println!("hello from io_uring");
        42
    });

    io.run();
}
````

`IoContext::new()` sets up the io_uring ring with default parameters. `get_executor()` hands you a handle for spawning. Each `spawn()` returns a `JoinHandle<T>` that implements `Future<Output = T>` - await it inside another task to get the result.

`run()` blocks the current thread, processing completions until every spawned task finishes. It returns the count of completed tasks.

For non-default ring sizes, use the builder:

````rust
let mut io = IoContext::builder()
    .sq_entries(256)
    .cq_entries(4096)
    .num_files(12000)
    .build()
    .unwrap();
````

The builder configures submission queue depth, completion queue depth, and the fixed-file table capacity. Fiona sets `DEFER_TASKRUN`, `COOP_TASKRUN`, and `SINGLE_ISSUER` on the ring automatically.

## TCP Networking

### Listening and Accepting

Bind a listener on IPv4 or IPv6. Multishot accept means one SQE handles many incoming connections without resubmission.

````rust
use fiona::net::TcpListener;
use std::net::Ipv4Addr;

let listener = TcpListener::bind_ipv4(&ex, Ipv4Addr::UNSPECIFIED, 8080).unwrap();

loop {
    let stream = listener.accept().await.unwrap();
    ex.spawn(handle_client(stream));
}
````

For port reuse (multi-threaded load balancing with `SO_REUSEPORT`):

````rust
use fiona::net::{TcpListener, TcpListenerOpts};

let opts = TcpListenerOpts {
    reuse_addr: true,
    reuse_port: true,
};
let listener = TcpListener::bind_ipv4_with_params(&ex, Ipv4Addr::UNSPECIFIED, 8080, &opts)
    .unwrap();
````

### Connecting

Connect to a remote endpoint. The default timeout is 3 seconds; override it with `with_timeout`. If the peer does not respond within the timeout, the connect fails.

````rust
use fiona::net::TcpClient;
use std::net::Ipv4Addr;
use std::time::Duration;

let stream = TcpClient::new(&ex)
    .with_timeout(Duration::from_secs(5))
    .connect_ipv4(Ipv4Addr::new(10, 0, 0, 1), 9000)
    .await
    .unwrap();
````

### Sending Data

TCP sends use zero-copy mode by default. The kernel avoids copying your data on the send path.

````rust
let buf = b"hello".to_vec();
let (result, buf) = stream.send(buf).await;
let bytes_sent = result.unwrap();
````

`send()` takes ownership of the buffer and returns it alongside the result, so you can reuse the allocation. To send a subrange:

````rust
let buf = vec![0u8; 4096];
let (result, buf) = stream.send_subspan(100..500, buf).await;
````

For maximum throughput, send from pre-registered fixed buffers:

````rust
let fixed_buf = ex.get_fixed_buf().unwrap();
let (result, fixed_buf) = stream.send_fixed(fixed_buf).await;
````

### Receiving Data

Receives use multishot bundled mode with kernel-provided buffers. You register a buffer group first (see Buffer Management below), then assign it to the stream before calling `recv()`.

````rust
stream.set_buf_group(0);
let bufs = stream.recv().await.unwrap();

for chunk in bufs.into_iter() {
    // chunk is &[u8], zero-copy from the kernel buffer ring
    process(chunk);
}
// buffers automatically recycle when `bufs` drops
````

`BorrowedBufs` implements `IntoIterator` yielding `&[u8]` slices. When the `BorrowedBufs` value drops, buffers return to the ring for reuse.

### Stream Lifecycle

Shut down one direction, close the socket, or cancel all in-flight operations:

````rust
stream.shutdown(libc::SHUT_WR).await.unwrap();
stream.close().await.unwrap();
stream.cancel().await.unwrap();
````

TCP streams, listeners, and their handles are cheaply cloneable via reference counting.

## Timers and Timeouts

### Sleeping

`sleep` submits a kernel-level timeout - no userspace timer thread, no polling.

````rust
use fiona::time;
use std::time::Duration;

time::sleep(&ex, Duration::from_secs(1)).await;
````

For repeated timeouts, create a reusable `Timer` object:

````rust
let timer = time::Timer::new(&ex);
timer.wait(Duration::from_millis(250)).await;
timer.wait(Duration::from_millis(500)).await; // reuse same timer
````

Timers cancel automatically when their future is dropped.

### Per-Stream Inactivity Timeout

Set a timeout on a TCP stream. If no send or recv activity occurs within the duration, pending operations auto-cancel.

````rust
stream.set_timeout(Duration::from_secs(30));
````

You can update the timeout on a live stream at any time.

## Buffer Management

Fiona provides two zero-copy buffer mechanisms.

### Buffer Groups (for receives)

Register a group of kernel-provided buffers. The runtime hands received data to you as borrowed references that recycle when dropped.

````rust
ex.register_buf_group(0, 128, 4096); // group 0, 128 buffers, 4096 bytes each

stream.set_buf_group(0);
let bufs = stream.recv().await.unwrap();
// iterate over bufs; they return to the ring on drop
````

Choose buffer count and size based on your concurrency and message size. More buffers handle more concurrent receives without stalling.

### Fixed Buffers (for sends and file writes)

Register pre-registered buffers with the kernel for an even faster I/O path. The kernel skips page-table lookups for these buffers.

````rust
ex.register_fixed_buffers(32, 65536); // 32 buffers, 64KB each

let mut buf = ex.get_fixed_buf().unwrap();
// fill buf with data...
let (result, buf) = stream.send_fixed(buf).await;
// buf returned for reuse
````

Fixed buffer registration can only be done once per context. A second call panics.

## TLS

Fiona provides TLS transport using rustls 0.23. A TLS handshake consumes a `TcpStream` and produces a `TlsStream` (server) or `TlsClient` (client).

### Client Handshake

````rust
use fiona::tls;
use std::sync::Arc;

let config = Arc::new(client_config); // rustls::ClientConfig
let tls_client = tls::client_handshake(stream, config, server_name)
    .await
    .unwrap();
````

### Server Handshake

````rust
let config = Arc::new(server_config); // rustls::ServerConfig
let tls_stream = tls::server_handshake(stream, config).await.unwrap();
````

### Reading and Writing

````rust
let mut plaintext = Vec::new();
let n = tls_stream.read(&mut plaintext).await.unwrap();

tls_stream.write(b"response data").unwrap();
tls_stream.flush(16384).await.unwrap();
````

`write` buffers plaintext into the TLS session synchronously. `read` is async - it pulls ciphertext from the TCP stream and decrypts. `flush` is async - it encrypts buffered plaintext and sends the ciphertext over TCP, up to the byte limit you specify.

### Shutdown

Initiate a graceful TLS shutdown by sending `close_notify`:

````rust
tls_stream.write_shutdown();
````

The peer detects `close_notify` and receives `Ok(0)` on its next read.

TLS errors are reported through a typed enum: `BadHandshake`, `InvalidCiphertext`, `InvalidServerConfiguration`, `InvalidClientConfiguration`, `TcpSendFailed`, `TcpRecvFailed`.

## File I/O

Files open with `O_DIRECT` for unbuffered I/O. Writes use pre-registered fixed buffers.

````rust
use fiona::fs::File;

let file = File::open(&ex, "/tmp/data.bin").await.unwrap();

let mut buf = ex.get_fixed_buf().unwrap();
// fill buf with data...
let (result, buf) = file.write_at(buf, 0).await;
result.unwrap();
// buf returned for reuse; write subranges with write_subspan_at
````

`write_at` writes at a specific byte offset. The buffer returns to you after the write completes. Files close automatically on drop via `io_uring_prep_close_direct`.

O_DIRECT requires aligned buffers and has filesystem-specific alignment constraints. Fixed buffers satisfy this naturally.

## Concurrency

### Cancel on Drop

In-flight async operations cancel automatically when you drop their futures. No manual cancellation required - just stop awaiting.

````rust
{
    let connect_future = TcpClient::new(&ex)
        .connect_ipv4(Ipv4Addr::new(10, 0, 0, 1), 9000);
    // drop without awaiting - the connect SQE is cancelled
}
````

### Multi-Threaded Operation

The runtime is single-threaded per io_uring ring. For multi-threaded designs, run one ring per OS thread with `SO_REUSEPORT` for load balancing across listeners.

Cross-thread wakeups use ring-to-ring messaging (`io_uring_prep_msg_ring`) so one thread can wake another thread's event loop without syscall overhead.

### Shared Ownership

TCP streams, listeners, TLS streams, and timers support cheap cloning via reference counting to share ownership across tasks.

## Configuration

### Builder Options

| Method | Purpose | Default |
|--------|---------|---------|
| `sq_entries(n)` | Submission queue depth | Ring default |
| `cq_entries(n)` | Completion queue depth | Ring default |
| `num_files(n)` | Fixed-file table capacity | Ring default |

### Cargo Profiles

Fiona provides opt-in Cargo profiles:

````toml
# Thin LTO with single codegen unit (invoke with --profile lto)
[profile.lto]
lto = "thin"
codegen-units = 1

# Release with debug symbols
[profile.release-with-debug]
inherits = "release"
debug = "full"
````

### Sanitizer Support

Enable address or thread sanitizers:

````bash
RUSTFLAGS="-Zsanitizer=address" cargo test --features sanitizers
RUSTFLAGS="-Zsanitizer=thread" cargo test --features sanitizers
````

### Docker

io_uring syscalls are blocked by Docker's default seccomp profile. Disable it:

````bash
docker run -it --security-opt seccomp=unconfined your-image
````

### RLIMIT_MEMLOCK

Zero-copy sends require sufficient locked memory. Without tuning, sends at scale fail with `ENOMEM`. Set memlock unlimited:

Add to `/etc/security/limits.conf`:
````
* - memlock unlimited
````

Add to `/etc/pam.d/common-session`:
````
session required pam_limits.so
````

## Benchmarking

Fiona ships benchmark binaries that compare against Tokio and Compio:

| Binary | What it measures |
|--------|-----------------|
| `echo2` | Single-threaded TCP echo |
| `echo_mt` | Multi-threaded TCP echo |
| `timer` | Timer operations |
| `ring_msg` | Ring-to-ring messaging |

Run on two machines:

````bash
# Server
cargo bench --bench echo2 -- --server --fiona --nr-files 6000

# Client (separate machine)
cargo bench --bench echo2 -- --client --fiona --nr-files 6000
````

Switch `--fiona` to `--tokio` or `--compio` to compare runtimes on the same workload.

*2026-08-12 07:45 - claude-opus-4-6-medium-thinking*

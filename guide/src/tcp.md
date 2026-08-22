# TCP Networking

Fiona's TCP stack is built directly on io_uring primitives. Multishot accept handles
connection storms with a single submission queue entry, sends use zero-copy by default,
and receives deliver data straight from kernel-provided buffers without copying.

## Listening and Accepting

### Binding

Create a `TcpListener` by binding to an IPv4 or IPv6 address. Bind functions are
**synchronous** - they return `Result<TcpListener>`, not a `Future`.

```rust
use fiona::net::TcpListener;
use std::net::Ipv4Addr;

let listener = TcpListener::bind_ipv4(&ex, Ipv4Addr::UNSPECIFIED, 8080).unwrap();
```

Four bind variants are available:

| Function | Address type | Options |
|----------|-------------|---------|
| `bind_ipv4` | `Ipv4Addr` | defaults |
| `bind_ipv6` | `Ipv6Addr` | defaults |
| `bind_ipv4_with_params` | `Ipv4Addr` | custom `TcpListenerOpts` |
| `bind_ipv6_with_params` | `Ipv6Addr` | custom `TcpListenerOpts` |

All bind functions accept typed address values (`Ipv4Addr` or `Ipv6Addr`), not strings.

### Listener Options

`TcpListenerOpts` controls socket options applied at bind time:

```rust
use fiona::net::{TcpListener, TcpListenerOpts};

let opts = TcpListenerOpts {
    reuse_addr: true,   // SO_REUSEADDR (default: true)
    reuse_port: true,   // SO_REUSEPORT (default: false)
};
let listener = TcpListener::bind_ipv4_with_params(
    &ex, Ipv4Addr::UNSPECIFIED, 8080, &opts,
).unwrap();
```

`reuse_addr` defaults to `true`, so you can restart a server without waiting for
`TIME_WAIT` sockets to expire. `reuse_port` defaults to `false` - enable it for
multi-threaded load balancing where each thread binds the same port.

### IPv6

Binding to IPv6 works the same way:

```rust
use std::net::Ipv6Addr;

let listener = TcpListener::bind_ipv6(&ex, Ipv6Addr::UNSPECIFIED, 8080).unwrap();
```

### Accepting Connections

`accept()` is async. Under the hood, the first call submits a single
`io_uring_prep_multishot_accept_direct` SQE that handles many incoming connections
without resubmission. Each `.await` yields the next connected `TcpStream`.

```rust
loop {
    let stream = listener.accept().await.unwrap();
    ex.spawn(handle_client(stream));
}
```

Because multishot accept uses a single SQE for the lifetime of the listener, there is
no per-connection submission overhead. The kernel fills completion queue entries as
connections arrive, and Fiona hands them to your code one at a time.

### Port Discovery

When you bind to port `0`, the OS assigns an ephemeral port. Retrieve it with `port()`:

```rust
let listener = TcpListener::bind_ipv4(&ex, Ipv4Addr::LOCALHOST, 0).unwrap();
let actual_port = listener.port();
```

## Connecting

`TcpClient` establishes outbound TCP connections. The default connection timeout is
3 seconds. Override it with `with_timeout`.

```rust
use fiona::net::TcpClient;
use std::net::Ipv4Addr;
use std::time::Duration;

let stream = TcpClient::new(&ex)
    .with_timeout(Duration::from_secs(5))
    .connect_ipv4(Ipv4Addr::new(10, 0, 0, 1), 9000)
    .await
    .unwrap();
```

`connect_ipv4` and `connect_ipv6` take `Ipv4Addr` and `Ipv6Addr` respectively, not
string addresses. If the peer does not respond within the timeout, the connect fails
with an error.

For IPv6:

```rust
use std::net::Ipv6Addr;

let stream = TcpClient::new(&ex)
    .connect_ipv6(Ipv6Addr::LOCALHOST, 9000)
    .await
    .unwrap();
```

## Sending Data

### Zero-Copy Send

TCP sends use zero-copy mode by default. Fiona calls `io_uring_prep_send_zc` under the
hood, which tells the kernel to avoid copying your buffer on the send path.

`send()` takes ownership of the buffer and returns it alongside the result, so you can
reuse the allocation:

```rust
let buf = b"hello".to_vec();
let (result, buf) = stream.send(buf).await;
let bytes_sent = result.unwrap();
```

The return type is `(Result<usize>, Vec<u8>)` - you always get the buffer back, whether
the send succeeded or failed.

### Sending a Subrange

`send_subspan` sends a byte subrange from a buffer without copying it into a new `Vec`:

```rust
let buf = vec![0u8; 4096];
let (result, buf) = stream.send_subspan(100..500, buf).await;
```

The range parameter accepts any `RangeBounds<usize>` value.

### Fixed Buffer Send

For maximum throughput, send from pre-registered fixed buffers. The kernel skips
page-table lookups for these buffers, removing overhead on the send path.

```rust
let fixed_buf = ex.get_fixed_buf().unwrap();
let (result, fixed_buf) = stream.send_fixed(fixed_buf).await;
```

`send_subspan_fixed` combines subrange sends with fixed buffers:

```rust
let fixed_buf = ex.get_fixed_buf().unwrap();
let (result, fixed_buf) = stream.send_subspan_fixed(100..500, fixed_buf).await;
```

Both fixed-buffer send variants use `io_uring_prep_send_zc_fixed` internally and return
the `FixedBuf` after completion for reuse.

## Receiving Data

Receives use multishot bundled mode with kernel-provided buffers. Fiona submits a
single `io_uring_prep_recv_multishot` SQE with `IORING_RECVSEND_BUNDLE`, which lets the
kernel fill multiple buffers from the buffer group in a single completion.

### Setup

Before calling `recv()`, assign a buffer group to the stream. The buffer group must
already be registered with the executor (see [Buffer Management](buffers.md)):

```rust
stream.set_buf_group(0);
```

`set_buf_group` takes a `u16` buffer group ID that references a group previously
registered via `Executor::register_buf_group()`. You must call it before the first
`recv()`.

### Receiving

```rust
let bufs = stream.recv().await.unwrap();

for chunk in bufs.into_iter() {
    process(chunk);
}
```

`recv()` returns `Result<BorrowedBufs>`. `BorrowedBufs` implements `IntoIterator`
yielding `&[u8]` slices - each slice is a zero-copy reference into a kernel-provided
buffer. When the `BorrowedBufs` value drops, all buffers automatically return to the
buffer ring for reuse.

A typical receive loop:

```rust
stream.set_buf_group(0);

loop {
    let bufs = stream.recv().await.unwrap();
    if bufs.into_iter().next().is_none() {
        break; // peer closed the connection
    }
    for chunk in bufs.into_iter() {
        process(chunk);
    }
}
```

## Stream Configuration

### Inactivity Timeout

Set a per-stream inactivity timeout. If no send or recv activity occurs within the
duration, pending operations auto-cancel.

```rust
stream.set_timeout(Duration::from_secs(30));
```

You can update the timeout on a live stream at any time. Internally this calls
`io_uring_prep_timeout_update` to adjust the multishot timeout without cancelling and
resubmitting it.

### Buffer Group Assignment

`set_buf_group(bgid)` associates a buffer group with the stream for receives. It must
be called before the first `recv()`. The buffer group is identified by the same `u16`
ID used when registering it with the executor.

## Stream Lifecycle

### Shutdown

Shut down one or both directions of the TCP connection:

```rust
stream.shutdown(libc::SHUT_WR).await.unwrap();   // stop sending
stream.shutdown(libc::SHUT_RD).await.unwrap();   // stop receiving
stream.shutdown(libc::SHUT_RDWR).await.unwrap();  // stop both
```

### Close

Explicitly close the socket. After this call, the file descriptor is released:

```rust
stream.close().await.unwrap();
```

### Cancel

Cancel all in-flight operations on the stream:

```rust
stream.cancel().await.unwrap();
```

This cancels every pending I/O operation associated with the stream's file descriptor.

### Listener Lifecycle

`TcpListener` also supports `close()` and `cancel()` with the same semantics:

```rust
listener.close().await.unwrap();
listener.cancel().await.unwrap();
```

## Shared Ownership

Both `TcpStream` and `TcpListener` implement `Clone`. Cloning is cheap - it increments
an internal reference count (`RefCount`) rather than duplicating the socket. This lets
you share a stream or listener across multiple spawned tasks:

```rust
let stream2 = stream.clone();
ex.spawn(async move {
    let buf = b"from task 2".to_vec();
    stream2.send(buf).await;
});
```

When the last clone drops, the underlying socket is closed automatically.

## Complete Example

A minimal TCP echo server:

```rust
use fiona::IoContext;
use fiona::net::TcpListener;
use std::net::Ipv4Addr;

fn main() {
    let mut io = IoContext::new().unwrap();
    let ex = io.get_executor();

    ex.register_buf_group(0, 128, 4096);

    let ex2 = ex.clone();
    ex.spawn(async move {
        let listener = TcpListener::bind_ipv4(
            &ex2, Ipv4Addr::UNSPECIFIED, 8080,
        ).unwrap();

        loop {
            let stream = listener.accept().await.unwrap();
            let ex3 = ex2.clone();
            ex2.spawn(async move {
                stream.set_buf_group(0);

                loop {
                    let bufs = stream.recv().await.unwrap();
                    let mut data = Vec::new();
                    for chunk in bufs.into_iter() {
                        data.extend_from_slice(chunk);
                    }
                    if data.is_empty() {
                        break;
                    }
                    let (result, _) = stream.send(data).await;
                    result.unwrap();
                }
            });
        }
    });

    io.run();
}
```

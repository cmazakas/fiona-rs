# TLS

Fiona provides TLS transport powered by [rustls](https://docs.rs/rustls/0.23)
0.23 (with rustls-pki-types 1.14.1). A TLS session is established by performing
a handshake over an existing `TcpStream`. The handshake consumes the TCP stream
and produces a typed TLS wrapper - this is a type-state transition. Once you
hand a `TcpStream` to a handshake function, you interact with the connection
exclusively through the TLS type.

There are two handshake functions producing two result types:

| Function | Result type | Use case |
|----------|-------------|----------|
| `server_handshake` | `TlsStream` | Server accepting a client connection |
| `client_handshake` | `TlsClient` | Client connecting to a server |

Both `TlsStream` and `TlsClient` offer the same four-method API: `write`,
`read`, `flush`, and `write_shutdown`.

## Server Handshake

`server_handshake` takes a connected `TcpStream` and an `Arc<rustls::ServerConfig>`,
performs the TLS handshake asynchronously, and returns a `TlsStream`:

```rust
use fiona::tls;
use std::sync::Arc;

let config = Arc::new(server_config); // rustls::ServerConfig
let tls_stream = tls::server_handshake(stream, config).await.unwrap();
```

The full signature:

```rust
pub async fn server_handshake(
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
) -> Result<TlsStream, Error>
```

If the `ServerConfig` is invalid, the function returns
`Error::InvalidServerConfiguration` without touching the network. If the
handshake exchange itself fails, it returns `Error::BadHandshake`.

## Client Handshake

`client_handshake` takes a connected `TcpStream`, an `Arc<rustls::ClientConfig>`,
and a `ServerName` identifying the peer:

```rust
use fiona::tls;
use std::sync::Arc;

let config = Arc::new(client_config); // rustls::ClientConfig
let server_name = "example.com".try_into().unwrap();
let tls_client = tls::client_handshake(stream, config, server_name)
    .await
    .unwrap();
```

The full signature:

```rust
pub async fn client_handshake(
    stream: TcpStream,
    config: Arc<rustls::ClientConfig>,
    server_name: rustls_pki_types::ServerName<'static>,
) -> Result<TlsClient, Error>
```

An invalid `ClientConfig` returns `Error::InvalidClientConfiguration`. A failed
handshake returns `Error::BadHandshake`.

## Reading and Writing

The read/write API is deliberately split into synchronous and asynchronous
operations. Understanding which calls are sync and which are async is essential
to using TLS correctly in Fiona.

### write (synchronous)

`write` buffers plaintext into the TLS session. It does not touch the network
and does not encrypt anything yet. Because it is purely a memory operation,
it is **synchronous** - do not `.await` it:

```rust
let n = tls_stream.write(b"hello, world").unwrap();
```

The signature:

```rust
pub fn write(&self, plaintext: &[u8]) -> Result<usize, Error>
```

You can call `write` multiple times to accumulate plaintext before flushing.
After `write_shutdown` has been called, `write` returns `Ok(0)` without
buffering anything.

### flush (async)

`flush` encrypts the buffered plaintext and sends the resulting ciphertext
over TCP. The `max_send_size` parameter caps how many bytes of ciphertext are
sent in a single operation:

```rust
let bytes_sent = tls_stream.flush(16384).await.unwrap();
```

The signature:

```rust
pub async fn flush(&self, max_send_size: usize) -> Result<usize, Error>
```

If there is no buffered data to send, `flush` returns `Ok(0)` immediately.
Otherwise it encrypts via rustls, writes the ciphertext into an internal
buffer, and submits a TCP send through io_uring. The return value is the
number of ciphertext bytes sent over the wire.

A typical write cycle is: call `write` one or more times, then call `flush`
to push everything to the peer.

```rust
tls_stream.write(b"HTTP/1.1 200 OK\r\n").unwrap();
tls_stream.write(b"Content-Length: 5\r\n\r\n").unwrap();
tls_stream.write(b"hello").unwrap();
tls_stream.flush(65536).await.unwrap();
```

### read (async)

`read` pulls ciphertext from the TCP stream via io_uring, decrypts it through
rustls, and appends the resulting plaintext to the provided buffer:

```rust
let mut plaintext = Vec::new();
let n = tls_stream.read(&mut plaintext).await.unwrap();
```

The signature:

```rust
pub async fn read(&self, buf: &mut Vec<u8>) -> Result<usize, Error>
```

`read` blocks (in the async sense) until at least one byte of plaintext is
available or the peer closes the connection. If the peer sends a `close_notify`
alert, `read` detects it, sends a reciprocal `close_notify` automatically, and
returns `Ok(0)`.

If the received ciphertext is malformed, `read` returns
`Error::InvalidCiphertext`.

## Shutdown

TLS connections are shut down gracefully using the `close_notify` alert defined
by the TLS specification.

### write_shutdown (synchronous)

`write_shutdown` queues a `close_notify` alert into the TLS session. Like
`write`, it is **synchronous** - it does not send anything over the network:

```rust
tls_stream.write_shutdown();
```

The signature:

```rust
pub fn write_shutdown(&self)
```

After calling `write_shutdown`, you must call `flush` to actually transmit the
`close_notify` to the peer:

```rust
tls_stream.write_shutdown();
tls_stream.flush(4096).await.unwrap();
```

Calling `write_shutdown` more than once is harmless - subsequent calls are
no-ops. Once shutdown has been initiated, `write` returns `Ok(0)`.

### Peer shutdown detection

When the peer sends `close_notify`, the next `read` call detects it
automatically. Fiona sends a reciprocal `close_notify` on your behalf and
`read` returns `Ok(0)`. You do not need to call `write_shutdown` manually in
this case.

## Cloning

Both `TlsStream` and `TlsClient` implement `Clone`. Internally they wrap
their state in `Rc<StreamImpl<...>>`, so cloning is cheap - it increments a
reference count. This lets you share a TLS connection across tasks on the
same executor:

```rust
let tls_reader = tls_stream.clone();
let tls_writer = tls_stream;

ex.spawn(async move {
    let mut buf = Vec::new();
    tls_reader.read(&mut buf).await.unwrap();
});

ex.spawn(async move {
    tls_writer.write(b"ping").unwrap();
    tls_writer.flush(4096).await.unwrap();
});
```

Because the internals use `Rc` (not `Arc`), the TLS types are `!Send` - they
cannot be moved across threads. This matches Fiona's single-threaded-per-ring
design.

## Error Handling

TLS operations report errors through a dedicated `tls::Error` enum:

| Variant | Meaning |
|---------|---------|
| `BadHandshake` | The TLS handshake failed (invalid certificate, protocol mismatch, unexpected EOF) |
| `InvalidCiphertext` | Received ciphertext that rustls could not decrypt or verify |
| `InvalidServerConfiguration` | The `rustls::ServerConfig` could not produce a valid `ServerConnection` |
| `InvalidClientConfiguration` | The `rustls::ClientConfig` could not produce a valid `ClientConnection` |
| `TcpSendFailed` | The underlying TCP send operation failed |
| `TcpRecvFailed` | The underlying TCP recv operation failed |

The error type derives `Debug` and `PartialEq`, so you can match on it
directly in tests:

```rust
use fiona::tls::Error;

match tls_stream.read(&mut buf).await {
    Ok(0) => println!("peer closed"),
    Ok(n) => println!("read {n} bytes"),
    Err(Error::InvalidCiphertext) => eprintln!("corrupted data"),
    Err(e) => eprintln!("tls error: {e:?}"),
}
```

## Complete Example

A minimal TLS echo server that accepts one connection, reads a message,
echoes it back, and shuts down:

```rust
use fiona::{IoContext, net::TcpListener, tls};
use std::sync::Arc;

fn main() {
    let mut io = IoContext::new().unwrap();
    let ex = io.get_executor();

    let server_config = Arc::new(make_server_config());

    ex.spawn(async move {
        let listener = TcpListener::new(&ex, "127.0.0.1", 4443).unwrap();
        let stream = listener.accept().await.unwrap();

        let tls_stream = tls::server_handshake(stream, server_config)
            .await
            .unwrap();

        let mut buf = Vec::new();
        let n = tls_stream.read(&mut buf).await.unwrap();

        tls_stream.write(&buf[..n]).unwrap();
        tls_stream.flush(65536).await.unwrap();

        tls_stream.write_shutdown();
        tls_stream.flush(4096).await.unwrap();
    });

    io.run();
}
```

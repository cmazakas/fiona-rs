# Buffer Management

Fiona provides two zero-copy buffer mechanisms that map directly onto
io_uring kernel features. Buffer groups handle the receive path - the
kernel fills buffers on your behalf during multishot receives. Fixed
buffers handle the send and file-write path - you pre-register memory
so the kernel skips page-table lookups when transferring data out. The
two mechanisms serve different purposes, and most programs use both.

## Buffer Groups (Provided Buffers)

A buffer group is a ring of kernel-provided buffers. When a multishot
receive completes, the kernel picks buffers from the ring, fills them
with incoming data, and hands you borrowed references. You never
allocate per-receive buffers yourself.

### Registering a Group

```rust
// group 0, 128 buffers, 4096 bytes each
ex.register_buf_group(0, 128, 4096);
```

`register_buf_group` takes three arguments:

- `bgid: u16` - the buffer group ID, an arbitrary label you choose
- `num_bufs: u32` - how many buffers to allocate in the ring
- `buf_len: u32` - the byte size of each buffer

Under the hood this sets up an `io_uring_buf_ring` that the kernel
draws from when completing receive operations.

### Assigning a Group to a Stream

Before calling `recv()`, tell the stream which buffer group to use:

```rust
stream.set_buf_group(0);
let bufs = stream.recv().await.unwrap();
```

The group must already be registered. Receives on this stream will
pull buffers from group 0, fill them with incoming data, and bundle
the results into a single completion.

### Iterating Over Received Data

`recv()` returns a `BorrowedBufs` value. It implements `IntoIterator`,
yielding `&[u8]` slices - one per buffer the kernel filled:

```rust
stream.set_buf_group(0);
let bufs = stream.recv().await.unwrap();

for chunk in bufs.iter() {
    // chunk is &[u8], zero-copy from the kernel buffer ring
    process(chunk);
}
```

Each slice points directly into the buffer ring. There is no copy from
kernel space into a userspace `Vec` - this is the zero-copy receive
path.

### Automatic Recycling

When a `BorrowedBufs` value drops, its buffers return to the ring
automatically. You do not need to free or recycle them manually:

```rust
{
    let bufs = stream.recv().await.unwrap();
    for chunk in bufs.iter() {
        process(chunk);
    }
} // bufs drops here - buffers recycle back to the ring
```

This means you can hold a `BorrowedBufs` across `.await` points if
you need to, but the buffers stay claimed until the value is dropped.
In a high-throughput server, drop borrowed buffers promptly so they
are available for the next receive.

### Sizing Guidance

The buffer count controls how many concurrent receives the group
can serve before stalling. If every buffer in the ring is claimed by
in-flight receives, new receive completions must wait.

Rules of thumb:

- **One stream, simple echo** - 16 to 32 buffers is plenty.
- **Many concurrent connections sharing a group** - size the ring
  to at least the expected number of simultaneous receives. 128 to
  256 is a reasonable starting point.
- **Large messages** - increase `buf_len` so each buffer can hold a
  full message without fragmentation.

You can register multiple groups with different IDs and assign
different groups to different streams if your workload has varied
buffer size requirements.

## Fixed Buffers (Pre-Registered Buffers)

Fixed buffers are memory regions registered with the kernel ahead of
time. When the kernel performs I/O against a fixed buffer, it skips the
page-table walk it would normally do for arbitrary userspace memory.
This is the fastest send and write path Fiona offers.

### Registering Fixed Buffers

```rust
// 32 buffers, 64KB each
ex.register_fixed_buffers(32, 65536);
```

`register_fixed_buffers` takes:

- `num_bufs: u32` - how many buffers to create
- `buf_len: u32` - the byte size of each buffer

This calls `io_uring_register_buffers` to pin the memory with the
kernel. **Registration can only be done once per `IoContext`.** A
second call panics:

```text
A fixed buffer sequence is already registered.
```

Plan your buffer count and size at startup. If you need more buffers
later, allocate a larger pool up front.

### Acquiring a Buffer

```rust
let mut buf = ex.get_fixed_buf().unwrap();
```

`get_fixed_buf()` returns `Option<FixedBuf>`. It returns `None` if
every buffer in the pool is currently checked out. The caller is
responsible for retrying or structuring the program so buffers are
returned promptly.

`FixedBuf` implements `Deref<Target = [u8]>` and `DerefMut`, so you
can read and write it like a regular byte slice:

```rust
let mut buf = ex.get_fixed_buf().unwrap();
buf[..5].copy_from_slice(b"hello");
```

### Sending with Fixed Buffers

Use `send_fixed` on a `TcpStream` to send from a fixed buffer:

```rust
let mut buf = ex.get_fixed_buf().unwrap();
buf[..5].copy_from_slice(b"hello");

let (result, buf) = stream.send_fixed(buf).await;
let bytes_sent = result.unwrap();
```

`send_fixed` takes ownership of the `FixedBuf` and returns it after
the send completes, so you can reuse it immediately. To send a
subrange:

```rust
let (result, buf) = stream.send_subspan_fixed(0..5, buf).await;
```

### Writing Files with Fixed Buffers

File I/O in Fiona uses `O_DIRECT`, which requires aligned buffers.
Fixed buffers satisfy this naturally. Use `write_at` to write at a
byte offset:

```rust
let file = File::open(&ex, "/tmp/data.bin").await.unwrap();

let mut buf = ex.get_fixed_buf().unwrap();
buf[..payload.len()].copy_from_slice(&payload);

let (result, buf) = file.write_at(buf, 0).await;
result.unwrap();
// buf returned for reuse
```

For writing a subrange of the buffer:

```rust
let (result, buf) = file.write_subspan_at(0..payload.len(), buf, offset).await;
```

### Buffer Lifecycle

A `FixedBuf` returns to the free pool automatically when it drops.
Every I/O operation that accepts a `FixedBuf` returns it on completion,
so the typical pattern is:

```rust
let mut buf = ex.get_fixed_buf().unwrap();
// fill buf...
let (result, buf) = stream.send_fixed(buf).await;
// buf is ready to fill again
let (result, buf) = stream.send_fixed(buf).await;
// or just let buf drop to return it to the pool
```

If you drop a `FixedBuf` without using it (for example, during error
handling), it still returns to the pool. No buffers leak.

## Fixed File Descriptors

Fiona uses io_uring's fixed-file table for all file descriptors -
sockets, listeners, and files. When a file is opened with
`File::open`, Fiona passes `IORING_FILE_INDEX_ALLOC` to
`io_uring_prep_open_direct`, which allocates a slot in the
kernel's fixed-file table rather than using a regular file descriptor.

Subsequent I/O operations on these descriptors set `IOSQE_FIXED_FILE`,
telling the kernel to look up the descriptor in the pre-registered
table instead of the process's file descriptor table. This avoids
atomic reference counting on the file structure for every I/O
submission.

Closing is handled by `io_uring_prep_close_direct`. Files close
automatically when dropped - you do not need to call close explicitly.

The fixed-file table capacity is set at `IoContext` creation time via
the builder:

```rust
let mut io = IoContext::builder()
    .num_files(12000)
    .build()
    .unwrap();
```

Size the table to accommodate the maximum number of simultaneously open
file descriptors (sockets, listeners, and files combined).

## Choosing the Right Mechanism

| Mechanism | Direction | How it works | Typical use |
|-----------|-----------|-------------|-------------|
| Buffer groups | Receive | Kernel fills ring buffers during multishot recv | TCP server receive path |
| Fixed buffers | Send / Write | Pre-registered memory skips page-table lookups | TCP sends, file writes |
| Fixed files | Both | Descriptors in a kernel table skip fd lookups | All I/O (automatic) |

A typical server setup registers both:

```rust
let mut io = IoContext::builder()
    .num_files(6000)
    .build()
    .unwrap();
let ex = io.get_executor();

// Buffer group for receives
ex.register_buf_group(0, 256, 4096);

// Fixed buffers for sends and file writes
ex.register_fixed_buffers(64, 65536);

ex.spawn(async move {
    let listener = TcpListener::bind_ipv4(&ex, Ipv4Addr::UNSPECIFIED, 8080).unwrap();

    loop {
        let stream = listener.accept().await.unwrap();
        let ex = ex.clone();
        ex.spawn(async move {
            stream.set_buf_group(0);

            // Receive with zero-copy provided buffers
            let bufs = stream.recv().await.unwrap();

            // Send response with a fixed buffer
            let mut out = ex.get_fixed_buf().unwrap();
            let response = build_response(&bufs);
            out[..response.len()].copy_from_slice(&response);
            let (result, _out) = stream.send_subspan_fixed(0..response.len(), out).await;
            result.unwrap();
        });
    }
});

io.run();
```

Buffer groups give you zero-copy receives. Fixed buffers give you
the fastest send and write path. Fixed file descriptors are automatic -
Fiona uses them for everything. Together they eliminate copies and
kernel bookkeeping overhead across the entire I/O pipeline.

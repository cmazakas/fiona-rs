# File I/O

Fiona exposes asynchronous file operations through `io_uring`, bypassing the kernel page cache entirely. Files open with `O_DIRECT`, writes go through pre-registered fixed buffers, and every operation - open, write, close, cancel - is a single SQE submission. There is no read API yet; the file interface is currently write-only.

## Opening a File

`File::open` is async. It submits an `io_uring_prep_open_direct` SQE and returns a future that resolves to a `File` handle:

```rust
use fiona::fs::File;

let file = File::open(&ex, "/tmp/data.bin").await.unwrap();
```

The open uses these flags:

- `O_RDWR | O_CREAT | O_DIRECT` - read-write access, create if missing, bypass page cache
- `IORING_FILE_INDEX_ALLOC` - the kernel allocates a slot in the fixed-file table automatically

Because the file descriptor lives in io_uring's fixed-file table rather than the process file-descriptor table, subsequent I/O on the file sets `IOSQE_FIXED_FILE` and avoids per-operation `fget`/`fput` overhead in the kernel.

The path accepts anything that implements `AsRef<Path>`, so `&str`, `String`, `PathBuf`, and friends all work.

## Writing Data

Writes require a `FixedBuf` - a pre-registered buffer obtained from the executor. You must register fixed buffers before performing any file writes:

```rust
ex.register_fixed_buffers(32, 65536); // 32 buffers, 64KB each
```

### write_at

Write the entire buffer contents at a byte offset:

```rust
let mut buf = ex.get_fixed_buf().unwrap();
// fill buf with data...
let (result, buf) = file.write_at(buf, 0).await;
let bytes_written = result.unwrap();
```

The signature is:

```rust
fn write_at(&self, buf: FixedBuf, offset: u64)
    -> impl Future<Output = (Result<usize, Error>, FixedBuf)>
```

### write_subspan_at

Write a byte subrange of the buffer. The range parameter accepts any `RangeBounds<usize>` - inclusive, exclusive, half-open, or full:

```rust
let (result, buf) = file.write_subspan_at(0..4096, buf, 0).await;
```

This is useful when only part of a large fixed buffer contains meaningful data.

### Ownership Transfer

Both write methods take ownership of the `FixedBuf` and return it alongside the result. This ownership transfer pattern prevents aliasing - the kernel holds a pointer to the buffer memory during the I/O operation, so no other code should touch it until the write completes. Once the future resolves, you get the buffer back and can refill and reuse it:

```rust
let mut buf = ex.get_fixed_buf().unwrap();

// first write
buf[..5].copy_from_slice(b"hello");
let (result, mut buf) = file.write_at(buf, 0).await;
result.unwrap();

// reuse the same buffer for a second write
buf[..5].copy_from_slice(b"world");
let (result, buf) = file.write_at(buf, 4096).await;
result.unwrap();
```

Only one write can be in flight per `File` handle at a time. Attempting a second concurrent write panics.

## O_DIRECT Constraints

`O_DIRECT` bypasses the kernel page cache, which gives predictable latency and avoids double-buffering, but imposes alignment requirements:

- The buffer memory address must be aligned (typically to 512 bytes or the filesystem block size)
- The write offset must be aligned to the same boundary
- The write length must be a multiple of the alignment

These constraints are filesystem-specific. Fixed buffers satisfy the memory alignment requirement naturally because `register_fixed_buffers` allocates page-aligned memory. You are still responsible for aligning offsets and lengths to your filesystem's requirements.

## Automatic Close on Drop

When a `File` is dropped, Fiona submits an `io_uring_prep_close_direct` SQE to close the fixed-file descriptor. The close is fire-and-forget: it sets `IOSQE_CQE_SKIP_SUCCESS` so the kernel does not generate a completion entry on success. No `.await` is needed; the file cleans up after itself when it goes out of scope:

```rust
{
    let file = File::open(&ex, "/tmp/ephemeral.bin").await.unwrap();
    let (result, buf) = file.write_at(buf, 0).await;
    result.unwrap();
    // file drops here - close submitted automatically
}
```

## Cancel on Drop

If you drop an `OpenFuture` or `WriteFuture` before it completes, Fiona cancels the in-flight SQE by submitting `io_uring_prep_cancel64`. This prevents completed-but-unobserved operations from leaking resources:

```rust
{
    let open_future = File::open(&ex, "/tmp/maybe.bin");
    // drop without awaiting - the open SQE is cancelled
}
```

The same applies to write futures. If a task is cancelled or a branch of `select!`-style logic drops a write future, the underlying kernel operation is cancelled rather than left dangling.

For open operations specifically, if the open already completed but the future was never polled to observe the result, the drop handler closes the newly opened file descriptor to avoid leaking it.

## Complete Example

```rust
use fiona::IoContext;
use fiona::fs::File;

fn main() {
    let mut io = IoContext::new().unwrap();
    let ex = io.get_executor();

    ex.register_fixed_buffers(4, 4096);

    ex.spawn(async move {
        let file = File::open(&ex, "/tmp/output.bin").await.unwrap();

        let mut buf = ex.get_fixed_buf().unwrap();
        let data = b"fiona writes to io_uring";
        buf[..data.len()].copy_from_slice(data);

        let (result, _buf) = file.write_at(buf, 0).await;
        result.unwrap();

        // file closes automatically when dropped
    });

    io.run();
}
```

## Error Handling

File operations return `fiona::fs::Error`:

```rust
pub enum Error {
    OpenError,
    WriteError,
}
```

`OpenError` maps to a negative result from the open SQE - the kernel could not open the file. Common causes include missing directories in the path, permission errors, or filesystem-level failures.

`WriteError` maps to a negative result from the write SQE. With `O_DIRECT`, alignment violations surface here as `EINVAL`.

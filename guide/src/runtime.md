# The Runtime

Everything in Fiona starts with an `IoContext`. It owns the io_uring ring,
manages the task queue, and drives completions. You create one, obtain an
`Executor` handle for spawning tasks, and call `run()` to enter the event loop.

```rust
use fiona::IoContext;

fn main() {
    let mut io = IoContext::new();
    let ex = io.get_executor();

    ex.spawn(async move {
        println!("hello from io_uring");
    });

    io.run();
}
```

This is the minimal Fiona program. `IoContext::new()` sets up the ring with
sensible defaults, `get_executor()` gives you a handle to spawn work, and
`run()` blocks the current thread until every spawned task finishes.

## Creating an IoContext

Fiona offers two construction paths.

### `IoContext::new()`

Creates a ring with default parameters:

| Parameter | Default |
|-----------|---------|
| `sq_entries` | 256 |
| `cq_entries` | 1024 |
| `num_files` | 1024 |

Use `new()` when you have no reason to change the ring geometry. It is
equivalent to calling `IoContext::builder().build()`.

### `IoContext::builder()`

Returns an `IoContextBuilder` that lets you configure the ring before
construction:

```rust
let mut io = IoContext::builder()
    .sq_entries(512)
    .cq_entries(8192)
    .num_files(12000)
    .build();
```

The three builder methods control:

- **`sq_entries(n)`** - Submission queue depth. Each in-flight I/O operation
  occupies one SQ slot. If you expect bursts of many concurrent submissions
  (e.g. a batch of connects followed by a batch of sends), increase this.

- **`cq_entries(n)`** - Completion queue depth. Multishot operations like
  `accept` and `recv` can generate completions faster than you consume them.
  A larger CQ prevents the kernel from stalling submissions when the queue is
  full.

- **`num_files(n)`** - Fixed-file table capacity. Fiona uses direct descriptors
  for sockets and files, which are allocated from this table. Set it high enough
  to cover the maximum number of simultaneous open file descriptors your
  application needs.

Use the builder when you know your workload exceeds the defaults - high
connection counts, deep I/O pipelines, or large file descriptor pools.

## io_uring Flags

Fiona sets three io_uring setup flags on every ring automatically. You cannot
disable them. Understanding what they mean helps explain the runtime's
constraints.

### `DEFER_TASKRUN`

The kernel defers completion processing until the application explicitly asks
for it (via `io_uring_enter`). This avoids surprise interrupts from kernel
threads and keeps all completion work on the thread that owns the ring. It is
the reason `run()` must be called from the same thread that created the
`IoContext`.

### `COOP_TASKRUN`

Completions are processed cooperatively rather than via async interrupts. The
kernel only delivers completions when the application enters the ring. Combined
with `DEFER_TASKRUN`, this means the event loop has full control over when work
happens - no signal-driven context switches.

### `SINGLE_ISSUER`

Tells the kernel that only one thread will ever submit to this ring. The kernel
can skip internal locking, which reduces overhead. This flag enforces the
single-threaded-per-ring design: you must not submit SQEs from a thread other
than the one that created the `IoContext`.

Together, these flags give Fiona a cooperative, single-threaded event loop with
no kernel-side locking and no async interrupts. For multi-threaded designs,
run one `IoContext` per OS thread.

## The Executor

`IoContext::get_executor()` returns an `Executor` handle. The `Executor` is
the interface for spawning tasks and registering buffers.

```rust
let ex = io.get_executor();
```

`Executor` implements `Clone`. Internally it holds an `Rc` to the same
`IoContext` frame, so cloning is cheap. You will pass the executor (or a clone
of it) into spawned tasks, I/O constructors, and buffer registration calls.

Because the executor is `Rc`-based (not `Arc`), it is `!Send` - you cannot
move it to another thread. This is intentional: the io_uring ring is
single-threaded.

```rust
let ex = io.get_executor();

let ex2 = ex.clone();
ex.spawn(async move {
    // ex2 is available inside the task
    fiona::time::sleep(&ex2, Duration::from_secs(1)).await;
});
```

## Spawning Tasks

`Executor::spawn()` creates a new async task on the runtime and returns a
`JoinHandle<T>`:

```rust
let handle: JoinHandle<i32> = ex.spawn(async move {
    42
});
```

### JoinHandle

`JoinHandle<T>` implements `Future<Output = T>`. You can `.await` it from
inside another task to retrieve the spawned task's return value:

```rust
use fiona::IoContext;

fn main() {
    let mut io = IoContext::new();
    let ex = io.get_executor();

    let ex2 = ex.clone();
    ex.spawn(async move {
        let h = ex2.spawn(async move {
            100 + 23
        });

        let result = h.await;
        assert_eq!(result, 123);
    });

    io.run();
}
```

If you do not need the return value, you can drop the `JoinHandle`. The spawned
task still runs to completion - dropping the handle just means nobody is waiting
for the result.

## Running the Event Loop

`IoContext::run()` blocks the calling thread and processes completions until
every spawned task finishes. It returns a `u64` - the count of tasks that
completed during the run:

```rust
let mut io = IoContext::new();
let ex = io.get_executor();

ex.spawn(async { /* task A */ });
ex.spawn(async { /* task B */ });

let completed = io.run();
assert_eq!(completed, 2);
```

`run()` must be called from the same thread that created the `IoContext`. This
is a requirement of the `DEFER_TASKRUN` and `SINGLE_ISSUER` flags - the kernel
expects the issuing thread to also be the one processing completions.

Once `run()` returns, you can spawn more tasks and call `run()` again. The
`IoContext` is reusable across multiple run cycles.

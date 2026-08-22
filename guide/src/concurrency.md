# Concurrency

Fiona's concurrency model follows a single-threaded-per-ring design. Each `IoContext` owns one io_uring instance, and the `SINGLE_ISSUER` flag guarantees that only one thread ever submits to that ring. Parallelism comes from running multiple rings on multiple OS threads, not from sharing a single ring across threads.

## Cancel on Drop

In-flight async operations cancel automatically when you drop their futures. No manual cancellation required - just stop awaiting.

```rust
use fiona::net::TcpClient;
use std::net::Ipv4Addr;

async fn example(ex: fiona::Executor) {
    {
        let connect_future = TcpClient::new(&ex)
            .connect_ipv4(Ipv4Addr::new(10, 0, 0, 1), 9000);
        // drop without awaiting - the connect SQE is cancelled
    }
}
```

### How it works

When a future backed by an io_uring operation is dropped before completion, the runtime:

1. Sets `eager_dropped = true` on the internal operation state
2. Submits a cancel SQE using `IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED`
3. When the cancelled CQE arrives, the completion handler drains it without waking any task

This means the kernel cleans up in-flight operations deterministically. You never leak SQEs or accumulate stale completions. The same mechanism applies to timers (which use `io_uring_prep_timeout_remove` on drop), file opens, file writes, TCP connects, and multishot receives.

For TCP streams and listeners, you can also cancel all in-flight operations explicitly:

```rust
stream.cancel().await.unwrap();
```

This submits a cancel SQE with `IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED` for that fixed file descriptor, cancelling every pending operation on the stream in a single submission.

## Shared Ownership

TCP streams, listeners, TLS streams, and timers all support cheap cloning via internal reference counting. The `RefCount` struct tracks both object references (clones of the handle) and operation references (in-flight SQEs). The underlying resource is freed only when both counts reach zero.

```rust
use fiona::net::TcpListener;
use std::net::Ipv4Addr;

async fn example(ex: fiona::Executor) {
    let listener = TcpListener::bind_ipv4(&ex, Ipv4Addr::UNSPECIFIED, 8080).unwrap();
    let listener2 = listener.clone(); // cheap Rc-style clone

    ex.spawn(async move {
        let stream = listener2.accept().await.unwrap();
        // handle connection
    });
}
```

Cloning a `TcpStream` or `Timer` increments the object reference count. When the last clone drops, pending operations continue to hold the resource alive until their completions arrive and are drained.

## Stable Submit

Fiona tracks pending io_uring operations in a `SlotMap`. When the map needs to grow (because you have submitted more operations than its current capacity), the runtime flushes the submission queue to the kernel first. This ensures that all SQE `user_data` pointers remain valid - they never dangle due to a reallocation moving the backing storage.

This property - called stable submit - means you can submit new io_uring operations from within completion handlers or from deeply nested async code without worrying about pointer invalidation. It enables pipelining: the completion of one operation can immediately enqueue the next without returning to the event loop.

```rust
use fiona::time::Timer;
use std::time::Duration;

async fn pipeline(ex: fiona::Executor) {
    let mut join_handles = Vec::new();
    for _ in 0..16_000 {
        join_handles.push(ex.spawn(async {
            let timer = fiona::time::Timer::new(&ex);
            timer.wait(Duration::from_millis(250)).await.unwrap();
            1u64
        }));
    }

    let mut count = 0u64;
    for h in join_handles {
        count += h.await;
    }
    assert_eq!(count, 16_000);
}
```

Even when the number of concurrent operations exceeds the initial SlotMap capacity (1024 by default), the runtime grows safely by flushing before reallocating.

## Cross-Thread Waking

Each task carries a `Waker` that external threads can use to signal readiness. When a thread calls `wake()` on a Fiona waker:

1. A `Weak` pointer to the task is sent through an mpsc channel into the target ring's task queue
2. If `needs_wake` is true (meaning the ring is blocked in `io_uring_submit_and_wait`), the waker calls `io_uring_prep_msg_ring` followed by `io_uring_register_sync_msg` to poke the target ring's completion queue

This ring-to-ring messaging wakes the event loop without a separate system call. The `needs_wake` atomic flag prevents redundant wakeups when the ring is already processing completions.

```rust
use std::thread;
use std::time::Duration;

fn cross_thread_example() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(async move {
        // The futures mpsc channel uses Fiona's Waker internally.
        // A sender on another thread calling send() triggers cross-thread waking.
        let (tx, mut rx) = futures::channel::mpsc::channel::<i32>(1024);

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.clone().try_send(42).unwrap();
        });

        use futures::StreamExt;
        let value = rx.next().await.unwrap();
        assert_eq!(value, 42);
    });

    ioc.run();
}
```

This mechanism also enables integration with thread pools like Rayon. Spawn CPU-heavy work onto a thread pool, and when it completes, the pool thread wakes the Fiona task through the standard `Waker` interface:

```rust
use std::sync::{Arc, Mutex};

async fn offload_to_rayon(ex: fiona::Executor, pool: &rayon::ThreadPool) {
    let result = Arc::new(Mutex::new(None));
    let r = result.clone();

    // Get our waker for the thread pool to call
    let waker = futures::future::poll_fn(|cx| {
        std::task::Poll::Ready(cx.waker().clone())
    }).await;

    pool.spawn(move || {
        let hash = expensive_computation();
        *r.lock().unwrap() = Some(hash);
        waker.wake();
    });

    // Task resumes when the pool thread calls wake()
}
```

## Multi-Threaded Operation with SO_REUSEPORT

Since each ring is single-threaded, the scaling pattern for multi-core servers is: one `IoContext` per OS thread, each with its own ring, and TCP listeners bound with `reuse_port: true`. The kernel distributes incoming connections across listeners on the same port.

```rust
use fiona::{IoContext, net::{TcpListener, TcpListenerOpts}};
use std::net::Ipv4Addr;
use std::thread;

fn multi_threaded_server() {
    let num_threads = num_cpus::get();
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        handles.push(thread::spawn(|| {
            let mut ioc = IoContext::new();
            let ex = ioc.get_executor();

            let opts = TcpListenerOpts {
                reuse_addr: true,
                reuse_port: true,
            };
            let listener = TcpListener::bind_ipv4_with_params(
                &ex, Ipv4Addr::UNSPECIFIED, 8080, &opts
            ).unwrap();

            ex.spawn(async move {
                loop {
                    let stream = listener.accept().await.unwrap();
                    // handle connection on this thread's ring
                }
            });

            ioc.run();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}
```

Each thread operates independently - no locks, no shared submission queue, no contention. The kernel's `SO_REUSEPORT` load balancer distributes connections, and each ring processes its own completions in isolation.

## Task Spawning and Join Handles

Spawning returns a `JoinHandle<T>` that implements `Future<Output = T>`. Await it from another task to collect the result.

```rust
async fn parent(ex: fiona::Executor) {
    let h1 = ex.spawn(async { 1 });
    let h2 = ex.spawn(async { 2 });
    let h3 = ex.spawn(async { 3 });

    let total = h1.await + h2.await + h3.await;
    assert_eq!(total, 6);
}
```

Tasks are `!Send` - they live on the ring that spawned them. This enables `Rc`, `RefCell`, and other non-atomic types inside task futures without any performance penalty from atomic reference counting.

### Cooperative Scheduling

Fiona sets `COOP_TASKRUN` and `DEFER_TASKRUN` on every ring. Tasks run cooperatively - a task that never yields monopolizes the thread. Use yield points (awaiting I/O, timers, or other tasks) to give sibling tasks a chance to run.

For CPU-bound work that needs to interleave with other tasks, use `poll_fn` with a manual `wake_by_ref()` to yield after processing a chunk:

```rust
use std::future::poll_fn;
use std::task::Poll;

async fn chunked_work(items: &mut Vec<i32>) {
    let mut processed = 0;
    poll_fn(|cx| {
        // Process a batch
        for _ in 0..100 {
            if processed >= items.len() {
                return Poll::Ready(());
            }
            items[processed] *= 2;
            processed += 1;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }).await;
}
```

### Compatibility with Futures Combinators

`JoinHandle<T>` works with standard futures combinators. Use `FuturesUnordered` to process results as they complete:

```rust
use futures::{StreamExt, stream::FuturesUnordered};

async fn concurrent_timers(ex: fiona::Executor) {
    let mut tasks = FuturesUnordered::new();
    for i in 0..10 {
        tasks.push(ex.spawn(async move { i * i }));
    }

    while let Some(result) = tasks.next().await {
        println!("got: {}", result);
    }
}
```

## Summary

| Aspect | Design |
|--------|--------|
| Threading | Single-threaded per ring (`SINGLE_ISSUER`) |
| Scaling | One ring per OS thread + `SO_REUSEPORT` |
| Cancellation | Automatic on future drop via cancel SQEs |
| Cross-thread wake | mpsc channel + `io_uring_prep_msg_ring` |
| Shared handles | `Clone` via `RefCount` (object + operation refs) |
| Stable submit | SlotMap flushes SQ before growing |
| Task types | `!Send` - enables `Rc`/`RefCell` without atomics |

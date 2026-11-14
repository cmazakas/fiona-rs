# Timers and Timeouts

Fiona's timers are backed by the kernel. When you sleep, the runtime submits
`io_uring_prep_timeout` to the ring - there is no userspace timer thread, no
polling loop, and no signal-based wakeup. The completion arrives through the
same CQE path as every other I/O operation.

## One-shot sleep

The simplest way to wait is `fiona::time::sleep`. It creates a `Timer`
internally, calls `wait`, and awaits the result:

```rust
use fiona::time;
use std::time::Duration;

time::sleep(&ex, Duration::from_secs(1)).await;
```

This is a convenience wrapper. Under the hood it does exactly:

```rust
let timer = time::Timer::new(&ex);
timer.wait(dur).await.unwrap();
```

Use `sleep` when you need a single delay and have no reason to hold onto the
timer object afterward.

## Reusable timers

When you need repeated timeouts - heartbeat intervals, retry backoff, periodic
flushes - create a `Timer` once and call `wait` as many times as you like:

```rust
use fiona::time::Timer;
use std::time::Duration;

let timer = Timer::new(&ex);

// first delay
timer.wait(Duration::from_millis(250)).await.unwrap();
do_first_thing();

// second delay, same timer object
timer.wait(Duration::from_millis(500)).await.unwrap();
do_second_thing();
```

Each call to `wait` submits a fresh `io_uring_prep_timeout` SQE. The `Timer`
itself is just a lightweight handle with reference counting - it does not carry
accumulated state between waits.

### API surface

| Item | Signature | Notes |
|------|-----------|-------|
| `Timer::new` | `fn new(ex: &Executor) -> Timer` | `#[must_use]` |
| `Timer::wait` | `fn wait(&self, dur: Duration) -> TimerFuture<'_>` | `#[must_use]` |
| `Timer::get_executor` | `fn get_executor(&self) -> Executor` | Returns a clone of the executor handle |

`Timer` implements `Clone`. Cloning increments the internal reference count so
the underlying allocation stays alive as long as any clone exists. This is the
same shared-ownership pattern used by `TcpStream` and `TcpListener`.

Only one `wait` may be outstanding on a given `Timer` at a time. Calling `wait`
while a previous `TimerFuture` is still live will panic.

## Cancel on drop

Dropping a `TimerFuture` before it completes cancels the in-flight timeout. The
drop implementation submits `io_uring_prep_timeout_remove` with
`IOSQE_CQE_SKIP_SUCCESS` so the kernel discards the pending timeout without
generating a completion event:

```rust
{
    let fut = timer.wait(Duration::from_secs(60));
    // fut drops here - the 60-second timeout is cancelled
}
// timer is still usable
timer.wait(Duration::from_millis(100)).await.unwrap();
```

This follows Fiona's cancel-on-drop contract: if you stop awaiting, the
runtime cleans up. No manual cancellation call is needed.

## Per-stream inactivity timeout

TCP streams support a built-in inactivity timeout. If no send or recv activity
occurs within the specified duration, pending operations on that stream are
automatically cancelled:

```rust
use std::time::Duration;

stream.set_timeout(Duration::from_secs(30));
```

You can update the timeout on a live stream at any time. The
[TCP Networking](tcp.md) chapter covers stream timeouts in detail.

## How the timer wheel works

You never interact with the timer wheel directly, but understanding it explains
why Fiona handles large numbers of timers efficiently.

Internally, Fiona maintains a hierarchical timer wheel inspired by Tokio's
design. It has 6 levels with 64 slots each. Each level covers durations that
are a power of 64 larger than the previous one:

| Level | Slot granularity | Level range |
|-------|------------------|-------------|
| 0 | 1 ms | 64 ms |
| 1 | 64 ms | 4.096 s |
| 2 | 4.096 s | ~4.37 min |
| 3 | ~4.37 min | ~4.66 hr |
| 4 | ~4.66 hr | ~12.43 days |
| 5 | ~12.43 days | ~2.18 years |

The maximum representable duration is `(1 << 36) - 1` milliseconds - roughly
2.18 years. Timers with deadlines beyond this are clamped to the last slot.

When a timer is added, `add_timer` places it in the appropriate level and slot
based on how far its deadline diverges from the current elapsed time. The
placement uses a bit-level comparison: the position of the most significant
differing bit between the elapsed time and the deadline determines the level,
and the corresponding 6-bit word determines the slot.

When the event loop ticks, `poll(now)` walks levels from finest to coarsest,
processing every slot whose deadline has passed. When a coarser slot expires,
its timers cascade - those whose deadlines have arrived fire immediately, while
those with time remaining are re-inserted into finer-grained levels. This is
the same approach used by the classic Varghese-Lauck hierarchical timer wheel.

Within a single slot, timers fire in FIFO order. The wheel tracks which slots
are occupied with a 64-bit bitmask per level and uses bit rotation to find the
next occupied slot in constant time.

The result: inserting and cancelling a timer is O(1), and polling is O(expired
timers) rather than O(total timers). For workloads with thousands of concurrent
connections each carrying their own timeout, this keeps the per-tick cost flat.

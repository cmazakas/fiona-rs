// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    future::poll_fn,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::FuturesUnordered};

struct DurationGuard {
    timepoint: Instant,
    dur: Duration,
    max: Duration,
}

impl DurationGuard {
    fn new(dur: Duration) -> DurationGuard {
        let max = Duration::from_secs_f64(dur.as_secs_f64() * 1.05);
        DurationGuard {
            timepoint: Instant::now(),
            dur,
            max,
        }
    }

    fn with_max(dur: Duration, max: Duration) -> DurationGuard {
        DurationGuard {
            timepoint: Instant::now(),
            dur,
            max,
        }
    }
}

impl Drop for DurationGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let d = now - self.timepoint;
        let max = self.max;

        if cfg!(feature = "bsan") {
            if d < self.dur {
                eprintln!("{d:?} vs {:?}", self.dur);
            }
            if d > max {
                eprintln!("{d:?} <= {max:?}");
            }
        } else {
            assert!(d >= self.dur, "{d:?} vs {:?}", self.dur);
            assert!(d <= max, "{d:?} <= {max:?}");
        }
    }
}

//-----------------------------------------------------------------------------

struct PoisonWaker {}

impl Wake for PoisonWaker {
    fn wake(self: std::sync::Arc<Self>) {
        unreachable!()
    }
}

//-----------------------------------------------------------------------------

#[test]
fn timer_wheel_sleep() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let _guard = DurationGuard::new(Duration::from_millis(100));
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn timer_wheel_sleep_multi() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let sleep_time = Duration::from_millis(100);

            for _ in 0..3 {
                let _guard = DurationGuard::new(sleep_time);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    ex.spawn({
        let ex = ex.clone();
        async move {
            let sleep_time = Duration::from_millis(100);

            for _ in 0..3 {
                let _guard = DurationGuard::new(sleep_time);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn timer_wheel_sleep_multi_precise() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let sleep_time = Duration::from_millis(50);

            for _ in 0..3 {
                let _guard = DurationGuard::new(sleep_time);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    ex.spawn({
        let ex = ex.clone();
        async move {
            let sleep_time = Duration::from_millis(50);

            for _ in 0..3 {
                let _guard = DurationGuard::new(sleep_time);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn timer_wheel_sub_millisecond_wait() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let sleep_time = Duration::from_micros(250);
            let _guard = DurationGuard::with_max(sleep_time, Duration::from_micros(2500));
            fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn timer_wheel_stress_test() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let num_timers = if cfg!(feature = "bsan") { 100 } else { 100_000 };

            let sleep_time = Duration::from_millis(100);

            let join_set: FuturesUnordered<_> =
                std::iter::repeat_with(|| fiona::timer_wheel::sleep_for(&ex, sleep_time))
                    .take(num_timers)
                    .collect();

            let done: Vec<_> = join_set.collect().await;
            assert_eq!(done.len(), num_timers);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn timer_wheel_panic() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)).await;
        }
    });

    ex.spawn(async { panic!() });

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            ioc.run();
        }))
        .is_err()
    );
}

#[test]
fn timer_wheel_externally_polled() {
    // Test what happens when we externally create a timer_wheel::TimerFuture, poll
    // it manually and then drop the backing I/O context and all other external
    // Executor instances. The TimerFuture should keep the runtime data alive and
    // because it was not dropped, its entry in the timer wheel should be active. If
    // we wait long enough inside our run of the event loop, it should be marked
    // complete.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let timer_future = fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100));
    let timer_future2 = fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(10000));

    let mut cx = Context::from_waker(Waker::noop());

    let mut timer_future = std::pin::pin!(timer_future);
    assert!(timer_future.as_mut().poll(&mut cx).is_pending());

    let mut timer_future2 = std::pin::pin!(timer_future2);
    assert!(timer_future2.as_mut().poll(&mut cx).is_pending());

    ex.spawn({
        let ex = ex.clone();
        async move {
            let _guard = DurationGuard::new(Duration::from_millis(500));
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(500)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    drop(ioc);
    drop(ex);

    assert!(timer_future.as_mut().poll(&mut cx).is_ready());
    assert!(timer_future2.as_mut().poll(&mut cx).is_pending());
}

#[test]
fn timer_wheel_externally_polled_double_run() {
    // Same as the test above be we spread it out over 2 `ioc.run()` calls.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let timer_future = fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100));
    let timer_future2 = fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(1000));

    let mut cx = Context::from_waker(Waker::noop());

    let mut timer_future = std::pin::pin!(timer_future);
    assert!(timer_future.as_mut().poll(&mut cx).is_pending());

    let mut timer_future2 = std::pin::pin!(timer_future2);
    assert!(timer_future2.as_mut().poll(&mut cx).is_pending());

    ex.spawn({
        let ex = ex.clone();
        async move {
            let _guard = DurationGuard::new(Duration::from_millis(500));
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(500)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    assert!(timer_future.as_mut().poll(&mut cx).is_ready());
    assert!(timer_future2.as_mut().poll(&mut cx).is_pending());

    // This proves that our wheel start time member is preserved properly and
    // the TimerWheel works properly when spread across multiple run()
    // calls.
    ex.spawn({
        let ex = ex.clone();
        async move {
            let _guard = DurationGuard::new(Duration::from_millis(500));
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(500)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    drop(ioc);
    drop(ex);

    assert!(timer_future2.as_mut().poll(&mut cx).is_ready());
}

#[test]
fn timer_wheel_cancel_on_drop() {
    // Test that cancel-on-drop semantics remove the timer from the wheel and
    // that it doesn't trigger a wake-up.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut noop_ctx = std::task::Context::from_waker(Waker::noop());
            let poison_waker = Arc::new(PoisonWaker {}).into();
            let mut poison_ctx = std::task::Context::from_waker(&poison_waker);

            let _guard = DurationGuard::new(Duration::from_millis(321));

            let mut t1 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(250)));
            assert!(t1.as_mut().poll(&mut noop_ctx).is_pending());

            {
                let mut t2 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)));
                assert!(t2.as_mut().poll(&mut poison_ctx).is_pending());
            }

            let mut t3 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(300)));
            assert!(t3.as_mut().poll(&mut noop_ctx).is_pending());

            {
                let mut t4 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(123)));
                assert!(t4.as_mut().poll(&mut poison_ctx).is_pending());
            }

            let mut t5 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(321)));
            assert!(t5.as_mut().poll(&mut noop_ctx).is_pending());

            {
                let mut t6 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(50)));
                assert!(t6.as_mut().poll(&mut poison_ctx).is_pending());
            }

            t3.await;
            t5.await;
            t1.await;
        }
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            let poison_waker = Arc::new(PoisonWaker {}).into();
            let mut poison_ctx = std::task::Context::from_waker(&poison_waker);

            let _guard = DurationGuard::new(Duration::from_millis(250));

            {
                let mut t1 = pin!(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(250)));
                assert!(t1.as_mut().poll(&mut poison_ctx).is_pending());
            }

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            let poison_waker = Arc::new(PoisonWaker {}).into();
            let mut poison_ctx = std::task::Context::from_waker(&poison_waker);

            let _guard = DurationGuard::new(Duration::from_millis(250));

            {
                let mut t1 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(250)));
                assert!(t1.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t2 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(250)));
                assert!(t2.as_mut().poll(&mut poison_ctx).is_pending());

                drop(t1);
                drop(t2);
            }

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            let poison_waker = Arc::new(PoisonWaker {}).into();
            let mut poison_ctx = std::task::Context::from_waker(&poison_waker);

            let _guard = DurationGuard::new(Duration::from_millis(250));

            {
                let mut t1 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(250)));
                assert!(t1.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t2 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)));
                assert!(t2.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t3 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(300)));
                assert!(t3.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t4 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(123)));
                assert!(t4.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t5 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(321)));
                assert!(t5.as_mut().poll(&mut poison_ctx).is_pending());

                let mut t6 =
                    Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(50)));
                assert!(t6.as_mut().poll(&mut poison_ctx).is_pending());

                drop(t4);
                drop(t6);
                drop(t2);
                drop(t3);
                drop(t1);
                drop(t5);
            }

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 4);
}

#[test]
fn timer_wheel_forget() {
    // Prove that if we poll() a TimerFuture that our I/O loop will expire it
    // and invoke the Waker.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut timer =
                Box::pin(fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)));

            poll_fn(|cx| {
                assert!(timer.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            std::mem::forget(timer);
        }
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            fiona::time::sleep(&ex, Duration::from_millis(150)).await;
            fiona::time::sleep(&ex, Duration::from_millis(150)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 2);

    ex.spawn({
        let ex = ex.clone();
        async move {
            let _guard = DurationGuard::new(Duration::from_millis(100));
            fiona::timer_wheel::sleep_for(&ex, Duration::from_millis(100)).await;
        }
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            fiona::time::sleep(&ex, Duration::from_millis(150)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 2);
}

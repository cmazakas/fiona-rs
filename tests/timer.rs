// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    future::Future,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt};

//-----------------------------------------------------------------------------

struct DurationGuard {
    timepoint: Instant,
    dur: Duration,
}

impl DurationGuard {
    fn new(dur: Duration) -> DurationGuard {
        DurationGuard {
            timepoint: Instant::now(),
            dur,
        }
    }
}

impl Drop for DurationGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let d = now - self.timepoint;
        assert!(d >= self.dur);
        assert!(d <= Duration::from_secs_f64(self.dur.as_secs_f64() * 1.05));
    }
}

//-----------------------------------------------------------------------------

struct WakerFuture;

impl Future for WakerFuture {
    type Output = std::task::Waker;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(cx.waker().clone())
    }
}

//-----------------------------------------------------------------------------

#[test]
fn timer_simple() {
    // we should be able to await the most simple timeout operation

    static mut NUM_RUNS: u64 = 0;

    async fn f1(ex: fiona::Executor) {
        let timer = fiona::time::Timer::new(ex);
        let dur = Duration::from_millis(250);

        let _guard = DurationGuard::new(dur);
        let m_ok = timer.wait(dur).await;
        assert!(m_ok.is_ok());
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn timer_multi() {
    // we should be able to reuse the timer object for multiple waits

    static mut NUM_RUNS: u64 = 0;

    async fn f1(ex: fiona::Executor) {
        let timer = fiona::time::Timer::new(ex);
        for _ in 0..3 {
            let dur = Duration::from_millis(250);
            let _guard = DurationGuard::new(dur);

            let m_ok = timer.wait(dur).await;
            assert!(m_ok.is_ok());
        }
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn timer_early_drop() {
    // dropping the timeout Future early should schedule a cancel.
    // we should then be able to immediately reuse the io object

    static mut NUM_RUNS: u64 = 0;

    async fn f1(ex: fiona::Executor) {
        let dur = Duration::from_millis(200);
        let timer = fiona::time::Timer::new(ex);

        let _guard = DurationGuard::new(dur);
        for _ in 0..5 {
            let mut f = timer.wait(dur);
            {
                let w = WakerFuture.await;
                assert!(std::pin::pin!(&mut f)
                    .poll(&mut std::task::Context::from_waker(&w))
                    .is_pending());

                drop(f);
            }
        }

        timer.wait(dur).await.unwrap();
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
#[should_panic = "assertion failed: !timer_impl.timeout_pending"]
fn timer_shared_panic() {
    // we use interior mutability for an ergonomic API but we should never
    // permit more than one concurrent timeout operation on the Timer object.

    async fn f1(timer: fiona::time::Timer) {
        timer.wait(Duration::from_millis(250)).await.unwrap();
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let timer = fiona::time::Timer::new(ex.clone());

    ex.clone().spawn(f1(timer.clone()));
    ex.clone().spawn(f1(timer.clone()));

    let _n = ioc.run();
}

#[test]
#[inline(never)]
#[should_panic = "assertion failed: !timer_impl.timeout_pending"]
fn timer_forget_expired() {
    // this function must be marked inline(never) because if its name disappears,
    // it's cumbersome to ignore it in an lsan suppression file
    // preventing it from being inlined ensures that its name is always visible for
    // the purposes of suppression and matching

    // because we allocate operation state per Future, if we forget()
    // a timer future, we should only get a spurious poll() (which is sound)
    // and then a memory leak
    // the io object should be still be usable without issue

    static mut NUM_RUNS: u64 = 0;

    async fn f1(ex: fiona::Executor) {
        let timer = fiona::time::Timer::new(ex.clone());
        let mut f = timer.wait(Duration::from_millis(100));
        {
            let w = WakerFuture.await;
            assert!(std::pin::pin!(&mut f)
                .poll(&mut std::task::Context::from_waker(&w))
                .is_pending());

            std::mem::forget(f);
        }

        {
            let _guard = DurationGuard::new(Duration::from_millis(100));
            let timer2 = fiona::time::Timer::new(ex);
            timer2.wait(Duration::from_millis(100)).await.unwrap();
        }

        timer.wait(Duration::from_millis(100)).await.unwrap();
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn timer_multiple_eager_drops() {
    // want to test creating a Future, poll()'ing then immediately drop()'ing
    // it multiple times.
    // this can create an odd sequence of SQEs in the SQ:
    // SQ: { timeout, timeout_remove, timeout, timeout_remove, timeout }
    //
    // our code needs to be sound under such a scenario

    static mut NUM_RUNS: u64 = 0;

    async fn f1(ex: fiona::Executor) {
        let timer = fiona::time::Timer::new(ex.clone());
        let w = WakerFuture.await;
        let mut cx = std::task::Context::from_waker(&w);

        let dur = Duration::from_millis(100);

        let _guard = DurationGuard::new(dur);

        {
            let mut f = timer.wait(dur);
            assert!(std::pin::pin!(&mut f).poll(&mut cx).is_pending());
            drop(f);
        }

        {
            let mut f = timer.wait(dur);
            assert!(std::pin::pin!(&mut f).poll(&mut cx).is_pending());
            drop(f);
        }

        {
            let mut f = timer.wait(dur);
            assert!(std::pin::pin!(&mut f).poll(&mut cx).is_pending());
            drop(f);
        }

        timer.wait(dur).await.unwrap();
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

//-----------------------------------------------------------------------------

async fn wait_for<T>(ex: fiona::Executor, dur: Duration, t: T) -> T {
    let timer = fiona::time::Timer::new(ex);
    timer.wait(dur).await.unwrap();
    t
}

#[test]
fn timer_futures_unordered() {
    // we should be able to use a wrapper library like the futures crate and have
    // things complete in the proper unordered order

    {
        async fn launch_timers(ex: fiona::Executor) {
            let _guard = DurationGuard::new(Duration::from_millis(400));

            let f1 = ex.spawn(wait_for(ex.clone(), Duration::from_millis(300), 3));
            let f2 = ex.spawn(wait_for(ex.clone(), Duration::from_millis(400), 4));
            let f3 = ex.spawn(wait_for(ex.clone(), Duration::from_millis(100), 1));
            let f4 = ex.spawn(wait_for(ex.clone(), Duration::from_millis(200), 2));

            let futures = [f1, f2, f3, f4];
            let mut unordered: futures::stream::FuturesUnordered<fiona::SpawnFuture<i32>> =
                futures.into_iter().collect();

            assert!(!unordered.is_empty());

            let mut i = 1;
            while !unordered.is_empty() {
                let x = unordered.next().await.unwrap();
                assert_eq!(x, i);
                i += 1;
            }
        }

        let mut ioc = fiona::IoContext::new();
        let ex = ioc.get_executor();

        ex.spawn(launch_timers(ex.clone()));

        let n = ioc.run();
        assert_eq!(n, 5);
    }

    {
        async fn launch_timers(ex: fiona::Executor) {
            let _guard = DurationGuard::new(Duration::from_millis(400));

            let f1 = wait_for(ex.clone(), Duration::from_millis(300), 3);
            let f2 = wait_for(ex.clone(), Duration::from_millis(400), 4);
            let f3 = wait_for(ex.clone(), Duration::from_millis(100), 1);
            let f4 = wait_for(ex.clone(), Duration::from_millis(200), 2);

            let futures = [f1, f2, f3, f4];
            let mut tasks: futures::stream::FuturesUnordered<_> = futures.into_iter().collect();

            let mut i = 1;
            while !tasks.is_empty() {
                let x = tasks.next().await.unwrap();
                assert_eq!(x, i);
                i += 1;
            }
        }

        let mut ioc = fiona::IoContext::new();
        let ex = ioc.get_executor();

        ex.spawn(launch_timers(ex.clone()));

        let n = ioc.run();
        assert_eq!(n, 1);
    }
}

#[test]
fn timer_futures_select() {
    // test that our io object works with select!, which relies on cancel-on-drop
    // quite heavily.
    // also test that our waker is correctly implemented as well.

    async fn f1(ex: fiona::Executor) {
        let timer1 = fiona::time::Timer::new(ex.clone());
        let timer2 = fiona::time::Timer::new(ex.clone());
        let timer3 = fiona::time::Timer::new(ex.clone());
        let timer4 = fiona::time::Timer::new(ex.clone());
        let timer5 = fiona::time::Timer::new(ex.clone());

        {
            let _guard = DurationGuard::new(Duration::from_millis(100));

            let result = futures::select! {
                _a_res = timer1.wait(Duration::from_millis(500)).fuse() => 5,
                _b_res = timer2.wait(Duration::from_millis(300)).fuse() => 3,
                _c_res = timer3.wait(Duration::from_millis(200)).fuse() => 2,
                _d_res = timer4.wait(Duration::from_millis(100)).fuse() => 1,
                _e_res = timer5.wait(Duration::from_millis(400)).fuse() => 4,
            };

            assert_eq!(result, 1);
        }

        {
            let _guard = DurationGuard::new(Duration::from_millis(100));

            let result = futures::select! {
                a_res = ex.spawn(async move { timer1.wait(Duration::from_millis(100)).await.unwrap(); 5 }).fuse() => a_res,
                b_res = ex.spawn(async move { timer2.wait(Duration::from_millis(300)).await.unwrap(); 3 }).fuse() => b_res,
                c_res = ex.spawn(async move { timer3.wait(Duration::from_millis(400)).await.unwrap(); 2 }).fuse() => c_res,
                d_res = ex.spawn(async move { timer4.wait(Duration::from_millis(500)).await.unwrap(); 1 }).fuse() => d_res,
                e_res = ex.spawn(async move { timer5.wait(Duration::from_millis(200)).await.unwrap(); 4 }).fuse() => e_res,
            };

            assert_eq!(result, 5);
        }
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(f1(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 6);
}

// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::time::{Duration, Instant};

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
        assert!(d >= self.dur, "{d:?} vs {:?}", self.dur);
        let max = self.max;
        assert!(d <= max, "{d:?} <= {max:?}");
    }
}

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
            let num_timers = 100_000;
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

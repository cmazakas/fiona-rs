// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::time::{Duration, Instant};

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
        assert!(d >= self.dur, "{d:?} vs {:?}", self.dur);
        let max = Duration::from_secs_f64(self.dur.as_secs_f64() * 1.05);
        assert!(d <= max, "{d:?} <= {max:?}");
    }
}

#[test]
fn time_wheel_sleep() {
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
fn time_wheel_sleep_multi() {
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
            let sleep_time = Duration::from_millis(30);

            for _ in 0..3 {
                let _guard = DurationGuard::new(sleep_time);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

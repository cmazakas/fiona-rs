// Copyright 2025-2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

mod utils;

use std::time::Duration;

use tokio::task::JoinSet;

const NUM_TIMERS: usize = 1_000_000;

fn fiona_timer() -> Result<(), String> {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    for _ in 0..NUM_TIMERS {
        ex.spawn({
            let ex = ex.clone();
            async move {
                let sleep_time = Duration::from_millis(100);
                fiona::timer_wheel::sleep_for(&ex, sleep_time).await;
            }
        });
    }

    let n = ioc.run();
    assert_eq!(n, NUM_TIMERS as _);

    Ok(())
}

fn tokio_timer() -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let sleep_time = Duration::from_millis(100);

        let mut join_set = JoinSet::new();

        for _ in 0..NUM_TIMERS {
            join_set.spawn(async move {
                tokio::time::sleep(sleep_time).await;
            });
        }

        let done = join_set.join_all().await;
        assert_eq!(done.len(), NUM_TIMERS)
    });

    Ok(())
}

fn main() {
    utils::run_once("fiona_timer", fiona_timer).unwrap();
    utils::run_once("tokio_timer", tokio_timer).unwrap();
}

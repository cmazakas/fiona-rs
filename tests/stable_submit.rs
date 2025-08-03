// Copyright 2025 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::time::Duration;

extern crate fiona;

#[test]
fn slotmap_stable_submit()
{
    // Abuse the fact that our default io_ops size is 1024.
    // Create a sufficiently long SQ such that we have to repeatedly grow the
    // SlotMap without submitting. This tests whether or not we handle the SQE's
    // data remaining stable until submission is complete.

    const TOTAL_OPS: u64 = 1024 * 16;

    let mut ioc = fiona::IoContext::with_params(&fiona::IoContextParams { sq_entries: 16 * 1024,
                                                                          cq_entries: 32 * 1024,
                                                                          nr_files: 1024 });

    let ex = ioc.get_executor();

    async fn timer_op(ex: fiona::Executor) -> u64
    {
        let timer = fiona::time::Timer::new(ex);
        timer.wait(Duration::from_millis(250)).await.unwrap();
        1
    }

    async fn test(ex: fiona::Executor)
    {
        let mut join_handles = Vec::new();
        for _ in 0..TOTAL_OPS {
            join_handles.push(ex.spawn(timer_op(ex.clone())));
        }

        let mut count = 0;
        for h in join_handles {
            let x = h.await;
            count += x;
        }

        assert_eq!(count, TOTAL_OPS);
    }

    ex.spawn(test(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1 + TOTAL_OPS);
}

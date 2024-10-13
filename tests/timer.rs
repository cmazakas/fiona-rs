// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::time::Duration;

#[test]
fn timer_simple() {
    async fn timer_test_impl(ex: fiona::Executor) {
        let mut timer = fiona::time::Timer::new(ex);
        let dur = Duration::from_millis(250);
        let m_ok = timer.wait(dur).await;
        assert!(m_ok.is_ok());
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(timer_test_impl(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 1);
}

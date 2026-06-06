// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#[test]
fn fixed_bufs_register() {
    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.register_fixed_buffers(1024, 1024).unwrap();
}

#[test]
#[should_panic = "A fixed buffer sequence is already registered."]
fn fixed_bufs_register_panic() {
    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.register_fixed_buffers(1024, 1024).unwrap();
    ex.register_fixed_buffers(1024, 1024).unwrap();
}

#[test]
fn fixed_bufs_register_empty() {
    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    assert!(ex.register_fixed_buffers(0, 0).is_err());
}

#[test]
fn fixed_bufs_get_bufs() {
    // Test that our runtime can handle recycling fixed buffers in random order.

    use rand::seq::SliceRandom;

    let num_bufs = 1024;
    let buf_len = 128;

    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.register_fixed_buffers(num_bufs, buf_len).unwrap();

    let mut rng = rand::rng();

    for _ in 0..3 {
        let mut fixed_bufs = Vec::new();
        for _ in 0..num_bufs {
            fixed_bufs.push(ex.get_fixed_buf().unwrap());
        }

        for fixed_buf in &mut fixed_bufs {
            fixed_buf.iter_mut().for_each(|x| {
                *x = 123;
            });
        }

        fixed_bufs.shuffle(&mut rng);

        for fixed_buf in &fixed_bufs {
            assert!(fixed_buf.iter().all(|x| *x == 123));
        }
    }
}

#[test]
fn fixed_bufs_outlives_ioc() {
    let num_bufs = 1024;
    let buf_len = 128;

    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.register_fixed_buffers(num_bufs, buf_len).unwrap();

    let mut fixed_buf = ex.get_fixed_buf().unwrap();

    drop(ioc);

    fixed_buf.iter_mut().for_each(|x| {
        *x = 123;
    });

    assert!(fixed_buf.iter().all(|x| *x == 123));
}

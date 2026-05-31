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

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(dead_code, clippy::needless_pass_by_value, unused_variables)]

use std::{marker::PhantomData, os::fd::AsRawFd};

use nix::sys::socket::{
    bind, getsockname, listen, socket, AddressFamily, Backlog, SockFlag, SockProtocol, SockType,
    SockaddrIn,
};

use crate::{Executor, IoUringOp, RefCount};

#[repr(C)]
struct AcceptorImpl {
    ref_count: RefCount,
    ex: Executor,
    accept_op: IoUringOp,
}

#[repr(C)]
struct ClientImpl {
    ref_count: RefCount,
    ex: Executor,
    connect_op: IoUringOp,
}

pub struct Acceptor {
    p: *mut AcceptorImpl,
    phantom: PhantomData<AcceptorImpl>,
}

pub struct Client {
    p: *mut ClientImpl,
    phantom: PhantomData<ClientImpl>,
}

pub struct AcceptFuture<'a> {
    acceptor: &'a mut Acceptor,
    initiated: bool,
    completed: bool,
}

pub struct ConnectFuture<'a> {
    client: &'a mut Client,
    initiated: bool,
    completed: bool,
}

impl Acceptor {
    #[must_use]
    pub fn new(ex: Executor) -> Acceptor {
        let socket = socket(
            AddressFamily::Inet,
            SockType::Stream,
            SockFlag::empty(),
            SockProtocol::Tcp,
        )
        .unwrap();

        let addr = SockaddrIn::new(127, 0, 0, 1, 0);

        bind(socket.as_raw_fd(), &addr).unwrap();
        listen(&socket, Backlog::new(256).unwrap()).unwrap();

        let addr = getsockname::<SockaddrIn>(socket.as_raw_fd()).unwrap();
        let fd = ex.p.borrow_mut().available_fds.pop_front().unwrap();

        todo!()
    }
}

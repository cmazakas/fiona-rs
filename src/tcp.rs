// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(dead_code, clippy::needless_pass_by_value, unused_variables)]

use std::{
    alloc::Layout,
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
};

use nix::{
    errno::Errno,
    libc::ENFILE,
    sys::socket::{
        bind, getsockname, listen, socket, AddressFamily, Backlog, SockFlag, SockProtocol,
        SockType, SockaddrIn, SockaddrStorage,
    },
};

use crate::{
    release, release_impl, reserve_sqes, submit_ring,
    uring::{io_uring_get_sqe, io_uring_prep_close_direct, io_uring_register_files_update},
    Executor, RefCount, Result,
};

#[repr(C)]
struct AcceptorImpl {
    ref_count: RefCount,
    ex: Executor,
    fd: i32,
    addr: SockaddrStorage,
}

#[repr(C)]
struct ClientImpl {
    ref_count: RefCount,
    ex: Executor,
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
    completed: bool,
}

pub struct ConnectFuture<'a> {
    client: &'a mut Client,
    completed: bool,
}

impl Acceptor {
    pub fn new(ex: Executor, ipv4_addr: Ipv4Addr, port: u16) -> Result<Acceptor> {
        let socket = socket(
            AddressFamily::Inet,
            SockType::Stream,
            SockFlag::empty(),
            SockProtocol::Tcp,
        )?;

        let addr = SocketAddrV4::new(ipv4_addr, port);
        let addr: SockaddrIn = addr.into();

        bind(socket.as_raw_fd(), &addr)?;
        listen(&socket, Backlog::new(256).unwrap())?;

        let ring = ex.ring();
        let fd = socket.as_raw_fd();
        let addr = getsockname::<SockaddrIn>(socket.as_raw_fd())?;

        let offset = ex.p.borrow_mut().available_fds.pop_front();
        if offset.is_none() {
            return Err(Errno::from_raw(ENFILE));
        }

        let offset = offset.unwrap();

        let ret = unsafe { io_uring_register_files_update(ring, offset, &raw const fd, 1) };
        if ret < 0 {
            return Err(Errno::from_raw(-ret));
        }
        assert_eq!(ret, 1);

        let layout = Layout::new::<AcceptorImpl>();
        let p = unsafe { std::alloc::alloc(layout) };

        let ref_count = RefCount {
            count: 1,
            release_impl: release_impl::<AcceptorImpl>,
            obj: p,
        };

        let acceptor_impl = AcceptorImpl {
            ref_count,
            ex,
            fd: offset.try_into().unwrap(),
            addr: SocketAddrV4::new(addr.ip(), addr.port()).into(),
        };

        let p = p.cast::<AcceptorImpl>();
        unsafe { std::ptr::write(p, acceptor_impl) };

        Ok(Acceptor {
            p,
            phantom: PhantomData,
        })
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        let acceptor_impl = unsafe { &*self.p };
        if let Some(addr) = acceptor_impl.addr.as_sockaddr_in() {
            return addr.port();
        }

        if let Some(addr) = acceptor_impl.addr.as_sockaddr_in6() {
            return addr.port();
        }

        unreachable!();
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        unsafe { release(self.p.cast::<RefCount>()) };
    }
}

impl Drop for AcceptorImpl {
    fn drop(&mut self) {
        if self.fd >= 0 {
            let ring = self.ex.ring();

            unsafe { reserve_sqes(ring, 1) };

            let sqe = unsafe { io_uring_get_sqe(ring) };
            unsafe { io_uring_prep_close_direct(sqe, self.fd.try_into().unwrap()) };
            unsafe { (*sqe).user_data = 0 };
            unsafe { submit_ring(ring) };

            self.ex
                .p
                .borrow_mut()
                .available_fds
                .push_back(self.fd.try_into().unwrap());
        }
    }
}

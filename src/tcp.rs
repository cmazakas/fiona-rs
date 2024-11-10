// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(dead_code, clippy::needless_pass_by_value, unused_variables)]

use std::{
    alloc::Layout,
    future::Future,
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
    task::Poll,
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
    add_ref, release, release_impl, reserve_sqes, submit_ring,
    uring::{
        io_uring_get_sqe, io_uring_prep_accept_direct, io_uring_prep_cancel_fd,
        io_uring_prep_close_direct, io_uring_register_files_update, io_uring_sqe_set_data,
        IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED,
    },
    Executor, IoUringOp, OpType, RefCount, Result,
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
    fd: i32,
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
    op: Option<Box<IoUringOp>>,
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

    #[must_use]
    pub fn accept(&mut self) -> AcceptFuture<'_> {
        let acceptor_impl = unsafe { &mut *self.p };
        let ref_count = &raw mut acceptor_impl.ref_count;

        let weak = acceptor_impl.ex.p.borrow().root_task.clone().unwrap();

        AcceptFuture {
            acceptor: self,
            completed: false,
            op: Some(Box::new(IoUringOp {
                ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                weak: Some(weak),
                op_type: OpType::TcpAccept,
            })),
        }
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

impl Future for AcceptFuture<'_> {
    type Output = Result<i32>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);

        let acceptor_impl = unsafe { &mut *self.acceptor.p };

        let mut op = self.op.take().unwrap();

        match (op.initiated, op.done) {
            (true, true) => {
                self.completed = true;

                let res = op.res;

                self.op = Some(op);
                if res < 0 {
                    let res = -res;
                    Poll::Ready(Err(Errno::from_raw(res)))
                } else {
                    Poll::Ready(Ok(res))
                }
            }
            (true, false) => {
                op.weak = Some(acceptor_impl.ex.p.borrow().root_task.clone().unwrap());
                self.op = Some(op);
                Poll::Pending
            }
            (false, true) => unreachable!(),
            (false, false) => {
                let ring = acceptor_impl.ex.ring();

                let file_index = acceptor_impl.ex.get_available_fd();
                if file_index.is_none() {
                    self.completed = true;
                    return Poll::Ready(Err(Errno::from_raw(ENFILE)));
                }

                let file_index = file_index.unwrap();

                unsafe { reserve_sqes(ring, 1) };
                let sqe = unsafe { io_uring_get_sqe(ring) };

                unsafe {
                    io_uring_prep_accept_direct(
                        sqe,
                        acceptor_impl.fd,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        0,
                        file_index,
                    );
                }
                unsafe { io_uring_sqe_set_data(sqe, Box::as_mut_ptr(&mut op).cast()) };

                unsafe { add_ref(&raw mut acceptor_impl.ref_count) };
                op.weak = Some(acceptor_impl.ex.p.borrow().root_task.clone().unwrap());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
        }
    }
}

impl Drop for AcceptFuture<'_> {
    fn drop(&mut self) {
        let p = self.acceptor.p;
        let acceptor_impl = unsafe { &mut *p };

        let op = self.op.as_mut().unwrap();

        if op.initiated && !op.done {
            let ring = acceptor_impl.ex.ring();
            unsafe { reserve_sqes(ring, 1) };
            let sqe = unsafe { io_uring_get_sqe(ring) };

            let user_data = Box::as_mut_ptr(op) as usize as u64;
            unsafe {
                io_uring_prep_cancel_fd(
                    sqe,
                    acceptor_impl.fd,
                    IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED,
                );
            }
            unsafe { (*sqe).user_data = 0 };

            // TODO: same as TimerFuture::drop() comments

            op.eager_dropped = true;
            op.weak = None;
            Box::leak(self.op.take().unwrap());
        }
    }
}
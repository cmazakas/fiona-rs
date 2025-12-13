// Copyright 2024-2025 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

extern crate liburing_rs;

use crate::{
    BorrowedBufs, Executor, OpType, RefCount, Result, add_obj_ref, add_op_ref, get_sqe,
    make_io_uring_op, release_impl, release_obj, reserve_sqes,
};
use core::panic;
use liburing_rs::{
    __kernel_timespec, IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED,
    IORING_RECVSEND_BUNDLE, IORING_RECVSEND_POLL_FIRST, IORING_TIMEOUT_MULTISHOT,
    IOSQE_BUFFER_SELECT, IOSQE_CQE_SKIP_SUCCESS, IOSQE_FIXED_FILE, IOSQE_IO_LINK, io_uring_get_sqe,
    io_uring_prep_accept_direct, io_uring_prep_cancel_fd, io_uring_prep_cancel64,
    io_uring_prep_close_direct, io_uring_prep_connect, io_uring_prep_link_timeout,
    io_uring_prep_recv_multishot, io_uring_prep_send_zc, io_uring_prep_shutdown,
    io_uring_prep_socket_direct, io_uring_prep_timeout, io_uring_prep_timeout_remove,
    io_uring_prep_timeout_update, io_uring_register_files_update, io_uring_sqe_set_buf_group,
    io_uring_sqe_set_data, io_uring_sqe_set_data64, io_uring_sqe_set_flags,
};
use nix::{
    errno::Errno,
    libc::{AF_INET, AF_INET6, ENFILE, IPPROTO_TCP, SOCK_STREAM},
    sys::socket::{
        AddressFamily, Backlog, SockFlag, SockProtocol, SockType, SockaddrIn, SockaddrIn6,
        SockaddrLike, SockaddrStorage, bind, getsockname, listen, setsockopt, socket,
        sockopt::{ReuseAddr, ReusePort},
    },
};
use slotmap::{DefaultKey, Key, KeyData};
use std::{
    alloc::Layout,
    future::Future,
    marker::PhantomData,
    mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
    ops::{Range, RangeBounds},
    os::fd::AsRawFd,
    ptr::{self, NonNull},
    task::Poll,
    time::{Duration, Instant},
};

//-----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct AcceptorOpts
{
    pub reuse_addr: bool,
    pub reuse_port: bool,
}

impl Default for AcceptorOpts
{
    fn default() -> Self
    {
        Self { reuse_addr: true,
               reuse_port: false }
    }
}

//-----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Acceptor
{
    p: NonNull<AcceptorImpl>,
}

impl Acceptor
{
    const DEFAULT_BACKLOG: i32 = 2 * 1024;

    fn bind_ip_impl(ex: Executor, ip_addr: &dyn SockaddrLike, opts: &AcceptorOpts,
                    af: AddressFamily)
                    -> Result<Acceptor>
    {
        let Some(offset) = ex.get_available_fd() else {
            return Err(Errno::from_raw(ENFILE));
        };

        let socket = socket(af, SockType::Stream, SockFlag::empty(), SockProtocol::Tcp)?;

        if opts.reuse_addr {
            setsockopt(&socket, ReuseAddr, &true)?;
        }

        if opts.reuse_port {
            setsockopt(&socket, ReusePort, &true)?;
        }

        bind(socket.as_raw_fd(), ip_addr)?;
        listen(&socket, Backlog::new(Self::DEFAULT_BACKLOG).unwrap())?;

        {
            let ring = ex.ring();
            let fd = socket.as_raw_fd();
            let ret = unsafe { io_uring_register_files_update(ring, offset, &raw const fd, 1) };
            if ret < 0 {
                return Err(Errno::from_raw(-ret));
            }
            assert_eq!(ret, 1);
        }

        let layout = Layout::new::<AcceptorImpl>();
        let p = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();

        let ref_count = RefCount { obj_count: 1,
                                   op_count: 0,
                                   release_impl: release_impl::<AcceptorImpl>,
                                   obj: p.as_ptr() };

        // we need to do this for when `port == 0` (the wildcard port)
        let addr = getsockname::<SockaddrStorage>(socket.as_raw_fd())?;

        let acceptor_impl = AcceptorImpl { fd_impl: FdImpl { ref_count,
                                                             ex,
                                                             fd: offset.try_into().unwrap(),
                                                             cancel_pending: false,
                                                             close_pending: false,
                                                             was_closed: false },
                                           addr,
                                           accept_pending: false };

        let p = p.cast::<AcceptorImpl>();
        unsafe { std::ptr::write(p.as_ptr(), acceptor_impl) };

        Ok(Acceptor { p })
    }

    pub fn bind_ipv4_with_params(ex: Executor, ipv4_addr: Ipv4Addr, port: u16, opts: &AcceptorOpts)
                                 -> Result<Acceptor>
    {
        let addr = SockaddrIn::from(SocketAddrV4::new(ipv4_addr, port));
        Self::bind_ip_impl(ex, &addr, opts, AddressFamily::Inet)
    }

    pub fn bind_ipv4(ex: Executor, ipv4_addr: Ipv4Addr, port: u16) -> Result<Acceptor>
    {
        let addr = SockaddrIn::from(SocketAddrV4::new(ipv4_addr, port));
        Self::bind_ip_impl(ex, &addr, &AcceptorOpts::default(), AddressFamily::Inet)
    }

    pub fn bind_ipv6(ex: Executor, ipv6_addr: Ipv6Addr, port: u16) -> Result<Acceptor>
    {
        let addr = SockaddrIn6::from(SocketAddrV6::new(ipv6_addr, port, 0, 0));
        Self::bind_ip_impl(ex, &addr, &AcceptorOpts::default(), AddressFamily::Inet6)
    }

    pub fn bind_ipv6_with_params(ex: Executor, ipv6_addr: Ipv6Addr, port: u16, opts: &AcceptorOpts)
                                 -> Result<Acceptor>
    {
        let addr = SockaddrIn6::from(SocketAddrV6::new(ipv6_addr, port, 0, 0));
        Self::bind_ip_impl(ex, &addr, opts, AddressFamily::Inet6)
    }

    #[must_use]
    pub fn port(&self) -> u16
    {
        let acceptor_impl = unsafe { &*self.p.as_ptr() };
        if let Some(addr) = acceptor_impl.addr.as_sockaddr_in() {
            return addr.port();
        }

        if let Some(addr) = acceptor_impl.addr.as_sockaddr_in6() {
            return addr.port();
        }

        unreachable!();
    }

    #[must_use]
    pub fn accept(&self) -> AcceptFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).accept_pending });

        let acceptor_impl = unsafe { &mut *self.p.as_ptr() };
        acceptor_impl.accept_pending = true;

        let ref_count = &raw mut acceptor_impl.fd_impl.ref_count;

        let key = acceptor_impl.fd_impl
                               .ex
                               .p
                               .io_ops
                               .borrow_mut()
                               .insert(make_io_uring_op(ref_count, OpType::TcpAccept { fd: -1 }),
                                       &acceptor_impl.fd_impl.ex);

        AcceptFuture { acceptor: self,
                       completed: false,
                       op: Some(key.data().as_ffi()) }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor
    {
        let acceptor_impl = unsafe { &mut *self.p.as_ptr() };
        acceptor_impl.fd_impl.ex.clone()
    }

    #[must_use]
    pub fn cancel(&self) -> CancelFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).fd_impl.cancel_pending });

        let acceptor = unsafe { &mut *self.p.as_ptr() };
        acceptor.fd_impl.cancel_pending = true;

        let ref_count = &raw mut acceptor.fd_impl.ref_count;
        let key =
            acceptor.fd_impl
                    .ex
                    .p
                    .io_ops
                    .borrow_mut()
                    .insert(make_io_uring_op(ref_count, OpType::TcpCancel), &acceptor.fd_impl.ex);

        CancelFuture { fd_impl: &raw mut acceptor.fd_impl,
                       completed: false,
                       op: Some(key.data().as_ffi()),
                       _m: PhantomData }
    }

    #[must_use]
    pub fn close(&self) -> CloseFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).fd_impl.close_pending });

        let acceptor_impl = unsafe { &mut *self.p.as_ptr() };
        acceptor_impl.fd_impl.close_pending = true;

        let ref_count = &raw mut acceptor_impl.fd_impl.ref_count;
        let key = acceptor_impl.fd_impl
                               .ex
                               .p
                               .io_ops
                               .borrow_mut()
                               .insert(make_io_uring_op(ref_count, OpType::TcpClose),
                                       &acceptor_impl.fd_impl.ex);

        CloseFuture { fd_impl: &raw mut acceptor_impl.fd_impl,
                      completed: false,
                      op: Some(key.data().as_ffi()),
                      _m: PhantomData }
    }
}

impl Clone for Acceptor
{
    fn clone(&self) -> Self
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).fd_impl.ref_count };
        unsafe { add_obj_ref(rc) };
        Self { p: self.p }
    }
}

impl Drop for Acceptor
{
    fn drop(&mut self)
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).fd_impl.ref_count };
        unsafe { release_obj(rc) };
    }
}

//-----------------------------------------------------------------------------

pub(crate) struct FdImpl
{
    ref_count: RefCount,
    ex: Executor,
    pub(crate) fd: i32,
    pub(crate) close_pending: bool,
    pub(crate) cancel_pending: bool,
    was_closed: bool,
}

//-----------------------------------------------------------------------------

struct AcceptorImpl
{
    fd_impl: FdImpl,
    addr: SockaddrStorage,
    accept_pending: bool,
}

impl Drop for AcceptorImpl
{
    fn drop(&mut self)
    {
        if self.fd_impl.fd >= 0 {
            let fd = self.fd_impl.fd.try_into().unwrap();

            if self.fd_impl.was_closed {
                self.fd_impl.ex.reclaim_fd(fd);
                return;
            }

            let key = self.fd_impl
                          .ex
                          .p
                          .io_ops
                          .borrow_mut()
                          .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }),
                                  &self.fd_impl.ex);

            let sqe = get_sqe(&self.fd_impl.ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
        }
    }
}

//-----------------------------------------------------------------------------

pub struct AcceptFuture<'a>
{
    acceptor: &'a Acceptor,
    completed: bool,
    op: Option<u64>,
}

impl Future for AcceptFuture<'_>
{
    type Output = Result<Stream>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> std::task::Poll<Self::Output>
    {
        assert!(!self.completed);

        let acceptor_impl = unsafe { &mut *self.acceptor.p.as_ptr() };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));

        let mut borrow_guard = acceptor_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let io_ops = &mut *borrow_guard;
        let op = io_ops.get_mut(key).unwrap();

        match (op.initiated, op.done) {
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, true) => unreachable!(),
            (false, false) => {
                let Some(file_index) = acceptor_impl.fd_impl.ex.get_available_fd() else {
                    self.completed = true;
                    return Poll::Ready(Err(Errno::from_raw(ENFILE)));
                };

                {
                    let OpType::TcpAccept { ref mut fd } = op.op_type else {
                        unreachable!()
                    };

                    *fd = file_index.try_into().unwrap();
                }

                let sqe = get_sqe(&acceptor_impl.fd_impl.ex);

                unsafe {
                    io_uring_prep_accept_direct(sqe,
                                                acceptor_impl.fd_impl.fd,
                                                std::ptr::null_mut(),
                                                std::ptr::null_mut(),
                                                0,
                                                file_index);
                }
                unsafe { io_uring_sqe_set_data64(sqe, key_data) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) };

                unsafe { add_op_ref(&raw mut acceptor_impl.fd_impl.ref_count) };
                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let res = op.res;

                if res < 0 {
                    let res = -res;
                    Poll::Ready(Err(Errno::from_raw(res)))
                } else {
                    let OpType::TcpAccept { fd } = op.op_type else {
                        unreachable!()
                    };

                    drop(borrow_guard);
                    Poll::Ready(Ok(Stream::new(acceptor_impl.fd_impl.ex.clone(), fd)))
                }
            }
        }
    }
}

impl Drop for AcceptFuture<'_>
{
    fn drop(&mut self)
    {
        let acceptor_impl = unsafe { &mut *self.acceptor.p.as_ptr() };
        acceptor_impl.accept_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));

        let mut borrow_guard = acceptor_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let io_ops = &mut *borrow_guard;
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            // We haven't seen the CQE yet, this means we can't reason about how long
            // io_uring needs our operation state to stay alive, thus we must leak.
            // But first, we attempt cancellation across the board for our file descriptor

            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = &raw mut acceptor_impl.fd_impl.ref_count;
            let key = io_ops.insert(make_io_uring_op(ref_count, OpType::DropCancel),
                                    &acceptor_impl.fd_impl.ex);

            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&acceptor_impl.fd_impl.ex);
            unsafe {
                io_uring_prep_cancel_fd(sqe,
                                        acceptor_impl.fd_impl.fd,
                                        IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED);
            }
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };

            self.op = None;
            return;
        }

        if !self.completed && op.res >= 0 {
            // We've seen the CQE and we've successfully accepted a client connection but
            // we're still eager-dropping the Future (i.e. it never returned Poll::Ready).
            // Because the user is never going to retrieve the RAII handle to the accepted
            // connection, we have to remember to close it manually.
            assert!(op.initiated);
            assert!(op.done);

            let fd = op.res.try_into().unwrap();

            io_ops.remove(key).unwrap();
            drop(borrow_guard);

            let ex = &acceptor_impl.fd_impl.ex;
            let key = ex.p
                        .io_ops
                        .borrow_mut()
                        .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }), ex);

            let sqe = get_sqe(ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
            return;
        }

        io_ops.remove(key).unwrap();
    }
}

//-----------------------------------------------------------------------------

pub(crate) struct StreamImpl
{
    pub(crate) fd_impl: FdImpl,
    pub(crate) send_pending: bool,
    pub(crate) shutdown_pending: bool,
    pub(crate) recv_op: Option<u64>,
    pub(crate) last_send: Instant,
    pub(crate) last_recv: Instant,
    ts: __kernel_timespec,
    buf_group: u16,
    recv_pending: bool,
    timeout_op: Option<u64>,
}

impl Drop for StreamImpl
{
    fn drop(&mut self)
    {
        if self.fd_impl.fd >= 0 {
            let fd = self.fd_impl.fd.try_into().unwrap();

            if self.fd_impl.was_closed {
                self.fd_impl.ex.reclaim_fd(fd);
                return;
            }

            let key = self.fd_impl
                          .ex
                          .p
                          .io_ops
                          .borrow_mut()
                          .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }),
                                  &self.fd_impl.ex);

            let sqe = get_sqe(&self.fd_impl.ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
        }
    }
}

//-----------------------------------------------------------------------------

pub struct Stream
{
    p: NonNull<StreamImpl>,
}

impl Stream
{
    #[must_use]
    fn new(ex: Executor, fd: i32) -> Stream
    {
        let layout = Layout::new::<StreamImpl>();
        let p;

        {
            let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();

            let ref_count = RefCount { obj_count: 1,
                                       op_count: 0,
                                       release_impl: release_impl::<StreamImpl>,
                                       obj: ptr.as_ptr() };

            let stream_impl = StreamImpl { fd_impl: FdImpl { ref_count,
                                                             ex,
                                                             fd,
                                                             close_pending: false,
                                                             cancel_pending: false,
                                                             was_closed: false },
                                           ts: Duration::from_secs(3).into(),
                                           buf_group: u16::MAX,
                                           send_pending: false,
                                           recv_pending: false,
                                           shutdown_pending: false,
                                           last_send: Instant::now(),
                                           last_recv: Instant::now(),
                                           timeout_op: None,
                                           recv_op: None };

            p = ptr.cast::<StreamImpl>();
            unsafe { std::ptr::write(p.as_ptr(), stream_impl) };
        }

        let stream_impl = unsafe { &mut *p.as_ptr() };
        let ref_count = &raw mut stream_impl.fd_impl.ref_count;

        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let key = io_ops.insert(make_io_uring_op(ref_count,
                                                 OpType::MultishotTimeout { ts: stream_impl.ts,
                                                                            stream: p.as_ptr() }),
                                &stream_impl.fd_impl.ex);

        let op = io_ops.get_mut(key).unwrap();
        let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
            unreachable!()
        };

        let ts = ptr::from_mut(ts).cast();
        let user_data = key.data().as_ffi();

        let sqe = get_sqe(&stream_impl.fd_impl.ex);
        unsafe { io_uring_prep_timeout(sqe, ts, 0, IORING_TIMEOUT_MULTISHOT) };
        unsafe { io_uring_sqe_set_data64(sqe, user_data) };

        stream_impl.timeout_op = Some(key.data().as_ffi());
        unsafe { add_op_ref(ref_count) };

        Stream { p }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor
    {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.fd_impl.ex.clone()
    }

    pub fn set_buf_group(&self, bgid: u16)
    {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        assert!(stream_impl.recv_op.is_none());
        stream_impl.buf_group = bgid;

        // TODO: eventually handle rescheduling/cancelling the recv op when this
        // gets called or at least figure out the correct behavior.
    }

    #[must_use]
    pub fn send(&self, buf: Vec<u8>) -> SendFuture<'_>
    {
        self.send_subspan(0..buf.len(), buf)
    }

    #[must_use]
    pub fn send_subspan<R: RangeBounds<usize>>(&self, subspan: R, buf: Vec<u8>) -> SendFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).send_pending });

        let start = match subspan.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match subspan.end_bound() {
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => buf.len(),
        };

        let subspan = Range { start, end };
        assert!(subspan.end <= buf.len());

        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.send_pending = true;
        stream_impl.last_send = Instant::now();

        let last_send = &raw mut stream_impl.last_send;
        let ref_count = &raw mut stream_impl.fd_impl.ref_count;

        let key = stream_impl.fd_impl
                             .ex
                             .p
                             .io_ops
                             .borrow_mut()
                             .insert(make_io_uring_op(ref_count,
                                                      OpType::TcpSend { buf,
                                                                        last_send,
                                                                        num_sent: 0,
                                                                        subspan }),
                                     &stream_impl.fd_impl.ex);

        SendFuture { stream: self,
                     completed: false,
                     op: Some(key.data().as_ffi()) }
    }

    #[must_use]
    pub fn recv(&self) -> RecvFuture<'_>
    {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        assert!(!stream_impl.recv_pending);
        stream_impl.recv_pending = true;

        RecvFuture { stream: self,
                     completed: false }
    }

    #[must_use]
    pub fn shutdown(&self, how: i32) -> ShutdownFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).shutdown_pending });

        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.shutdown_pending = true;

        let ref_count = &raw mut stream_impl.fd_impl.ref_count;
        let key = stream_impl.fd_impl
                             .ex
                             .p
                             .io_ops
                             .borrow_mut()
                             .insert(make_io_uring_op(ref_count, OpType::TcpShutdown),
                                     &stream_impl.fd_impl.ex);

        ShutdownFuture { stream: self,
                         completed: false,
                         op: Some(key.data().as_ffi()),
                         how }
    }

    #[must_use]
    pub fn close(&self) -> CloseFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).fd_impl.close_pending });

        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.fd_impl.close_pending = true;

        let ref_count = &raw mut stream_impl.fd_impl.ref_count;
        let key =
            stream_impl.fd_impl
                       .ex
                       .p
                       .io_ops
                       .borrow_mut()
                       .insert(make_io_uring_op(ref_count, OpType::TcpClose),
                               &stream_impl.fd_impl.ex);

        CloseFuture { fd_impl: &raw mut stream_impl.fd_impl,
                      completed: false,
                      op: Some(key.data().as_ffi()),
                      _m: PhantomData }
    }

    #[must_use]
    pub fn cancel(&self) -> CancelFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).fd_impl.cancel_pending });

        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.fd_impl.cancel_pending = true;

        let ref_count = &raw mut stream_impl.fd_impl.ref_count;
        let key =
            stream_impl.fd_impl
                       .ex
                       .p
                       .io_ops
                       .borrow_mut()
                       .insert(make_io_uring_op(ref_count, OpType::TcpCancel),
                               &stream_impl.fd_impl.ex);

        CancelFuture { completed: false,
                       op: Some(key.data().as_ffi()),
                       fd_impl: &raw mut stream_impl.fd_impl,
                       _m: PhantomData }
    }

    pub fn set_timeout(&self, dur: Duration)
    {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.ts = dur.into();

        if let Some(timeout_op) = stream_impl.timeout_op {
            let sqe = get_sqe(&stream_impl.fd_impl.ex);

            let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
            let op = io_ops.get_mut(DefaultKey::from(KeyData::from_ffi(timeout_op)))
                           .unwrap();

            let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
                unreachable!()
            };

            *ts = stream_impl.ts;

            let ts = ptr::from_mut(ts).cast::<__kernel_timespec>();
            let flags = 0;
            let user_data = timeout_op;
            unsafe { io_uring_prep_timeout_update(sqe, ts, user_data, flags) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
        }
    }
}

impl Drop for Stream
{
    fn drop(&mut self)
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).fd_impl.ref_count };
        if unsafe { (*rc).obj_count } == 1 {
            let stream_impl = unsafe { &mut *self.p.as_ptr() };

            {
                let key_data = stream_impl.timeout_op.take().unwrap();
                let key = DefaultKey::from(KeyData::from_ffi(key_data));

                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let op = io_ops.get_mut(key).unwrap();

                let user_data = key_data;
                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;

                let sqe = get_sqe(&stream_impl.fd_impl.ex);
                unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

                // unsafe { submit_ring(ring) };
            }

            if stream_impl.recv_op.is_some() {
                let key_data = stream_impl.recv_op.take().unwrap();
                let key = DefaultKey::from(KeyData::from_ffi(key_data));
                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let op = io_ops.get_mut(key).unwrap();

                stream_impl.recv_op = None;

                if op.done {
                    io_ops.remove(key).unwrap();
                } else {
                    let user_data = key_data;
                    // makes sure this gets cleaned up, not really an eager-dropped operation
                    op.eager_dropped = true;

                    let sqe = get_sqe(&stream_impl.fd_impl.ex);
                    unsafe { io_uring_prep_cancel64(sqe, user_data, 0) };
                    unsafe { io_uring_sqe_set_data64(sqe, 0) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
                }
            }
        }

        unsafe { release_obj(rc) };
    }
}

impl Clone for Stream
{
    fn clone(&self) -> Self
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).fd_impl.ref_count };
        unsafe { add_obj_ref(rc) };
        Self { p: self.p }
    }
}

//-----------------------------------------------------------------------------

pub struct Client
{
    p: NonNull<ClientImpl>,
}

impl Client
{
    #[must_use]
    pub fn new(ex: Executor) -> Client
    {
        let layout = Layout::new::<ClientImpl>();
        let p = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();

        let ref_count = RefCount { obj_count: 1,
                                   op_count: 0,
                                   release_impl: release_impl::<ClientImpl>,
                                   obj: p.as_ptr() };

        let stream = StreamImpl { fd_impl: FdImpl { ref_count,
                                                    ex,
                                                    fd: -1,
                                                    close_pending: false,
                                                    cancel_pending: false,
                                                    was_closed: false },
                                  ts: Duration::from_secs(3).into(),
                                  buf_group: u16::MAX,
                                  send_pending: false,
                                  recv_pending: false,
                                  shutdown_pending: false,
                                  last_send: Instant::now(),
                                  last_recv: Instant::now(),
                                  timeout_op: None,
                                  recv_op: None };

        let client_impl = ClientImpl { stream,
                                       connect_pending: false };

        let p = p.cast::<ClientImpl>();
        unsafe { std::ptr::write(p.as_ptr(), client_impl) };

        let stream_impl = unsafe { &mut (*p.as_ptr()).stream };
        let ref_count = &raw mut stream_impl.fd_impl.ref_count;

        let io_op = make_io_uring_op(ref_count,
                                     OpType::MultishotTimeout { ts: stream_impl.ts,
                                                                stream: &raw mut *stream_impl });
        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let key = io_ops.insert(io_op, &stream_impl.fd_impl.ex);

        let op = io_ops.get_mut(key).unwrap();
        let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
            unreachable!()
        };

        let sqe = get_sqe(&stream_impl.fd_impl.ex);
        let user_data = key.data().as_ffi();
        unsafe { io_uring_prep_timeout(sqe, ts, 0, IORING_TIMEOUT_MULTISHOT) };
        unsafe { io_uring_sqe_set_data64(sqe, user_data) };

        stream_impl.timeout_op = Some(key.data().as_ffi());
        unsafe { add_op_ref(ref_count) };

        Client { p }
    }

    #[must_use]
    pub fn connect_ipv4(&self, addr: Ipv4Addr, port: u16) -> ConnectFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).connect_pending });

        let client_impl = unsafe { &mut *self.p.as_ptr() };
        client_impl.connect_pending = true;

        let ref_count = &raw mut client_impl.stream.fd_impl.ref_count;
        let addr = SocketAddrV4::new(addr, port);

        let op = make_io_uring_op(ref_count,
                                  OpType::TcpConnect { addr: SockaddrStorage::from(addr),
                                                       _port: port,
                                                       ts: client_impl.stream.ts,
                                                       needs_socket: false,
                                                       got_socket: false,
                                                       fd: -1 });

        let key = client_impl.stream
                             .fd_impl
                             .ex
                             .p
                             .io_ops
                             .borrow_mut()
                             .insert(op, &client_impl.stream.fd_impl.ex);

        ConnectFuture { client: self,
                        completed: false,
                        op: Some(key.data().as_ffi()) }
    }

    #[must_use]
    pub fn connect_ipv6(&self, addr: Ipv6Addr, port: u16) -> ConnectFuture<'_>
    {
        assert!(unsafe { !(*self.p.as_ptr()).connect_pending });

        let client_impl = unsafe { &mut *self.p.as_ptr() };
        client_impl.connect_pending = true;

        let ref_count = &raw mut client_impl.stream.fd_impl.ref_count;
        let addr = SocketAddrV6::new(addr, port, 0, 0);

        let op = make_io_uring_op(ref_count,
                                  OpType::TcpConnect { addr: SockaddrStorage::from(addr),
                                                       _port: port,
                                                       ts: client_impl.stream.ts,
                                                       needs_socket: false,
                                                       got_socket: false,
                                                       fd: -1 });

        let key = client_impl.stream
                             .fd_impl
                             .ex
                             .p
                             .io_ops
                             .borrow_mut()
                             .insert(op, &client_impl.stream.fd_impl.ex);

        ConnectFuture { client: self,
                        completed: false,
                        op: Some(key.data().as_ffi()) }
    }

    #[must_use]
    pub fn as_stream(&self) -> Stream
    {
        // we know this function is sound because of:
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2024&gist=7a39da0d1b4c9d22b23ef0ffec238050

        let rc = unsafe { &raw mut (*self.p.as_ptr()).stream.fd_impl.ref_count };
        unsafe { add_obj_ref(rc) };

        let p = unsafe { &raw mut (*self.p.as_ptr()).stream };
        Stream { p: unsafe { NonNull::new_unchecked(p) } }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor
    {
        let client_impl = unsafe { &mut *self.p.as_ptr() };
        client_impl.stream.fd_impl.ex.clone()
    }

    pub fn set_buf_group(&self, bgid: u16)
    {
        let stream_impl = unsafe { &mut (*self.p.as_ptr()).stream };
        stream_impl.buf_group = bgid;
    }

    pub fn set_timeout(&self, dur: Duration)
    {
        let stream = self.as_stream();
        stream.set_timeout(dur);
    }

    pub async fn send(&self, buf: Vec<u8>) -> (Result<usize>, Vec<u8>)
    {
        let stream = self.as_stream();
        stream.send(buf).await
    }

    pub async fn recv(&self) -> Result<BorrowedBufs>
    {
        let stream = self.as_stream();
        stream.recv().await
    }
}

impl Clone for Client
{
    fn clone(&self) -> Self
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).stream.fd_impl.ref_count };
        unsafe { add_obj_ref(rc) };
        Self { p: self.p }
    }
}

impl Drop for Client
{
    fn drop(&mut self)
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).stream.fd_impl.ref_count };
        if unsafe { (*rc).obj_count } == 1 {
            let stream_impl = unsafe { &mut (*self.p.as_ptr()).stream };

            {
                let key_data = stream_impl.timeout_op.take().unwrap();
                let key = DefaultKey::from(KeyData::from_ffi(key_data));

                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let op = io_ops.get_mut(key).unwrap();

                let user_data = key_data;

                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;

                let sqe = get_sqe(&stream_impl.fd_impl.ex);
                unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }

            if stream_impl.recv_op.is_some() {
                let key_data = stream_impl.recv_op.take().unwrap();
                let key = DefaultKey::from(KeyData::from_ffi(key_data));
                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let op = io_ops.get_mut(key).unwrap();

                stream_impl.recv_op = None;
                if op.done {
                    io_ops.remove(key).unwrap();
                } else {
                    let user_data = key_data;
                    // makes sure this gets cleaned up, not really an eager-dropped operation
                    op.eager_dropped = true;

                    let sqe = get_sqe(&stream_impl.fd_impl.ex);
                    unsafe { io_uring_prep_cancel64(sqe, user_data, 0) };
                    unsafe { io_uring_sqe_set_data64(sqe, 0) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
                }
            }
        }

        unsafe { release_obj(rc) };
    }
}

//-----------------------------------------------------------------------------

struct ClientImpl
{
    stream: StreamImpl,
    connect_pending: bool,
}

//-----------------------------------------------------------------------------

pub struct ConnectFuture<'a>
{
    client: &'a Client,
    completed: bool,
    op: Option<u64>,
}

impl Future for ConnectFuture<'_>
{
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        assert!(!self.completed);

        let client_impl = unsafe { &mut *self.client.p.as_ptr() };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *client_impl.stream.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        match (op.initiated, op.done) {
            (false, true) => panic!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let ring = client_impl.stream.fd_impl.ex.ring();

                let user_data = key_data;

                let OpType::TcpConnect { ref addr,
                                         ref mut ts,
                                         ref mut needs_socket,
                                         ref mut fd,
                                         .. } = op.op_type
                else {
                    unreachable!();
                };

                let (af, addrlen) = {
                    if let Some(x) = addr.as_sockaddr_in() {
                        (AF_INET, std::mem::size_of_val(x))
                    } else if let Some(x) = addr.as_sockaddr_in6() {
                        (AF_INET6, std::mem::size_of_val(x))
                    } else {
                        unreachable!();
                    }
                };

                let file_index;

                *needs_socket = client_impl.stream.fd_impl.fd < 0;

                if *needs_socket {
                    let mfile_index = client_impl.stream.fd_impl.ex.get_available_fd();
                    let Some(file_idx) = mfile_index else {
                        return Poll::Ready(Err(Errno::from_raw(ENFILE)));
                    };

                    *fd = file_idx.try_into().unwrap();
                    file_index = file_idx;
                } else {
                    file_index = client_impl.stream.fd_impl.fd.try_into().unwrap();
                }

                unsafe { reserve_sqes(ring, if *needs_socket { 3 } else { 2 }) };

                if *needs_socket {
                    let sqe = unsafe { io_uring_get_sqe(ring) };

                    unsafe {
                        io_uring_prep_socket_direct(sqe,
                                                    af,
                                                    SOCK_STREAM,
                                                    IPPROTO_TCP,
                                                    file_index,
                                                    0);
                    }
                    unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK) }
                }

                {
                    let sqe = unsafe { io_uring_get_sqe(ring) };
                    unsafe {
                        io_uring_prep_connect(sqe,
                                              file_index.try_into().unwrap(),
                                              std::ptr::from_ref(addr).cast(),
                                              addrlen.try_into().unwrap());
                    }
                    unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK | IOSQE_FIXED_FILE) };
                }

                {
                    let sqe = unsafe { io_uring_get_sqe(ring) };
                    unsafe { io_uring_prep_link_timeout(sqe, std::ptr::from_mut(ts).cast(), 0) };
                    unsafe { io_uring_sqe_set_data(sqe, std::ptr::null_mut()) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
                }

                unsafe { add_op_ref(&raw mut client_impl.stream.fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpConnect { needs_socket,
                                         got_socket,
                                         fd,
                                         .. } = op.op_type
                else {
                    unreachable!();
                };

                if needs_socket && got_socket && op.res >= 0 {
                    assert!(client_impl.stream.fd_impl.fd < 0);
                    client_impl.stream.fd_impl.fd = fd;
                }

                let res = op.res;
                if res < 0 {
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl Drop for ConnectFuture<'_>
{
    fn drop(&mut self)
    {
        let client_impl = unsafe { &mut *self.client.p.as_ptr() };
        client_impl.connect_pending = false;

        let op_cancel_key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(op_cancel_key_data));

        let mut borrow_guard = client_impl.stream.fd_impl.ex.p.io_ops.borrow_mut();
        let io_ops = &mut *borrow_guard;
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let user_data = op_cancel_key_data;

            let ref_count = &raw mut client_impl.stream.fd_impl.ref_count;
            let key = io_ops.insert(make_io_uring_op(ref_count, OpType::DropCancel),
                                    &client_impl.stream.fd_impl.ex);

            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&client_impl.stream.fd_impl.ex);

            unsafe { io_uring_prep_cancel64(sqe, user_data, IORING_ASYNC_CANCEL_ALL as i32) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };

            self.op = None;
            return;
        }

        let OpType::TcpConnect { needs_socket,
                                 got_socket,
                                 fd,
                                 .. } = op.op_type
        else {
            unreachable!();
        };

        if !self.completed && needs_socket && got_socket {
            assert!(op.initiated);
            assert!(op.done);

            if fd >= 0 {
                let fd = fd.try_into().unwrap();

                io_ops.remove(key).unwrap();
                drop(borrow_guard);

                let ex = &client_impl.stream.fd_impl.ex;
                let key =
                    ex.p
                      .io_ops
                      .borrow_mut()
                      .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }), ex);

                let sqe = get_sqe(ex);
                unsafe { io_uring_prep_close_direct(sqe, fd) };
                unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
            }

            self.op = None;

            return;
        }

        io_ops.remove(key).unwrap();
    }
}

//-----------------------------------------------------------------------------

pub struct SendFuture<'a>
{
    stream: &'a Stream,
    completed: bool,
    op: Option<u64>,
}

impl Drop for SendFuture<'_>
{
    fn drop(&mut self)
    {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        stream_impl.send_pending = false;

        let op_cancel_key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(op_cancel_key_data));
        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = &raw mut stream_impl.fd_impl.ref_count;
            let key = io_ops.insert(make_io_uring_op(ref_count, OpType::DropCancel),
                                    &stream_impl.fd_impl.ex);

            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&stream_impl.fd_impl.ex);
            unsafe { io_uring_prep_cancel64(sqe, op_cancel_key_data, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };

            self.op = None;
        } else {
            io_ops.remove(key).unwrap();
        }
    }
}

impl Future for SendFuture<'_>
{
    type Output = (Result<usize>, Vec<u8>);

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        assert!(!self.completed);

        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        let user_data = key_data;

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let OpType::TcpSend { ref buf,
                                      ref subspan,
                                      .. } = op.op_type
                else {
                    unreachable!()
                };

                {
                    let sqe = get_sqe(&stream_impl.fd_impl.ex);
                    unsafe {
                        io_uring_prep_send_zc(sqe,
                                              stream_impl.fd_impl.fd,
                                              buf.as_ptr().add(subspan.start).cast(),
                                              subspan.end - subspan.start,
                                              0,
                                              0);
                    }

                    unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) }
                }

                unsafe { add_op_ref(&raw mut stream_impl.fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpSend { ref mut buf,
                                      num_sent,
                                      .. } = op.op_type
                else {
                    unreachable!()
                };

                let res = op.res;
                if res < 0 {
                    Poll::Ready((Err(Errno::from_raw(-res)), std::mem::take(buf)))
                } else {
                    Poll::Ready((Ok(num_sent), std::mem::take(buf)))
                }
            }
        }
    }
}

//-----------------------------------------------------------------------------

pub struct CloseFuture<'a>
{
    fd_impl: *mut FdImpl,
    completed: bool,
    op: Option<u64>,
    _m: PhantomData<&'a FdImpl>,
}

impl Future for CloseFuture<'_>
{
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        let fd_impl = unsafe { &mut *self.fd_impl };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        let user_data = key_data;

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let OpType::TcpClose = op.op_type else {
                    unreachable!()
                };

                let sqe = get_sqe(&fd_impl.ex);
                let fd = fd_impl.fd as u32;

                unsafe { io_uring_prep_close_direct(sqe, fd) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                unsafe { add_op_ref(&raw mut fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpClose = op.op_type else {
                    unreachable!()
                };

                let res = op.res;
                if res < 0 {
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    fd_impl.was_closed = true;
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl Drop for CloseFuture<'_>
{
    fn drop(&mut self)
    {
        let fd_impl = unsafe { &mut *self.fd_impl };
        fd_impl.close_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = &raw mut fd_impl.ref_count;
            let key = io_ops.insert(make_io_uring_op(ref_count, OpType::DropCancel), &fd_impl.ex);

            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&fd_impl.ex);
            let user_data = key.data().as_ffi();
            unsafe { io_uring_prep_cancel64(sqe, key_data, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, user_data) };
        } else {
            io_ops.remove(key).unwrap();
        }
    }
}

//-----------------------------------------------------------------------------

pub struct ShutdownFuture<'a>
{
    stream: &'a Stream,
    completed: bool,
    op: Option<u64>,
    how: i32,
}

impl Future for ShutdownFuture<'_>
{
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        let user_data = key_data;

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let OpType::TcpShutdown = op.op_type else {
                    unreachable!()
                };

                let sqe = get_sqe(&stream_impl.fd_impl.ex);
                let fd = stream_impl.fd_impl.fd;
                unsafe { io_uring_prep_shutdown(sqe, fd, self.how) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) };

                unsafe { add_op_ref(&raw mut stream_impl.fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpShutdown = op.op_type else {
                    unreachable!()
                };

                let res = op.res;
                if res < 0 {
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl Drop for ShutdownFuture<'_>
{
    fn drop(&mut self)
    {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        stream_impl.shutdown_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = &raw mut stream_impl.fd_impl.ref_count;
            let key = io_ops.insert(make_io_uring_op(ref_count, OpType::DropCancel),
                                    &stream_impl.fd_impl.ex);
            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&stream_impl.fd_impl.ex);
            let user_data = key.data().as_ffi();
            unsafe { io_uring_prep_cancel64(sqe, key_data, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, user_data) };
        } else {
            io_ops.remove(key).unwrap();
        }
    }
}

//-----------------------------------------------------------------------------

pub struct CancelFuture<'a>
{
    fd_impl: *mut FdImpl,
    completed: bool,
    op: Option<u64>,
    _m: PhantomData<&'a FdImpl>,
}

impl Future for CancelFuture<'_>
{
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));

        let fd_impl = unsafe { &mut *self.fd_impl };
        let mut io_ops = fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        let user_data = key_data;

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let OpType::TcpCancel = op.op_type else {
                    unreachable!()
                };

                let sqe = get_sqe(&fd_impl.ex);
                let fd = fd_impl.fd;
                let flags = IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED;

                unsafe { io_uring_prep_cancel_fd(sqe, fd, flags) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };

                unsafe { add_op_ref(&raw mut fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                let res = op.res;
                let OpType::TcpCancel = op.op_type else {
                    unreachable!()
                };

                drop(io_ops);

                self.completed = true;

                if res < 0 {
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl Drop for CancelFuture<'_>
{
    fn drop(&mut self)
    {
        let fd_impl = unsafe { &mut *self.fd_impl };
        fd_impl.cancel_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;
        } else {
            io_ops.remove(key).unwrap();
        }
    }
}

//-----------------------------------------------------------------------------

pub struct RecvFuture<'a>
{
    stream: &'a Stream,
    completed: bool,
}

impl Drop for RecvFuture<'_>
{
    fn drop(&mut self)
    {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        stream_impl.recv_pending = false;
    }
}

impl Future for RecvFuture<'_>
{
    type Output = Result<BorrowedBufs>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> Poll<Self::Output>
    {
        assert!(!self.completed);

        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        let ex = stream_impl.fd_impl.ex.clone();

        match stream_impl.recv_op {
            None => {
                let ioprio =
                    u16::try_from(IORING_RECVSEND_POLL_FIRST | IORING_RECVSEND_BUNDLE).unwrap();

                let ref_count = &raw mut stream_impl.fd_impl.ref_count;

                let bgid = stream_impl.buf_group;

                let buf_group = match ex.p.buf_groups.borrow().get(&bgid) {
                    None => {
                        self.completed = true;
                        return Poll::Ready(Err(Errno::ENOENT));
                    }
                    Some(buf_group) => buf_group.get(),
                };

                stream_impl.last_recv = Instant::now();
                let last_recv: *mut Instant = &raw mut stream_impl.last_recv;

                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let key = io_ops.insert(make_io_uring_op(ref_count,
                                              OpType::MultishotTcpRecv { bufs:
                                                                             BorrowedBufs::new(ex.clone(), buf_group),
                                                                         buf_group,
                                                                         last_recv }), &stream_impl.fd_impl.ex);

                let op = io_ops.get_mut(key).unwrap();

                let user_data = key.data().as_ffi();
                let sqe = get_sqe(&ex);

                let sockfd = stream_impl.fd_impl.fd;
                let flags = IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;

                unsafe { io_uring_prep_recv_multishot(sqe, sockfd, ptr::null_mut(), 0, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                unsafe { io_uring_sqe_set_buf_group(sqe, bgid.into()) };
                unsafe { io_uring_sqe_set_flags(sqe, flags) };
                unsafe { (*sqe).ioprio |= ioprio };

                unsafe { add_op_ref(ref_count) };

                op.initiated = true;
                op.local_waker = Some(cx.local_waker().clone());
                stream_impl.recv_op = Some(key.data().as_ffi());

                Poll::Pending
            }
            Some(key_data) => {
                let key = KeyData::from_ffi(key_data).into();
                let io_ops = &mut *stream_impl.fd_impl.ex.p.io_ops.borrow_mut();
                let op = io_ops.get_mut(key).unwrap();

                let OpType::MultishotTcpRecv { ref mut bufs,
                                               buf_group,
                                               .. } = op.op_type
                else {
                    unreachable!();
                };

                if !bufs.is_empty() {
                    self.completed = true;

                    let mut user_bufs = BorrowedBufs::new(ex.clone(), buf_group);
                    mem::swap(&mut user_bufs, bufs);

                    if op.done {
                        stream_impl.recv_op = None;
                        io_ops.remove(key).unwrap();
                    }

                    return Poll::Ready(Ok(user_bufs));
                }

                if op.res == 0 {
                    self.completed = true;
                    assert!(op.done);
                    if op.done {
                        stream_impl.recv_op = None;
                        io_ops.remove(key).unwrap();
                    }
                    return Poll::Ready(Ok(BorrowedBufs::new(ex.clone(), buf_group)));
                }

                if op.res < 0 {
                    self.completed = true;
                    let res = -op.res;
                    if op.done {
                        stream_impl.recv_op = None;
                        io_ops.remove(key).unwrap();
                    }
                    return Poll::Ready(Err(Errno::from_raw(res)));
                }

                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
        }
    }
}

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(dead_code, unused_variables)]

use std::{
    alloc::Layout,
    future::Future,
    mem,
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
    ptr::{self, NonNull},
    task::Poll,
    time::{Duration, Instant},
};

use nix::{
    errno::Errno,
    libc::{AF_INET, AF_INET6, ENFILE, IPPROTO_TCP, MSG_WAITALL, SOCK_STREAM},
    sys::{
        socket::{
            bind, getsockname, listen, socket, AddressFamily, Backlog, SockFlag, SockProtocol,
            SockType, SockaddrIn, SockaddrStorage,
        },
        time::TimeSpec,
    },
};

use crate::{
    add_obj_ref, add_op_ref, release_impl, release_obj, reserve_sqes, submit_ring,
    uring::{
        io_uring_buf_ring_add, io_uring_buf_ring_advance, io_uring_get_sqe,
        io_uring_prep_accept_direct, io_uring_prep_cancel64, io_uring_prep_cancel_fd,
        io_uring_prep_close_direct, io_uring_prep_connect, io_uring_prep_link_timeout,
        io_uring_prep_recv_multishot, io_uring_prep_send_zc, io_uring_prep_socket_direct,
        io_uring_prep_timeout, io_uring_prep_timeout_remove, io_uring_prep_timeout_update,
        io_uring_register_files_update, io_uring_sqe_set_buf_group, io_uring_sqe_set_data,
        io_uring_sqe_set_data64, io_uring_sqe_set_flags, IORING_ASYNC_CANCEL_ALL,
        IORING_ASYNC_CANCEL_FD_FIXED, IORING_RECVSEND_BUNDLE, IORING_RECVSEND_POLL_FIRST,
        IORING_TIMEOUT_MULTISHOT, IOSQE_BUFFER_SELECT, IOSQE_CQE_SKIP_SUCCESS, IOSQE_FIXED_FILE,
        IOSQE_IO_LINK,
    },
    Executor, IoUringOp, OpType, RefCount, Result,
};

struct AcceptorImpl {
    ref_count: RefCount,
    ex: Executor,
    fd: i32,
    addr: SockaddrStorage,
    accept_pending: bool,
}

pub(crate) struct StreamImpl {
    pub(crate) fd: i32,
    pub(crate) send_pending: bool,
    pub(crate) recv_op: Option<Box<IoUringOp>>,
    pub(crate) last_send: Instant,
    pub(crate) last_recv: Instant,
    ref_count: RefCount,
    ex: Executor,
    ts: TimeSpec,
    buf_group: u16,
    recv_pending: bool,
    timeout_op: Option<Box<IoUringOp>>,
}

struct ClientImpl {
    stream: StreamImpl,
    connect_pending: bool,
}

pub struct Acceptor {
    p: NonNull<AcceptorImpl>,
}

pub struct Stream {
    p: NonNull<StreamImpl>,
}

pub struct Client {
    p: NonNull<ClientImpl>,
}

pub struct AcceptFuture<'a> {
    acceptor: &'a Acceptor,
    completed: bool,
    op: Option<Box<IoUringOp>>,
}

pub struct ConnectFuture<'a> {
    client: &'a Client,
    completed: bool,
    op: Option<Box<IoUringOp>>,
}

pub struct SendFuture<'a> {
    stream: &'a Stream,
    completed: bool,
    op: Option<Box<IoUringOp>>,
}

pub struct RecvFuture<'a> {
    stream: &'a Stream,
    completed: bool,
}

pub struct CloseFuture<'a> {
    stream: &'a Stream,
    completed: bool,
    op: Option<Box<IoUringOp>>,
}

//-----------------------------------------------------------------------------

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

        // we need to do this for when `port == 0` (the wildcard port)
        let addr = getsockname::<SockaddrIn>(socket.as_raw_fd())?;

        let offset = ex.get_available_fd();
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
        let p = NonNull::new(p).unwrap();

        let ref_count = RefCount {
            obj_count: 1,
            op_count: 0,
            release_impl: release_impl::<AcceptorImpl>,
            obj: p.as_ptr(),
        };

        let acceptor_impl = AcceptorImpl {
            ref_count,
            ex,
            fd: offset.try_into().unwrap(),
            addr: SocketAddrV4::new(addr.ip(), addr.port()).into(),
            accept_pending: false,
        };

        let p = p.cast::<AcceptorImpl>();
        unsafe { std::ptr::write(p.as_ptr(), acceptor_impl) };

        Ok(Acceptor { p })
    }

    #[must_use]
    pub fn port(&self) -> u16 {
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
    pub fn accept(&self) -> AcceptFuture<'_> {
        assert!(unsafe { !(*self.p.as_ptr()).accept_pending });

        let acceptor_impl = unsafe { &mut *self.p.as_ptr() };
        acceptor_impl.accept_pending = true;

        let ref_count = &raw mut acceptor_impl.ref_count;

        AcceptFuture {
            acceptor: self,
            completed: false,
            op: Some(Box::new(IoUringOp {
                ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                weak: None,
                op_type: OpType::TcpAccept { fd: -1 },
            })),
        }
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        unsafe { release_obj(self.p.cast::<RefCount>().as_ptr()) };
    }
}

impl Drop for AcceptorImpl {
    fn drop(&mut self) {
        if self.fd >= 0 {
            let ring = self.ex.ring();
            let fd = self.fd.try_into().unwrap();

            unsafe { reserve_sqes(ring, 1) };

            let sqe = unsafe { io_uring_get_sqe(ring) };
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { submit_ring(ring) };

            self.ex.reclaim_fd(fd);
        }
    }
}

//-----------------------------------------------------------------------------

impl Future for AcceptFuture<'_> {
    type Output = Result<Stream>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);

        let acceptor_impl = unsafe { &mut *self.acceptor.p.as_ptr() };

        let mut op = self.op.take().unwrap();
        match (op.initiated, op.done) {
            (true, true) => {
                self.completed = true;

                let res = op.res;

                if res < 0 {
                    let res = -res;
                    self.op = Some(op);
                    Poll::Ready(Err(Errno::from_raw(res)))
                } else {
                    let OpType::TcpAccept { fd } = op.op_type else {
                        unreachable!()
                    };

                    self.op = Some(op);
                    Poll::Ready(Ok(Stream::new(acceptor_impl.ex.clone(), fd)))
                }
            }
            (true, false) => {
                op.weak = Some(acceptor_impl.ex.get_root_task());
                self.op = Some(op);
                Poll::Pending
            }
            (false, true) => unreachable!(),
            (false, false) => {
                let ring = acceptor_impl.ex.ring();

                let Some(file_index) = acceptor_impl.ex.get_available_fd() else {
                    self.completed = true;
                    self.op = Some(op);
                    return Poll::Ready(Err(Errno::from_raw(ENFILE)));
                };

                {
                    let OpType::TcpAccept { ref mut fd } = op.op_type else {
                        unreachable!()
                    };

                    *fd = file_index.try_into().unwrap();
                }

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
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) };

                unsafe { add_op_ref(&raw mut acceptor_impl.ref_count) };
                op.weak = Some(acceptor_impl.ex.get_root_task());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
        }
    }
}

impl Drop for AcceptFuture<'_> {
    fn drop(&mut self) {
        let acceptor_impl = unsafe { &mut *self.acceptor.p.as_ptr() };
        acceptor_impl.accept_pending = false;

        let op = self.op.as_mut().unwrap();

        if op.initiated && !op.done {
            // We haven't seen the CQE yet, this means we can't reason about how long
            // io_uring needs our operation state to stay alive, thus we must leak.
            // But first, we attempt cancellation across the board for our file descriptor

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

            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            // TODO: same as TimerFuture::drop() comments

            op.eager_dropped = true;
            op.weak = None;
            Box::leak(self.op.take().unwrap());
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

            let ring = acceptor_impl.ex.ring();

            unsafe { reserve_sqes(ring, 1) };

            unsafe {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_close_direct(sqe, fd);
                io_uring_sqe_set_data64(sqe, 0);
                io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
            }

            // unsafe { submit_ring(ring) };
            acceptor_impl.ex.reclaim_fd(fd);
        }
    }
}

//-----------------------------------------------------------------------------

impl Drop for StreamImpl {
    fn drop(&mut self) {
        if self.fd >= 0 {
            let ring = self.ex.ring();
            let fd = self.fd.try_into().unwrap();

            unsafe { reserve_sqes(ring, 1) };

            let sqe = unsafe { io_uring_get_sqe(ring) };
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            // unsafe { submit_ring(ring) };
            self.ex.reclaim_fd(fd);
        }
    }
}

//-----------------------------------------------------------------------------

impl Stream {
    #[must_use]
    fn new(ex: Executor, fd: i32) -> Stream {
        let layout = Layout::new::<StreamImpl>();
        let p;

        let ring = ex.ring();

        {
            let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();

            let ref_count = RefCount {
                obj_count: 1,
                op_count: 0,
                release_impl: release_impl::<StreamImpl>,
                obj: ptr.as_ptr(),
            };

            let stream_impl = StreamImpl {
                ref_count,
                ex,
                fd,
                ts: TimeSpec::from_duration(Duration::from_secs(3)),
                buf_group: u16::MAX,
                send_pending: false,
                recv_pending: false,
                last_send: Instant::now(),
                last_recv: Instant::now(),
                timeout_op: None,
                recv_op: None,
            };

            p = ptr.cast::<StreamImpl>();
            unsafe { std::ptr::write(p.as_ptr(), stream_impl) };
        }

        let stream_impl = unsafe { &mut *p.as_ptr() };
        let ref_count = &raw mut stream_impl.ref_count;

        let mut op = Box::new(IoUringOp {
            ref_count,
            initiated: false,
            done: false,
            eager_dropped: false,
            res: -1,
            weak: None,
            op_type: OpType::MultishotTimeout {
                ts: stream_impl.ts,
                stream: p.as_ptr(),
            },
        });

        let user_data = Box::as_mut_ptr(&mut op).cast();

        let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
            unreachable!()
        };

        let ts = ptr::from_mut(ts).cast();

        unsafe { reserve_sqes(ring, 1) };

        let sqe = unsafe { io_uring_get_sqe(ring) };
        unsafe { io_uring_prep_timeout(sqe, ts, 0, IORING_TIMEOUT_MULTISHOT) };
        unsafe { io_uring_sqe_set_data(sqe, user_data) };

        stream_impl.timeout_op = Some(op);
        unsafe { add_op_ref(ref_count) };

        Stream { p }
    }

    pub fn set_buf_group(&self, bgid: u16) {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.buf_group = bgid;

        // TODO: eventually handle rescheduling/cancelling the recv op when this
        // gets called or at least figure out the correct behavior.
    }

    #[must_use]
    pub fn send(&self, buf: Vec<u8>) -> SendFuture {
        assert!(unsafe { !(*self.p.as_ptr()).send_pending });

        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        stream_impl.send_pending = true;

        let last_send = &raw mut stream_impl.last_send;
        let ref_count = &raw mut stream_impl.ref_count;

        SendFuture {
            stream: self,
            completed: false,
            op: Some(Box::new(IoUringOp {
                ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                weak: None,
                op_type: OpType::TcpSend { buf, last_send },
            })),
        }
    }

    #[must_use]
    pub fn recv(&self) -> RecvFuture {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };
        assert!(!stream_impl.recv_pending);
        stream_impl.recv_pending = true;

        RecvFuture {
            stream: self,
            completed: false,
        }
    }

    pub fn set_timeout(&self, dur: Duration) {
        let stream_impl = unsafe { &mut *self.p.as_ptr() };

        stream_impl.ts = TimeSpec::from_duration(dur);

        if let Some(ref mut timeout_op) = stream_impl.timeout_op {
            let ring = stream_impl.ex.ring();

            unsafe { reserve_sqes(ring, 1) };

            let sqe = unsafe { io_uring_get_sqe(ring) };
            let user_data = Box::as_mut_ptr(timeout_op) as u64;

            let OpType::MultishotTimeout { ref mut ts, .. } = timeout_op.op_type else {
                unreachable!()
            };

            *ts = stream_impl.ts;
            let flags = 0;
            let ts = ptr::from_mut(ts).cast();

            unsafe { io_uring_prep_timeout_update(sqe, ts, user_data, flags) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
        }
    }
}

//-----------------------------------------------------------------------------

impl Client {
    #[must_use]
    pub fn new(ex: Executor) -> Client {
        let layout = Layout::new::<ClientImpl>();
        let p = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();
        let ring = ex.ring();

        let ref_count = RefCount {
            obj_count: 1,
            op_count: 0,
            release_impl: release_impl::<ClientImpl>,
            obj: p.as_ptr(),
        };

        let client_impl = ClientImpl {
            stream: StreamImpl {
                ref_count,
                ex,
                fd: -1,
                ts: TimeSpec::from_duration(Duration::from_secs(3)),
                buf_group: u16::MAX,
                send_pending: false,
                recv_pending: false,
                last_send: Instant::now(),
                last_recv: Instant::now(),
                timeout_op: None,
                recv_op: None,
            },
            connect_pending: false,
        };

        let p = p.cast::<ClientImpl>();
        unsafe { std::ptr::write(p.as_ptr(), client_impl) };

        let stream_impl = unsafe { &mut (*p.as_ptr()).stream };
        let ref_count = &raw mut stream_impl.ref_count;

        let mut op = Box::new(IoUringOp {
            ref_count,
            initiated: false,
            done: false,
            eager_dropped: false,
            res: -1,
            weak: None,
            op_type: OpType::MultishotTimeout {
                ts: stream_impl.ts,
                stream: &raw mut *stream_impl,
            },
        });

        let user_data = Box::as_mut_ptr(&mut op).cast();

        let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
            unreachable!()
        };

        let ts = std::ptr::from_mut(ts).cast();

        unsafe { reserve_sqes(ring, 1) };
        let sqe = unsafe { io_uring_get_sqe(ring) };

        unsafe { io_uring_prep_timeout(sqe, ts, 0, IORING_TIMEOUT_MULTISHOT) };
        unsafe { io_uring_sqe_set_data(sqe, user_data) };

        stream_impl.timeout_op = Some(op);
        unsafe { add_op_ref(ref_count) };

        Client { p }
    }

    #[must_use]
    pub fn connect_ipv4(&self, addr: Ipv4Addr, port: u16) -> ConnectFuture {
        assert!(unsafe { !(*self.p.as_ptr()).connect_pending });

        let client_impl = unsafe { &mut *self.p.as_ptr() };
        client_impl.connect_pending = true;

        let ref_count = &raw mut client_impl.stream.ref_count;
        let addr = SocketAddrV4::new(addr, port);

        ConnectFuture {
            client: self,
            completed: false,
            op: Some(Box::new(IoUringOp {
                ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                weak: None,
                op_type: OpType::TcpConnect {
                    addr: SockaddrStorage::from(addr),
                    port,
                    ts: client_impl.stream.ts,
                    needs_socket: false,
                    got_socket: false,
                    fd: -1,
                },
            })),
        }
    }

    #[must_use]
    pub fn as_stream(&self) -> Stream {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).stream.ref_count };
        unsafe { add_obj_ref(rc) };

        Stream { p: self.p.cast() }
    }

    pub fn set_buf_group(&self, bgid: u16) {
        let stream_impl = unsafe { &mut (*self.p.as_ptr()).stream };
        stream_impl.buf_group = bgid;
    }

    pub async fn send(&self, buf: Vec<u8>) -> Result<Vec<u8>> {
        let stream = self.as_stream();
        stream.send(buf).await
    }

    pub async fn recv(&self) -> Result<Vec<Vec<u8>>> {
        let stream = self.as_stream();
        stream.recv().await
    }
}

//-----------------------------------------------------------------------------

impl Drop for Stream {
    fn drop(&mut self) {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).ref_count };
        if unsafe { (*rc).obj_count } == 1 {
            let stream_impl = unsafe { &mut *self.p.as_ptr() };
            let ring = stream_impl.ex.ring();

            let num_sqes = if stream_impl.recv_op.is_some() { 2 } else { 1 };

            unsafe { reserve_sqes(ring, num_sqes) };

            {
                let mut op = stream_impl.timeout_op.take().unwrap();
                let user_data = Box::as_mut_ptr(&mut op) as u64;
                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;
                Box::leak(op);

                let sqe = unsafe { io_uring_get_sqe(ring) };
                unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }

            if stream_impl.recv_op.is_some() {
                let mut op = stream_impl.recv_op.take().unwrap();
                let user_data = Box::as_mut_ptr(&mut op) as u64;
                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;
                Box::leak(op);

                let sqe = unsafe { io_uring_get_sqe(ring) };
                unsafe { io_uring_prep_cancel64(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }
        }

        unsafe { release_obj(rc) };
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).stream.ref_count };
        if unsafe { (*rc).obj_count } == 1 {
            let stream_impl = unsafe { &mut (*self.p.as_ptr()).stream };
            let ring = stream_impl.ex.ring();

            let num_sqes = if stream_impl.recv_op.is_some() { 2 } else { 1 };
            unsafe { reserve_sqes(ring, num_sqes) };

            {
                let sqe = unsafe { io_uring_get_sqe(ring) };
                let mut op = stream_impl.timeout_op.take().unwrap();
                let user_data = Box::as_mut_ptr(&mut op) as u64;
                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;
                Box::leak(op);

                unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }

            if stream_impl.recv_op.is_some() {
                let sqe = unsafe { io_uring_get_sqe(ring) };
                let mut op = stream_impl.recv_op.take().unwrap();
                let user_data = Box::as_mut_ptr(&mut op) as u64;
                // makes sure this gets cleaned up, not really an eager-dropped operation
                op.eager_dropped = true;
                Box::leak(op);

                unsafe { io_uring_prep_cancel64(sqe, user_data, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }
        }

        unsafe { release_obj(rc) };
    }
}

//-----------------------------------------------------------------------------

impl Future for ConnectFuture<'_> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);

        let client_impl = unsafe { &mut *self.client.p.as_ptr() };

        let mut op = self.op.take().unwrap();

        match (op.initiated, op.done) {
            (false, true) => panic!(),
            (true, false) => {
                op.weak = Some(client_impl.stream.ex.get_root_task());
                self.op = Some(op);
                Poll::Pending
            }
            (false, false) => {
                let ring = client_impl.stream.ex.ring();

                let user_data = Box::as_mut_ptr(&mut op);

                let OpType::TcpConnect {
                    ref addr,
                    port,
                    ref mut ts,
                    ref mut needs_socket,
                    ref mut fd,
                    ..
                } = op.op_type
                else {
                    unreachable!();
                };

                assert!(*fd < 0);

                let (af, addrlen) = {
                    if let Some(x) = addr.as_sockaddr_in() {
                        (AF_INET, std::mem::size_of_val(x))
                    } else if let Some(x) = addr.as_sockaddr_in6() {
                        (AF_INET6, std::mem::size_of_val(x))
                    } else {
                        unreachable!();
                    }
                };

                let Some(file_index) = client_impl.stream.ex.get_available_fd() else {
                    self.op = Some(op);
                    return Poll::Ready(Err(Errno::from_raw(ENFILE)));
                };

                *fd = file_index.try_into().unwrap();

                unsafe { reserve_sqes(ring, 3) };

                {
                    *needs_socket = true;

                    let sqe = unsafe { io_uring_get_sqe(ring) };

                    unsafe {
                        io_uring_prep_socket_direct(
                            sqe,
                            af,
                            SOCK_STREAM,
                            IPPROTO_TCP,
                            file_index,
                            0,
                        );
                    }
                    unsafe { io_uring_sqe_set_data(sqe, user_data.cast()) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK) }
                }

                {
                    let sqe = unsafe { io_uring_get_sqe(ring) };
                    unsafe {
                        io_uring_prep_connect(
                            sqe,
                            file_index.try_into().unwrap(),
                            std::ptr::from_ref(addr).cast(),
                            addrlen.try_into().unwrap(),
                        );
                    }
                    unsafe { io_uring_sqe_set_data(sqe, user_data.cast()) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK | IOSQE_FIXED_FILE) };
                }

                {
                    let sqe = unsafe { io_uring_get_sqe(ring) };
                    unsafe { io_uring_prep_link_timeout(sqe, std::ptr::from_mut(ts).cast(), 0) };
                    unsafe { io_uring_sqe_set_data(sqe, std::ptr::null_mut()) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
                }

                unsafe { add_op_ref(&raw mut client_impl.stream.ref_count) };

                op.weak = Some(client_impl.stream.ex.get_root_task());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpConnect {
                    needs_socket,
                    got_socket,
                    fd,
                    ..
                } = op.op_type
                else {
                    unreachable!();
                };

                if needs_socket && got_socket {
                    if client_impl.stream.fd >= 0 {
                        self.op = Some(op);
                        todo!();
                    }

                    client_impl.stream.fd = fd;
                }

                let res = op.res;
                self.op = Some(op);
                if res < 0 {
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        let client_impl = unsafe { &mut *self.client.p.as_ptr() };
        client_impl.connect_pending = false;

        let op = self.op.as_mut().unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            Box::leak(self.op.take().unwrap());
            todo!();
        }
    }
}

//-----------------------------------------------------------------------------

impl Drop for SendFuture<'_> {
    fn drop(&mut self) {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };

        stream_impl.send_pending = false;

        let op = self.op.as_mut().unwrap();
        if op.initiated && !op.done {
            let ring = stream_impl.ex.ring();
            unsafe { reserve_sqes(ring, 1) };
            let sqe = unsafe { io_uring_get_sqe(ring) };

            let user_data = Box::as_mut_ptr(op) as usize as u64;
            unsafe {
                io_uring_prep_cancel_fd(
                    sqe,
                    stream_impl.fd,
                    IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED,
                );
            }

            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            // TODO: same as TimerFuture::drop() comments

            op.eager_dropped = true;
            op.weak = None;
            Box::leak(self.op.take().unwrap());
        }
    }
}

impl Future for SendFuture<'_> {
    type Output = Result<Vec<u8>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);

        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };

        let mut op = self.op.take().unwrap();
        let user_data = Box::as_mut_ptr(&mut op);

        match (op.initiated, op.done) {
            (false, true) => panic!(),
            (true, false) => {
                op.weak = Some(stream_impl.ex.get_root_task());
                self.op = Some(op);
                Poll::Pending
            }
            (false, false) => {
                let OpType::TcpSend { ref buf, .. } = op.op_type else {
                    unreachable!()
                };

                let ring = stream_impl.ex.ring();
                unsafe { reserve_sqes(ring, 1) };

                {
                    let sqe = unsafe { io_uring_get_sqe(ring) };
                    unsafe {
                        io_uring_prep_send_zc(
                            sqe,
                            stream_impl.fd,
                            buf.as_ptr().cast(),
                            buf.len(),
                            MSG_WAITALL,
                            0,
                        );
                    }

                    unsafe { io_uring_sqe_set_data(sqe, user_data.cast()) };
                    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) }
                }

                unsafe { add_op_ref(&raw mut stream_impl.ref_count) };

                op.weak = Some(stream_impl.ex.get_root_task());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::TcpSend { ref mut buf, .. } = op.op_type else {
                    unreachable!()
                };

                let res = op.res;
                if res < 0 {
                    self.op = Some(op);
                    Poll::Ready(Err(Errno::from_raw(-res)))
                } else {
                    let b = std::mem::take(buf);
                    self.op = Some(op);
                    Poll::Ready(Ok(b))
                }
            }
        }
    }
}

//-----------------------------------------------------------------------------

impl Drop for RecvFuture<'_> {
    fn drop(&mut self) {
        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        stream_impl.recv_pending = false;
    }
}

impl Future for RecvFuture<'_> {
    type Output = Result<Vec<Vec<u8>>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);

        let stream_impl = unsafe { &mut *self.stream.p.as_ptr() };
        let ring = stream_impl.ex.ring();

        match stream_impl.recv_op {
            None => {
                let bgid = stream_impl.buf_group;
                let ioprio =
                    u16::try_from(IORING_RECVSEND_POLL_FIRST | IORING_RECVSEND_BUNDLE).unwrap();

                let ref_count = &raw mut stream_impl.ref_count;

                let buf_group = match stream_impl
                    .ex
                    .p
                    .borrow_mut()
                    .buf_groups
                    .get(&stream_impl.buf_group)
                {
                    None => {
                        self.completed = true;
                        return Poll::Ready(Err(Errno::ENOENT));
                    }
                    Some(buf_group) => *buf_group,
                };

                stream_impl.last_recv = Instant::now();
                let last_recv: *mut Instant = &raw mut stream_impl.last_recv;

                let mut op = Box::new(IoUringOp {
                    ref_count,
                    initiated: false,
                    done: false,
                    eager_dropped: false,
                    res: -1,
                    weak: None,
                    op_type: OpType::MultishotTcpRecv {
                        bufs: Vec::new(),
                        buf_group,
                        last_recv,
                    },
                });

                let user_data = Box::as_mut_ptr(&mut op).cast();

                unsafe { reserve_sqes(ring, 1) };

                let sqe = unsafe { io_uring_get_sqe(ring) };

                let sockfd = stream_impl.fd;
                let flags = IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;

                unsafe { io_uring_prep_recv_multishot(sqe, sockfd, ptr::null_mut(), 0, 0) };
                unsafe { io_uring_sqe_set_data(sqe, user_data) };
                unsafe { io_uring_sqe_set_buf_group(sqe, bgid.into()) };
                unsafe { io_uring_sqe_set_flags(sqe, flags) };
                unsafe { (*sqe).ioprio |= ioprio };

                unsafe { add_op_ref(ref_count) };

                op.initiated = true;
                op.weak = Some(stream_impl.ex.get_root_task());
                stream_impl.recv_op = Some(op);

                Poll::Pending
            }
            Some(ref mut op) => {
                let OpType::MultishotTcpRecv {
                    ref mut bufs,
                    buf_group,
                    ..
                } = op.op_type
                else {
                    unreachable!();
                };

                if !bufs.is_empty() {
                    self.completed = true;
                    let buf_seq = mem::take(bufs);

                    let buf_group = unsafe { &mut *buf_group };
                    let mask = buf_group.num_bufs - 1;
                    let br = buf_group.buf_ring;

                    for buf_offset in 0..buf_seq.len() {
                        let bid = buf_group.tail.try_into().unwrap();

                        let buf = Vec::<u8>::with_capacity(buf_group.buf_len);
                        let (addr, _, _) = buf.into_raw_parts();
                        buf_group.bufs[usize::from(bid)] = addr;

                        let len = buf_group.buf_len.try_into().unwrap();
                        let addr = addr.cast();
                        let buf_offset = buf_offset.try_into().unwrap();

                        buf_group.tail = (buf_group.tail + 1) & mask;

                        let mask = mask.try_into().unwrap();

                        unsafe { io_uring_buf_ring_add(br, addr, len, bid, mask, buf_offset) };
                    }

                    let count = buf_seq.len().try_into().unwrap();
                    unsafe { io_uring_buf_ring_advance(br, count) };

                    return Poll::Ready(Ok(buf_seq));
                }

                if op.res < 0 {
                    self.completed = true;
                    let res = -op.res;
                    if op.done {
                        stream_impl.recv_op = None;
                    }
                    return Poll::Ready(Err(Errno::from_raw(res)));
                }

                op.weak = Some(stream_impl.ex.get_root_task());
                Poll::Pending
            }
        }
    }
}

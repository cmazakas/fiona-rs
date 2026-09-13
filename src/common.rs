// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{marker::PhantomData, mem::offset_of, task::Poll};

use crate::{FdImpl, OpType, add_op_ref, get_sqe, make_io_uring_op};
use liburing_rs::{
    IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED, io_uring_prep_cancel_fd,
    io_uring_prep_cancel64, io_uring_prep_close, io_uring_prep_close_direct,
    io_uring_sqe_set_data64,
};
use nix::errno::Errno;
use slotmap::{DefaultKey, Key, KeyData};

pub(crate) struct CancelFuture<'a> {
    pub(crate) fd_impl: *mut FdImpl,
    pub(crate) completed: bool,
    pub(crate) op: Option<u64>,
    pub(crate) _m: PhantomData<&'a FdImpl>,
}

impl Future for CancelFuture<'_> {
    type Output = Result<(), nix::Error>;

    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);

        let fd_impl = unsafe { &mut *self.fd_impl };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));

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
                if fd_impl.fd == -1 {
                    self.completed = true;
                    return Poll::Ready(Err(Errno::EBADF));
                }

                let OpType::FdCancel = op.op_type else {
                    unreachable!()
                };

                let sqe = get_sqe(&fd_impl.ex);
                let fd = fd_impl.fd;
                let mut flags = IORING_ASYNC_CANCEL_ALL;
                if fd_impl.is_fixed {
                    flags |= IORING_ASYNC_CANCEL_FD_FIXED;
                }

                unsafe { io_uring_prep_cancel_fd(sqe, fd, flags) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };

                unsafe { add_op_ref(&raw mut fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                let res = op.res;
                let OpType::FdCancel = op.op_type else {
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

impl Drop for CancelFuture<'_> {
    fn drop(&mut self) {
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

pub(crate) struct CloseFuture<'a> {
    pub(crate) fd_impl: *mut FdImpl,
    pub(crate) completed: bool,
    pub(crate) op: Option<u64>,
    pub(crate) _m: PhantomData<&'a FdImpl>,
}

impl Future for CloseFuture<'_> {
    type Output = Result<(), nix::Error>;

    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);
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
                if fd_impl.fd == -1 {
                    self.completed = true;
                    return Poll::Ready(Err(Errno::EBADF));
                }

                let OpType::FdClose = op.op_type else {
                    unreachable!()
                };

                let sqe = get_sqe(&fd_impl.ex);
                let fd = fd_impl.fd as u32;

                fd_impl.fd = -1;

                if fd_impl.is_fixed {
                    unsafe { io_uring_prep_close_direct(sqe, fd) };
                } else {
                    unsafe { io_uring_prep_close(sqe, fd as _) };
                }
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                unsafe { add_op_ref(&raw mut fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let OpType::FdClose = op.op_type else {
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

impl Drop for CloseFuture<'_> {
    fn drop(&mut self) {
        let fd_impl = unsafe { &mut *self.fd_impl };
        fd_impl.close_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = unsafe {
                self.fd_impl
                    .cast::<u8>()
                    .add(offset_of!(FdImpl, ref_count))
                    .cast()
            };

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

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

extern crate liburing_rs;

use std::{future::Future, ptr::NonNull, task::Poll, time::Duration};

use nix::{errno::Errno, libc::ETIME, sys::time::TimeSpec};

use liburing_rs::{
    __kernel_timespec, IOSQE_CQE_SKIP_SUCCESS, io_uring_get_sqe, io_uring_prep_timeout,
    io_uring_prep_timeout_remove, io_uring_sqe_set_data, io_uring_sqe_set_data64,
    io_uring_sqe_set_flags,
};

use crate::{
    Executor, IoUringOp, OpType, RefCount, Result, add_obj_ref, add_op_ref, make_io_uring_op,
    release_impl, release_obj, reserve_sqes,
};

struct TimerImpl
{
    ref_count: RefCount,
    ex: Executor,
    timeout_pending: bool,
}

pub struct Timer
{
    p: NonNull<TimerImpl>,
}

pub struct TimerFuture<'a>
{
    timer: &'a Timer,
    op: Option<Box<IoUringOp>>,
    completed: bool,
}

//-----------------------------------------------------------------------------

impl Timer
{
    #[must_use]
    pub fn new(ex: Executor) -> Self
    {
        let layout = std::alloc::Layout::new::<TimerImpl>();
        let p = unsafe { std::alloc::alloc(layout) };

        let ref_count = RefCount { obj_count: 1,
                                   op_count: 0,
                                   release_impl: release_impl::<TimerImpl>,
                                   obj: p };

        let timer_impl = TimerImpl { ref_count,
                                     ex,
                                     timeout_pending: false };

        let p = p.cast::<TimerImpl>();
        unsafe { std::ptr::write(p, timer_impl) };

        Self { p: NonNull::new(p).unwrap() }
    }

    #[inline]
    #[must_use]
    pub fn wait(&self, dur: Duration) -> TimerFuture
    {
        let timer_impl = unsafe { &mut *self.p.as_ptr() };
        assert!(!timer_impl.timeout_pending);

        timer_impl.timeout_pending = true;

        let ref_count = &raw mut timer_impl.ref_count;

        TimerFuture {
            timer: self,
            completed: false,
            op: Some(Box::new(make_io_uring_op(
                ref_count,
                OpType::Timeout {
                    dur: TimeSpec::new(
                        dur.as_secs().try_into().unwrap(),
                        dur.subsec_nanos().into(),
                    ),
                },
            ))),
        }
    }
}

impl Drop for Timer
{
    fn drop(&mut self)
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).ref_count };
        unsafe { release_obj(rc) };
    }
}

impl Clone for Timer
{
    fn clone(&self) -> Self
    {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).ref_count };
        unsafe { add_obj_ref(rc) };

        Self { p: self.p }
    }
}

//-----------------------------------------------------------------------------

impl Drop for TimerFuture<'_>
{
    fn drop(&mut self)
    {
        let timer_impl = unsafe { &mut *self.timer.p.as_ptr() };
        timer_impl.timeout_pending = false;

        let op = self.op.as_mut().unwrap();

        if op.initiated && !op.done {
            let ring = timer_impl.ex.ring();
            unsafe { reserve_sqes(ring, 1) };
            let sqe = unsafe { io_uring_get_sqe(ring) };

            let user_data = Box::as_mut_ptr(op) as usize as u64;
            unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            // TODO: should we eventually remove this? because we now allocate
            // per-Future state, so long as we mark this operation as eager-dropped,
            // our CQE loop can safely ignore it which means we no longer need to
            // cancel ASAP from the Drop impl and can instead rely on the next tick
            // of the event loop to cancel the operation for us
            //
            // unsafe { submit_ring(ring) };

            op.eager_dropped = true;
            op.local_waker = None;
            Box::leak(self.op.take().unwrap());
        }
    }
}

impl Future for TimerFuture<'_>
{
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> std::task::Poll<Self::Output>
    {
        assert!(!self.completed);

        let timer_impl = unsafe { &mut *self.timer.p.as_ptr() };

        let mut op = self.op.take().unwrap();

        match (op.initiated, op.done) {
            (true, true) => {
                self.completed = true;

                let res = op.res;

                self.op = Some(op);
                if res < 0 {
                    let res = -res;
                    if res == ETIME {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(Errno::from_raw(res)))
                    }
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                self.op = Some(op);
                Poll::Pending
            }
            (false, true) => panic!(),
            (false, false) => {
                let ring = timer_impl.ex.ring();
                unsafe { reserve_sqes(ring, 1) };
                let sqe = unsafe { io_uring_get_sqe(ring) };

                let ts = match op.op_type {
                    OpType::Timeout { ref mut dur } => &raw mut *dur,
                    _ => unreachable!(),
                };

                let user_data = Box::as_mut_ptr(&mut op);

                unsafe { io_uring_prep_timeout(sqe, ts.cast::<__kernel_timespec>(), 0, 0) };
                unsafe { io_uring_sqe_set_data(sqe, user_data.cast()) };

                unsafe { add_op_ref(&raw mut timer_impl.ref_count) };
                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
        }
    }
}

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::marker::PhantomData;
use std::{future::Future, task::Poll, time::Duration};

use nix::errno::Errno;
use nix::libc::ETIME;
use nix::sys::time::TimeSpec;

use crate::uring::{__kernel_timespec, io_uring_get_sqe};
use crate::uring::{io_uring_prep_timeout, io_uring_prep_timeout_remove};
use crate::Result;
use crate::{add_ref, IoUringOp};
use crate::{release, RefCount};
use crate::{release_impl, reserve_sqes};
use crate::{Executor, OpType};

#[repr(C)]
struct TimerImpl {
    ref_count: RefCount,
    ex: Executor,
    dur: Option<TimeSpec>,
}

pub struct Timer {
    p: *mut TimerImpl,
    phantom: PhantomData<TimerImpl>,
}

pub struct TimerFuture<'a> {
    timer: &'a mut Timer,
    op: Option<Box<IoUringOp>>,
    completed: bool,
}

impl Timer {
    #[must_use]
    pub fn new(ex: Executor) -> Self {
        let layout = std::alloc::Layout::new::<TimerImpl>();
        let p = unsafe { std::alloc::alloc(layout) };

        let ref_count = RefCount {
            count: 1,
            release_impl: release_impl::<TimerImpl>,
            obj: p,
        };

        let timer_impl = TimerImpl {
            ref_count,
            ex,
            dur: None,
        };

        let p = p.cast::<TimerImpl>();
        unsafe { std::ptr::write(p, timer_impl) };

        Self {
            p,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn wait(&mut self, dur: Duration) -> TimerFuture {
        let timer_impl = unsafe { &mut *self.p };

        timer_impl.dur = Some(TimeSpec::new(
            dur.as_secs().try_into().unwrap(),
            dur.subsec_nanos().into(),
        ));

        let weak = timer_impl.ex.p.borrow().root_task.clone().unwrap();

        TimerFuture {
            timer: self,
            completed: false,
            op: Some(Box::new(IoUringOp {
                ref_count: &raw mut timer_impl.ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                weak: Some(weak),
                op_type: OpType::Timeout,
            })),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        unsafe { release(self.p.cast::<RefCount>()) };
    }
}

impl Drop for TimerFuture<'_> {
    fn drop(&mut self) {
        let p = self.timer.p;
        let timer_impl = unsafe { &mut *p };

        let op = self.op.as_mut().unwrap();

        if op.initiated && !op.done {
            let ring = timer_impl.ex.ring();
            unsafe { reserve_sqes(ring, 1) };
            let sqe = unsafe { io_uring_get_sqe(ring) };

            let user_data = Box::as_mut_ptr(op) as usize as u64;
            unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
            unsafe { (*sqe).user_data = 0 };

            // TODO: manually add this at some point because bindgen isn't capable of
            // currently handling IOSQE_CQE_SKIP_SUCCESS_BIT being an enum so it never
            // generates the appropriate constants
            //
            // unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            // TODO: should we eventually remove this? because we now allocate
            // per-Future state, so long as we mark this operation as eager-dropped,
            // our CQE loop can safely ignore it which means we no longer need to
            // cancel ASAP from the Drop impl and can instead rely on the next tick
            // of the event loop to cancel the operation for us
            //
            // unsafe { submit_ring(ring) };

            op.eager_dropped = true;
            op.weak = None;
            Box::leak(self.op.take().unwrap());
        }
    }
}

impl Future for TimerFuture<'_> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);

        let p = self.timer.p;
        let timer_impl = unsafe { &mut *p };

        let mut op = self.op.take().unwrap();

        match (op.initiated, op.done) {
            (true, true) => {
                self.completed = true;

                let res = op.res;

                timer_impl.dur = None;

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
                op.weak = Some(timer_impl.ex.p.borrow().root_task.clone().unwrap());
                self.op = Some(op);
                Poll::Pending
            }
            (false, true) => panic!(),
            (false, false) => {
                let ring = timer_impl.ex.ring();
                unsafe { reserve_sqes(ring, 1) };
                let sqe = unsafe { io_uring_get_sqe(ring) };

                let ts = std::ptr::from_mut(timer_impl.dur.as_mut().unwrap());

                unsafe { io_uring_prep_timeout(sqe, ts.cast::<__kernel_timespec>(), 0, 0) };
                unsafe { (*sqe).user_data = Box::as_mut_ptr(&mut op) as usize as u64 };
                unsafe { add_ref(&raw mut timer_impl.ref_count) };

                op.weak = Some(timer_impl.ex.p.borrow().root_task.clone().unwrap());
                op.initiated = true;
                self.op = Some(op);
                Poll::Pending
            }
        }
    }
}

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::marker::PhantomData;
use std::ptr::addr_of_mut;
use std::{future::Future, task::Poll, time::Duration};

use nix::errno::Errno;
use nix::libc::ETIME;
use nix::sys::time::TimeSpec;

use crate::uring::{__kernel_timespec, io_uring_get_sqe};
use crate::uring::{io_uring_prep_timeout, io_uring_prep_timeout_remove};
use crate::{add_ref, IoUringOp};
use crate::{release, RefCount};
use crate::{release_impl, reserve_sqes};
use crate::{submit_ring, Result};
use crate::{Executor, OpType};

#[repr(C)]
struct TimerImpl {
    ref_count: RefCount,
    ex: Executor,
    dur: Option<TimeSpec>,
    timeout_op: IoUringOp,
    timeout_cancel_op: IoUringOp,
}

pub struct Timer {
    p: *mut TimerImpl,
    phantom: PhantomData<TimerImpl>,
}

pub struct TimerFuture<'a> {
    timer: &'a mut Timer,
    initiated: bool,
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
            timeout_op: IoUringOp {
                ref_count: p.cast::<RefCount>(),
                done: false,
                initiated: false,
                res: -1,
                waker: None,
                eager_dropped: false,
                op_type: OpType::Timeout,
            },
            timeout_cancel_op: IoUringOp {
                ref_count: p.cast::<RefCount>(),
                done: false,
                initiated: false,
                res: -1,
                waker: None,
                eager_dropped: false,
                op_type: OpType::TimeoutCancel,
            },
        };

        let p = p.cast::<TimerImpl>();
        unsafe { std::ptr::write(p, timer_impl) };

        Self {
            p,
            phantom: PhantomData,
        }
    }

    pub fn wait(&mut self, dur: Duration) -> TimerFuture {
        let timer_impl = unsafe { &mut *self.p };
        assert!(!timer_impl.timeout_op.initiated);

        timer_impl.dur = Some(TimeSpec::new(
            dur.as_secs().try_into().unwrap(),
            dur.subsec_nanos().into(),
        ));

        TimerFuture {
            timer: self,
            completed: false,
            initiated: false,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        unsafe { release(self.p.cast::<RefCount>()) };
    }
}

impl<'a> Drop for TimerFuture<'a> {
    fn drop(&mut self) {
        let p = self.timer.p;
        let timer_impl = unsafe { &mut *p };

        if timer_impl.timeout_op.initiated && !timer_impl.timeout_op.done {
            let ring = timer_impl.ex.ring();
            unsafe { reserve_sqes(ring, 1) };
            let sqe = unsafe { io_uring_get_sqe(ring) };

            let user_data = addr_of_mut!(timer_impl.timeout_op) as usize as u64;
            unsafe { io_uring_prep_timeout_remove(sqe, user_data, 0) };
            unsafe { (*sqe).user_data = 0 };
            // unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            unsafe { submit_ring(ring) };

            let op = &mut timer_impl.timeout_op;
            op.eager_dropped = true;
            op.waker = None;
            op.initiated = false;
            op.done = false;
            op.res = 0;
        }
    }
}

impl<'a> Future for TimerFuture<'a> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);

        let p = self.timer.p;
        let timer_impl = unsafe { &mut *p };

        if timer_impl.timeout_op.initiated {
            assert!(self.initiated);
        }

        match (timer_impl.timeout_op.initiated, timer_impl.timeout_op.done) {
            (true, true) => {
                self.completed = true;

                let res = timer_impl.timeout_op.res;

                timer_impl.dur = None;
                timer_impl.timeout_op.initiated = false;
                timer_impl.timeout_op.done = false;
                timer_impl.timeout_op.res = -1;
                timer_impl.timeout_op.waker = None;

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
                timer_impl.timeout_op.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            (false, true) => panic!(),
            (false, false) => {
                let ring = timer_impl.ex.ring();
                unsafe { reserve_sqes(ring, 1) };
                let sqe = unsafe { io_uring_get_sqe(ring) };

                let ts = std::ptr::from_mut(timer_impl.dur.as_mut().unwrap());

                timer_impl.timeout_op.waker = Some(cx.waker().clone());
                unsafe { io_uring_prep_timeout(sqe, ts.cast::<__kernel_timespec>(), 0, 0) };
                unsafe {
                    (*sqe).user_data = addr_of_mut!(timer_impl.timeout_op) as usize as u64;
                }
                unsafe { add_ref(addr_of_mut!(timer_impl.ref_count)) };

                timer_impl.timeout_op.initiated = true;
                self.initiated = true;
                Poll::Pending
            }
        }
    }
}

// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::marker::PhantomData;
use std::ptr::addr_of_mut;
use std::{future::Future, task::Poll, time::Duration};

use nix::errno::Errno;
use nix::libc::ETIME;
use nix::sys::time::TimeSpec;

use crate::uring::io_uring_prep_timeout;
use crate::uring::{__kernel_timespec, io_uring_get_sqe};
use crate::Result;
use crate::{add_ref, IoUringOp};
use crate::{release, RefCount};
use crate::{release_impl, reserve_sqes};
use crate::{Executor, OpType};

#[repr(C)]
struct TimerImpl {
    ref_count: RefCount,
    ex: Executor,
    dur: Option<Duration>,
    op: IoUringOp,
}

pub struct Timer {
    p: *mut TimerImpl,
    phantom: PhantomData<TimerImpl>,
}

pub struct TimerFuture<'a> {
    timer: &'a mut Timer,
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
            op: IoUringOp {
                ref_count: p.cast::<RefCount>(),
                done: false,
                initiated: false,
                res: -1,
                waker: None,
                op_type: OpType::Timer,
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
        unsafe { (*self.p).dur = Some(dur) };
        TimerFuture { timer: self }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        unsafe { release(self.p.cast::<RefCount>()) };
    }
}

impl<'a> Future for TimerFuture<'a> {
    type Output = Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let p = self.timer.p;
        let timer_impl = unsafe { &mut *p };

        match (timer_impl.op.initiated, timer_impl.op.done) {
            (true, true) => {
                let res = timer_impl.op.res;
                unsafe { release(self.timer.p.cast::<RefCount>()) };
                if res < 0 {
                    let res = -res;
                    if res == ETIME {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(Errno::from_raw(-res)))
                    }
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            (true, false) => {
                timer_impl.op.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            (false, true) => panic!(),
            (false, false) => {
                let ring = timer_impl.ex.ring();
                unsafe { reserve_sqes(ring, 1) };
                let sqe = unsafe { io_uring_get_sqe(ring) };

                let dur = timer_impl.dur.unwrap();

                let mut ts =
                    TimeSpec::new(dur.as_secs().try_into().unwrap(), dur.subsec_nanos().into());
                let ts = std::ptr::from_mut(&mut ts).cast::<__kernel_timespec>();

                timer_impl.op.waker = Some(cx.waker().clone());
                unsafe { io_uring_prep_timeout(sqe, ts, 1, 0) };
                unsafe {
                    (*sqe).user_data = std::ptr::addr_of_mut!(timer_impl.op) as usize as u64;
                }
                unsafe { add_ref(addr_of_mut!(timer_impl.ref_count)) };

                timer_impl.op.initiated = true;

                Poll::Pending
            }
        }
    }
}

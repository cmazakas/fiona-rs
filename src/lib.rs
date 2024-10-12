// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![warn(clippy::pedantic)]
#![allow(
    clippy::mutable_key_type,
    clippy::missing_panics_doc,
    clippy::cast_ptr_alignment
)]

extern crate nix;

use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::Ordering::Release;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::task::Poll;

use nix::libc::c_void;

use uring::io_uring;
use uring::io_uring_cq_advance;
use uring::io_uring_cq_ready;
use uring::io_uring_cqe;
use uring::io_uring_get_sqe;
use uring::io_uring_params;
use uring::io_uring_peek_batch_cqe;
use uring::io_uring_prep_read;
use uring::io_uring_queue_init_params;
use uring::io_uring_register_files_sparse;
use uring::io_uring_register_ring_fd;
use uring::io_uring_sq_space_left;
use uring::io_uring_submit_and_get_events;
use uring::io_uring_submit_and_wait;
use uring::IORING_SETUP_CQSIZE;
use uring::IORING_SETUP_DEFER_TASKRUN;
use uring::IORING_SETUP_SINGLE_ISSUER;

mod uring;

struct TaskInner<F: ?Sized + Future<Output = ()> + 'static> {
    strong: AtomicU64,
    weak: AtomicU64,
    future: ManuallyDrop<UnsafeCell<F>>,
}

//-----------------------------------------------------------------------------

struct Task {
    p: NonNull<TaskInner<dyn Future<Output = ()>>>,
    phantom: PhantomData<TaskInner<dyn Future<Output = ()> + 'static>>,
}

impl Task {
    #[must_use]
    pub fn new<F: Future<Output = ()> + 'static>(future: F) -> Self {
        Self {
            p: NonNull::from(Box::leak(Box::new(TaskInner {
                strong: AtomicU64::new(1),
                weak: AtomicU64::new(1),
                future: ManuallyDrop::new(UnsafeCell::new(future)),
            }))),
            phantom: PhantomData,
        }
    }

    fn inner(&self) -> &TaskInner<dyn Future<Output = ()>> {
        unsafe { self.p.as_ref() }
    }

    #[must_use]
    pub fn downgrade(this: &Task) -> Weak {
        this.inner().weak.fetch_add(1, Relaxed);

        Weak {
            p: this.p,
            phantom: PhantomData,
        }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if self.inner().strong.fetch_sub(1, Release) > 1 {
            return;
        }

        // delay the Acquire semantics until we know we need to Drop the T
        self.inner().strong.load(Acquire);

        unsafe {
            ManuallyDrop::drop(&mut (*self.p.as_ptr()).future);
        }

        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }

        self.inner().weak.load(Acquire);

        drop(unsafe { Box::from_raw(self.p.as_ptr()) });
    }
}

impl Clone for Task {
    fn clone(&self) -> Self {
        self.inner().strong.fetch_add(1, Relaxed);
        Self {
            p: self.p,
            phantom: PhantomData,
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(self.p.as_ptr(), other.p.as_ptr())
    }
}

impl Eq for Task {}

impl Hash for Task {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.p.hash(state);
    }
}

//-----------------------------------------------------------------------------

pub struct Weak {
    p: NonNull<TaskInner<dyn Future<Output = ()>>>,
    phantom: PhantomData<TaskInner<dyn Future<Output = ()>>>,
}

impl Weak {
    fn inner(&self) -> &TaskInner<dyn Future<Output = ()>> {
        unsafe { self.p.as_ref() }
    }

    fn upgrade(&self) -> Option<Task> {
        let mut c = self.inner().strong.load(Relaxed);

        loop {
            if c == 0 {
                return None;
            }

            let r = self
                .inner()
                .strong
                .compare_exchange_weak(c, c + 1, Relaxed, Relaxed);

            match r {
                Ok(_) => {
                    return Some(Task {
                        p: self.p,
                        phantom: PhantomData,
                    })
                }
                Err(c2) => {
                    c = c2;
                }
            };
        }
    }
}

impl Drop for Weak {
    fn drop(&mut self) {
        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }

        self.inner().weak.load(Acquire);

        drop(unsafe { Box::from_raw(self.p.as_ptr()) });
    }
}

impl Clone for Weak {
    fn clone(&self) -> Self {
        self.inner().weak.fetch_add(1, Relaxed);

        Self {
            p: self.p,
            phantom: PhantomData,
        }
    }
}

unsafe impl Sync for Weak {}
unsafe impl Send for Weak {}

//-----------------------------------------------------------------------------

pub struct TaskWaker {
    weak_ptr: Option<Weak>,
    sender: Sender<Weak>,
    event_fd: i32,
}

impl std::task::Wake for TaskWaker {
    fn wake(self: std::sync::Arc<Self>) {
        let s = self.sender.clone();
        if let Some(weak_ptr) = &self.weak_ptr {
            s.send(weak_ptr.clone()).unwrap();

            let buf = &0x01_u64.to_ne_bytes();
            unsafe {
                nix::libc::write(self.event_fd, buf.as_ptr().cast::<c_void>(), buf.len());
            }
        }
    }
}

//-----------------------------------------------------------------------------

struct IoContextFrame {
    ioring: io_uring,
    params: io_uring_params,
    available_fds: VecDeque<u32>,
    tasks: HashSet<Task>,
    receiver: Receiver<Weak>,
    sender: Sender<Weak>,
    root_task: Option<Weak>,
}

//-----------------------------------------------------------------------------

// struct RefCount {
//     count: usize,
//     release_impl: unsafe fn(p: *mut u8),
//     obj: *mut u8,
// }

// unsafe fn release_impl<T>(p: *mut u8) {
//     let layout = std::alloc::Layout::new::<T>();
//     std::ptr::drop_in_place(p.cast::<T>());
//     std::alloc::dealloc(p, layout);
// }

// unsafe fn release(rc: *mut RefCount) {
//     (*rc).count -= 1;
//     if (*rc).count == 0 {
//         let fp = (*rc).release_impl;
//         let p = (*rc).obj;
//         (fp)(p);
//     }
// }

//-----------------------------------------------------------------------------

pub struct IoContext {
    p: Rc<RefCell<IoContextFrame>>,
}

unsafe fn submit_ring(ring: *mut io_uring) {
    io_uring_submit_and_get_events(ring);
}

unsafe fn reserve_sqes(ring: *mut io_uring, n: u32) {
    let r = io_uring_sq_space_left(ring);
    if r < n {
        submit_ring(ring);
    }
}

impl IoContext {
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();

        let sq_entries = 256;
        let cq_entries = 4 * 1024;
        let num_fds = 1024; // matches the default for Linux processes

        let mut params = unsafe { std::mem::zeroed::<io_uring_params>() };
        params.cq_entries = cq_entries;
        params.flags |= IORING_SETUP_CQSIZE;
        params.flags |= IORING_SETUP_SINGLE_ISSUER;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;

        let ioring = unsafe { std::mem::zeroed::<io_uring>() };

        let p = Rc::new(RefCell::new(IoContextFrame {
            params,
            tasks: HashSet::new(),
            available_fds: VecDeque::new(),
            receiver: rx,
            sender: tx,
            root_task: None,
            ioring,
        }));

        {
            let ring = addr_of_mut!(p.borrow_mut().ioring);
            let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, addr_of_mut!(params)) };
            assert_eq!(ret, 0);

            let ret = unsafe { io_uring_register_ring_fd(ring) };
            assert_eq!(ret, 1);

            let ret = unsafe { io_uring_register_files_sparse(ring, num_fds) };
            assert_eq!(ret, 0);
        }

        {
            let available_fds = &mut p.borrow_mut().available_fds;
            available_fds.reserve(num_fds as usize);
            for i in 0..num_fds {
                available_fds.push_back(i);
            }
        }

        Self { p }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor {
        Executor { p: self.p.clone() }
    }

    fn ring(&self) -> *mut io_uring {
        unsafe { addr_of_mut!((*self.p.as_ptr()).ioring) }
    }

    pub fn run(&mut self) -> u64 {
        let mut num_completed = 0;

        let event_fd = nix::sys::eventfd::EventFd::new().unwrap();
        let mut event_count = 0_u64;

        let ex = self.get_executor();
        let ring = ex.ring();

        let cq_entries = ex.p.borrow().params.cq_entries;
        let mut cqes = Vec::<*mut io_uring_cqe>::with_capacity(cq_entries as usize);

        loop {
            if self.p.borrow().tasks.is_empty() {
                break;
            }

            loop {
                let m_item = self.p.borrow().receiver.try_recv();
                if let Ok(ref w) = m_item {
                    match w.upgrade() {
                        None => continue,
                        Some(task) => {
                            (*self.p).borrow_mut().root_task = Some(w.clone());

                            let sender = self.p.borrow().sender.clone();

                            let w = Arc::new(TaskWaker {
                                weak_ptr: Some(w.clone()),
                                sender,
                                event_fd: event_fd.as_raw_fd(),
                            })
                            .into();

                            let mut cx = std::task::Context::from_waker(&w);
                            if let Poll::Ready(()) =
                                unsafe { Pin::new_unchecked(&mut *(*task.p.as_ptr()).future.get()) }
                                    .poll(&mut cx)
                            {
                                (*self.p).borrow_mut().tasks.remove(&task);
                                num_completed += 1;
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            // TODO: clean this up at some point
            if self.p.borrow().tasks.is_empty() {
                break;
            }

            {
                unsafe { reserve_sqes(ring, 1) };

                let eventfd_sqe = unsafe { io_uring_get_sqe(ring) };
                let sqe = unsafe { &mut *eventfd_sqe };
                unsafe {
                    let p = std::ptr::from_mut::<u64>(&mut event_count).cast::<c_void>();
                    io_uring_prep_read(sqe, event_fd.as_raw_fd(), p, 8, 0);
                }
                sqe.user_data = 0x01; // sentinel value
            }

            let ret = unsafe { io_uring_submit_and_wait(ring, 1) };
            assert!(ret >= 0);

            let num_ready = unsafe { io_uring_cq_ready(ring) };
            let num_cqes = unsafe { io_uring_peek_batch_cqe(ring, cqes.as_mut_ptr(), num_ready) };
            unsafe { cqes.set_len(num_cqes as usize) };

            for cqe in &mut cqes {
                let cqe = unsafe { &mut **cqe };
                if cqe.user_data == 0 || cqe.user_data == 1 {
                    continue;
                }
            }

            unsafe { io_uring_cq_advance(ring, num_cqes) };
        }

        num_completed
    }
}

impl Drop for IoContext {
    fn drop(&mut self) {
        unsafe {
            uring::io_uring_queue_exit(self.ring());
        }
    }
}

impl Default for IoContext {
    fn default() -> Self {
        Self::new()
    }
}

//-----------------------------------------------------------------------------

struct SpawnValue<T> {
    t: Option<T>,
    waker: Option<std::task::Waker>,
}

//-----------------------------------------------------------------------------

struct WrapperFuture<T, F: Future<Output = T>> {
    f: F,
    value: Rc<RefCell<SpawnValue<T>>>,
}

impl<T, F: Future<Output = T>> Future for WrapperFuture<T, F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let r = unsafe { Pin::new_unchecked(&mut this.f).poll(cx) };
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(t) => {
                let state = &mut *(*this.value).borrow_mut();
                state.t = Some(t);
                if let Some(ref w) = state.waker {
                    w.wake_by_ref();
                }
                Poll::Ready(())
            }
        }
    }
}

//-----------------------------------------------------------------------------

pub struct SpawnFuture<T> {
    done: bool,
    value: Rc<RefCell<SpawnValue<T>>>,
}

impl<T> Future for SpawnFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done);

        let result = self.value.borrow_mut().t.take();

        match result {
            None => {
                self.value.borrow_mut().waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some(t) => {
                self.done = true;
                Poll::Ready(t)
            }
        }
    }
}

//-----------------------------------------------------------------------------

#[derive(Clone)]
pub struct Executor {
    p: Rc<RefCell<IoContextFrame>>,
}

impl Executor {
    fn ring(&self) -> *mut io_uring {
        unsafe { addr_of_mut!((*self.p.as_ptr()).ioring) }
    }

    // fn get_available_fd(&self) -> Option<u32> {
    //     self.p.borrow_mut().available_fds.pop_front()
    // }

    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T> {
        let value = Rc::new(RefCell::new(SpawnValue {
            t: None,
            waker: None,
        }));

        let sender = self.p.borrow().sender.clone();

        let wrapped = WrapperFuture {
            f,
            value: value.clone(),
        };

        let task = Task::new(wrapped);

        sender.send(Task::downgrade(&task)).unwrap();
        (*self.p).borrow_mut().tasks.insert(task);

        SpawnFuture { value, done: false }
    }
}

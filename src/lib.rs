// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![warn(clippy::pedantic)]
#![allow(
    clippy::mutable_key_type,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::cast_ptr_alignment,
    clippy::too_many_lines,
    clippy::similar_names
)]
#![feature(ptr_metadata)]
#![feature(box_as_ptr)]

extern crate nix;

use std::alloc::Layout;
use std::cell::RefCell;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::ptr::metadata;
use std::ptr::DynMetadata;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::Ordering::Release;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

use nix::libc::c_void;

use nix::sys::eventfd::EventFd;
use nix::sys::socket::SockaddrStorage;
use nix::sys::time::TimeSpec;
use uring::io_uring;
use uring::io_uring_cq_advance;
use uring::io_uring_cq_ready;
use uring::io_uring_cqe;
use uring::io_uring_cqe_seen;
use uring::io_uring_get_events;
use uring::io_uring_get_sqe;
use uring::io_uring_params;
use uring::io_uring_peek_batch_cqe;
use uring::io_uring_peek_cqe;
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

pub mod tcp;
pub mod time;
mod uring;

pub type Result<T> = std::result::Result<T, nix::Error>;

//-----------------------------------------------------------------------------

#[repr(C, align(128))]
struct AlignedAtomicU64(AtomicU64);

impl Deref for AlignedAtomicU64 {
    type Target = AtomicU64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct TaskInnerHeader {
    strong: AlignedAtomicU64,
    weak: AlignedAtomicU64,
    future_vtable: DynMetadata<dyn Future<Output = ()> + 'static>,
    sender: Option<Sender<Weak>>,
    event_fd: i32,
}

//-----------------------------------------------------------------------------

struct Task {
    p: NonNull<u8>,
    phantom: PhantomData<dyn Future<Output = ()> + 'static>,
}

fn align_up(n: usize, align: usize) -> usize {
    (n + (align - 1)) & !(align - 1)
}

impl Task {
    #[must_use]
    pub fn new<F: Future<Output = ()> + 'static>(
        f: F,
        sender: Sender<Weak>,
        event_fd: i32,
    ) -> Task {
        let layout = Layout::new::<TaskInnerHeader>()
            .extend(Layout::for_value(&f))
            .unwrap()
            .0
            .pad_to_align();

        let p = unsafe { std::alloc::alloc(layout) };
        let p = NonNull::new(p).unwrap();

        let header = TaskInnerHeader {
            strong: AlignedAtomicU64(AtomicU64::new(1)),
            weak: AlignedAtomicU64(AtomicU64::new(1)),
            future_vtable: metadata(std::ptr::from_ref(&f as &dyn Future<Output = ()>)),
            sender: Some(sender),
            event_fd,
        };

        unsafe { std::ptr::write(p.as_ptr().cast::<TaskInnerHeader>(), header) };

        let offset = align_up(size_of::<TaskInnerHeader>(), layout.align());
        unsafe { std::ptr::write(p.as_ptr().add(offset).cast::<F>(), f) };

        Task {
            p,
            phantom: PhantomData,
        }
    }

    fn inner(&self) -> &TaskInnerHeader {
        unsafe { &*self.p.as_ptr().cast::<TaskInnerHeader>() }
    }

    fn as_ptr(&self) -> *mut (dyn Future<Output = ()> + 'static) {
        let align = std::cmp::max(
            align_of::<TaskInnerHeader>(),
            self.inner().future_vtable.align_of(),
        );

        let offset = align_up(size_of::<TaskInnerHeader>(), align);

        unsafe {
            std::ptr::from_raw_parts_mut::<dyn Future<Output = ()> + 'static>(
                self.p.as_ptr().add(offset),
                self.inner().future_vtable,
            )
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<()> {
        let future = unsafe { &mut *self.as_ptr() };
        let future = unsafe { Pin::new_unchecked(future) };

        future.poll(cx)
    }

    #[must_use]
    fn downgrade(this: &Task) -> Weak {
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

        // delay the Acquire semantics until we know we need to drop() the Future
        self.inner().strong.load(Acquire);

        unsafe { std::ptr::drop_in_place(self.as_ptr()) };
        unsafe { &mut *self.p.as_ptr().cast::<TaskInnerHeader>() }.sender = None;

        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }

        self.inner().weak.load(Acquire);

        let align = std::cmp::max(
            align_of::<TaskInnerHeader>(),
            self.inner().future_vtable.align_of(),
        );

        let offset = align_up(size_of::<TaskInnerHeader>(), align);

        let layout = Layout::from_size_align(
            align_up(offset + self.inner().future_vtable.size_of(), align),
            align,
        )
        .unwrap();

        unsafe {
            std::alloc::dealloc(self.p.as_ptr(), layout);
        };
    }
}

impl Clone for Task {
    fn clone(&self) -> Task {
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

struct Weak {
    p: NonNull<u8>,
    phantom: PhantomData<dyn Future<Output = ()> + 'static>,
}

impl Weak {
    fn into_raw(self) -> *const () {
        let p = self.p.as_ptr().cast();
        std::mem::forget(self);
        p
    }

    unsafe fn from_raw(p: *const ()) -> Weak {
        Weak {
            p: NonNull::new(p.cast_mut().cast::<u8>()).unwrap(),
            phantom: PhantomData,
        }
    }

    fn inner(&self) -> &TaskInnerHeader {
        unsafe { &*self.p.as_ptr().cast::<TaskInnerHeader>() }
    }

    // cannot be safely upgraded across thread boundaries
    // must only be upgraded on the main thread running the ring
    unsafe fn upgrade(&self) -> Option<Task> {
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

        let align = std::cmp::max(
            align_of::<TaskInnerHeader>(),
            self.inner().future_vtable.align_of(),
        );

        let offset = align_up(size_of::<TaskInnerHeader>(), align);

        let layout = Layout::from_size_align(
            align_up(offset + self.inner().future_vtable.size_of(), align),
            align,
        )
        .unwrap();

        unsafe {
            std::alloc::dealloc(self.p.as_ptr(), layout);
        };
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

unsafe fn task_waker_clone(p: *const ()) -> RawWaker {
    let weak = unsafe { Weak::from_raw(p) };
    std::mem::forget(weak.clone());
    std::mem::forget(weak);
    RawWaker::new(p, &TASK_WAKER_VTABLE)
}

unsafe fn task_wake(p: *const ()) {
    let weak = unsafe { Weak::from_raw(p) };
    let task = Weak::upgrade(&weak);
    match task {
        None => {}
        Some(task) => {
            weak.inner()
                .sender
                .as_ref()
                .unwrap()
                .send(Task::downgrade(&task))
                .unwrap();

            let buf = &0x01_u64.to_ne_bytes();
            unsafe {
                nix::libc::write(
                    weak.inner().event_fd,
                    buf.as_ptr().cast::<c_void>(),
                    buf.len(),
                );
            }
        }
    }
}

unsafe fn task_wake_by_ref(p: *const ()) {
    let weak = unsafe { Weak::from_raw(p) };
    let task = Weak::upgrade(&weak);
    match task {
        None => {}
        Some(task) => {
            weak.inner()
                .sender
                .as_ref()
                .unwrap()
                .send(Task::downgrade(&task))
                .unwrap();

            let buf = &0x01_u64.to_ne_bytes();
            unsafe {
                nix::libc::write(
                    weak.inner().event_fd,
                    buf.as_ptr().cast::<c_void>(),
                    buf.len(),
                );
            }
        }
    }
    std::mem::forget(weak);
}

unsafe fn task_drop(p: *const ()) {
    let weak = unsafe { Weak::from_raw(p) };
    drop(weak);
}

static TASK_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(task_waker_clone, task_wake, task_wake_by_ref, task_drop);

fn make_waker(weak: Weak) -> Waker {
    let raw_waker = RawWaker::new(weak.into_raw(), &TASK_WAKER_VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
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
    local_task_queue: VecDeque<Weak>,
    event_fd: EventFd,
}

//-----------------------------------------------------------------------------

struct RefCount {
    count: usize,
    release_impl: unsafe fn(p: *mut u8),
    obj: *mut u8,
}

unsafe fn add_ref(rc: *mut RefCount) {
    (*rc).count += 1;
}

unsafe fn release_impl<T>(p: *mut u8) {
    let layout = std::alloc::Layout::new::<T>();
    std::ptr::drop_in_place(p.cast::<T>());
    std::alloc::dealloc(p, layout);
}

unsafe fn release(rc: *mut RefCount) {
    (*rc).count -= 1;
    if (*rc).count == 0 {
        let fp = (*rc).release_impl;
        let p = (*rc).obj;
        (fp)(p);
    }
}

//-----------------------------------------------------------------------------

enum OpType {
    Timeout {
        dur: TimeSpec,
    },
    TcpAccept,
    TcpConnect {
        addr: SockaddrStorage,
        port: u16,
        ts: TimeSpec,
        needs_socket: bool,
        got_socket: bool,
        fd: i32,
    },
    #[allow(dead_code)]
    TimeoutCancel,
}

struct IoUringOp {
    ref_count: *mut RefCount,
    initiated: bool,
    done: bool,
    eager_dropped: bool,
    res: i32,
    weak: Option<Weak>,
    op_type: OpType,
}

//-----------------------------------------------------------------------------

pub struct IoContext {
    p: Rc<RefCell<IoContextFrame>>,
}

//-----------------------------------------------------------------------------

pub struct IoContextParams {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub nr_files: u32,
}

impl IoContextParams {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sq_entries: 256,
            cq_entries: 1024,
            nr_files: 1024,
        }
    }
}

impl Default for IoContextParams {
    fn default() -> Self {
        Self::new()
    }
}

//-----------------------------------------------------------------------------

unsafe fn submit_ring(ring: *mut io_uring) {
    let _r = io_uring_submit_and_get_events(ring);
}

unsafe fn reserve_sqes(ring: *mut io_uring, n: u32) {
    let r = io_uring_sq_space_left(ring);
    if r < n {
        submit_ring(ring);
    }
}

struct RunGuard {
    p: Rc<RefCell<IoContextFrame>>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let ring;
        {
            let frame = &mut *self.p.borrow_mut();
            ring = &raw mut frame.ioring;
            unsafe { submit_ring(ring) };
            frame.tasks.clear();
            frame.local_task_queue.clear();
        }

        unsafe { io_uring_get_events(ring) };
        let mut cqe = std::ptr::null_mut::<io_uring_cqe>();
        while unsafe { io_uring_peek_cqe(ring, &raw mut cqe) } == 0 {
            let cqe = unsafe { &mut *cqe };
            if cqe.user_data != 0 && cqe.user_data != 1 {
                let op = unsafe { &mut *(cqe.user_data as *mut IoUringOp) };
                unsafe { release(op.ref_count) };
                if op.eager_dropped {
                    drop(unsafe { Box::from_raw(op) });
                }
            }

            unsafe { io_uring_cqe_seen(ring, cqe) };
        }
    }
}

impl IoContext {
    #[must_use]
    pub fn new() -> Self {
        let sq_entries = 256;
        let cq_entries = 4 * 1024;
        let nr_files = 1024; // matches the default for Linux processes

        let params = IoContextParams {
            sq_entries,
            cq_entries,
            nr_files,
        };

        Self::with_params(&params)
    }

    #[must_use]
    pub fn with_params(params: &IoContextParams) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();

        let IoContextParams {
            sq_entries,
            cq_entries,
            nr_files,
        } = *params;

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
            local_task_queue: VecDeque::new(),
            event_fd: nix::sys::eventfd::EventFd::new().unwrap(),
        }));

        {
            let ring = &raw mut p.borrow_mut().ioring;
            let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, &raw mut params) };
            assert_eq!(ret, 0);

            let ret = unsafe { io_uring_register_ring_fd(ring) };
            assert_eq!(ret, 1);

            let ret = unsafe { io_uring_register_files_sparse(ring, nr_files) };
            assert_eq!(ret, 0);
        }

        {
            let available_fds = &mut p.borrow_mut().available_fds;
            available_fds.reserve(nr_files as usize);
            for i in 0..nr_files {
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
        unsafe { &raw mut (*self.p.as_ptr()).ioring }
    }

    pub fn run(&mut self) -> u64 {
        let _guard = RunGuard { p: self.p.clone() };

        let mut num_completed = 0;

        let event_fd = self.p.borrow().event_fd.as_raw_fd();
        let mut event_count = 0_u64;

        let ex = self.get_executor();
        let ring = ex.ring();

        let cq_entries = ex.p.borrow().params.cq_entries;
        let mut cqes = Vec::<*mut io_uring_cqe>::with_capacity(cq_entries as usize);

        let mut need_eventfd_read = true;

        // let sender = self.p.borrow().sender.clone();

        loop {
            if self.p.borrow().tasks.is_empty() {
                break;
            }

            loop {
                let m_item = self.p.borrow_mut().local_task_queue.pop_front();
                if let Some(ref w) = m_item {
                    match unsafe { w.upgrade() } {
                        None => continue,
                        Some(mut task) => {
                            (*self.p).borrow_mut().root_task = Some(w.clone());

                            let w = make_waker(w.clone());
                            let mut cx = std::task::Context::from_waker(&w);

                            if let Poll::Ready(()) = task.poll(&mut cx) {
                                (*self.p).borrow_mut().tasks.remove(&task);
                                num_completed += 1;
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            loop {
                let m_item = self.p.borrow().receiver.try_recv();
                if let Ok(ref w) = m_item {
                    match unsafe { w.upgrade() } {
                        None => continue,
                        Some(mut task) => {
                            (*self.p).borrow_mut().root_task = Some(w.clone());

                            let w = make_waker(w.clone());
                            let mut cx = std::task::Context::from_waker(&w);

                            if let Poll::Ready(()) = task.poll(&mut cx) {
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

            if need_eventfd_read {
                unsafe { reserve_sqes(ring, 1) };

                let eventfd_sqe = unsafe { io_uring_get_sqe(ring) };
                let sqe = unsafe { &mut *eventfd_sqe };
                unsafe {
                    let p = std::ptr::from_mut(&mut event_count).cast::<c_void>();
                    io_uring_prep_read(sqe, event_fd, p, 8, 0);
                }
                sqe.user_data = 0x01; // sentinel value
                need_eventfd_read = false;
            }

            let ret = unsafe { io_uring_submit_and_wait(ring, 1) };
            assert!(ret >= 0);

            let num_ready = unsafe { io_uring_cq_ready(ring) };
            let num_cqes = unsafe { io_uring_peek_batch_cqe(ring, cqes.as_mut_ptr(), num_ready) };
            unsafe { cqes.set_len(num_cqes as usize) };

            for cqe in &mut cqes {
                let cqe = unsafe { &mut **cqe };
                if cqe.user_data == 0 {
                    continue;
                }

                if cqe.user_data == 1 {
                    need_eventfd_read = true;
                    continue;
                }

                let op = unsafe { &mut *(cqe.user_data as *mut IoUringOp) };
                match op.op_type {
                    OpType::Timeout { .. } => {
                        unsafe { release(op.ref_count) };

                        if op.eager_dropped {
                            drop(unsafe { Box::from_raw(op) });
                            continue;
                        }

                        op.done = true;
                        op.res = cqe.res;
                        if let Some(weak) = op.weak.take() {
                            self.p.borrow_mut().local_task_queue.push_back(weak);
                        }
                    }
                    OpType::TimeoutCancel => {
                        todo!()
                    }
                    OpType::TcpAccept => {
                        unsafe { release(op.ref_count) };

                        let res = cqe.res;

                        if op.eager_dropped {
                            drop(unsafe { Box::from_raw(op) });
                            if res >= 0 {
                                todo!("must close the tcp stream here");
                            }
                            continue;
                        }

                        op.done = true;
                        op.res = res;
                        if let Some(weak) = op.weak.take() {
                            self.p.borrow_mut().local_task_queue.push_back(weak);
                        }
                    }
                    OpType::TcpConnect {
                        ref mut needs_socket,
                        ref mut got_socket,
                        ..
                    } => {
                        if op.eager_dropped {
                            // if the Future was eager-dropped:
                            // * we need to release the borrowed direct descriptor back to the pool
                            // * if the connect() succeeded, we need to close() it

                            unsafe { release(op.ref_count) };
                            drop(unsafe { Box::from_raw(op) });
                            // continue;
                            todo!();
                        }

                        if cqe.res < 0 || *got_socket {
                            unsafe { release(op.ref_count) };

                            op.res = cqe.res;
                            op.done = true;
                            if let Some(weak) = op.weak.take() {
                                self.p.borrow_mut().local_task_queue.push_back(weak);
                            }
                            continue;
                        }

                        if *needs_socket {
                            *got_socket = true;
                        }
                    }
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
    waker: Option<Waker>,
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
        unsafe { &raw mut (*self.p.as_ptr()).ioring }
    }

    fn get_available_fd(&self) -> Option<u32> {
        self.p.borrow_mut().available_fds.pop_front()
    }

    fn reclaim_fd(&self, fd: u32) {
        self.p.borrow_mut().available_fds.push_back(fd);
    }

    fn get_root_task(&self) -> Weak {
        self.p.borrow().root_task.clone().unwrap()
    }

    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T> {
        let value = Rc::new(RefCell::new(SpawnValue {
            t: None,
            waker: None,
        }));

        let sender = self.p.borrow().sender.clone();
        let event_fd = self.p.borrow().event_fd.as_raw_fd();

        let wrapped = WrapperFuture {
            f,
            value: value.clone(),
        };

        let task = Task::new(wrapped, sender.clone(), event_fd);

        sender.send(Task::downgrade(&task)).unwrap();
        (*self.p).borrow_mut().tasks.insert(task);

        SpawnFuture { value, done: false }
    }
}

//-----------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::mem::zeroed;

    use nix::libc::{ECANCELED, ETIME};

    use crate::uring::{
        __kernel_timespec, io_uring, io_uring_cqe, io_uring_cqe_seen, io_uring_get_sqe,
        io_uring_prep_timeout, io_uring_prep_timeout_remove, io_uring_queue_exit,
        io_uring_queue_init, io_uring_submit_and_wait, io_uring_wait_cqe,
    };

    #[test]
    fn timeout_inline_submit_cancel() {
        // want to prove that timeouts and their removals are
        // processed inline with ring submission.
        // io_uring processes the SQ from left-to-right and if
        // the operations complete inline, we should see behavior
        // reflecting this
        // this is supposed to replicate a user abusing our Timer future's
        // usage of cancel-on-drop and also proves that a user can generate
        // any number of CQEs associated with our user_data, and we must
        // ignore these CQEs as well

        struct DropGuard {
            ring: *mut io_uring,
        }

        impl Drop for DropGuard {
            fn drop(&mut self) {
                unsafe { io_uring_queue_exit(self.ring) };
            }
        }

        unsafe {
            let mut ioring = zeroed::<io_uring>();
            let ring = &raw mut ioring;

            let ret = io_uring_queue_init(64, ring, 0);
            assert_eq!(ret, 0);

            let _guard = DropGuard { ring };

            let mut ts = __kernel_timespec {
                tv_sec: 1,
                tv_nsec: 0,
            };

            {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_timeout(sqe, &raw mut ts, 0, 0);
                (*sqe).user_data = 1;
            }

            {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_timeout_remove(sqe, 1, 0);
                (*sqe).user_data = 2;
            }

            {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_timeout(sqe, &raw mut ts, 0, 0);
                (*sqe).user_data = 1;
            }

            {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_timeout_remove(sqe, 1, 0);
                (*sqe).user_data = 3;
            }

            {
                let sqe = io_uring_get_sqe(ring);
                io_uring_prep_timeout(sqe, &raw mut ts, 0, 0);
                (*sqe).user_data = 1;
            }

            io_uring_submit_and_wait(ring, 1);

            let mut cqe = std::ptr::null_mut::<io_uring_cqe>();

            {
                io_uring_wait_cqe(ring, &mut cqe);
                assert_eq!((*cqe).user_data, 2);
                assert_eq!((*cqe).res, 0);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &mut cqe);
                assert_eq!((*cqe).user_data, 3);
                assert_eq!((*cqe).res, 0);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ECANCELED);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ECANCELED);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ETIME);
                io_uring_cqe_seen(ring, cqe);
            }
        }
    }
}

// Copyright 2024-2025 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![feature(ptr_metadata, box_as_ptr, vec_into_raw_parts, local_waker)]
#![warn(clippy::pedantic)]
#![allow(clippy::mutable_key_type,
         clippy::missing_panics_doc,
         clippy::missing_errors_doc,
         clippy::cast_ptr_alignment,
         clippy::too_many_lines,
         clippy::similar_names,
         clippy::cast_possible_wrap)]

extern crate liburing_rs;
extern crate nix;

use std::{
    alloc::Layout,
    cell::UnsafeCell,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    hash::Hash,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::Deref,
    os::fd::AsRawFd,
    pin::Pin,
    ptr::{self, DynMetadata, NonNull, metadata, null_mut},
    rc::Rc,
    sync::{
        atomic::{
            AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
        },
        mpsc::{Receiver, Sender},
    },
    task::{Context, ContextBuilder, LocalWaker, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

use nix::{
    errno::Errno,
    libc::{ETIME, c_void},
    sys::{eventfd::EventFd, socket::SockaddrStorage, time::TimeSpec},
};

use liburing_rs::{
    IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED, IORING_CQE_BUFFER_SHIFT,
    IORING_CQE_F_MORE, IORING_CQE_F_NOTIF, IORING_SETUP_CQSIZE, IORING_SETUP_DEFER_TASKRUN,
    IORING_SETUP_SINGLE_ISSUER, IORING_TIMEOUT_MULTISHOT, IOSQE_CQE_SKIP_SUCCESS, io_uring,
    io_uring_buf_ring, io_uring_buf_ring_add, io_uring_buf_ring_advance, io_uring_buf_ring_mask,
    io_uring_cq_advance, io_uring_cqe, io_uring_cqe_seen, io_uring_for_each_cqe,
    io_uring_free_buf_ring, io_uring_get_events, io_uring_get_sqe, io_uring_params,
    io_uring_peek_cqe, io_uring_prep_cancel_fd, io_uring_prep_close_direct, io_uring_prep_read,
    io_uring_prep_timeout, io_uring_queue_exit, io_uring_queue_init_params,
    io_uring_register_files_sparse, io_uring_register_ring_fd, io_uring_setup_buf_ring,
    io_uring_sq_space_left, io_uring_sqe, io_uring_sqe_set_data, io_uring_sqe_set_data64,
    io_uring_sqe_set_flags, io_uring_submit_and_get_events, io_uring_submit_and_wait,
};

pub mod tcp;
pub mod time;

use tcp::StreamImpl;

pub type Result<T> = std::result::Result<T, nix::Error>;

//-----------------------------------------------------------------------------

#[repr(C, align(64))]
struct AlignedAtomicU64(AtomicU64);

impl Deref for AlignedAtomicU64
{
    type Target = AtomicU64;
    fn deref(&self) -> &Self::Target
    {
        &self.0
    }
}

//-----------------------------------------------------------------------------

struct TaskInnerHeader
{
    strong: AlignedAtomicU64,
    weak: AlignedAtomicU64,
    future_vtable: DynMetadata<dyn Future<Output = ()> + 'static>,
    sender: Option<Sender<Weak>>,
    event_fd: i32,
    ex: *const UnsafeCell<IoContextFrame>,
}

//-----------------------------------------------------------------------------

struct Task
{
    p: NonNull<u8>,
    phantom: PhantomData<dyn Future<Output = ()> + 'static>,
}

fn align_up(n: usize, align: usize) -> usize
{
    (n + (align - 1)) & !(align - 1)
}

impl Task
{
    #[must_use]
    pub fn new<F: Future<Output = ()> + 'static>(f: F, sender: Sender<Weak>, event_fd: i32,
                                                 ex: Executor)
                                                 -> Task
    {
        let layout = Layout::new::<TaskInnerHeader>().extend(Layout::for_value(&f))
                                                     .unwrap()
                                                     .0
                                                     .pad_to_align();

        let p = unsafe { std::alloc::alloc(layout) };
        let p = NonNull::new(p).unwrap();

        let header = TaskInnerHeader { strong: AlignedAtomicU64(AtomicU64::new(1)),
                                       weak: AlignedAtomicU64(AtomicU64::new(1)),
                                       future_vtable:
                                           metadata(std::ptr::from_ref(&f
                                                                       as &dyn Future<Output = ()>)),
                                       sender: Some(sender),
                                       event_fd,
                                       ex: Rc::into_raw(ex.p) };

        unsafe { std::ptr::write(p.as_ptr().cast::<TaskInnerHeader>(), header) };

        let offset = align_up(size_of::<TaskInnerHeader>(), layout.align());
        unsafe { std::ptr::write(p.as_ptr().add(offset).cast::<F>(), f) };

        Task { p,
               phantom: PhantomData }
    }

    fn inner(&self) -> &TaskInnerHeader
    {
        unsafe { &*self.p.as_ptr().cast::<TaskInnerHeader>() }
    }

    fn as_ptr(&self) -> *mut (dyn Future<Output = ()> + 'static)
    {
        let align =
            std::cmp::max(align_of::<TaskInnerHeader>(), self.inner().future_vtable.align_of());

        let offset = align_up(size_of::<TaskInnerHeader>(), align);

        unsafe {
            std::ptr::from_raw_parts_mut::<dyn Future<Output = ()> + 'static>(self.p
                                                                                  .as_ptr()
                                                                                  .add(offset),
                                                                              self.inner()
                                                                                  .future_vtable)
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<()>
    {
        let future = unsafe { &mut *self.as_ptr() };
        let future = unsafe { Pin::new_unchecked(future) };

        future.poll(cx)
    }

    #[must_use]
    fn downgrade(this: &Task) -> Weak
    {
        this.inner().weak.fetch_add(1, Relaxed);

        Weak { p: this.p,
               phantom: PhantomData }
    }
}

impl Drop for Task
{
    fn drop(&mut self)
    {
        if self.inner().strong.fetch_sub(1, Release) > 1 {
            return;
        }
        self.inner().strong.load(Acquire);

        unsafe { std::ptr::drop_in_place(self.as_ptr()) };
        drop(unsafe { Rc::from_raw(self.inner().ex) });

        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }
        self.inner().weak.load(Acquire);

        unsafe {
            std::ptr::drop_in_place(self.p.as_ptr().cast::<TaskInnerHeader>());
        };

        let layout = {
            let align =
                std::cmp::max(align_of::<TaskInnerHeader>(), self.inner().future_vtable.align_of());

            let offset = align_up(size_of::<TaskInnerHeader>(), align);

            Layout::from_size_align(align_up(offset + self.inner().future_vtable.size_of(), align),
                                    align).unwrap()
        };

        unsafe {
            std::alloc::dealloc(self.p.as_ptr(), layout);
        };
    }
}

impl Clone for Task
{
    fn clone(&self) -> Task
    {
        self.inner().strong.fetch_add(1, Relaxed);
        Self { p: self.p,
               phantom: PhantomData }
    }
}

impl PartialEq for Task
{
    fn eq(&self, other: &Self) -> bool
    {
        std::ptr::addr_eq(self.p.as_ptr(), other.p.as_ptr())
    }
}

impl Eq for Task {}

impl Hash for Task
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H)
    {
        self.p.hash(state);
    }
}

//-----------------------------------------------------------------------------

struct Weak
{
    p: NonNull<u8>,
    phantom: PhantomData<dyn Future<Output = ()> + 'static>,
}

impl Weak
{
    fn into_raw(self) -> *const ()
    {
        let p = self.p.as_ptr().cast();
        std::mem::forget(self);
        p
    }

    unsafe fn from_raw(p: *const ()) -> Weak
    {
        Weak { p: NonNull::new(p.cast_mut().cast::<u8>()).unwrap(),
               phantom: PhantomData }
    }

    fn inner(&self) -> &TaskInnerHeader
    {
        unsafe { &*self.p.as_ptr().cast::<TaskInnerHeader>() }
    }

    // cannot be safely upgraded across thread boundaries
    // must only be upgraded on the main thread running the ring
    unsafe fn upgrade(&self) -> Option<Task>
    {
        let mut c = self.inner().strong.load(Relaxed);

        loop {
            if c == 0 {
                return None;
            }

            let r = self.inner()
                        .strong
                        .compare_exchange_weak(c, c + 1, Relaxed, Relaxed);

            match r {
                Ok(_) => {
                    return Some(Task { p: self.p,
                                       phantom: PhantomData });
                }
                Err(c2) => {
                    c = c2;
                }
            }
        }
    }
}

impl Drop for Weak
{
    fn drop(&mut self)
    {
        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }
        self.inner().weak.load(Acquire);

        unsafe {
            std::ptr::drop_in_place(self.p.as_ptr().cast::<TaskInnerHeader>());
        };

        let layout = {
            let align =
                std::cmp::max(align_of::<TaskInnerHeader>(), self.inner().future_vtable.align_of());

            let offset = align_up(size_of::<TaskInnerHeader>(), align);

            Layout::from_size_align(align_up(offset + self.inner().future_vtable.size_of(), align),
                                    align).unwrap()
        };

        unsafe {
            std::alloc::dealloc(self.p.as_ptr(), layout);
        };
    }
}

impl Clone for Weak
{
    fn clone(&self) -> Self
    {
        self.inner().weak.fetch_add(1, Relaxed);

        Self { p: self.p,
               phantom: PhantomData }
    }
}

unsafe impl Sync for Weak {}
unsafe impl Send for Weak {}

//-----------------------------------------------------------------------------

unsafe fn task_waker_clone(p: *const ()) -> RawWaker
{
    let weak = unsafe { Weak::from_raw(p) };
    std::mem::forget(weak.clone());
    std::mem::forget(weak);
    RawWaker::new(p, &TASK_WAKER_VTABLE)
}

unsafe fn task_wake(p: *const ())
{
    let weak = unsafe { Weak::from_raw(p) };
    if let Some(sender) = weak.inner().sender.as_ref() {
        sender.send(weak.clone()).unwrap();
        let buf = &0x01_u64.to_ne_bytes();
        unsafe {
            nix::libc::write(weak.inner().event_fd, buf.as_ptr().cast::<c_void>(), buf.len());
        }
    }
}

unsafe fn task_wake_by_ref(p: *const ())
{
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(p) });
    if let Some(sender) = weak.inner().sender.as_ref() {
        sender.send(ManuallyDrop::into_inner(weak.clone())).unwrap();
        let buf = &0x01_u64.to_ne_bytes();
        unsafe {
            nix::libc::write(weak.inner().event_fd, buf.as_ptr().cast::<c_void>(), buf.len());
        }
    }
}

unsafe fn task_drop(p: *const ())
{
    let weak = unsafe { Weak::from_raw(p) };
    drop(weak);
}

static TASK_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(task_waker_clone, task_wake, task_wake_by_ref, task_drop);

fn make_waker(weak: Weak) -> Waker
{
    let raw_waker = RawWaker::new(weak.into_raw(), &TASK_WAKER_VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}

//-----------------------------------------------------------------------------

unsafe fn task_local_waker_clone(p: *const ()) -> RawWaker
{
    let weak = unsafe { Weak::from_raw(p) };
    std::mem::forget(weak.clone());
    std::mem::forget(weak);
    RawWaker::new(p, &TASK_LOCAL_WAKER_VTABLE)
}

unsafe fn task_local_wake(p: *const ())
{
    let weak = unsafe { Weak::from_raw(p) };
    if let Some(_task) = unsafe { weak.upgrade() } {
        let ex = unsafe { &*weak.inner().ex };
        unsafe { (*ex.get()).local_task_queue.push_back(weak) };
    }
}

unsafe fn task_local_wake_by_ref(p: *const ())
{
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(p) });
    if let Some(_task) = unsafe { weak.upgrade() } {
        let ex = unsafe { &*weak.inner().ex };
        unsafe { (*ex.get()).local_task_queue.push_back((*weak).clone()) };
    }
}

unsafe fn task_local_drop(p: *const ())
{
    let weak = unsafe { Weak::from_raw(p) };
    drop(weak);
}

static TASK_LOCAL_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(task_local_waker_clone,
                                                                     task_local_wake,
                                                                     task_local_wake_by_ref,
                                                                     task_local_drop);

fn make_local_waker(weak: Weak) -> LocalWaker
{
    let raw_waker = RawWaker::new(weak.into_raw(), &TASK_LOCAL_WAKER_VTABLE);
    unsafe { LocalWaker::from_raw(raw_waker) }
}

//-----------------------------------------------------------------------------

struct BufGroup
{
    buf_ring: *mut io_uring_buf_ring,
    num_bufs: u32,
    buf_len: usize,
    bgid: u16,
    bufs: Vec<*mut u8>,
    ring: *mut io_uring,
    tail: u32,
}

impl BufGroup
{
    unsafe fn release(&mut self)
    {
        unsafe {
            io_uring_free_buf_ring(self.ring, self.buf_ring, self.num_bufs, self.bgid.into())
        };

        let bufs = &mut self.bufs;

        for ptr in bufs.iter() {
            if ptr.is_null() {
                continue;
            }
            drop(unsafe { Vec::<u8>::from_raw_parts(*ptr, self.buf_len, self.buf_len) });
        }

        bufs.shrink_to_fit();
    }
}

//-----------------------------------------------------------------------------

struct IoContextFrame
{
    ioring: io_uring,
    _params: io_uring_params,
    available_fds: VecDeque<u32>,
    tasks: HashSet<Task>,
    receiver: Receiver<Weak>,
    sender: Sender<Weak>,
    event_fd: EventFd,
    buf_groups: HashMap<u16, *mut BufGroup>,
    runguard_blacklist: HashSet<u64>,
    local_task_queue: VecDeque<Weak>,
}

//-----------------------------------------------------------------------------

#[derive(Debug)]
struct RefCount
{
    obj_count: usize,
    op_count: usize,
    release_impl: unsafe fn(p: *mut u8),
    obj: *mut u8,
}

unsafe fn add_obj_ref(rc: *mut RefCount)
{
    unsafe { (*rc).obj_count += 1 };
    // println!("add_obj_ref: {:?}", *rc);
}

unsafe fn add_op_ref(rc: *mut RefCount)
{
    unsafe { (*rc).op_count += 1 };
    // println!("add_op_ref: {:?}", *rc);
}

unsafe fn release_impl<T>(p: *mut u8)
{
    let layout = std::alloc::Layout::new::<T>();
    unsafe { std::ptr::drop_in_place(p.cast::<T>()) };
    unsafe { std::alloc::dealloc(p, layout) };
}

unsafe fn release_obj(rc: *mut RefCount)
{
    unsafe { (*rc).obj_count -= 1 };
    // println!("release_obj: {:?}", *rc);
    if unsafe { (*rc).obj_count == 0 && (*rc).op_count == 0 } {
        let fp = unsafe { (*rc).release_impl };
        let p = unsafe { (*rc).obj };
        unsafe { (fp)(p) };
    }
}

unsafe fn release_op(rc: *mut RefCount)
{
    unsafe { (*rc).op_count -= 1 };
    // println!("release_op: {:?}", *rc);
    if unsafe { (*rc).obj_count == 0 && (*rc).op_count == 0 } {
        let fp = unsafe { (*rc).release_impl };
        let p = unsafe { (*rc).obj };
        unsafe { (fp)(p) };
    }
}

//-----------------------------------------------------------------------------

enum OpType
{
    Timeout
    {
        dur: TimeSpec
    },
    #[allow(dead_code)]
    TimeoutCancel,
    MultishotTimeout
    {
        ts: TimeSpec,
        stream: *mut StreamImpl,
    },
    TcpAccept
    {
        fd: i32
    },
    TcpConnect
    {
        addr: SockaddrStorage,
        port: u16,
        ts: TimeSpec,
        needs_socket: bool,
        got_socket: bool,
        fd: i32,
    },
    TcpSend
    {
        buf: Vec<u8>,
        last_send: *mut Instant,
    },
    MultishotTcpRecv
    {
        bufs: Vec<Vec<u8>>,
        buf_group: *mut BufGroup,
        last_recv: *mut Instant,
    },
}

struct IoUringOp
{
    ref_count: *mut RefCount,
    initiated: bool,
    done: bool,
    eager_dropped: bool,
    res: i32,
    op_type: OpType,
    local_waker: Option<LocalWaker>,
}

fn make_io_uring_op(ref_count: *mut RefCount, op_type: OpType) -> IoUringOp
{
    IoUringOp { ref_count,
                initiated: false,
                done: false,
                eager_dropped: false,
                res: -1,
                local_waker: None,
                op_type }
}

//-----------------------------------------------------------------------------

pub struct IoContext
{
    p: Rc<UnsafeCell<IoContextFrame>>,
}

//-----------------------------------------------------------------------------

pub struct IoContextParams
{
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub nr_files: u32,
}

impl IoContextParams
{
    #[must_use]
    pub fn new() -> Self
    {
        Self { sq_entries: 256,
               cq_entries: 1024,
               nr_files: 1024 }
    }
}

impl Default for IoContextParams
{
    fn default() -> Self
    {
        Self::new()
    }
}

//-----------------------------------------------------------------------------

unsafe fn submit_ring(ring: *mut io_uring)
{
    let _r = unsafe { io_uring_submit_and_get_events(ring) };
}

fn get_sqe(ex: &Executor) -> *mut io_uring_sqe
{
    let ring = ex.ring();

    let mut sqe = unsafe { io_uring_get_sqe(ring) };
    while sqe.is_null() {
        unsafe { submit_ring(ring) };
        sqe = unsafe { io_uring_get_sqe(ring) };
    }

    sqe
}

unsafe fn reserve_sqes(ring: *mut io_uring, n: u32)
{
    let r = unsafe { io_uring_sq_space_left(ring) };
    if r < n {
        unsafe { submit_ring(ring) };
    }
}

//-----------------------------------------------------------------------------

struct RunGuard
{
    p: Rc<UnsafeCell<IoContextFrame>>,
}

struct CQESeenGuard<'a>
{
    ring: *mut io_uring,
    cqe: &'a mut io_uring_cqe,
}

impl Drop for CQESeenGuard<'_>
{
    fn drop(&mut self)
    {
        unsafe { io_uring_cqe_seen(self.ring, self.cqe) };
    }
}

struct CQEAdvanceGuard
{
    ring: *mut io_uring,
    n: usize,
}

impl Drop for CQEAdvanceGuard
{
    fn drop(&mut self)
    {
        if self.n > 0 {
            unsafe { io_uring_cq_advance(self.ring, self.n.try_into().unwrap()) };
        }
    }
}

struct IncrGuard<'a>
{
    n: &'a mut usize,
}

impl Drop for IncrGuard<'_>
{
    fn drop(&mut self)
    {
        *self.n += 1;
    }
}

impl Drop for RunGuard
{
    fn drop(&mut self)
    {
        let ring = unsafe { &raw mut (*self.p.get()).ioring };
        {
            let tasks = unsafe { &mut (*self.p.get()).tasks };
            tasks.clear();
        }

        unsafe { submit_ring(ring) };
        unsafe { io_uring_get_events(ring) };

        let blacklist = unsafe { &mut (*self.p.get()).runguard_blacklist };

        let mut cqe = std::ptr::null_mut::<io_uring_cqe>();
        while unsafe { io_uring_peek_cqe(ring, &raw mut cqe) } == 0 {
            let cqe = unsafe { &mut *cqe };
            let user_data = cqe.user_data;

            {
                let _g = CQESeenGuard { ring, cqe };

                if user_data != 0 && user_data != 1 {
                    if blacklist.contains(&user_data) {
                        continue;
                    }

                    blacklist.insert(user_data);
                    let op = unsafe { &mut *(user_data as *mut IoUringOp) };
                    unsafe { release_op(op.ref_count) };
                    if op.eager_dropped {
                        drop(unsafe { Box::from_raw(op) });
                    }
                }
            }

            // unsafe { submit_ring(ring) };
            unsafe { io_uring_get_events(ring) };
        }
    }
}

impl IoContext
{
    #[must_use]
    pub fn new() -> Self
    {
        let sq_entries = 256;
        let cq_entries = 4 * 1024;
        let nr_files = 1024; // matches the default for Linux processes

        let params = IoContextParams { sq_entries,
                                       cq_entries,
                                       nr_files };

        Self::with_params(&params)
    }

    #[must_use]
    pub fn with_params(params: &IoContextParams) -> Self
    {
        let (tx, rx) = std::sync::mpsc::channel();

        let IoContextParams { sq_entries,
                              cq_entries,
                              nr_files, } = *params;

        let mut params = unsafe { std::mem::zeroed::<io_uring_params>() };
        params.cq_entries = cq_entries;
        params.flags |= IORING_SETUP_CQSIZE;

        params.flags |= IORING_SETUP_SINGLE_ISSUER;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;

        let ioring = unsafe { std::mem::zeroed::<io_uring>() };

        let p =
            Rc::new(UnsafeCell::new(IoContextFrame { _params: params,
                                                     tasks: HashSet::new(),
                                                     available_fds: VecDeque::new(),
                                                     receiver: rx,
                                                     sender: tx,
                                                     ioring,
                                                     event_fd:
                                                         nix::sys::eventfd::EventFd::new().unwrap(),
                                                     buf_groups: HashMap::new(),
                                                     runguard_blacklist: HashSet::new(),
                                                     local_task_queue: VecDeque::new() }));

        let ring = unsafe { &raw mut (*p.get()).ioring };
        let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, &raw mut params) };
        assert_eq!(ret, 0);

        let ret = unsafe { io_uring_register_ring_fd(ring) };
        assert_eq!(ret, 1);

        let ret = unsafe { io_uring_register_files_sparse(ring, nr_files) };
        assert_eq!(ret, 0);

        let available_fds = unsafe { &mut (*p.get()).available_fds };
        available_fds.reserve(nr_files as usize);
        for i in 0..nr_files {
            available_fds.push_back(i);
        }

        Self { p }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor
    {
        Executor { p: self.p.clone() }
    }

    fn ring(&self) -> *mut io_uring
    {
        unsafe { &raw mut (*self.p.get()).ioring }
    }

    pub fn run(&mut self) -> u64
    {
        fn on_work_item(weak: Weak, ex: &Executor) -> u64
        {
            match unsafe { weak.upgrade() } {
                None => 0,
                Some(mut task) => {
                    let w = make_waker(weak.clone());
                    let lw = make_local_waker(weak);

                    let mut cx = ContextBuilder::from_waker(&w).local_waker(&lw).build();

                    if let Poll::Ready(()) = task.poll(&mut cx) {
                        unsafe { (*ex.p.get()).tasks.remove(&task) };
                        1
                    } else {
                        0
                    }
                }
            }
        }

        let _guard = RunGuard { p: self.p.clone() };

        let mut num_completed = 0;

        let event_fd = unsafe { (*self.p.get()).event_fd.as_raw_fd() };
        let mut event_count = 0_u64;

        let ex = self.get_executor();
        let ring = ex.ring();

        // let cq_entries = ex.p.borrow().params.cq_entries;
        // let mut cqes = Vec::<*mut io_uring_cqe>::with_capacity(cq_entries as usize);

        let mut need_eventfd_read = true;

        loop {
            if unsafe { (*self.p.get()).tasks.is_empty() } {
                break;
            }

            while let Some(weak) = unsafe { (*self.p.get()).local_task_queue.pop_front() } {
                num_completed += on_work_item(weak, &ex);
            }

            while let Ok(weak) = unsafe { (*self.p.get()).receiver.try_recv() } {
                num_completed += on_work_item(weak, &ex);
            }

            if unsafe { (*self.p.get()).tasks.is_empty() } {
                break;
            }

            if need_eventfd_read {
                let eventfd_sqe = get_sqe(&ex);
                let sqe = unsafe { &mut *eventfd_sqe };
                let p = std::ptr::from_mut(&mut event_count).cast::<c_void>();
                unsafe { io_uring_prep_read(sqe, event_fd, p, 8, 0) };
                unsafe { io_uring_sqe_set_data64(sqe, 0x01) };
                need_eventfd_read = false;
            }

            let ret = unsafe { io_uring_submit_and_wait(ring, 1) };
            debug_assert!(ret >= 0);

            let mut guard = CQEAdvanceGuard { ring, n: 0 };

            let on_cqe = |cqe: *mut io_uring_cqe| {
                let _g = IncrGuard { n: &mut guard.n };

                let cqe = unsafe { &mut *cqe };

                if cqe.user_data == 0 {
                    return;
                }

                if cqe.user_data == 1 {
                    need_eventfd_read = true;
                    return;
                }

                let op = unsafe { &mut *(cqe.user_data as *mut IoUringOp) };
                match op.op_type {
                    OpType::Timeout { .. } => on_timeout(op, cqe, ring, &ex),
                    OpType::TimeoutCancel => todo!(),
                    OpType::TcpAccept { .. } => on_tcp_accept(op, cqe, ring, &ex),
                    OpType::TcpConnect { .. } => on_tcp_connect(op, cqe, ring, &ex),
                    OpType::TcpSend { .. } => on_tcp_send(op, cqe, ring, &ex),
                    OpType::MultishotTcpRecv { .. } => {
                        on_multishot_tcp_recv(op, cqe, ring, &ex);
                    }
                    OpType::MultishotTimeout { .. } => {
                        on_multishot_timeout(op, cqe, ring, &ex);
                    }
                }
            };

            unsafe { io_uring_for_each_cqe(ring, on_cqe) };
        }

        num_completed
    }
}

fn on_timeout(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring, ex: &Executor)
{
    let _ = ex;
    unsafe { release_op(op.ref_count) };

    if op.eager_dropped {
        drop(unsafe { Box::from_raw(op) });
        return;
    }

    op.done = true;
    op.res = cqe.res;

    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_tcp_accept(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring, ex: &Executor)
{
    unsafe { release_op(op.ref_count) };

    let res = cqe.res;

    if op.eager_dropped {
        if res < 0 {
            drop(unsafe { Box::from_raw(op) });
        } else {
            let OpType::TcpAccept { fd } = op.op_type else {
                unreachable!()
            };

            let fd = fd.try_into().unwrap();

            let sqe = get_sqe(ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            // unsafe { submit_ring(ring) };

            drop(unsafe { Box::from_raw(op) });
            unsafe { ex.reclaim_fd(fd) };
        }

        return;
    }

    op.done = true;
    op.res = res;
    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_tcp_connect(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring, ex: &Executor)
{
    let _ = ex;

    if op.eager_dropped {
        // if the Future was eager-dropped:
        // * we need to release the borrowed direct descriptor back to the pool
        // * if the connect() succeeded, we need to close() it

        let OpType::TcpConnect { ref mut needs_socket,
                                 ref mut got_socket,
                                 fd,
                                 .. } = op.op_type
        else {
            unreachable!()
        };

        let is_last_cqe = cqe.res < 0 || *got_socket || !*needs_socket;

        if *needs_socket && !*got_socket && cqe.res >= 0 {
            *got_socket = true;
        }

        if !is_last_cqe {
            return;
        }

        // if our connect() op is borrowing an FD from the runtime,
        // we need to close it and return it
        if *needs_socket {
            // this means our socket() call completed with a success
            if *got_socket {
                let sqe = get_sqe(ex);

                unsafe { io_uring_prep_close_direct(sqe, fd.try_into().unwrap()) };
                unsafe { io_uring_sqe_set_data64(sqe, 0) };
                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
            }

            unsafe { ex.reclaim_fd(fd.try_into().unwrap()) };
        }

        unsafe { release_op(op.ref_count) };
        drop(unsafe { Box::from_raw(op) });

        return;
    }

    let OpType::TcpConnect { ref mut needs_socket,
                             ref mut got_socket,
                             .. } = op.op_type
    else {
        unreachable!()
    };

    // if we hit an error, this is our last CQE
    // if we don't need a socket, this is our last CQE
    // if we've already gotten a socket, this is our last CQE
    if cqe.res < 0 || !*needs_socket || *got_socket {
        unsafe { release_op(op.ref_count) };

        op.res = cqe.res;
        op.done = true;
        if let Some(local_waker) = op.local_waker.take() {
            local_waker.wake();
        }
        return;
    }

    // we expect one more CQE after this, the result of the actual
    // connect() call
    if *needs_socket {
        *got_socket = true;
    }
}

fn on_tcp_send(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring, ex: &Executor)
{
    let _ = ex;

    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;

    if op.eager_dropped {
        if !has_more_cqes {
            unsafe { release_op(op.ref_count) };
            drop(unsafe { Box::from_raw(op) });
        }
        return;
    }

    let OpType::TcpSend { ref mut buf,
                          last_send, } = op.op_type
    else {
        unreachable!()
    };

    unsafe { *last_send = Instant::now() };

    if has_more_cqes {
        op.res = cqe.res;
        if cqe.res >= 0 {
            let n: usize = cqe.res.try_into().unwrap();
            buf.drain(0..n);
        }
        return;
    }
    assert!(cqe.flags & IORING_CQE_F_NOTIF > 0);
    op.done = true;

    unsafe { release_op(op.ref_count) };

    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_multishot_tcp_recv(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring,
                         ex: &Executor)
{
    let _ = ex;

    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;
    let OpType::MultishotTcpRecv { ref mut bufs,
                                   buf_group,
                                   last_recv, } = op.op_type
    else {
        unreachable!()
    };

    if op.eager_dropped {
        if cqe.res > 0 {
            let buf_group = unsafe { &mut *buf_group };
            let mask = buf_group.num_bufs - 1;
            let br = buf_group.buf_ring;

            let len = buf_group.buf_len;

            let num_bytes = usize::try_from(cqe.res).unwrap();

            let num_buffers = (num_bytes / len) + usize::from(num_bytes % len > 0);

            for buf_offset in 0..num_buffers {
                let bid = buf_group.tail.try_into().unwrap();

                let buf = Vec::<u8>::with_capacity(buf_group.buf_len);
                let (addr, _, _) = buf.into_raw_parts();
                buf_group.bufs[usize::from(bid)] = addr;

                let len = buf_group.buf_len.try_into().unwrap();
                let addr = addr.cast();
                let buf_offset = buf_offset.try_into().unwrap();

                buf_group.tail = (buf_group.tail + 1) & mask;

                let mask = mask.try_into().unwrap();

                unsafe {
                    io_uring_buf_ring_add(br, addr, len, bid, mask, buf_offset);
                }
            }

            let count = num_buffers.try_into().unwrap();
            unsafe { io_uring_buf_ring_advance(br, count) };
        }

        if has_more_cqes {
            return;
        }

        op.done = true;
        unsafe { release_op(op.ref_count) };

        drop(unsafe { Box::from_raw(op) });
        return;
    }

    if !has_more_cqes {
        op.done = true;
        unsafe { release_op(op.ref_count) };
    }

    unsafe { *last_recv = Instant::now() };

    op.res = cqe.res;
    if op.res > 0 {
        let mut bid = usize::try_from(cqe.flags >> IORING_CQE_BUFFER_SHIFT).unwrap();

        let buf_group = unsafe { &mut *buf_group };

        let buf_len = buf_group.buf_len;
        let num_bufs = buf_group.num_bufs;
        let num_bufs_mask = usize::try_from(num_bufs - 1).unwrap();
        let ring_bufs = &mut buf_group.bufs;

        let mut num_bytes = usize::try_from(cqe.res).unwrap();
        while num_bytes > 0 {
            let to_read = std::cmp::min(num_bytes, buf_len);

            let p = &mut ring_bufs[bid];
            let ptr = *p;
            *p = null_mut();

            let buf = unsafe { Vec::from_raw_parts(ptr, to_read, buf_len) };
            bufs.push(buf);

            bid = (bid + 1) & num_bufs_mask;
            num_bytes -= to_read;
        }
    }

    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_multishot_timeout(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring,
                        ex: &Executor)
{
    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;

    let io_object_dropped = op.eager_dropped;
    if io_object_dropped {
        if has_more_cqes {
            return;
        }

        unsafe { release_op(op.ref_count) };
        drop(unsafe { Box::from_raw(op) });
        return;
    }

    if !has_more_cqes {
        let user_data: *mut IoUringOp = &raw mut *op;
        let user_data = user_data.cast();

        let OpType::MultishotTimeout { ref mut ts, .. } = op.op_type else {
            unreachable!()
        };

        let ts = ptr::from_mut(ts).cast();

        let sqe = get_sqe(ex);
        unsafe { io_uring_prep_timeout(sqe, ts, 0, IORING_TIMEOUT_MULTISHOT) };
        unsafe { io_uring_sqe_set_data(sqe, user_data) };
    }

    if cqe.res != -ETIME {
        return;
    }

    let OpType::MultishotTimeout { ref mut ts, stream } = op.op_type else {
        unreachable!()
    };

    let dur = Duration::new(ts.tv_sec().try_into().unwrap(), ts.tv_nsec().try_into().unwrap());

    let now = Instant::now();
    let stream_impl = unsafe { &mut *stream };

    let recv_expired =
        stream_impl.recv_op.is_some() && now.duration_since(stream_impl.last_recv) > dur;

    let send_expired = stream_impl.send_pending && now.duration_since(stream_impl.last_send) > dur;

    if recv_expired || send_expired {
        let sqe = get_sqe(ex);
        let fd = stream_impl.fd;
        let flags = IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED;
        unsafe { io_uring_prep_cancel_fd(sqe, fd, flags) };
        unsafe { io_uring_sqe_set_data64(sqe, 0) };
        unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
    }
}

impl Drop for IoContext
{
    fn drop(&mut self)
    {
        drop(RunGuard { p: self.p.clone() });

        let buf_groups = unsafe { &mut (*self.p.get()).buf_groups };
        for (_, buf_group) in buf_groups.iter() {
            unsafe { (**buf_group).release() };
            drop(unsafe { Box::from_raw(*buf_group) });
        }
        buf_groups.clear();

        unsafe { io_uring_queue_exit(self.ring()) };
    }
}

impl Default for IoContext
{
    fn default() -> Self
    {
        Self::new()
    }
}

//-----------------------------------------------------------------------------

struct SpawnValue<T>
{
    t: Option<T>,
    waker: Option<Waker>,
}

//-----------------------------------------------------------------------------

struct WrapperFuture<T, F: Future<Output = T>>
{
    f: F,
    value: Rc<UnsafeCell<SpawnValue<T>>>,
}

impl<T, F: Future<Output = T>> Future for WrapperFuture<T, F>
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        let r = unsafe { Pin::new_unchecked(&mut this.f).poll(cx) };
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(t) => {
                let state = unsafe { &mut *(*this.value).get() };
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

pub struct SpawnFuture<T>
{
    done: bool,
    value: Rc<UnsafeCell<SpawnValue<T>>>,
}

impl<T> Future for SpawnFuture<T>
{
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
    {
        assert!(!self.done);

        let result = unsafe { (*self.value.get()).t.take() };

        match result {
            None => {
                unsafe { (*self.value.get()).waker = Some(cx.waker().clone()) };
                // TODO: do we need to assign the LocalWaker here as well?

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
pub struct Executor
{
    p: Rc<UnsafeCell<IoContextFrame>>,
}

impl Executor
{
    fn ring(&self) -> *mut io_uring
    {
        unsafe { &raw mut (*self.p.get()).ioring }
    }

    unsafe fn get_available_fd(&self) -> Option<u32>
    {
        let fds = unsafe { &mut (*self.p.get()).available_fds };
        fds.pop_front()
    }

    unsafe fn reclaim_fd(&self, fd: u32)
    {
        let fds = unsafe { &mut (*self.p.get()).available_fds };
        fds.push_back(fd);
    }

    pub fn register_buf_group(&self, bgid: u16, num_bufs: u32, buf_len: usize) -> Result<()>
    {
        let mut ret = 0_i32;

        let ring = self.ring();
        let nentries = num_bufs;
        let flags = 0;
        let err = &mut ret;

        let buf_ring = unsafe { io_uring_setup_buf_ring(ring, nentries, bgid.into(), flags, err) };
        if buf_ring.is_null() {
            return Err(Errno::from_raw(-ret));
        }

        let mut bufs = Vec::<*mut u8>::with_capacity(num_bufs.try_into().unwrap());

        let mask = io_uring_buf_ring_mask(num_bufs);
        for bid in 0..num_bufs {
            let buf = Vec::<u8>::with_capacity(buf_len);

            let (addr, _, cap) = buf.into_raw_parts();

            bufs.push(addr);

            unsafe {
                io_uring_buf_ring_add(buf_ring,
                                      addr.cast(),
                                      cap.try_into().unwrap(),
                                      bid.try_into().unwrap(),
                                      mask,
                                      bid.try_into().unwrap());
            }
        }

        unsafe { io_uring_buf_ring_advance(buf_ring, num_bufs.try_into().unwrap()) };

        unsafe {
            (*self.p.get()).buf_groups.insert(bgid,
                                              Box::into_raw(Box::new(BufGroup { buf_ring,
                                                                                num_bufs,
                                                                                buf_len,
                                                                                bgid,
                                                                                bufs,
                                                                                ring,
                                                                                tail: 0 })));
        }

        Ok(())
    }

    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T>
    {
        let value = Rc::new(UnsafeCell::new(SpawnValue { t: None,
                                                         waker: None }));

        let sender = unsafe { (*self.p.get()).sender.clone() };
        let event_fd = unsafe { (*self.p.get()).event_fd.as_raw_fd() };

        let wrapped = WrapperFuture { f,
                                      value: value.clone() };

        let task = Task::new(wrapped, sender.clone(), event_fd, self.clone());

        sender.send(Task::downgrade(&task)).unwrap();
        unsafe { (*(*self.p).get()).tasks.insert(task) };

        SpawnFuture { value, done: false }
    }
}

//-----------------------------------------------------------------------------

#[cfg(test)]
mod test
{
    use std::mem::zeroed;

    use nix::libc::{ECANCELED, ETIME};

    use liburing_rs::{
        __kernel_timespec, io_uring, io_uring_cqe, io_uring_cqe_seen, io_uring_get_sqe,
        io_uring_prep_timeout, io_uring_prep_timeout_remove, io_uring_queue_exit,
        io_uring_queue_init, io_uring_submit_and_wait, io_uring_wait_cqe,
    };

    #[test]
    fn timeout_inline_submit_cancel()
    {
        // want to prove that timeouts and their removals are
        // processed inline with ring submission.
        // io_uring processes the SQ from left-to-right and if
        // the operations complete inline, we should see behavior
        // reflecting this
        // this is supposed to replicate a user abusing our Timer future's
        // usage of cancel-on-drop and also proves that a user can generate
        // any number of CQEs associated with our user_data, and we must
        // ignore these CQEs as well

        struct DropGuard
        {
            ring: *mut io_uring,
        }

        impl Drop for DropGuard
        {
            fn drop(&mut self)
            {
                unsafe { io_uring_queue_exit(self.ring) };
            }
        }

        unsafe {
            let mut ioring = zeroed::<io_uring>();
            let ring = &raw mut ioring;

            let ret = io_uring_queue_init(64, ring, 0);
            assert_eq!(ret, 0);

            let _guard = DropGuard { ring };

            let mut ts = __kernel_timespec { tv_sec: 1,
                                             tv_nsec: 0 };

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
                io_uring_wait_cqe(ring, &raw mut cqe);
                assert_eq!((*cqe).user_data, 2);
                assert_eq!((*cqe).res, 0);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &raw mut cqe);
                assert_eq!((*cqe).user_data, 3);
                assert_eq!((*cqe).res, 0);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &raw mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ECANCELED);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &raw mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ECANCELED);
                io_uring_cqe_seen(ring, cqe);
            }

            {
                io_uring_wait_cqe(ring, &raw mut cqe);
                assert_eq!((*cqe).user_data, 1);
                assert_eq!(-(*cqe).res, ETIME);
                io_uring_cqe_seen(ring, cqe);
            }
        }
    }
}

// Copyright 2024-2025 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![feature(ptr_metadata, box_as_ptr, local_waker, sync_unsafe_cell)]
#![warn(clippy::pedantic)]
#![allow(clippy::mutable_key_type,
         clippy::missing_panics_doc,
         clippy::missing_errors_doc,
         clippy::cast_ptr_alignment,
         clippy::too_many_lines,
         clippy::similar_names,
         clippy::cast_possible_wrap,
         clippy::cast_sign_loss,
         clippy::cast_possible_truncation,
         clippy::struct_excessive_bools)]

extern crate liburing_rs;
extern crate nix;

use std::{
    alloc::Layout,
    cell::{RefCell, SyncUnsafeCell, UnsafeCell},
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    mem::{ManuallyDrop, forget},
    ops::Deref,
    ops::Range,
    pin::Pin,
    ptr::{self, DynMetadata, NonNull, metadata},
    rc::Rc,
    slice,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release},
        },
        mpsc::{Receiver, Sender},
    },
    task::{Context, ContextBuilder, LocalWaker, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

use nix::{errno::Errno, libc::ETIME, sys::socket::SockaddrStorage};

use liburing_rs::{
    __kernel_timespec, IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED,
    IORING_CQE_BUFFER_SHIFT, IORING_CQE_F_MORE, IORING_CQE_F_NOTIF, IORING_SETUP_CQSIZE,
    IORING_SETUP_DEFER_TASKRUN, IORING_SETUP_SINGLE_ISSUER, IOSQE_CQE_SKIP_SUCCESS, io_uring,
    io_uring_buf_ring, io_uring_buf_ring_add, io_uring_buf_ring_advance, io_uring_buf_ring_mask,
    io_uring_cq_advance, io_uring_cqe, io_uring_cqe_seen, io_uring_for_each_cqe,
    io_uring_free_buf_ring, io_uring_get_events, io_uring_get_sqe, io_uring_params,
    io_uring_peek_cqe, io_uring_prep_cancel_fd, io_uring_prep_close_direct, io_uring_prep_msg_ring,
    io_uring_queue_exit, io_uring_queue_init_params, io_uring_register_files_sparse,
    io_uring_register_ring_fd, io_uring_register_sync_msg, io_uring_setup_buf_ring,
    io_uring_sq_space_left, io_uring_sqe, io_uring_sqe_set_data64, io_uring_sqe_set_flags,
    io_uring_submit_and_get_events, io_uring_submit_and_wait, io_uring_unregister_buf_ring,
};

pub mod tcp;
pub mod time;

use slotmap::{DefaultKey, Key, KeyData};
use tcp::StreamImpl;

use crate::io_ops::IoOpsMap;

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

#[repr(C, align(64))]
struct AlignedAtomicBool(AtomicBool);

impl Deref for AlignedAtomicBool
{
    type Target = AtomicBool;

    fn deref(&self) -> &Self::Target
    {
        &self.0
    }
}

//-----------------------------------------------------------------------------

struct DanglingExecutor(*const IoContextFrame);
unsafe impl Send for DanglingExecutor {}
unsafe impl Sync for DanglingExecutor {}

struct DanglingTask(SyncUnsafeCell<*mut TaskHeader>);

unsafe impl Send for DanglingTask {}
unsafe impl Sync for DanglingTask {}

impl DanglingTask
{
    fn new(task_header: &TaskHeader) -> Self
    {
        Self(SyncUnsafeCell::new(ptr::from_ref(task_header).cast_mut()))
    }

    unsafe fn unsafe_clone(&self) -> DanglingTask
    {
        unsafe { DanglingTask(SyncUnsafeCell::new(*self.0.get())) }
    }

    unsafe fn task_header<'a>(&self) -> &'a TaskHeader
    {
        unsafe { &**self.0.get() }
    }

    unsafe fn is_null(&self) -> bool
    {
        unsafe { (*self.0.get()).is_null() }
    }

    unsafe fn get_inner(&self) -> *mut TaskHeader
    {
        unsafe { *self.0.get() }
    }

    unsafe fn set_inner(&self, t: &DanglingTask)
    {
        unsafe { *self.0.get() = *t.0.get() };
    }

    unsafe fn set_next(&self, t: &DanglingTask)
    {
        unsafe { self.task_header().next.set_inner(t) };
    }

    unsafe fn set_prev(&self, t: &DanglingTask)
    {
        unsafe { self.task_header().prev.set_inner(t) };
    }

    unsafe fn get_next(&self) -> DanglingTask
    {
        unsafe { self.task_header().next.unsafe_clone() }
    }

    unsafe fn _get_prev(&self) -> DanglingTask
    {
        unsafe { self.task_header().prev.unsafe_clone() }
    }
}

//-----------------------------------------------------------------------------

struct TaskHeader
{
    strong: SyncUnsafeCell<u64>,
    weak: AlignedAtomicU64,
    task_layout: Layout,
    value_offset: usize,
    future_offset: usize,
    value_vtable: DynMetadata<dyn Erased>,
    future_vtable: DynMetadata<dyn Future<Output = ()> + 'static>,
    sender: Option<Sender<Weak>>,
    ring_fd: i32,
    ex: DanglingExecutor,
    needs_wake: Arc<AlignedAtomicBool>,
    done: SyncUnsafeCell<bool>,
    next: DanglingTask,
    prev: DanglingTask,
}

//-----------------------------------------------------------------------------

struct Task
{
    p: NonNull<u8>,
    phantom: PhantomData<dyn Future<Output = ()> + 'static>,
}

trait Erased {}
impl<T> Erased for T {}

impl Task
{
    fn task_header(&self) -> &TaskHeader
    {
        unsafe { &*self.p.as_ptr().cast::<TaskHeader>() }
    }

    // unsafe because of potential aliasing violations as Task implements Clone with
    // Rc-like semantics
    unsafe fn poll(&mut self, cx: &mut Context) -> Poll<()>
    {
        let offset = self.task_header().future_offset;
        let p = unsafe {
            std::ptr::from_raw_parts_mut::<dyn Future<Output = ()> + 'static>(self.p
                                                                                  .as_ptr()
                                                                                  .add(offset),
                                                                              self.task_header()
                                                                                  .future_vtable)
        };

        let future = unsafe { Pin::new_unchecked(&mut *p) };
        future.poll(cx)
    }

    #[must_use]
    fn downgrade(this: &Task) -> Weak
    {
        this.task_header().weak.fetch_add(1, Relaxed);
        Weak { p: this.p,
               phantom: PhantomData }
    }

    fn into_raw(this: Self) -> *mut TaskHeader
    {
        let p = this.p.as_ptr();
        forget(this);
        p.cast::<TaskHeader>()
    }

    unsafe fn from_raw(p: *mut TaskHeader) -> Task
    {
        Task { p: NonNull::new(p.cast()).unwrap(),
               phantom: PhantomData }
    }
}

impl Drop for Task
{
    fn drop(&mut self)
    {
        unsafe { (*self.task_header().strong.get()) -= 1 };
        if unsafe { *self.task_header().strong.get() > 0 } {
            return;
        }

        let future_offset = self.task_header().future_offset;
        let future = unsafe { self.p.add(future_offset).as_ptr() };
        let future =
            std::ptr::from_raw_parts_mut::<dyn Future<Output = ()> + 'static>(future,
                                                                              self.task_header()
                                                                                  .future_vtable);
        let value = unsafe { self.p.add(self.task_header().value_offset).as_ptr() };
        let value =
            std::ptr::from_raw_parts_mut::<dyn Erased>(value, self.task_header().value_vtable);

        unsafe { std::ptr::drop_in_place(value) };
        unsafe { std::ptr::drop_in_place(future) };
        drop(unsafe { Rc::from_raw(self.task_header().ex.0) });

        let weak_count = self.task_header().weak.fetch_sub(1, Release);
        if weak_count > 1 {
            return;
        }
        self.task_header().weak.load(Acquire);

        let layout = self.task_header().task_layout;

        unsafe { std::ptr::drop_in_place(self.p.as_ptr().cast::<TaskHeader>()) };
        unsafe { std::alloc::dealloc(self.p.as_ptr(), layout) };
    }
}

impl Clone for Task
{
    fn clone(&self) -> Task
    {
        unsafe { *self.task_header().strong.get() += 1 };
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

    fn task_header(&self) -> &TaskHeader
    {
        unsafe { &*self.p.as_ptr().cast::<TaskHeader>() }
    }

    // cannot be safely upgraded across thread boundaries
    // must only be upgraded on the main thread running the ring
    unsafe fn upgrade(&self) -> Option<Task>
    {
        let c = unsafe { *self.task_header().strong.get() };
        if c == 0 {
            return None;
        }
        unsafe { *self.task_header().strong.get() += 1 };
        Some(Task { p: self.p,
                    phantom: PhantomData })
    }
}

impl Drop for Weak
{
    fn drop(&mut self)
    {
        let weak_count = self.task_header().weak.fetch_sub(1, Release);
        if weak_count > 1 {
            return;
        }
        self.task_header().weak.load(Acquire);
        let layout = self.task_header().task_layout;
        unsafe { std::ptr::drop_in_place(self.p.as_ptr().cast::<TaskHeader>()) };
        unsafe { std::alloc::dealloc(self.p.as_ptr(), layout) };
    }
}

impl Clone for Weak
{
    fn clone(&self) -> Self
    {
        self.task_header().weak.fetch_add(1, Relaxed);
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

    let Some(sender) = weak.task_header().sender.as_ref() else {
        return;
    };

    if sender.send(weak.clone()).is_err() {
        return;
    }

    if weak.task_header().needs_wake.swap(false, AcqRel) {
        let ring_fd = weak.task_header().ring_fd;
        let mut sqe = unsafe { std::mem::zeroed::<io_uring_sqe>() };
        unsafe { io_uring_prep_msg_ring(&raw mut sqe, ring_fd, 0, 0, 0) };
        unsafe { io_uring_register_sync_msg(&raw mut sqe) };
    }
}

unsafe fn task_wake_by_ref(p: *const ())
{
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(p) });

    let Some(sender) = weak.task_header().sender.as_ref() else {
        return;
    };

    if sender.send(ManuallyDrop::into_inner(weak.clone())).is_err() {
        return;
    }

    if weak.task_header().needs_wake.swap(false, AcqRel) {
        let ring_fd = weak.task_header().ring_fd;
        let mut sqe = unsafe { std::mem::zeroed::<io_uring_sqe>() };
        unsafe { io_uring_prep_msg_ring(&raw mut sqe, ring_fd, 0, 0, 0) };
        unsafe { io_uring_register_sync_msg(&raw mut sqe) };
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
        let ex = unsafe { &*weak.task_header().ex.0 };
        ex.local_task_queue.borrow_mut().push_back(weak);
    }
}

unsafe fn task_local_wake_by_ref(p: *const ())
{
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(p) });
    if let Some(_task) = unsafe { weak.upgrade() } {
        let ex = unsafe { &*weak.task_header().ex.0 };
        ex.local_task_queue.borrow_mut().push_back((*weak).clone());
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

#[derive(Copy, Clone)]
struct BufHeader
{
    next: isize,
    prev: isize,
    len: usize,
}

struct BufGroup
{
    br: *mut io_uring_buf_ring,
    num_bufs: u32,
    buf_len: usize,
    bgid: u16,
    bufs: Vec<u8>,
    buf_headers: Vec<BufHeader>,
    bid_map: Vec<u16>,
    ring: *mut io_uring,
    tail: u32,
}

impl BufGroup
{
    fn buf_as_ptr(&mut self, bid: u16) -> *mut u8
    {
        assert!(u32::from(bid) < self.num_bufs);
        unsafe { self.bufs.as_mut_ptr().add(self.buf_len * usize::from(bid)) }
    }

    unsafe fn release(&mut self)
    {
        let ring = self.ring;
        let bgid = self.bgid.into();

        unsafe { io_uring_unregister_buf_ring(ring, bgid) };
        unsafe { io_uring_free_buf_ring(ring, self.br, self.num_bufs, bgid) };
    }
}

//-----------------------------------------------------------------------------

pub struct BorrowedBufs
{
    buf_group: *mut BufGroup,
    head: isize,
    tail: isize,
    ex: Executor,
}

pub struct BorrowedBufsIterator<'a>
{
    bufs: &'a BorrowedBufs,
    buf_id: isize,
}

impl BorrowedBufs
{
    fn new(ex: Executor, buf_group: *mut BufGroup) -> BorrowedBufs
    {
        BorrowedBufs { buf_group,
                       ex,
                       head: -1,
                       tail: -1 }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool
    {
        self.head == -1
    }

    #[must_use]
    pub fn iter(&self) -> BorrowedBufsIterator<'_>
    {
        self.into_iter()
    }
}

impl Drop for BorrowedBufs
{
    fn drop(&mut self)
    {
        let buf_group = unsafe { &mut *self.buf_group };
        let br = buf_group.br;
        let mask = io_uring_buf_ring_mask(buf_group.num_bufs);

        let mut count = 0;

        let mut buf_id = self.head;
        while buf_id > 0 {
            let addr = buf_group.buf_as_ptr(buf_id as u16).cast();
            let tail = &mut buf_group.tail;
            let len = buf_group.buf_len as _;

            unsafe { io_uring_buf_ring_add(br, addr, len, *tail as _, mask, count) };
            buf_group.bid_map[*tail as usize] = buf_id as _;

            *tail = (*tail + 1) & mask as u32;
            count += 1;

            let header = &mut buf_group.buf_headers[buf_id as usize];
            let next = header.next;
            header.next = -1;
            header.prev = -1;
            header.len = 0;

            buf_id = next;
        }

        unsafe { io_uring_buf_ring_advance(br, count) };
        let _ = self.ex; // suppress unused warnings
    }
}

impl Debug for BorrowedBufs
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let iter = self.iter();
        for buf in iter {
            write!(f, "{buf:?}").unwrap();
        }
        Ok(())
    }
}

impl<'a> IntoIterator for &'a BorrowedBufs
{
    type Item = &'a [u8];

    type IntoIter = BorrowedBufsIterator<'a>;

    fn into_iter(self) -> Self::IntoIter
    {
        BorrowedBufsIterator { bufs: self,
                               buf_id: self.head }
    }
}

impl<'a> Iterator for BorrowedBufsIterator<'a>
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.buf_id < 0 {
            return None;
        }

        let buf_group = unsafe { &mut *self.bufs.buf_group };
        let curr = self.buf_id as usize;
        let len = buf_group.buf_headers[curr].len;
        let next = buf_group.buf_headers[curr].next;
        self.buf_id = next;

        let p = unsafe { (*self.bufs.buf_group).buf_as_ptr(curr as u16) };
        unsafe { Some(slice::from_raw_parts(p, len)) }
    }
}

//-----------------------------------------------------------------------------

mod io_ops
{
    use slotmap::{DefaultKey, SlotMap};

    use crate::{Executor, IoUringOp, submit_ring};

    pub struct IoOpsMap
    {
        slots: SlotMap<DefaultKey, IoUringOp>,
    }

    impl IoOpsMap
    {
        pub fn with_capacity(capacity: usize) -> IoOpsMap
        {
            IoOpsMap { slots: SlotMap::with_capacity(capacity) }
        }

        pub fn len(&self) -> usize
        {
            self.slots.len()
        }

        pub fn get(&self, key: DefaultKey) -> Option<&IoUringOp>
        {
            self.slots.get(key)
        }

        pub fn get_mut(&mut self, key: DefaultKey) -> Option<&mut IoUringOp>
        {
            self.slots.get_mut(key)
        }

        pub fn remove(&mut self, key: DefaultKey) -> Option<IoUringOp>
        {
            self.slots.remove(key)
        }

        pub fn insert(&mut self, value: IoUringOp, ex: &Executor) -> DefaultKey
        {
            if self.slots.len() == self.slots.capacity() {
                unsafe { submit_ring(ex.ring()) };
            }
            self.slots.insert(value)
        }
    }
}

//-----------------------------------------------------------------------------

struct IoContextFrame
{
    ioring: RefCell<io_uring>,
    params: RefCell<io_uring_params>,
    available_fds: RefCell<VecDeque<u32>>,
    receiver: RefCell<Receiver<Weak>>,
    sender: RefCell<Sender<Weak>>,
    buf_groups: RefCell<HashMap<u16, Box<UnsafeCell<BufGroup>>>>,
    runguard_blacklist: RefCell<HashSet<u64>>,
    local_task_queue: RefCell<VecDeque<Weak>>,
    io_ops: RefCell<IoOpsMap>,
    needs_wake: Arc<AlignedAtomicBool>,
    head: DanglingTask,
    tail: DanglingTask,
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
}

unsafe fn add_op_ref(rc: *mut RefCount)
{
    unsafe { (*rc).op_count += 1 };
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
    if unsafe { (*rc).obj_count == 0 && (*rc).op_count == 0 } {
        let fp = unsafe { (*rc).release_impl };
        let p = unsafe { (*rc).obj };
        unsafe { (fp)(p) };
    }
}

unsafe fn release_op(rc: *mut RefCount)
{
    unsafe { (*rc).op_count -= 1 };
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
        dur: __kernel_timespec,
    },
    #[allow(dead_code)]
    TimeoutCancel,
    MultishotTimeout
    {
        ts: __kernel_timespec,
        stream: *mut StreamImpl,
    },
    TcpAccept
    {
        fd: i32,
    },
    TcpConnect
    {
        addr: SockaddrStorage,
        _port: u16,
        ts: __kernel_timespec,
        needs_socket: bool,
        got_socket: bool,
        fd: i32,
    },
    TcpSend
    {
        num_sent: usize,
        subspan: Range<usize>,
        buf: Vec<u8>,
        last_send: *mut Instant,
    },
    MultishotTcpRecv
    {
        bufs: BorrowedBufs,
        buf_group: *mut BufGroup,
        last_recv: *mut Instant,
    },
    TcpShutdown,
    TcpClose,
    TcpCancel,
    DropCancel,
    DropClose
    {
        fd: u32,
    },
}

type CqeHandler = fn(ex: &Executor, cqe: &mut io_uring_cqe);

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
    p: Rc<IoContextFrame>,
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
    p: Rc<IoContextFrame>,
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

unsafe fn clear_task_list(p: &Rc<IoContextFrame>)
{
    let mut curr = unsafe { p.head.unsafe_clone() };
    while unsafe { !curr.is_null() } {
        let next = unsafe { curr.get_next() };
        drop(unsafe { Task::from_raw(*curr.0.get()) });
        curr = next;
    }

    unsafe {
        p.head
         .set_inner(&DanglingTask(SyncUnsafeCell::new(ptr::null_mut())));

        p.tail
         .set_inner(&DanglingTask(SyncUnsafeCell::new(ptr::null_mut())));
    }
}

impl Drop for RunGuard
{
    #![allow(clippy::redundant_closure_call)]
    fn drop(&mut self)
    {
        {
            let local_tasks = &mut *self.p.local_task_queue.borrow_mut();
            local_tasks.clear();

            while let Ok(task) = self.p.receiver.borrow_mut().try_recv() {
                drop(task);
            }

            unsafe { clear_task_list(&self.p) };
        }

        let ring = (|| &raw mut *self.p.ioring.borrow_mut())();
        unsafe { submit_ring(ring) };
        unsafe { io_uring_get_events(ring) };

        let blacklist = &mut *self.p.runguard_blacklist.borrow_mut();

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

                    let key = DefaultKey::from(KeyData::from_ffi(user_data));

                    let op = self.p.io_ops.borrow_mut().remove(key).unwrap();
                    if let OpType::DropClose { .. } = op.op_type {
                        continue;
                    }

                    let rc = op.ref_count;
                    unsafe { release_op(rc) };
                }
            }

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
        params.sq_entries = sq_entries;
        params.cq_entries = cq_entries;
        params.flags |= IORING_SETUP_CQSIZE;
        params.flags |= IORING_SETUP_SINGLE_ISSUER;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;

        let ioring = unsafe { std::mem::zeroed::<io_uring>() };

        let ioc_frame = IoContextFrame { params: RefCell::new(params),
                                         available_fds: RefCell::new(VecDeque::new()),
                                         receiver: RefCell::new(rx),
                                         sender: RefCell::new(tx),
                                         ioring: RefCell::new(ioring),
                                         buf_groups: RefCell::new(HashMap::new()),
                                         runguard_blacklist: RefCell::new(HashSet::new()),
                                         local_task_queue: RefCell::new(VecDeque::new()),
                                         io_ops: RefCell::new(IoOpsMap::with_capacity(1024)),
                                         needs_wake:
                                             Arc::new(AlignedAtomicBool(AtomicBool::new(true))),
                                         head: DanglingTask(SyncUnsafeCell::new(ptr::null_mut())),
                                         tail: DanglingTask(SyncUnsafeCell::new(ptr::null_mut())) };

        let p = Rc::new(ioc_frame);

        {
            let ring = &raw mut *p.ioring.borrow_mut();
            let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, &raw mut params) };
            assert_eq!(ret, 0);

            let ret = unsafe { io_uring_register_ring_fd(ring) };
            assert_eq!(ret, 1);

            let ret = unsafe { io_uring_register_files_sparse(ring, nr_files) };
            assert_eq!(ret, 0);
            {
                let available_fds = &mut *p.available_fds.borrow_mut();
                available_fds.reserve(nr_files as usize);
                for i in 0..nr_files {
                    available_fds.push_back(i);
                }
            }
        }

        let ioc = Self { p };
        ioc.init_task_list();
        ioc
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor
    {
        Executor { p: self.p.clone() }
    }

    fn ring(&self) -> *mut io_uring
    {
        &raw mut *self.p.ioring.borrow_mut()
    }

    fn process_task_queues(&mut self, num_completed: &mut u64) -> bool
    {
        loop {
            let next = unsafe { self.p.head.get_next().get_inner() };
            let tail = unsafe { self.p.tail.get_inner() };
            if next == tail {
                return true;
            }

            let (local_task, task) = (self.p.local_task_queue.borrow_mut().pop_front(),
                                      self.p.receiver.borrow_mut().try_recv());

            match (local_task, task) {
                (Some(weak1), Ok(weak2)) => {
                    *num_completed += on_work_item(weak1);
                    *num_completed += on_work_item(weak2);
                }
                (Some(weak1), Err(_)) => {
                    *num_completed += on_work_item(weak1);
                }
                (None, Ok(weak2)) => {
                    *num_completed += on_work_item(weak2);
                }
                _ => {
                    return false;
                }
            }
        }
    }

    fn init_task_list(&self)
    {
        #[allow(clippy::unused_async)]
        async fn empty()
        {
            unreachable!()
        }

        let head = &self.p.head;
        let tail = &self.p.tail;

        if !unsafe { head.is_null() } {
            assert!(unsafe { !tail.is_null() });
            return;
        }

        let ex = self.get_executor();

        let dummy_task_head =
            DanglingTask(SyncUnsafeCell::new(Task::into_raw(ex.spawn_impl_helper(empty()))));
        let dummy_task_tail =
            DanglingTask(SyncUnsafeCell::new(Task::into_raw(ex.spawn_impl_helper(empty()))));

        unsafe { head.set_inner(&dummy_task_head) };
        unsafe { tail.set_inner(&dummy_task_tail) };

        unsafe { head.set_next(tail) };
        unsafe { tail.set_prev(head) };
    }

    pub fn run(&mut self) -> u64
    {
        let _guard = RunGuard { p: self.p.clone() };

        let mut num_completed = 0;

        let ex = self.get_executor();
        let ring = ex.ring();

        loop {
            if self.process_task_queues(&mut num_completed) {
                break;
            }

            self.p.needs_wake.store(true, Release);

            if self.process_task_queues(&mut num_completed) {
                break;
            }

            unsafe { io_uring_submit_and_wait(ring, 1) };

            let mut guard = CQEAdvanceGuard { ring, n: 0 };

            let f = |cqe: *mut io_uring_cqe| {
                unsafe { on_cqe(&ex, cqe, &mut guard) };
            };

            unsafe { io_uring_for_each_cqe(ring, f) };
        }

        num_completed
    }
}

unsafe fn on_cqe(ex: &Executor, cqe: *mut io_uring_cqe, guard: &mut CQEAdvanceGuard)
{
    let _g = IncrGuard { n: &mut guard.n };

    let cqe = unsafe { &mut *cqe };

    if cqe.user_data == 0 {
        return;
    }

    let cqe_handler = {
        let key_data = cqe.user_data;
        let key = DefaultKey::from(KeyData::from_ffi(key_data));

        let mut borrow_guard = ex.p.io_ops.borrow_mut();
        let io_ops = &mut *borrow_guard;
        let op = io_ops.get(key).unwrap();
        get_cqe_handler(op)
    };

    (cqe_handler)(ex, cqe);
}

fn on_work_item(weak: Weak) -> u64
{
    match unsafe { weak.upgrade() } {
        None => 0,
        Some(mut task) => {
            let done = unsafe { &mut *task.task_header().done.get() };
            if *done {
                return 0;
            }

            let w = make_waker(weak.clone());
            let lw = make_local_waker(weak);

            let mut cx = ContextBuilder::from_waker(&w).local_waker(&lw).build();

            if let Poll::Ready(()) = unsafe { task.poll(&mut cx) } {
                *done = true;

                let prev_task = unsafe { task.task_header().prev.unsafe_clone() };
                let next_task = unsafe { task.task_header().next.unsafe_clone() };

                unsafe { prev_task.set_next(&next_task) };
                unsafe { next_task.set_prev(&prev_task) };

                drop(unsafe { Task::from_raw(task.p.as_ptr().cast()) });

                1
            } else {
                0
            }
        }
    }
}

fn get_cqe_handler(op: &IoUringOp) -> CqeHandler
{
    match &op.op_type {
        OpType::Timeout { .. } => on_timeout,
        OpType::TimeoutCancel => todo!(),
        OpType::MultishotTimeout { .. } => on_timeout_multishot,
        OpType::TcpAccept { .. } => on_tcp_accept,
        OpType::TcpConnect { .. } => on_tcp_connect,
        OpType::TcpSend { .. } => on_tcp_send,
        OpType::MultishotTcpRecv { .. } => on_tcp_multishot_recv,
        OpType::TcpShutdown | OpType::TcpClose | OpType::TcpCancel => on_tcp_close,
        OpType::DropCancel => on_drop_cancel,
        OpType::DropClose { .. } => on_drop_close,
    }
}

fn on_timeout(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    op.done = true;
    op.res = cqe.res;
    if op.eager_dropped {
        let op = io_ops.remove(key).unwrap();
        drop(borrow_guard);

        unsafe { release_op(op.ref_count) };
        return;
    }

    unsafe { release_op(op.ref_count) };
    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_tcp_accept(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    let res = cqe.res;
    op.res = res;
    op.done = true;

    if op.eager_dropped {
        let op = io_ops.remove(key).unwrap();
        drop(borrow_guard);

        unsafe { release_op(op.ref_count) };

        if op.res >= 0 {
            let OpType::TcpAccept { fd } = op.op_type else {
                unreachable!()
            };

            let fd = fd.try_into().unwrap();
            let key = ex.p
                        .io_ops
                        .borrow_mut()
                        .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }), ex);

            let sqe = get_sqe(ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
        }

        return;
    }

    unsafe { release_op(op.ref_count) };

    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_tcp_connect(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    if op.eager_dropped {
        // if the Future was eager-dropped:
        // * we need to release the borrowed direct descriptor back to the pool
        // * if the connect() succeeded, we need to close() it

        let OpType::TcpConnect { ref mut needs_socket,
                                 ref mut got_socket,
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

        let mut op = io_ops.remove(key).unwrap();
        let OpType::TcpConnect { ref mut needs_socket,
                                 ref mut got_socket,
                                 fd,
                                 .. } = op.op_type
        else {
            unreachable!()
        };

        drop(borrow_guard);

        // if our connect() op is borrowing an FD from the runtime,
        // we need to close it and return it
        if *needs_socket {
            // this means our socket() call completed with a success
            if *got_socket {
                let sqe = get_sqe(ex);

                let fd = fd.try_into().unwrap();
                let key =
                    ex.p
                      .io_ops
                      .borrow_mut()
                      .insert(make_io_uring_op(ptr::null_mut(), OpType::DropClose { fd }), ex);

                unsafe { io_uring_prep_close_direct(sqe, fd) };
                unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
            } else {
                ex.reclaim_fd(fd.try_into().unwrap());
            }
        }

        unsafe { release_op(op.ref_count) };
        return;
    }

    let OpType::TcpConnect { ref mut needs_socket,
                             ref mut got_socket,
                             ref mut fd,
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

        // if our connect() op is borrowing an FD from the runtime,
        // we need to close it and return it
        if cqe.res < 0 && *needs_socket {
            let reclaimed_fd = (*fd).try_into().unwrap();
            *fd = -1;

            // this means our socket() call completed with a success
            if *got_socket {
                drop(borrow_guard);

                let sqe = get_sqe(ex);

                let key = ex.p
                            .io_ops
                            .borrow_mut()
                            .insert(make_io_uring_op(ptr::null_mut(),
                                                     OpType::DropClose { fd: reclaimed_fd }),
                                    ex);

                unsafe { io_uring_prep_close_direct(sqe, reclaimed_fd) };
                unsafe { io_uring_sqe_set_data64(sqe, key.data().as_ffi()) };
            } else {
                ex.reclaim_fd(reclaimed_fd);
            }
        }

        return;
    }

    // we expect one more CQE after this, the result of the actual
    // connect() call
    if *needs_socket {
        *got_socket = true;
    }
}

fn on_tcp_send(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;

    if op.eager_dropped {
        if has_more_cqes {
            return;
        }

        let op = io_ops.remove(key).unwrap();
        drop(borrow_guard);
        unsafe { release_op(op.ref_count) };
        return;
    }

    let OpType::TcpSend { ref mut num_sent,
                          last_send,
                          .. } = op.op_type
    else {
        unreachable!()
    };

    unsafe { *last_send = Instant::now() };

    if has_more_cqes {
        if cqe.res >= 0 {
            *num_sent = cqe.res.try_into().unwrap();
            op.res = cqe.res;
        }

        if cqe.res < 0 {
            op.res = cqe.res;
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

fn on_tcp_close(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    op.done = true;
    op.res = cqe.res;

    if op.eager_dropped {
        let op = io_ops.remove(key).unwrap();

        drop(borrow_guard);

        let rc = op.ref_count;
        unsafe { release_op(rc) };
        return;
    }

    unsafe { release_op(op.ref_count) };
    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_tcp_multishot_recv(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;
    let OpType::MultishotTcpRecv { ref mut bufs,
                                   buf_group,
                                   last_recv, } = op.op_type
    else {
        unreachable!()
    };

    if op.eager_dropped {
        if cqe.res > 0 {
            unsafe { append_recv_buffers(buf_group, cqe, bufs) };
        }

        if has_more_cqes {
            return;
        }

        op.done = true;

        let op = io_ops.remove(key).unwrap();
        assert!(op.done);

        drop(borrow_guard);
        let rc = op.ref_count;
        unsafe { release_op(rc) };

        return;
    }

    if !has_more_cqes {
        op.done = true;
        unsafe { release_op(op.ref_count) };
    }

    unsafe { *last_recv = Instant::now() };

    op.res = cqe.res;
    if cqe.res > 0 {
        unsafe { append_recv_buffers(buf_group, cqe, bufs) };
    }

    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }
}

fn on_timeout_multishot(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.get_mut(key).unwrap();

    let has_more_cqes = (cqe.flags & IORING_CQE_F_MORE) > 0;
    let io_object_dropped = op.eager_dropped;
    if io_object_dropped {
        if has_more_cqes {
            return;
        }

        let op = io_ops.remove(key).unwrap();

        drop(borrow_guard);

        unsafe { release_op(op.ref_count) };
        return;
    }

    if !has_more_cqes {
        unimplemented!();
        // let user_data: *mut IoUringOp = &raw mut *op;
        // let user_data = user_data.cast();

        // let OpType::MultishotTimeout { ref mut ts, .. } =
        // op.op_type else {
        //     unreachable!()
        // };

        // let ts = ptr::from_mut(ts).cast();

        // let sqe = get_sqe(ex);
        // unsafe { io_uring_prep_timeout(sqe, ts, 0,
        // IORING_TIMEOUT_MULTISHOT) };
        // unsafe { io_uring_sqe_set_data(sqe, user_data) };
    }

    if cqe.res != -ETIME {
        assert!(!has_more_cqes);
        return;
    }

    let OpType::MultishotTimeout { ref mut ts, stream } = op.op_type else {
        unreachable!()
    };

    let dur = Duration::new(ts.tv_sec.try_into().unwrap(), ts.tv_nsec.try_into().unwrap());

    let now = Instant::now();
    let stream_impl = unsafe { &mut *stream };

    let recv_expired =
        stream_impl.recv_op.is_some() && now.duration_since(stream_impl.last_recv) > dur;

    let send_expired = stream_impl.send_pending && now.duration_since(stream_impl.last_send) > dur;

    if recv_expired || send_expired {
        let sqe = get_sqe(ex);
        let fd = stream_impl.fd_impl.fd;
        let flags = IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED;
        unsafe { io_uring_prep_cancel_fd(sqe, fd, flags) };
        unsafe { io_uring_sqe_set_data64(sqe, 0) };
        unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
    }
}

fn on_drop_cancel(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.remove(key).unwrap();
    drop(borrow_guard);
    unsafe { release_op(op.ref_count) };
}

fn on_drop_close(ex: &Executor, cqe: &mut io_uring_cqe)
{
    let mut borrow_guard = ex.p.io_ops.borrow_mut();
    let io_ops = &mut *borrow_guard;

    let key_data = cqe.user_data;
    let key = DefaultKey::from(KeyData::from_ffi(key_data));

    let op = io_ops.remove(key).unwrap();
    let OpType::DropClose { fd } = op.op_type else {
        unreachable!()
    };

    drop(borrow_guard);
    ex.reclaim_fd(fd);
}

unsafe fn append_recv_buffers(buf_group: *mut BufGroup, cqe: &mut io_uring_cqe,
                              bufs: &mut BorrowedBufs)
{
    let buf_group = unsafe { &mut *buf_group };
    let buf_len = buf_group.buf_len;
    let num_bufs = buf_group.num_bufs;
    let bid_map = &buf_group.bid_map;
    let buf_headers = &mut buf_group.buf_headers;

    let mask = io_uring_buf_ring_mask(num_bufs) as u32;

    let mut bid = cqe.flags >> IORING_CQE_BUFFER_SHIFT;
    let mut num_bytes = cqe.res as usize;

    while num_bytes > 0 {
        let mut n = buf_len;
        if n > num_bytes {
            n = num_bytes;
        }

        let bg_bid = bid_map[bid as usize];

        if bufs.head < 0 {
            bufs.head = bg_bid as _;
        }

        if bufs.tail >= 0 {
            buf_headers[bufs.tail as usize].next = bg_bid as _;
            buf_headers[bg_bid as usize].prev = bufs.tail;
        }

        buf_headers[bg_bid as usize].len = n;
        bid = (bid + 1) & mask;
        num_bytes -= n;
        bufs.tail = bg_bid as _;

        if num_bytes > 0 {
            buf_headers[bg_bid as usize].next = bid_map[bid as usize] as isize;
            buf_headers[bid_map[bid as usize] as usize].prev = bg_bid as isize;
        }
    }
}

impl Drop for IoContext
{
    fn drop(&mut self)
    {
        drop(RunGuard { p: self.p.clone() });

        let buf_groups = &mut *self.p.buf_groups.borrow_mut();
        for (_, buf_group) in buf_groups.iter() {
            unsafe { (*buf_group.get()).release() };
        }
        buf_groups.clear();

        unsafe { io_uring_queue_exit(self.ring()) };

        let n = self.p.io_ops.borrow().len();
        if n > 0 {
            eprintln!("IO operation leak detected. {n} ops leaked.");
        }
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
    waker: Option<LocalWaker>,
}

//-----------------------------------------------------------------------------

struct WrapperFuture<T, F: Future<Output = T>>
{
    f: F,
    value: *mut SpawnValue<T>,
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
                let spawn_value = unsafe { &mut *this.value };
                spawn_value.t = Some(t);
                if let Some(w) = spawn_value.waker.take() {
                    w.wake();
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
    task: Option<Task>,
    _marker: PhantomData<T>,
}

impl<T: Unpin> Unpin for SpawnFuture<T> {}

impl<T> Future for SpawnFuture<T>
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
    {
        assert!(!self.done);
        let this = unsafe { self.get_unchecked_mut() };
        let offset = this.task.as_ref().unwrap().task_header().value_offset;
        let spawn_value = unsafe {
            &mut *this.task
                      .as_ref()
                      .unwrap()
                      .p
                      .add(offset)
                      .cast::<SpawnValue<T>>()
                      .as_ptr()
        };

        let result = spawn_value.t.take();
        match result {
            None => {
                spawn_value.waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            Some(t) => {
                this.done = true;
                this.task = None;
                Poll::Ready(t)
            }
        }
    }
}

//-----------------------------------------------------------------------------

#[derive(Clone)]
pub struct Executor
{
    p: Rc<IoContextFrame>,
}

impl Executor
{
    fn ring(&self) -> *mut io_uring
    {
        &raw mut *self.p.ioring.borrow_mut()
    }

    fn get_available_fd(&self) -> Option<u32>
    {
        let fds = &mut *self.p.available_fds.borrow_mut();
        fds.pop_front()
    }

    fn reclaim_fd(&self, fd: u32)
    {
        let fds = &mut *self.p.available_fds.borrow_mut();
        fds.push_back(fd);
    }

    #[must_use]
    pub fn get_params(&self) -> IoContextParams
    {
        let ring_params = &*self.p.params.borrow();
        let nr_files = self.p.available_fds.borrow().len() as u32;
        IoContextParams { sq_entries: ring_params.sq_entries,
                          cq_entries: ring_params.cq_entries,
                          nr_files }
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

        let mut bufs = vec![0; buf_len * num_bufs as usize];
        let mut bid_map = Vec::with_capacity(num_bufs as usize);
        let buf_headers = vec![
            BufHeader { next: -1,
                        prev: -1,
                        len: 0 };
            num_bufs as usize
        ];

        let mask = io_uring_buf_ring_mask(num_bufs);
        for bid in 0..num_bufs {
            let addr = unsafe { bufs.as_mut_ptr().add(buf_len * bid as usize) };
            unsafe {
                io_uring_buf_ring_add(buf_ring,
                                      addr.cast(),
                                      buf_len as _,
                                      bid as _,
                                      mask,
                                      bid as _);
            }

            bid_map.push(bid as u16);
        }

        unsafe { io_uring_buf_ring_advance(buf_ring, num_bufs.try_into().unwrap()) };

        let bg = BufGroup { br: buf_ring,
                            num_bufs,
                            buf_len,
                            bgid,
                            bufs,
                            buf_headers,
                            ring,
                            tail: 0,
                            bid_map };

        self.p
            .buf_groups
            .borrow_mut()
            .insert(bgid, Box::new(UnsafeCell::new(bg)));

        Ok(())
    }

    fn spawn_impl_helper<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> Task
    {
        let sender = self.p.sender.borrow().clone();

        let future_vtable =
            metadata::<dyn Future<Output = ()>>(std::ptr::null::<WrapperFuture<T, F>>());
        let value_vtable = metadata::<dyn Erased>(std::ptr::null::<SpawnValue<T>>());

        let (layout, value_offset) = Layout::new::<TaskHeader>().extend(value_vtable.layout())
                                                                .unwrap();
        let (layout, future_offset) = layout.extend(future_vtable.layout()).unwrap();
        let layout = layout.pad_to_align();

        let ring_fd = self.p.ioring.borrow().ring_fd;

        let task_header = TaskHeader { strong: SyncUnsafeCell::new(1),
                                       weak: AlignedAtomicU64(AtomicU64::new(1)),
                                       value_offset,
                                       future_offset,
                                       future_vtable,
                                       value_vtable,
                                       sender: Some(sender),
                                       ring_fd,
                                       ex: DanglingExecutor(Rc::into_raw(self.clone().p)),
                                       task_layout: layout,
                                       needs_wake: self.p.needs_wake.clone(),
                                       done: SyncUnsafeCell::new(false),
                                       next: DanglingTask(SyncUnsafeCell::new(ptr::null_mut())),
                                       prev: DanglingTask(SyncUnsafeCell::new(ptr::null_mut())) };

        let p = unsafe { std::alloc::alloc(layout) };
        assert!(!p.is_null());

        let wrapped = WrapperFuture { f,
                                      value: unsafe { p.add(value_offset).cast() } };

        let value = SpawnValue::<T> { t: None,
                                      waker: None };

        unsafe { std::ptr::write(p.cast::<TaskHeader>(), task_header) };
        unsafe { std::ptr::write(p.add(value_offset).cast::<SpawnValue<T>>(), value) };
        unsafe { std::ptr::write(p.add(future_offset).cast::<WrapperFuture<T, F>>(), wrapped) };

        Task { p: NonNull::new(p).unwrap(),
               phantom: PhantomData }
    }

    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T>
    {
        let task = self.spawn_impl_helper(f);

        let header = task.task_header();

        unsafe { header.next.set_inner(&self.p.head.get_next()) };
        unsafe { header.prev.set_inner(&self.p.head.unsafe_clone()) };

        unsafe { self.p.head.get_next().set_prev(&DanglingTask::new(header)) };
        unsafe { self.p.head.set_next(&DanglingTask::new(header)) };

        forget(task.clone());

        self.p
            .local_task_queue
            .borrow_mut()
            .push_back(Task::downgrade(&task));

        SpawnFuture::<T> { task: Some(task),
                           done: false,
                           _marker: PhantomData }
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

    use crate::TaskHeader;

    fn send_only<T: Send>() {}
    fn sync_only<T: Sync>() {}

    #[test]
    fn is_task_header_send()
    {
        send_only::<TaskHeader>();
        sync_only::<TaskHeader>();
    }

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
        // any number of CQEs associated with our usea, and we must
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

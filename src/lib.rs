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
         clippy::cast_possible_wrap,
         clippy::cast_sign_loss,
         clippy::cast_possible_truncation)]

extern crate liburing_rs;
extern crate nix;

use std::{
    alloc::Layout,
    cell::{RefCell, UnsafeCell},
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, Index},
    os::fd::AsRawFd,
    pin::Pin,
    ptr::{self, DynMetadata, NonNull, metadata},
    rc::Rc,
    slice,
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
    sys::{eventfd::EventFd, socket::SockaddrStorage},
};

use liburing_rs::{
    __kernel_timespec, IORING_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_FD_FIXED,
    IORING_CQE_BUFFER_SHIFT, IORING_CQE_F_MORE, IORING_CQE_F_NOTIF, IORING_SETUP_CQSIZE,
    IORING_SETUP_DEFER_TASKRUN, IORING_SETUP_SINGLE_ISSUER, IORING_TIMEOUT_MULTISHOT,
    IOSQE_CQE_SKIP_SUCCESS, io_uring, io_uring_buf_ring, io_uring_buf_ring_add,
    io_uring_buf_ring_advance, io_uring_buf_ring_mask, io_uring_cq_advance, io_uring_cqe,
    io_uring_cqe_seen, io_uring_for_each_cqe, io_uring_free_buf_ring, io_uring_get_events,
    io_uring_get_sqe, io_uring_params, io_uring_peek_cqe, io_uring_prep_cancel_fd,
    io_uring_prep_close_direct, io_uring_prep_read, io_uring_prep_timeout, io_uring_queue_exit,
    io_uring_queue_init_params, io_uring_register_files_sparse, io_uring_register_ring_fd,
    io_uring_setup_buf_ring, io_uring_sq_space_left, io_uring_sqe, io_uring_sqe_set_data,
    io_uring_sqe_set_data64, io_uring_sqe_set_flags, io_uring_submit_and_get_events,
    io_uring_submit_and_wait, io_uring_unregister_buf_ring,
};

pub mod tcp;
pub mod time;

use slotmap::{DefaultKey, KeyData, SlotMap};
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

struct DanglingExecutor(*const IoContextFrame);
unsafe impl Send for DanglingExecutor {}

//-----------------------------------------------------------------------------

struct TaskHeader
{
    strong: UnsafeCell<u64>,
    weak: AlignedAtomicU64,
    task_layout: Layout,
    value_offset: usize,
    future_offset: usize,
    value_vtable: DynMetadata<dyn Erased>,
    future_vtable: DynMetadata<dyn Future<Output = ()> + 'static>,
    sender: Option<Sender<Weak>>,
    event_fd: i32,
    ex: DanglingExecutor,
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

    fn get_future(&mut self) -> Pin<&mut (dyn Future<Output = ()> + 'static)>
    {
        let offset = self.task_header().future_offset;
        let p = unsafe {
            std::ptr::from_raw_parts_mut::<dyn Future<Output = ()> + 'static>(self.p
                                                                                  .as_ptr()
                                                                                  .add(offset),
                                                                              self.task_header()
                                                                                  .future_vtable)
        };
        unsafe { Pin::new_unchecked(&mut *p) }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<()>
    {
        let future = self.get_future();
        future.poll(cx)
    }

    #[must_use]
    fn downgrade(this: &Task) -> Weak
    {
        this.task_header().weak.fetch_add(1, Relaxed);
        Weak { p: this.p,
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
    if let Some(sender) = weak.task_header().sender.as_ref() {
        sender.send(weak.clone()).unwrap();
        let buf = &0x01_u64.to_ne_bytes();
        unsafe {
            nix::libc::write(weak.task_header().event_fd, buf.as_ptr().cast::<c_void>(), buf.len());
        }
    }
}

unsafe fn task_wake_by_ref(p: *const ())
{
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(p) });
    if let Some(sender) = weak.task_header().sender.as_ref() {
        sender.send(ManuallyDrop::into_inner(weak.clone())).unwrap();
        let buf = &0x01_u64.to_ne_bytes();
        unsafe {
            nix::libc::write(weak.task_header().event_fd, buf.as_ptr().cast::<c_void>(), buf.len());
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

struct BufGroup
{
    br: *mut io_uring_buf_ring,
    num_bufs: u32,
    buf_len: usize,
    bgid: u16,
    bufs: Vec<u8>,
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

const SBO_NUM_BUFS: u32 = 16;

#[derive(Copy, Clone, Debug)]
struct BorrowedBuf
{
    bid: u16,
    len: usize,
}

enum BufSequenceIds
{
    Inline
    {
        bufs: [BorrowedBuf; SBO_NUM_BUFS as usize],
        num_bufs: u32,
    },
    Allocated
    {
        bufs: Vec<BorrowedBuf>,
        num_bufs: u32,
    },
}

impl BufSequenceIds
{
    fn new() -> BufSequenceIds
    {
        BufSequenceIds::Inline { bufs: [BorrowedBuf { bid: 0, len: 0 }; SBO_NUM_BUFS as usize],
                                 num_bufs: 0 }
    }

    fn push(&mut self, borrowed_buf: BorrowedBuf)
    {
        match self {
            BufSequenceIds::Inline { bufs: buf_ids,
                                     num_bufs, } => {
                let max = SBO_NUM_BUFS;
                if *num_bufs == max {
                    let mut bids = Vec::new();
                    for bid in buf_ids {
                        bids.push(*bid);
                    }
                    bids.push(borrowed_buf);
                    *self = BufSequenceIds::Allocated { bufs: bids,
                                                        num_bufs: max + 1 };
                    return;
                }
                buf_ids[*num_bufs as usize] = borrowed_buf;
                *num_bufs += 1;
            }
            BufSequenceIds::Allocated { bufs: buf_ids,
                                        num_bufs, } => {
                buf_ids.push(borrowed_buf);
                *num_bufs += 1;
            }
        }
    }

    fn num_bufs(&self) -> u32
    {
        match self {
            BufSequenceIds::Inline { num_bufs, .. }
            | BufSequenceIds::Allocated { num_bufs, .. } => *num_bufs,
        }
    }

    fn as_slice(&self) -> &[BorrowedBuf]
    {
        let slice = match self {
            BufSequenceIds::Inline { bufs: buf_ids, .. } => buf_ids,
            BufSequenceIds::Allocated { bufs: buf_ids, .. } => buf_ids.as_slice(),
        };
        &slice[0..self.num_bufs() as usize]
    }
}

pub struct BorrowedBufs
{
    buf_group: *mut BufGroup,
    buf_ids: BufSequenceIds,
    ex: Executor,
}

pub struct BorrowedBufsIterator<'a>
{
    bufs: &'a BorrowedBufs,
    idx: usize,
}

impl BorrowedBufs
{
    fn new(ex: Executor, buf_group: *mut BufGroup) -> BorrowedBufs
    {
        BorrowedBufs { buf_group,
                       buf_ids: BufSequenceIds::new(),
                       ex }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool
    {
        self.buf_ids.num_bufs() == 0
    }

    #[must_use]
    pub fn len(&self) -> usize
    {
        self.buf_ids.num_bufs() as usize
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
        let mut count = 0;
        let mask = io_uring_buf_ring_mask(buf_group.num_bufs);

        for bid in self.buf_ids.as_slice() {
            let addr = buf_group.buf_as_ptr(bid.bid).cast();
            let tail = &mut buf_group.tail;
            let len = buf_group.buf_len as _;
            unsafe { io_uring_buf_ring_add(br, addr, len, *tail as _, mask, count) };
            buf_group.bid_map[*tail as usize] = bid.bid;
            *tail = (*tail + 1) & mask as u32;
            count += 1;
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

impl Index<usize> for BorrowedBufs
{
    type Output = [u8];
    fn index(&self, index: usize) -> &Self::Output
    {
        assert!(index < self.buf_ids.num_bufs() as usize);

        let buf_id = self.buf_ids.as_slice()[index];
        let p = unsafe { (*self.buf_group).buf_as_ptr(buf_id.bid) };
        unsafe { slice::from_raw_parts(p, buf_id.len) }
    }
}

impl<'a> IntoIterator for &'a BorrowedBufs
{
    type Item = &'a [u8];

    type IntoIter = BorrowedBufsIterator<'a>;

    fn into_iter(self) -> Self::IntoIter
    {
        BorrowedBufsIterator { bufs: self, idx: 0 }
    }
}

impl<'a> Iterator for BorrowedBufsIterator<'a>
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.idx >= self.bufs.buf_ids.num_bufs() as usize {
            return None;
        }

        let buf_id = self.bufs.buf_ids.as_slice()[self.idx];
        let p = unsafe { (*self.bufs.buf_group).buf_as_ptr(buf_id.bid) };
        self.idx += 1;
        unsafe { Some(slice::from_raw_parts(p, buf_id.len)) }
    }
}

//-----------------------------------------------------------------------------

struct IoContextFrame
{
    ioring: RefCell<io_uring>,
    _params: RefCell<io_uring_params>,
    available_fds: RefCell<VecDeque<u32>>,
    tasks: RefCell<HashSet<Task>>,
    receiver: RefCell<Receiver<Weak>>,
    sender: RefCell<Sender<Weak>>,
    event_fd: RefCell<EventFd>,
    buf_groups: RefCell<HashMap<u16, Box<UnsafeCell<BufGroup>>>>,
    runguard_blacklist: RefCell<HashSet<u64>>,
    local_task_queue: RefCell<VecDeque<Weak>>,
    io_ops: RefCell<SlotMap<DefaultKey, IoUringOp>>,
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
        dur: __kernel_timespec
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
        fd: i32
    },
    TcpConnect
    {
        addr: SockaddrStorage,
        port: u16,
        ts: __kernel_timespec,
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
        bufs: BorrowedBufs,
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

impl Drop for RunGuard
{
    fn drop(&mut self)
    {
        {
            let tasks = &mut *self.p.tasks.borrow_mut();
            let local_tasks = &mut *self.p.local_task_queue.borrow_mut();
            tasks.clear();
            local_tasks.clear();
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

                    let io_ops = &mut *self.p.io_ops.borrow_mut();
                    if let Some(op) = io_ops.get(key) {
                        unsafe { release_op(op.ref_count) };
                        io_ops.remove(key).unwrap();
                        continue;
                    }

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

        let ioc_frame =
            IoContextFrame { _params: RefCell::new(params),
                             tasks: RefCell::new(HashSet::new()),
                             available_fds: RefCell::new(VecDeque::new()),
                             receiver: RefCell::new(rx),
                             sender: RefCell::new(tx),
                             ioring: RefCell::new(ioring),
                             event_fd: RefCell::new(nix::sys::eventfd::EventFd::new().unwrap()),
                             buf_groups: RefCell::new(HashMap::new()),
                             runguard_blacklist: RefCell::new(HashSet::new()),
                             local_task_queue: RefCell::new(VecDeque::new()),
                             io_ops: RefCell::new(SlotMap::with_capacity(512 * 1024)) };

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

        Self { p }
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
                        ex.p.tasks.borrow_mut().remove(&task);
                        1
                    } else {
                        0
                    }
                }
            }
        }

        let _guard = RunGuard { p: self.p.clone() };

        let mut num_completed = 0;

        let event_fd = self.p.event_fd.borrow().as_raw_fd();
        let mut event_count = 0_u64;

        let ex = self.get_executor();
        let ring = ex.ring();

        let mut need_eventfd_read = true;

        loop {
            if self.p.tasks.borrow().is_empty() {
                break;
            }

            let (local_task, task) = (self.p.local_task_queue.borrow_mut().pop_front(),
                                      self.p.receiver.borrow_mut().try_recv());

            match (local_task, task) {
                (Some(weak1), Ok(weak2)) => {
                    num_completed += on_work_item(weak1, &ex);
                    num_completed += on_work_item(weak2, &ex);
                    continue;
                }
                (Some(weak1), Err(_)) => {
                    num_completed += on_work_item(weak1, &ex);
                    continue;
                }
                (None, Ok(weak2)) => {
                    num_completed += on_work_item(weak2, &ex);
                    continue;
                }
                _ => {}
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

                let key_data = cqe.user_data;
                let key = DefaultKey::from(KeyData::from_ffi(key_data));

                let io_ops = &mut *self.p.io_ops.borrow_mut();
                if let Some(op) = io_ops.get_mut(key) {
                    match op.op_type {
                        OpType::Timeout { .. } => {
                            if on_timeout(op, cqe, ring, &ex) {
                                io_ops.remove(key);
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    let op = unsafe { &mut *(cqe.user_data as *mut IoUringOp) };
                    match op.op_type {
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
                        OpType::Timeout { .. } => unreachable!(),
                    }
                }
            };

            unsafe { io_uring_for_each_cqe(ring, on_cqe) };
        }

        num_completed
    }
}

fn on_timeout(op: &mut IoUringOp, cqe: &mut io_uring_cqe, _ring: *mut io_uring, ex: &Executor)
              -> bool
{
    let _ = ex;
    unsafe { release_op(op.ref_count) };
    if op.eager_dropped {
        return true;
    }
    op.done = true;
    op.res = cqe.res;
    if let Some(local_waker) = op.local_waker.take() {
        local_waker.wake();
    }

    false
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
            unimplemented!("eager-dropped multishot recv with buffers");
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
        let mut bid = cqe.flags >> IORING_CQE_BUFFER_SHIFT;
        let buf_group = unsafe { &mut *buf_group };
        let buf_len = buf_group.buf_len;
        let num_bufs = buf_group.num_bufs;
        let mask = io_uring_buf_ring_mask(num_bufs) as u32;
        let bid_map = &buf_group.bid_map;
        let mut num_bytes = usize::try_from(cqe.res).unwrap();
        while num_bytes > 0 {
            let mut n = buf_len;
            if n > num_bytes {
                n = num_bytes;
            }
            let bg_bid = bid_map[bid as usize];
            bufs.buf_ids.push(BorrowedBuf { bid: bg_bid,
                                            len: n });
            bid = (bid + 1) & mask;
            num_bytes -= n;
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

    let dur = Duration::new(ts.tv_sec.try_into().unwrap(), ts.tv_nsec.try_into().unwrap());

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

        let buf_groups = &mut *self.p.buf_groups.borrow_mut();
        for (_, buf_group) in buf_groups.iter() {
            unsafe { (*buf_group.get()).release() };
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

    unsafe fn get_available_fd(&self) -> Option<u32>
    {
        let fds = &mut *self.p.available_fds.borrow_mut();
        fds.pop_front()
    }

    unsafe fn reclaim_fd(&self, fd: u32)
    {
        let fds = &mut *self.p.available_fds.borrow_mut();
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

        let mut bufs = vec![0; buf_len * num_bufs as usize];
        let mut bid_map = Vec::with_capacity(num_bufs as usize);

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
                            ring,
                            tail: 0,
                            bid_map };

        self.p
            .buf_groups
            .borrow_mut()
            .insert(bgid, Box::new(UnsafeCell::new(bg)));

        Ok(())
    }

    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T>
    {
        let sender = self.p.sender.borrow().clone();
        let event_fd = self.p.event_fd.borrow().as_raw_fd();

        let future_vtable =
            metadata::<dyn Future<Output = ()>>(std::ptr::null::<WrapperFuture<T, F>>());
        let value_vtable = metadata::<dyn Erased>(std::ptr::null::<SpawnValue<T>>());

        let (layout, value_offset) = Layout::new::<TaskHeader>().extend(value_vtable.layout())
                                                                .unwrap();
        let (layout, future_offset) = layout.extend(future_vtable.layout()).unwrap();
        let layout = layout.pad_to_align();

        let task_header = TaskHeader { strong: UnsafeCell::new(1),
                                       weak: AlignedAtomicU64(AtomicU64::new(1)),
                                       value_offset,
                                       future_offset,
                                       future_vtable,
                                       value_vtable,
                                       sender: Some(sender),
                                       event_fd,
                                       ex: DanglingExecutor(Rc::into_raw(self.clone().p)),
                                       task_layout: layout };

        let p = unsafe { std::alloc::alloc(layout) };
        assert!(!p.is_null());

        let wrapped = WrapperFuture { f,
                                      value: unsafe { p.add(value_offset).cast() } };

        let value = SpawnValue::<T> { t: None,
                                      waker: None };

        unsafe { std::ptr::write(p.cast::<TaskHeader>(), task_header) };
        unsafe { std::ptr::write(p.add(value_offset).cast::<SpawnValue<T>>(), value) };
        unsafe { std::ptr::write(p.add(future_offset).cast::<WrapperFuture<T, F>>(), wrapped) };

        let task = Task { p: NonNull::new(p).unwrap(),
                          phantom: PhantomData };

        let weak = Task::downgrade(&task);

        self.p.tasks.borrow_mut().insert(task.clone());
        self.p.local_task_queue.borrow_mut().push_back(weak.clone());
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

    #[test]
    fn is_task_header_send()
    {
        send_only::<TaskHeader>();
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

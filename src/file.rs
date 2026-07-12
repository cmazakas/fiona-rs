// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use crate::{
    Executor, FdImpl, FixedBuf, OpType, RefCount, add_op_ref, get_sqe, make_io_uring_op,
    release_impl, release_obj,
};
use liburing_rs::{
    IORING_FILE_INDEX_ALLOC, IOSQE_CQE_SKIP_SUCCESS, IOSQE_FIXED_FILE, io_uring_prep_cancel64,
    io_uring_prep_close_direct, io_uring_prep_open_direct, io_uring_prep_write_fixed,
    io_uring_sqe_set_data64, io_uring_sqe_set_flags,
};
use nix::libc::{O_CREAT, O_DIRECT, O_RDWR, S_IRGRP, S_IROTH, S_IRUSR, S_IWGRP, S_IWUSR};
use slotmap::{DefaultKey, Key, KeyData};
use std::{
    alloc::Layout,
    ffi::CString,
    ops::RangeBounds,
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr::{self, NonNull},
    range::Range,
    task::Poll,
};

//-----------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum Error {
    OpenError,
    WriteError,
}

//-----------------------------------------------------------------------------

pub(crate) struct FileImpl {
    pub(crate) fd_impl: FdImpl,
    write_pending: bool,
}

impl Drop for FileImpl {
    fn drop(&mut self) {
        if self.fd_impl.fd >= 0 {
            let fd = self.fd_impl.fd.try_into().unwrap();

            let sqe = get_sqe(&self.fd_impl.ex);
            unsafe { io_uring_prep_close_direct(sqe, fd) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
        }
    }
}
//-----------------------------------------------------------------------------

#[derive(Debug)]
pub struct File {
    p: NonNull<FileImpl>,
}

impl File {
    pub fn open(
        ex: &Executor, path: impl AsRef<Path>,
    ) -> impl Future<Output = Result<File, Error>> + 'static {
        let ex = ex.clone();

        let path = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();
        let op = make_io_uring_op(ptr::null_mut(), OpType::FileOpen { path });
        let key = ex.p.io_ops.borrow_mut().insert(op, &ex);

        OpenFuture {
            completed: false,
            op: Some(key.data().as_ffi()),
            ex,
        }
    }

    pub fn write_subspan_at<R: RangeBounds<usize>>(
        &self, range: R, buf: FixedBuf, offset: u64,
    ) -> impl Future<Output = (Result<usize, Error>, FixedBuf)> {
        let file_impl = unsafe { &mut *self.p.as_ptr() };
        assert!(!file_impl.write_pending, "A write is already pending.");
        file_impl.write_pending = true;

        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => buf.len(),
        };

        let subspan = Range { start, end };
        assert!(subspan.end <= buf.len());

        let ref_count = &raw mut file_impl.fd_impl.ref_count;
        let key = file_impl.fd_impl.ex.p.io_ops.borrow_mut().insert(
            make_io_uring_op(
                ref_count,
                OpType::FileWrite {
                    buf: Some(buf),
                    subspan,
                    offset,
                },
            ),
            &file_impl.fd_impl.ex,
        );

        WriteFuture {
            file: self,
            completed: false,
            op: Some(key.data().as_ffi()),
        }
    }

    pub fn write_at(
        &self, buf: FixedBuf, offset: u64,
    ) -> impl Future<Output = (Result<usize, Error>, FixedBuf)> {
        self.write_subspan_at(.., buf, offset)
    }

    #[must_use]
    fn new(ex: &Executor, fd: i32) -> File {
        let ex = ex.clone();
        let layout = Layout::new::<FileImpl>();
        let p;

        {
            let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) }).unwrap();

            let ref_count = RefCount {
                obj_count: 1,
                op_count: 0,
                release_impl: release_impl::<FileImpl>,
                obj: ptr.as_ptr(),
            };

            let file_impl = FileImpl {
                fd_impl: FdImpl {
                    ref_count,
                    ex,
                    fd,
                    close_pending: false,
                    cancel_pending: false,
                    was_closed: false,
                    is_fixed: true,
                },
                write_pending: false,
            };

            p = ptr.cast::<FileImpl>();
            unsafe { std::ptr::write(p.as_ptr(), file_impl) };
        }

        File { p }
    }
}

impl Drop for File {
    fn drop(&mut self) {
        let rc = unsafe { &raw mut (*self.p.as_ptr()).fd_impl.ref_count };
        unsafe { release_obj(rc) };
    }
}

//-----------------------------------------------------------------------------

struct OpenFuture {
    completed: bool,
    op: Option<u64>,
    ex: Executor,
}

impl Future for OpenFuture {
    type Output = Result<File, Error>;

    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);

        let ex = self.ex.clone();

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let mut io_ops = ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, false) => {
                let user_data = key_data;

                let sqe = get_sqe(&ex);

                let OpType::FileOpen { ref path } = op.op_type else {
                    unreachable!()
                };

                unsafe {
                    io_uring_prep_open_direct(
                        sqe,
                        path.as_ptr(),
                        O_RDWR | O_CREAT | O_DIRECT,
                        S_IRUSR | S_IWUSR | S_IWGRP | S_IRGRP | S_IROTH,
                        IORING_FILE_INDEX_ALLOC as _,
                    );
                }

                unsafe { io_uring_sqe_set_data64(sqe, user_data) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;

                let res = op.res;
                if res < 0 {
                    Poll::Ready(Err(Error::OpenError))
                } else {
                    let fd = res;
                    drop(io_ops);
                    Poll::Ready(Ok(File::new(&ex, fd)))
                }
            }
        }
    }
}

impl Drop for OpenFuture {
    fn drop(&mut self) {
        let op_key = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(op_key));

        let mut borrow_guard = self.ex.p.io_ops.borrow_mut();
        let io_ops = &mut *borrow_guard;
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;

            let sqe = get_sqe(&self.ex);
            unsafe { io_uring_prep_cancel64(sqe, op_key, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };

            return;
        }

        if op.initiated && op.done && !self.completed && op.res >= 0 {
            let sqe = get_sqe(&self.ex);
            let fd = op.res as _;
            unsafe { io_uring_prep_close_direct(sqe, fd) };
        }

        io_ops.remove(key).unwrap();
    }
}

//-----------------------------------------------------------------------------

struct WriteFuture<'a> {
    file: &'a File,
    completed: bool,
    op: Option<u64>,
}

impl Future for WriteFuture<'_> {
    type Output = (Result<usize, Error>, FixedBuf);

    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        assert!(!self.completed);
        let file_impl = unsafe { &mut *self.file.p.as_ptr() };

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *file_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        let user_data = key_data;

        match (op.initiated, op.done) {
            (false, true) => unreachable!(),
            (true, false) => {
                op.local_waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (true, true) => {
                self.completed = true;
                let OpType::FileWrite { buf, .. } = &mut op.op_type else {
                    unreachable!()
                };

                let res = op.res;
                if res < 0 {
                    Poll::Ready((Err(Error::WriteError), buf.take().unwrap()))
                } else {
                    Poll::Ready((Ok(op.res as _), buf.take().unwrap()))
                }
            }
            (false, false) => {
                let &OpType::FileWrite {
                    ref buf,
                    subspan,
                    offset,
                } = &op.op_type
                else {
                    unreachable!()
                };

                let sqe = get_sqe(&file_impl.fd_impl.ex);

                let fd = file_impl.fd_impl.fd;
                let buf = buf.as_ref().unwrap();

                unsafe {
                    io_uring_prep_write_fixed(
                        sqe,
                        fd,
                        buf.as_ptr().add(subspan.start).cast(),
                        (subspan.end - subspan.start).try_into().unwrap(),
                        offset,
                        buf.buf_idx as _,
                    );
                }

                unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) };
                unsafe { io_uring_sqe_set_data64(sqe, user_data) };
                unsafe { add_op_ref(&raw mut file_impl.fd_impl.ref_count) };

                op.local_waker = Some(cx.local_waker().clone());
                op.initiated = true;
                Poll::Pending
            }
        }
    }
}

impl Drop for WriteFuture<'_> {
    fn drop(&mut self) {
        let file_impl = unsafe { &mut *self.file.p.as_ptr() };
        file_impl.write_pending = false;

        let key_data = self.op.unwrap();
        let key = DefaultKey::from(KeyData::from_ffi(key_data));
        let io_ops = &mut *file_impl.fd_impl.ex.p.io_ops.borrow_mut();
        let op = io_ops.get_mut(key).unwrap();

        if op.initiated && !op.done {
            op.eager_dropped = true;
            op.local_waker = None;

            let ref_count = &raw mut file_impl.fd_impl.ref_count;
            let key = io_ops
                .insert(make_io_uring_op(ref_count, OpType::DropCancel), &file_impl.fd_impl.ex);
            unsafe { add_op_ref(ref_count) };

            let sqe = get_sqe(&file_impl.fd_impl.ex);
            let user_data = key.data().as_ffi();
            unsafe { io_uring_prep_cancel64(sqe, key_data, 0) };
            unsafe { io_uring_sqe_set_data64(sqe, user_data) };
        } else {
            io_ops.remove(key).unwrap();
        }
    }
}

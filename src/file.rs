// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use crate::{
    Executor, FdImpl, OpType, RefCount, get_sqe, make_io_uring_op, release_impl, release_obj,
};
use liburing_rs::{
    IORING_FILE_INDEX_ALLOC, IOSQE_CQE_SKIP_SUCCESS, io_uring_prep_close_direct,
    io_uring_prep_open_direct, io_uring_sqe_set_data64, io_uring_sqe_set_flags,
};
use nix::libc::{O_CREAT, O_DIRECT, O_RDWR, S_IRGRP, S_IROTH, S_IRUSR, S_IWGRP, S_IWUSR};
use slotmap::{DefaultKey, Key, KeyData};
use std::{
    alloc::Layout,
    ffi::CString,
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr::{self, NonNull},
    task::Poll,
};

//-----------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    OpenError,
}

//-----------------------------------------------------------------------------

pub(crate) struct FileImpl {
    pub(crate) fd_impl: FdImpl,
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
    ) -> impl Future<Output = Result<File, Error>> {
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
            todo!("attempt to cancel the I/O request here")
        }

        io_ops.remove(key).unwrap();
    }
}
